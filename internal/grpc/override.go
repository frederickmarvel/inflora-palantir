package grpc

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/frederickmarvel/inflora-palantir/internal/repo"
	"github.com/frederickmarvel/inflora-shared/events"
	"github.com/frederickmarvel/inflora-shared/keys"
	"github.com/frederickmarvel/inflora-shared/outbox"
	"github.com/frederickmarvel/inflora-shared/provider"
	pb "github.com/frederickmarvel/inflora-shared/gen/go/palantir/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// AdminServer implements the trusted, auditable operations that originate
// from the admin facade rather than a provider webhook.
type AdminServer struct {
	pb.UnimplementedAdminServiceServer
	DB           *sql.DB
	Provider     provider.Provider
	ProviderName string
	Repo         *repo.GatewayTopUpRepo
}

// NewAdminServer constructs an AdminServer with its dependencies.
func NewAdminServer(db *sql.DB, p provider.Provider, providerName string, r *repo.GatewayTopUpRepo) *AdminServer {
	return &AdminServer{DB: db, Provider: p, ProviderName: providerName, Repo: r}
}

// donationMutable reports whether Saruman can still transition this donation.
// Saruman's top-up consumers (completeTopup / markFailed) only ever act on a
// donation in INTENT_CREATED; any other status is treated as terminal and the
// event is dropped. A gateway row that is already CAPTURED but whose donation
// is still INTENT_CREATED is exactly the "lost result event" case the override
// exists to repair, so gateway terminality must NOT gate the override.
func donationMutable(status string) bool {
	return strings.EqualFold(strings.TrimSpace(status), "INTENT_CREATED")
}

// amountMismatch returns a human-readable reason when the gateway row and the
// donation snapshot cannot satisfy Saruman's capture invariants, or "" when
// they can. Mirrors the checks in saruman's completeTopup:
//
//	gross_charged_idr == net_idr + platform_fee_idr + mdr_idr
//	amount_idr          == net_idr + platform_fee_idr
func amountMismatch(topup repo.GatewayTopUp, donation repo.DonationSnapshot) string {
	if topup.GrossChargedIDR != donation.NetIDR+donation.PlatformFeeIDR+donation.MDRIDR {
		return fmt.Sprintf("gateway gross_charged_idr %d does not equal net_idr %d + platform_fee_idr %d + mdr_idr %d; the donation amounts are inconsistent and cannot be captured",
			topup.GrossChargedIDR, donation.NetIDR, donation.PlatformFeeIDR, donation.MDRIDR)
	}
	if topup.AmountIDR != donation.NetIDR+donation.PlatformFeeIDR {
		return fmt.Sprintf("gateway amount_idr %d does not equal net_idr %d + platform_fee_idr %d; the donation amounts are inconsistent and cannot be captured",
			topup.AmountIDR, donation.NetIDR, donation.PlatformFeeIDR)
	}
	return ""
}

// OverrideDonation forces a stuck gateway top-up to a terminal state and
// publishes the exact same provider-result event a real webhook would, so
// Saruman performs its normal ledger postings and the OBS overlay fires (or
// does not) identically. It is idempotent: an already-terminal top-up returns
// AlreadyTerminal=true without publishing anything new.
func (s *AdminServer) OverrideDonation(ctx context.Context, req *pb.OverrideDonationRequest) (*pb.OverrideDonationResponse, error) {
	if strings.TrimSpace(req.DonationId) == "" || strings.TrimSpace(req.Reason) == "" || strings.TrimSpace(req.Actor) == "" {
		return nil, status.Error(codes.InvalidArgument, "donation_id, reason and actor are required")
	}
	donationID, err := uuid.Parse(req.DonationId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid donation_id: %v", err)
	}
	normalized, eventType, failureReason := "", "", ""
	switch req.Decision {
	case pb.OverrideDecision_OVERRIDE_SUCCESS:
		normalized, eventType = "CAPTURED", topupCompleted
	case pb.OverrideDecision_OVERRIDE_FAILURE:
		normalized, eventType, failureReason = "FAILED", topupFailed, "ADMIN_OVERRIDE"
	default:
		return nil, status.Error(codes.InvalidArgument, "decision must be SUCCESS or FAILURE")
	}

	// The palantir_ref is derived deterministically from the donation id, so
	// this is the same idempotency key CreateTopUp used (D-012).
	ref := keys.PalantirTopUpKey(req.DonationId)
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "begin tx: %v", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	topup, err := s.Repo.LockForUpdate(ctx, tx, ref)
	if errors.Is(err, repo.ErrNotFound) {
		return nil, status.Error(codes.NotFound, "no gateway top-up exists for this donation; the provider charge was never created")
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "lock top-up: %v", err)
	}
	if topup.SarumanDonationID != donationID {
		return nil, status.Error(codes.FailedPrecondition, "gateway top-up donation id does not match request")
	}

	donation, err := s.Repo.DonationSnapshot(ctx, tx, topup.SarumanDonationID)
	if err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "donation snapshot not found: %v", err)
	}
	if !donationMutable(donation.Status) {
		// Saruman will not transition a donation that is already past
		// INTENT_CREATED. Report the current donation status rather than the
		// target so the admin sees the truth.
		if err := tx.Commit(); err != nil {
			return nil, status.Errorf(codes.Internal, "commit no-op: %v", err)
		}
		committed = true
		return &pb.OverrideDonationResponse{DonationId: req.DonationId, Status: donation.Status, AlreadyTerminal: true}, nil
	}
	if normalized == "CAPTURED" {
		// Saruman's completeTopup rejects a capture whose amounts do not add
		// up, and the rejection is only visible after the event has already
		// been enqueued — i.e. as a silent dead letter. Check the frozen
		// invariants up front so the admin gets the mismatched numbers back
		// instead of a success response that does nothing downstream.
		if reason := amountMismatch(topup, donation); reason != "" {
			if err := tx.Commit(); err != nil {
				return nil, status.Errorf(codes.Internal, "commit no-op: %v", err)
			}
			committed = true
			return nil, status.Error(codes.FailedPrecondition, reason)
		}
	}

	// Transition the gateway row, then publish the canonical provider-result
	// event inside the same transaction. TransitionStatus is a no-op update
	// when the row is already in the target status (e.g. CAPTURED whose result
	// event was lost), so it is safe to call unconditionally.
	if err := s.Repo.TransitionStatus(ctx, tx, topup.ID, normalized); err != nil {
		return nil, status.Errorf(codes.Internal, "transition top-up: %v", err)
	}
	now := time.Now().UTC()
	providerEventID := "admin-override:" + req.DonationId + ":" + normalized
	chargeID := topup.ProviderChargeID
	if strings.TrimSpace(chargeID) == "" {
		// A captured top-up must carry a non-empty provider_charge_id or
		// Saruman rejects the event. Synthesise a stable, recognisable one.
		chargeID = "admin-override:" + req.DonationId
	}
	payload := topUpEventPayload(topup, donation, providerEventID, chargeID, "", normalized, failureReason, uuid.New(), now, normalized)
	payload["override_actor"] = req.Actor
	payload["override_reason"] = req.Reason

	env, err := events.NewEnvelope(eventType, "palantir", stringPtr(topup.StreamerID.String()), payload)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "build event: %v", err)
	}
	env.CausationID = providerEventID
	env.CorrelationID = topup.PalantirRef
	if _, err := outbox.Enqueue(ctx, tx, "palantir", "donation", topup.SarumanDonationID, &topup.StreamerID, env); err != nil {
		return nil, status.Errorf(codes.Internal, "enqueue event: %v", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, status.Errorf(codes.Internal, "commit: %v", err)
	}
	committed = true
	return &pb.OverrideDonationResponse{DonationId: req.DonationId, Status: normalized, AlreadyTerminal: false}, nil
}
