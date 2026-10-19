// Package grpc implements Palantir's gRPC services. Payout execution
// (WithdrawalService: Create/Settle/GetWithdrawalStatus) is implemented in
// withdrawal.go; top-up creation and settlement live in topup.go/settle.go.
package grpc

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/frederickmarvel/inflora-palantir/internal/repo"
	"github.com/frederickmarvel/inflora-shared/events"
	pb "github.com/frederickmarvel/inflora-shared/gen/go/palantir/v1"
	"github.com/frederickmarvel/inflora-shared/keys"
	"github.com/frederickmarvel/inflora-shared/outbox"
	"github.com/frederickmarvel/inflora-shared/provider"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	withdrawalCompletedType = "pg.gateway.withdrawal.completed.v1"
	withdrawalFailedType    = "pg.gateway.withdrawal.failed.v1"
)

// WithdrawalServer implements the WithdrawalService gRPC interface (Phase C,
// D-051/D-055). The mirror rows are palantir-written; money transitions stay
// with Saruman, which listens for the pg.gateway.withdrawal.* events.
type WithdrawalServer struct {
	pb.UnimplementedWithdrawalServiceServer
	DB           *sql.DB
	Provider     provider.Provider
	ProviderName string
	Repo         *repo.GatewayWithdrawalRepo
	// DevAutosettle marks the pivot-dev short circuit (T13): a freshly created
	// withdrawal settles to COMPLETED immediately and emits the completed
	// event, so the dev stack closes the payout loop without a provider.
	DevAutosettle bool
}

// NewWithdrawalServer constructs a WithdrawalServer with its dependencies.
func NewWithdrawalServer(db *sql.DB, p provider.Provider, providerName string, r *repo.GatewayWithdrawalRepo, devAutosettle bool) *WithdrawalServer {
	return &WithdrawalServer{DB: db, Provider: p, ProviderName: providerName, Repo: r, DevAutosettle: devAutosettle}
}

// CreateWithdrawal books the provider-side withdrawal for a payout. It is
// idempotent: the same saruman payout id always lands on the same mirror row
// (palantir_ref = keys.PalantirWithdrawalKey), and a replay returns the
// existing row without calling the provider again (advisory lock held across
// the provider call, mirroring CreateTopUp).
func (s *WithdrawalServer) CreateWithdrawal(ctx context.Context, req *pb.CreateWithdrawalRequest) (*pb.CreateWithdrawalResponse, error) {
	if req.SarumanPayoutId == "" || req.StreamerId == "" {
		return nil, status.Error(codes.InvalidArgument, "saruman_payout_id and streamer_id are required")
	}
	if req.AmountIdr <= 0 {
		return nil, status.Error(codes.InvalidArgument, "amount_idr must be positive")
	}
	payoutID, err := uuid.Parse(req.SarumanPayoutId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid saruman_payout_id: %v", err)
	}
	streamerID, err := uuid.Parse(req.StreamerId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid streamer_id: %v", err)
	}
	ref := keys.PalantirWithdrawalKey(payoutID.String())
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
	if err := s.Repo.LockOperation(ctx, tx, ref); err != nil {
		return nil, status.Errorf(codes.Internal, "lock operation: %v", err)
	}
	existing, err := s.Repo.LockForUpdate(ctx, tx, ref)
	if err == nil {
		// Replay: return the existing row verbatim, no provider call.
		_ = tx.Commit()
		committed = true
		return withdrawResponse(existing), nil
	}
	if !errors.Is(err, repo.ErrNotFound) {
		return nil, status.Errorf(codes.Internal, "lookup gateway withdrawal: %v", err)
	}
	// Provider call inside the transaction: the advisory lock makes the
	// create-a-withdrawal idempotent across replicas (see CreateTopUp).
	res, err := s.Provider.CreateWithdrawal(ctx, provider.CreateWithdrawalRequest{
		ExternalID:     payoutID.String(),
		AmountIDR:      req.AmountIdr,
		BeneficiaryRef: req.BankAccountRef,
		RequestID:      ref,
	})
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "provider: %v", err)
	}
	statusValue := firstNonEmpty(res.Status, "PROCESSING")
	providerName := s.ProviderName
	providerPayoutID := firstNonEmpty(res.WithdrawalID, ref)
	inserted, row, err := s.Repo.InsertWithdrawal(ctx, tx, repo.GatewayWithdrawal{
		SarumanPayoutID:  payoutID,
		StreamerID:       streamerID,
		AmountIDR:        req.AmountIdr,
		Status:           statusValue,
		ProviderName:     &providerName,
		ProviderPayoutID: &providerPayoutID,
		BankAccountRef:   stringPtrIf(req.BankAccountRef),
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "insert gateway withdrawal: %v", err)
	}
	if !inserted {
		_ = tx.Commit()
		committed = true
		return withdrawResponse(row), nil
	}
	// Pivot dev autosettle: close the loop locally so the dev stack reaches
	// SETTLED without a provider webhook. The event is enqueued in the same
	// transaction as the transition.
	if s.DevAutosettle && strings.EqualFold(s.ProviderName, "pivot") {
		if err := s.Repo.TransitionStatus(ctx, tx, row.ID, "COMPLETED"); err != nil {
			return nil, status.Errorf(codes.Internal, "dev autosettle: %v", err)
		}
		now := time.Now().UTC()
		if err := s.enqueueWithdrawalEvent(ctx, tx, withdrawalCompletedType, "payout", payoutID, streamerID, ref, map[string]any{
			"payout_id":          payoutID.String(),
			"streamer_id":        streamerID.String(),
			"provider_payout_id": providerPayoutID,
			"bank_account_ref":   req.BankAccountRef,
			"settled_at":         now,
		}); err != nil {
			return nil, status.Errorf(codes.Internal, "enqueue dev autosettle event: %v", err)
		}
		row.Status = "COMPLETED"
	}
	if err := tx.Commit(); err != nil {
		return nil, status.Errorf(codes.Internal, "commit: %v", err)
	}
	committed = true
	return withdrawResponse(row), nil
}

// SettleWithdrawal applies a verified provider payout webhook to the mirror.
// VerifyWebhook parity and webhook_events dedupe match SettleTopUp. An
// unknown payout leaves the webhook event recorded-but-unprocessed (a
// deadletter) and returns NotFound — the edge never crashes on it.
func (s *WithdrawalServer) SettleWithdrawal(ctx context.Context, req *pb.SettleWithdrawalRequest) (*pb.SettleWithdrawalResponse, error) {
	if req.ProviderEventId == "" || req.ProviderName == "" || len(bytes.TrimSpace(req.RawPayload)) == 0 {
		return nil, status.Error(codes.InvalidArgument, "provider_event_id, provider_name and raw_payload are required")
	}
	if req.ProviderPayoutId == "" && req.SarumanPayoutId == "" {
		return nil, status.Error(codes.InvalidArgument, "provider_payout_id or saruman_payout_id is required")
	}
	trimmedPayload := bytes.TrimSpace(req.RawPayload)
	if !json.Valid(trimmedPayload) || len(trimmedPayload) == 0 || trimmedPayload[0] != '{' {
		return nil, status.Error(codes.InvalidArgument, "raw_payload must be a JSON object")
	}
	if !strings.EqualFold(req.ProviderName, s.ProviderName) {
		return nil, status.Errorf(codes.InvalidArgument, "provider %q is not configured", req.ProviderName)
	}
	verified, err := s.Provider.VerifyWebhook(ctx, provider.VerifyWebhookRequest{Body: req.RawPayload, Signature: req.Signature, Headers: req.Headers})
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "verify webhook: %v", err)
	}
	if !verified.Valid {
		return nil, status.Error(codes.Unauthenticated, "invalid provider signature")
	}
	if err := validateWithdrawalIdentifiers(req, verified); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	normalized, eventType, failureReason, err := normalizeWithdrawalStatus(verified)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	hash := sha256.Sum256(req.RawPayload)
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
	webhookID, duplicate, err := repo.InsertWebhookEvent(ctx, tx, strings.ToLower(req.ProviderName), req.ProviderEventId, hex.EncodeToString(hash[:]), req.RawPayload)
	if errors.Is(err, repo.ErrWebhookPayloadConflict) {
		return nil, status.Error(codes.AlreadyExists, "provider event id reused with different payload")
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "persist webhook: %v", err)
	}
	sarumanPayoutID := uuid.Nil
	if req.SarumanPayoutId != "" {
		parsed, parseErr := uuid.Parse(req.SarumanPayoutId)
		if parseErr != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid saruman_payout_id: %v", parseErr)
		}
		sarumanPayoutID = parsed
	}
	gw, err := s.Repo.LockForSettlement(ctx, tx, strings.ToLower(req.ProviderName), req.ProviderPayoutId, sarumanPayoutID)
	if errors.Is(err, repo.ErrNotFound) {
		// Deadletter: the webhook event row persists unprocessed for ops to
		// replay later. No crash, no retry storm.
		if commitErr := tx.Commit(); commitErr != nil {
			return nil, status.Errorf(codes.Internal, "commit deadletter: %v", commitErr)
		}
		committed = true
		return nil, status.Error(codes.NotFound, "gateway withdrawal not found")
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "lock withdrawal: %v", err)
	}
	if duplicate {
		if err := tx.Commit(); err != nil {
			return nil, status.Errorf(codes.Internal, "commit duplicate: %v", err)
		}
		committed = true
		return withdrawSettleResponse(gw), nil
	}
	// PROCESSING webhooks (payout acceptance, non-terminal) refresh the
	// mirror columns and record the webhook without a state change.
	if normalized == "PROCESSING" {
		if err := s.Repo.UpdateProviderState(ctx, tx, gw.ID, "PROCESSING", req.ProviderPayoutId, gwBankRef(gw)); err != nil {
			return nil, status.Errorf(codes.Internal, "refresh provider state: %v", err)
		}
		if err := repo.MarkWebhookProcessed(ctx, tx, webhookID); err != nil {
			return nil, status.Errorf(codes.Internal, "mark webhook processed: %v", err)
		}
		if err := tx.Commit(); err != nil {
			return nil, status.Errorf(codes.Internal, "commit processing webhook: %v", err)
		}
		committed = true
		return withdrawSettleResponse(gw), nil
	}
	if strings.EqualFold(gw.Status, normalized) {
		if err := repo.MarkWebhookProcessed(ctx, tx, webhookID); err != nil {
			return nil, status.Errorf(codes.Internal, "mark webhook processed: %v", err)
		}
		if err := tx.Commit(); err != nil {
			return nil, status.Errorf(codes.Internal, "commit repeated state: %v", err)
		}
		committed = true
		return withdrawSettleResponse(gw), nil
	}
	if err := s.Repo.TransitionStatus(ctx, tx, gw.ID, normalized); err != nil {
		return nil, status.Errorf(codes.Internal, "transition withdrawal: %v", err)
	}
	payload := map[string]any{
		"payout_id":  gw.SarumanPayoutID.String(),
		"streamer_id": gw.StreamerID.String(),
	}
	now := time.Now().UTC()
	if normalized == "COMPLETED" {
		payload["provider_payout_id"] = firstNonEmpty(req.ProviderPayoutId, stringPtrValue(gw.ProviderPayoutID))
		payload["bank_account_ref"] = gwBankRef(gw)
		payload["settled_at"] = now
	} else {
		payload["failure_reason"] = failureReason
		payload["provider_error"] = rawString(verified.Payload, "failure_code")
		payload["failed_at"] = now
	}
	if err := s.enqueueWithdrawalEvent(ctx, tx, eventType, "payout", gw.SarumanPayoutID, gw.StreamerID, firstNonEmpty(stringPtrValue(gw.ProviderPayoutID), gw.PalantirRef), payload); err != nil {
		return nil, status.Errorf(codes.Internal, "enqueue withdrawal event: %v", err)
	}
	if err := repo.MarkWebhookProcessed(ctx, tx, webhookID); err != nil {
		return nil, status.Errorf(codes.Internal, "mark webhook processed: %v", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, status.Errorf(codes.Internal, "commit: %v", err)
	}
	committed = true
	gw.Status = normalized
	return withdrawSettleResponse(gw), nil
}

// GetWithdrawalStatus is the D-055 provider probe: it refreshes the
// provider-truth mirror columns and returns them. Providers without a status
// API (Midtrans IRIS) surface ErrNotImplemented as Unimplemented so the
// caller falls back to the sweep + review flow. Under the pivot dev
// autosettle flag the answer is derived from the mirror row itself.
func (s *WithdrawalServer) GetWithdrawalStatus(ctx context.Context, req *pb.GetWithdrawalStatusRequest) (*pb.GetWithdrawalStatusResponse, error) {
	if req.SarumanPayoutId == "" && req.ProviderPayoutId == "" {
		return nil, status.Error(codes.InvalidArgument, "saruman_payout_id or provider_payout_id is required")
	}
	payoutID := uuid.Nil
	if req.SarumanPayoutId != "" {
		parsed, err := uuid.Parse(req.SarumanPayoutId)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid saruman_payout_id: %v", err)
		}
		payoutID = parsed
	}
	gw, err := s.Repo.FindByPayoutOrProvider(ctx, payoutID, req.ProviderPayoutId)
	if errors.Is(err, repo.ErrNotFound) {
		return nil, status.Error(codes.NotFound, "gateway withdrawal not found")
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "find gateway withdrawal: %v", err)
	}
	var providerStatus, failureCode, bankRef string
	switch {
	case s.DevAutosettle && strings.EqualFold(s.ProviderName, "pivot"):
		// Pivot stub holds no provider state; under dev autosettle the local
		// mirror row IS the provider truth (T5).
		if strings.EqualFold(gw.Status, "COMPLETED") {
			providerStatus = "COMPLETED"
		} else if strings.EqualFold(gw.Status, "FAILED") {
			providerStatus = "FAILED"
		} else {
			providerStatus = "PROCESSING"
		}
		bankRef = gwBankRef(gw)
	default:
		res, probeErr := s.Provider.GetWithdrawalStatus(ctx, provider.GetWithdrawalStatusRequest{
			ExternalID:           payoutID.String(),
			ProviderWithdrawalID: stringPtrValue(gw.ProviderPayoutID),
			RequestID:            gw.PalantirRef,
		})
		if errors.Is(probeErr, provider.ErrNotImplemented) {
			return nil, status.Error(codes.Unimplemented, "provider has no withdrawal status API")
		}
		if probeErr != nil {
			return nil, status.Errorf(codes.Unavailable, "provider status: %v", probeErr)
		}
		providerStatus = res.Status
		failureCode = res.FailureCode
		bankRef = res.BankReference
	}
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
	if providerStatus != "" {
		if err := s.Repo.UpdateProviderState(ctx, tx, gw.ID, providerStatus, stringPtrValue(gw.ProviderPayoutID), bankRef); err != nil {
			return nil, status.Errorf(codes.Internal, "refresh mirror: %v", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, status.Errorf(codes.Internal, "commit: %v", err)
	}
	committed = true
	return &pb.GetWithdrawalStatusResponse{
		PayoutId:         gw.SarumanPayoutID.String(),
		ProviderPayoutId: firstNonEmpty(stringPtrValue(gw.ProviderPayoutID), req.ProviderPayoutId),
		Status:           providerStatus,
		FailureCode:      failureCode,
		BankReference:    bankRef,
		CheckedAtUnix:    time.Now().Unix(),
	}, nil
}

// enqueueWithdrawalEvent emits a pg.gateway.withdrawal.* event inside the
// caller's transaction. Correlation is the mirror ref so Saruman can tie the
// s4/s5 postings back to this provider event.
func (s *WithdrawalServer) enqueueWithdrawalEvent(ctx context.Context, tx *sql.Tx, eventType, aggregateType string, payoutID, streamerID uuid.UUID, correlationID string, payload map[string]any) error {
	env, err := events.NewEnvelope(eventType, "palantir", stringPtr(streamerID.String()), payload)
	if err != nil {
		return err
	}
	env.CorrelationID = correlationID
	if _, err := outbox.Enqueue(ctx, tx, "palantir", aggregateType, payoutID, &streamerID, env); err != nil {
		return err
	}
	return nil
}

func normalizeWithdrawalStatus(result provider.VerifyWebhookResult) (normalized, eventType, failureReason string, err error) {
	raw := strings.ToUpper(strings.TrimSpace(result.Status))
	switch raw {
	case "COMPLETED", "SUCCESS", "SUCCEEDED", "PAID", "SETTLED", "SUCCESSFUL":
		return "COMPLETED", withdrawalCompletedType, "", nil
	case "FAILED", "FAILURE", "REJECTED", "CANCELLED", "CANCELED", "REVERSED":
		return "FAILED", withdrawalFailedType, "PROVIDER_FAILURE", nil
	case "PROCESSING", "PENDING", "ACCEPTED", "IN_PROGRESS", "":
		return "PROCESSING", "", "", nil
	default:
		return "", "", "", errors.New("unsupported provider status: " + result.Status)
	}
}

func validateWithdrawalIdentifiers(req *pb.SettleWithdrawalRequest, result provider.VerifyWebhookResult) error {
	if result.TransactionID != "" && req.ProviderPayoutId != "" && result.TransactionID != req.ProviderPayoutId {
		return errors.New("verified provider payout id does not match request")
	}
	switch strings.ToLower(req.ProviderName) {
	case "midtrans", "pivot":
		expected := rawString(result.Payload, "event_id")
		if expected == "" && strings.EqualFold(req.ProviderName, "pivot") {
			return errors.New("pivot payout webhook must carry event_id")
		}
		if expected != "" && req.ProviderEventId != expected {
			return errors.New("provider_event_id does not match verified payload")
		}
	}
	return nil
}

func withdrawResponse(gw repo.GatewayWithdrawal) *pb.CreateWithdrawalResponse {
	return &pb.CreateWithdrawalResponse{
		ProviderPayoutId: firstNonEmpty(stringPtrValue(gw.ProviderPayoutID), gw.PalantirRef),
		Status:           gw.Status,
	}
}

func withdrawSettleResponse(gw repo.GatewayWithdrawal) *pb.SettleWithdrawalResponse {
	statusValue := gw.Status
	if statusValue == "" {
		statusValue = "PROCESSING"
	}
	return &pb.SettleWithdrawalResponse{
		PayoutId:         gw.SarumanPayoutID.String(),
		Status:           statusValue,
		FailureCode:      "",
		BankReference:    gwBankRef(gw),
	}
}

func gwBankRef(gw repo.GatewayWithdrawal) string { return stringPtrValue(gw.BankAccountRef) }

func stringPtrValue(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

func stringPtrIf(v string) *string {
	if strings.TrimSpace(v) == "" {
		return nil
	}
	return &v
}

// firstNonEmpty returns the first non-whitespace argument ("" when none are).
func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}