package grpc

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/frederickmarvel/inflora-palantir/internal/repo"
	"github.com/frederickmarvel/inflora-shared/events"
	pb "github.com/frederickmarvel/inflora-shared/gen/go/palantir/v1"
	"github.com/frederickmarvel/inflora-shared/outbox"
	"github.com/frederickmarvel/inflora-shared/provider"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	topupCompleted = "pg.gateway.topup.completed.v1"
	topupFailed    = "pg.gateway.topup.failed.v1"
	topupExpired   = "pg.gateway.topup.expired.v1"
)

func (s *TopUpServer) SettleTopUp(ctx context.Context, req *pb.SettleTopUpRequest) (*pb.SettleTopUpResponse, error) {
	if req.ProviderEventId == "" || req.ProviderName == "" || req.ChargeId == "" || len(req.RawPayload) == 0 {
		return nil, status.Error(codes.InvalidArgument, "provider_event_id, provider_name, charge_id and raw_payload are required")
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
	if err := validateVerifiedIdentifiers(req, verified); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	normalized, eventType, failureReason, err := normalizeTopUpStatus(verified)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	chargeID := firstSet(verified.TransactionID, req.ChargeId)
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
	topup, err := s.Repo.LockForSettlement(ctx, tx, strings.ToLower(req.ProviderName), chargeID, verified.ExternalID)
	if errors.Is(err, repo.ErrNotFound) {
		return nil, status.Error(codes.NotFound, "gateway top-up not found")
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "lock top-up: %v", err)
	}
	if duplicate {
		if err := tx.Commit(); err != nil {
			return nil, status.Errorf(codes.Internal, "commit duplicate: %v", err)
		}
		committed = true
		return settleResponse(topup, chargeID), nil
	}
	if strings.EqualFold(topup.Status, normalized) {
		if err := repo.MarkWebhookProcessed(ctx, tx, webhookID); err != nil {
			return nil, status.Errorf(codes.Internal, "mark webhook processed: %v", err)
		}
		if err := tx.Commit(); err != nil {
			return nil, status.Errorf(codes.Internal, "commit repeated state: %v", err)
		}
		committed = true
		return settleResponse(topup, chargeID), nil
	}
	if err := validateTopUpTransition(topup.Status, normalized); err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	if err := s.Repo.TransitionStatus(ctx, tx, topup.ID, normalized); err != nil {
		return nil, status.Errorf(codes.Internal, "transition top-up: %v", err)
	}
	donation, err := s.Repo.DonationSnapshot(ctx, tx, topup.SarumanDonationID)
	if errors.Is(err, repo.ErrNotFound) {
		return nil, status.Error(codes.FailedPrecondition, "donation snapshot not found")
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "load donation snapshot: %v", err)
	}
	now := time.Now().UTC()
	payload := topUpEventPayload(topup, donation, req.ProviderEventId, chargeID, rawString(verified.Payload, "fraud_status"), verified.Status, failureReason, webhookID, now, normalized)
	env, err := events.NewEnvelope(eventType, "palantir", stringPtr(topup.StreamerID.String()), payload)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "build event: %v", err)
	}
	env.CausationID = req.ProviderEventId
	env.CorrelationID = topup.PalantirRef
	if _, err := outbox.Enqueue(ctx, tx, "palantir", "donation", topup.SarumanDonationID, &topup.StreamerID, env); err != nil {
		return nil, status.Errorf(codes.Internal, "enqueue event: %v", err)
	}
	if err := repo.MarkWebhookProcessed(ctx, tx, webhookID); err != nil {
		return nil, status.Errorf(codes.Internal, "mark webhook processed: %v", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, status.Errorf(codes.Internal, "commit: %v", err)
	}
	committed = true
	return settleResponse(topup, chargeID), nil
}

func normalizeTopUpStatus(result provider.VerifyWebhookResult) (normalized, eventType, failureReason string, err error) {
	// Pivot signals outcomes via a top-level event name; its data.status may
	// lag the charge (e.g. CHARGE.SUCCESS keeps the session status ACTIVE).
	// Map the event first when one is present.
	if event, _ := result.Payload["event"].(string); event != "" {
		switch strings.ToUpper(strings.TrimSpace(event)) {
		case "PAYMENT.PAID", "PAYMENT.SUCCESS", "CHARGE.SUCCESS":
			return "CAPTURED", topupCompleted, "", nil
		case "PAYMENT.CANCELLED", "PAYMENT.CANCELED":
			return "FAILED", topupFailed, "USER_CANCELLED", nil
		case "PAYMENT.EXPIRED", "PAYMENT.EXPIRE", "PAYMENT.TIMEOUT":
			return "EXPIRED", topupExpired, "TIMEOUT", nil
		case "PAYMENT.PROCESSING":
			return "", "", "", errors.New("pivot processing webhook is not terminal")
		}
	}
	raw := strings.ToUpper(strings.TrimSpace(result.Status))
	fraud := strings.ToUpper(rawString(result.Payload, "fraud_status"))
	switch raw {
	case "CAPTURE":
		switch fraud {
		case "", "ACCEPT":
			return "CAPTURED", topupCompleted, "", nil
		case "DENY":
			return "FAILED", topupFailed, "FRAUD_BLOCKED", nil
		default:
			return "", "", "", errors.New("capture webhook is not terminal")
		}
	case "SETTLEMENT", "PAID", "COMPLETED", "SUCCESS", "SUCCEEDED", "TOPUP.COMPLETED":
		return "CAPTURED", topupCompleted, "", nil
	case "EXPIRE", "EXPIRED", "TIMEOUT":
		return "EXPIRED", topupExpired, "TIMEOUT", nil
	case "CANCEL", "CANCELLED", "CANCELED":
		return "FAILED", topupFailed, "USER_CANCELLED", nil
	case "DENY", "DENIED", "FAILED", "FAILURE":
		return "FAILED", topupFailed, "PROVIDER_ERROR", nil
	default:
		return "", "", "", errors.New("unsupported provider status: " + result.Status)
	}
}

func validateVerifiedIdentifiers(req *pb.SettleTopUpRequest, result provider.VerifyWebhookResult) error {
	if result.TransactionID != "" && result.TransactionID != req.ChargeId {
		return errors.New("verified provider charge id does not match request")
	}
	switch strings.ToLower(req.ProviderName) {
	case "midtrans":
		expected := canonicalProviderEventID(result.TransactionID, result.Status, rawString(result.Payload, "fraud_status"))
		if expected == "" || req.ProviderEventId != expected {
			return errors.New("provider_event_id does not match verified Midtrans payload")
		}
	case "pivot":
		// Real Pivot callbacks carry the event name at the top level of the
		// payload ({"event":"PAYMENT.PAID","data":{...}}), extracted by
		// pivot.Client.VerifyWebhook into payload["event"]. The stub (and the
		// legacy flat shape) use "event_id"; accept both so a verified payload
		// never fails on the wrong key. The event name alone is shared across
		// donations, so ingest folds the charge id into the provider_event_id
		// ("<charge_id>:<event>"); reconstruct it here for the match.
		event := rawString(result.Payload, "event")
		if event == "" {
			event = rawString(result.Payload, "event_id")
		}
		expected := canonicalProviderEventID(result.TransactionID, event, "")
		if expected == "" || req.ProviderEventId != expected {
			return errors.New("provider_event_id does not match verified Pivot payload")
		}
	}
	return nil
}

func canonicalProviderEventID(chargeID, providerStatus, fraudStatus string) string {
	if strings.TrimSpace(chargeID) == "" || strings.TrimSpace(providerStatus) == "" {
		return ""
	}
	parts := []string{strings.TrimSpace(chargeID), strings.ToLower(strings.TrimSpace(providerStatus))}
	if strings.TrimSpace(fraudStatus) != "" {
		parts = append(parts, strings.ToLower(strings.TrimSpace(fraudStatus)))
	}
	return strings.Join(parts, ":")
}

func validateTopUpTransition(current, next string) error {
	current = strings.ToUpper(current)
	if current == next {
		return nil
	}
	switch current {
	case "PENDING", "CREATED", "PROCESSING", "":
		return nil
	case "CAPTURED", "SETTLED", "FAILED", "EXPIRED":
		return errors.New("gateway top-up is already terminal")
	default:
		return errors.New("unsupported current gateway top-up status: " + current)
	}
}

func topUpEventPayload(t repo.GatewayTopUp, donation repo.DonationSnapshot, providerEventID, chargeID, fraudStatus, rawStatus, failureReason string, webhookID uuid.UUID, now time.Time, normalized string) map[string]any {
	p := map[string]any{
		"donation_id": t.SarumanDonationID.String(), "intent_id": donation.IntentID.String(),
		"saruman_donation_id": t.SarumanDonationID.String(), "streamer_id": t.StreamerID.String(),
		"amount_idr": t.AmountIDR, "mdr_idr": donation.MDRIDR, "mdr_rate_bps": donation.MDRRateBPS,
		"gross_charged_idr": t.GrossChargedIDR, "platform_fee_idr": donation.PlatformFeeIDR, "net_idr": donation.NetIDR,
		"currency": "IDR", "provider": t.ProviderName, "payment_method": t.PaymentMethod,
		"provider_charge_id": chargeID, "provider_event_id": providerEventID,
		"raw_provider_status": rawStatus, "gateway_topup_palantir_ref": t.PalantirRef,
		"webhook_event_id": webhookID.String(),
	}
	if fraudStatus != "" {
		p["raw_provider_fraud_status"] = fraudStatus
	}
	switch normalized {
	case "CAPTURED":
		p["captured_at"] = now
	case "EXPIRED":
		p["expired_at"] = now
	case "FAILED":
		p["failure_reason"], p["failed_at"] = failureReason, now
	}
	return p
}

func settleResponse(t repo.GatewayTopUp, chargeID string) *pb.SettleTopUpResponse {
	return &pb.SettleTopUpResponse{DonationId: t.SarumanDonationID.String(), ProviderChargeId: chargeID, NetIdr: t.AmountIDR}
}

func rawString(payload map[string]interface{}, key string) string {
	value, _ := payload[key].(string)
	return value
}

func firstSet(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func stringPtr(value string) *string { return &value }
