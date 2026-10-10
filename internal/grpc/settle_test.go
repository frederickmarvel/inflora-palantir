package grpc

import (
	"testing"
	"time"

	"github.com/frederickmarvel/inflora-palantir/internal/repo"
	pb "github.com/frederickmarvel/inflora-shared/gen/go/palantir/v1"
	"github.com/frederickmarvel/inflora-shared/provider"
	"github.com/google/uuid"
)

func TestNormalizeTopUpStatus(t *testing.T) {
	tests := []struct {
		status, fraud, wantState, wantEvent, wantReason string
	}{
		{"settlement", "", "CAPTURED", topupCompleted, ""},
		{"capture", "accept", "CAPTURED", topupCompleted, ""},
		{"capture", "deny", "FAILED", topupFailed, "FRAUD_BLOCKED"},
		{"cancel", "", "FAILED", topupFailed, "USER_CANCELLED"},
		{"deny", "", "FAILED", topupFailed, "PROVIDER_ERROR"},
		{"expire", "", "EXPIRED", topupExpired, "TIMEOUT"},
		{"PAID", "", "CAPTURED", topupCompleted, ""},
	}
	for _, tt := range tests {
		t.Run(tt.status+tt.fraud, func(t *testing.T) {
			state, event, reason, err := normalizeTopUpStatus(provider.VerifyWebhookResult{Status: tt.status, Payload: map[string]interface{}{"fraud_status": tt.fraud}})
			if err != nil || state != tt.wantState || event != tt.wantEvent || reason != tt.wantReason {
				t.Fatalf("got state=%q event=%q reason=%q err=%v", state, event, reason, err)
			}
		})
	}
}

func TestNormalizeTopUpStatusPivotEvents(t *testing.T) {
	tests := []struct {
		event, wantState, wantEvent, wantReason string
	}{
		{"PAYMENT.PAID", "CAPTURED", topupCompleted, ""},
		{"CHARGE.SUCCESS", "CAPTURED", topupCompleted, ""},
		{"PAYMENT.CANCELLED", "FAILED", topupFailed, "USER_CANCELLED"},
		{"PAYMENT.EXPIRED", "EXPIRED", topupExpired, "TIMEOUT"},
	}
	for _, tt := range tests {
		t.Run(tt.event, func(t *testing.T) {
			// Real Pivot keeps data.status=ACTIVE on success; the event name
			// must win over the lagging session status.
			state, event, reason, err := normalizeTopUpStatus(provider.VerifyWebhookResult{
				Status:  "ACTIVE",
				Payload: map[string]interface{}{"event": tt.event},
			})
			if err != nil || state != tt.wantState || event != tt.wantEvent || reason != tt.wantReason {
				t.Fatalf("got state=%q event=%q reason=%q err=%v", state, event, reason, err)
			}
		})
	}
}

func TestTopUpEventPayloadUsesDonationSnapshot(t *testing.T) {
	donationID, intentID, streamerID := uuid.New(), uuid.New(), uuid.New()
	payload := topUpEventPayload(repo.GatewayTopUp{
		SarumanDonationID: donationID, StreamerID: streamerID, AmountIDR: 10_000,
		GrossChargedIDR: 10_070, ProviderName: "midtrans", PaymentMethod: "QRIS", PalantirRef: "ref-1",
	}, repo.DonationSnapshot{IntentID: intentID, MDRIDR: 70, MDRRateBPS: 70, PlatformFeeIDR: 1, NetIDR: 9_999},
		"charge-1:settlement", "charge-1", "accept", "settlement", "", uuid.New(), time.Now(), "CAPTURED")
	if payload["intent_id"] != intentID.String() || payload["mdr_idr"] != int64(70) || payload["platform_fee_idr"] != int64(1) || payload["net_idr"] != int64(9_999) {
		t.Fatalf("payload=%v", payload)
	}
}

func TestValidateTopUpTransition(t *testing.T) {
	if err := validateTopUpTransition("PENDING", "CAPTURED"); err != nil {
		t.Fatal(err)
	}
	if err := validateTopUpTransition("CAPTURED", "FAILED"); err == nil {
		t.Fatal("expected terminal-state regression rejection")
	}
}

func TestValidateVerifiedMidtransIdentifiers(t *testing.T) {
	req := &pb.SettleTopUpRequest{ProviderName: "midtrans", ProviderEventId: "charge-1:settlement", ChargeId: "charge-1"}
	result := provider.VerifyWebhookResult{TransactionID: "charge-1", Status: "settlement", Payload: map[string]interface{}{}}
	if err := validateVerifiedIdentifiers(req, result); err != nil {
		t.Fatal(err)
	}
	req.ProviderEventId = "charge-1:capture"
	if err := validateVerifiedIdentifiers(req, result); err == nil {
		t.Fatal("expected provider event mismatch")
	}
}

func TestValidateVerifiedPivotIdentifiers(t *testing.T) {
	// Real Pivot: ingest folds charge id into the event id (charge-1:payment.paid).
	req := &pb.SettleTopUpRequest{ProviderName: "pivot", ProviderEventId: "charge-1:payment.paid", ChargeId: "charge-1"}
	result := provider.VerifyWebhookResult{
		TransactionID: "charge-1",
		Payload:       map[string]interface{}{"event": "PAYMENT.PAID"},
	}
	if err := validateVerifiedIdentifiers(req, result); err != nil {
		t.Fatal(err)
	}
	// Stub/legacy flat shape still accepted via event_id.
	req.ProviderEventId = "charge-1:settled"
	result.Payload = map[string]interface{}{"event_id": "settled"}
	if err := validateVerifiedIdentifiers(req, result); err != nil {
		t.Fatal(err)
	}
	req.ProviderEventId = "charge-1:capture"
	if err := validateVerifiedIdentifiers(req, result); err == nil {
		t.Fatal("expected provider event mismatch")
	}
}
