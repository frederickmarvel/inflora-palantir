// Package repo owns the SQL access for Palantir's gateway_topups,
// gateway_withdrawals, gateway_refunds, and webhook_events tables (Phase 0
// ownership D-002).
package repo

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/frederickmarvel/inflora-shared/keys"
	"github.com/google/uuid"
)

// ErrNotFound indicates the requested gateway row does not exist.
var ErrNotFound = errors.New("repo: not found")

// GatewayTopUp is the in-process projection of a gateway_topups row.
type GatewayTopUp struct {
	ID                uuid.UUID
	PalantirRef       string
	SarumanDonationID uuid.UUID
	StreamerID        uuid.UUID
	AmountIDR         int64
	MDRIDR            int64
	GrossChargedIDR   int64
	ProviderName      string
	ProviderRequestID string
	RequestHash       string
	PaymentMethod     string
	ProviderChargeID  string
	PaymentURL        string
	ExpiresAt         *time.Time
	Status            string
	CreatedAt         time.Time
	CapturedAt        *time.Time
	SettledAt         *time.Time
}

// DonationSnapshot contains immutable values required by the frozen
// Palantir-to-Saruman provider-result event. Palantir never mutates donations.
type DonationSnapshot struct {
	IntentID       uuid.UUID
	MDRIDR         int64
	MDRRateBPS     int64
	PlatformFeeIDR int64
	NetIDR         int64
}

// GatewayTopUpRepo is the SQL adapter.
type GatewayTopUpRepo struct {
	DB *sql.DB
}

// NewGatewayTopUpRepo constructs a repository.
func NewGatewayTopUpRepo(db *sql.DB) *GatewayTopUpRepo {
	return &GatewayTopUpRepo{DB: db}
}

func (r *GatewayTopUpRepo) DonationSnapshot(ctx context.Context, tx *sql.Tx, donationID uuid.UUID) (DonationSnapshot, error) {
	var snapshot DonationSnapshot
	if err := tx.QueryRowContext(ctx, `
		SELECT intent_id, mdr_idr, mdr_rate_bps, platform_fee_idr, net_idr
		FROM donations WHERE id=$1
	`, donationID).Scan(&snapshot.IntentID, &snapshot.MDRIDR, &snapshot.MDRRateBPS, &snapshot.PlatformFeeIDR, &snapshot.NetIDR); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return DonationSnapshot{}, ErrNotFound
		}
		return DonationSnapshot{}, fmt.Errorf("repo: load donation snapshot: %w", err)
	}
	return snapshot, nil
}

// LockForUpdate fetches a row inside a serializable transaction. The caller
// MUST commit (or roll back).
func (r *GatewayTopUpRepo) LockForUpdate(ctx context.Context, tx *sql.Tx, palantirRef string) (GatewayTopUp, error) {
	var gt GatewayTopUp
	var expiresAt, capturedAt, settledAt sql.NullTime
	err := tx.QueryRowContext(ctx, `
		SELECT id, palantir_ref, saruman_donation_id, streamer_id, amount_idr,
		       mdr_idr, gross_charged_idr, provider_name, COALESCE(provider_request_id, ''),
		       COALESCE(request_hash, ''), COALESCE(payment_method, ''), provider_charge_id,
		       payment_url, expires_at, status, created_at, captured_at, settled_at
		FROM gateway_topups WHERE palantir_ref = $1 FOR UPDATE
	`, palantirRef).Scan(
		&gt.ID, &gt.PalantirRef, &gt.SarumanDonationID, &gt.StreamerID,
		&gt.AmountIDR, &gt.MDRIDR, &gt.GrossChargedIDR, &gt.ProviderName,
		&gt.ProviderRequestID, &gt.RequestHash, &gt.PaymentMethod, &gt.ProviderChargeID,
		&gt.PaymentURL, &expiresAt, &gt.Status, &gt.CreatedAt,
		&capturedAt, &settledAt,
	)
	if expiresAt.Valid {
		gt.ExpiresAt = &expiresAt.Time
	}
	if errors.Is(err, sql.ErrNoRows) {
		return GatewayTopUp{}, ErrNotFound
	}
	if err != nil {
		return GatewayTopUp{}, fmt.Errorf("repo: lock gateway_topups: %w", err)
	}
	if capturedAt.Valid {
		gt.CapturedAt = &capturedAt.Time
	}
	if settledAt.Valid {
		gt.SettledAt = &settledAt.Time
	}
	return gt, nil
}

// LockForSettlement finds a top-up using provider identifiers from a verified
// webhook. Some providers echo the original external (donation) ID rather
// than the charge ID returned during creation, so both are accepted.
func (r *GatewayTopUpRepo) LockForSettlement(ctx context.Context, tx *sql.Tx, providerName, chargeID, externalID string) (GatewayTopUp, error) {
	var gt GatewayTopUp
	var expiresAt, capturedAt, settledAt sql.NullTime
	err := tx.QueryRowContext(ctx, `
		SELECT id, palantir_ref, saruman_donation_id, streamer_id, amount_idr,
		       mdr_idr, gross_charged_idr, provider_name, COALESCE(provider_request_id, ''),
		       COALESCE(request_hash, ''), COALESCE(payment_method, ''), provider_charge_id,
		       payment_url, expires_at, status, created_at, captured_at, settled_at
		FROM gateway_topups
		WHERE provider_name = $1 AND (
			provider_charge_id = $2 OR saruman_donation_id::text = $3 OR palantir_ref = $3
		)
		ORDER BY CASE WHEN provider_charge_id = $2 THEN 0 ELSE 1 END
		LIMIT 1 FOR UPDATE
	`, providerName, chargeID, externalID).Scan(
		&gt.ID, &gt.PalantirRef, &gt.SarumanDonationID, &gt.StreamerID,
		&gt.AmountIDR, &gt.MDRIDR, &gt.GrossChargedIDR, &gt.ProviderName,
		&gt.ProviderRequestID, &gt.RequestHash, &gt.PaymentMethod, &gt.ProviderChargeID,
		&gt.PaymentURL, &expiresAt, &gt.Status, &gt.CreatedAt, &capturedAt, &settledAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return GatewayTopUp{}, ErrNotFound
	}
	if err != nil {
		return GatewayTopUp{}, fmt.Errorf("repo: lock gateway_topup for settlement: %w", err)
	}
	if expiresAt.Valid {
		gt.ExpiresAt = &expiresAt.Time
	}
	if capturedAt.Valid {
		gt.CapturedAt = &capturedAt.Time
	}
	if settledAt.Valid {
		gt.SettledAt = &settledAt.Time
	}
	return gt, nil
}

// TransitionStatus applies a legal settlement transition while the row is
// locked by LockForSettlement.
func (r *GatewayTopUpRepo) TransitionStatus(ctx context.Context, tx *sql.Tx, id uuid.UUID, status string) error {
	var query string
	switch status {
	case "CAPTURED":
		query = `UPDATE gateway_topups SET status=$2, captured_at=COALESCE(captured_at,NOW()) WHERE id=$1`
	case "FAILED", "EXPIRED":
		query = `UPDATE gateway_topups SET status=$2 WHERE id=$1`
	default:
		return fmt.Errorf("repo: unsupported top-up status %q", status)
	}
	if _, err := tx.ExecContext(ctx, query, id, status); err != nil {
		return fmt.Errorf("repo: transition gateway_topup: %w", err)
	}
	return nil
}

// LockOperation serializes creation for a stable operation key across all
// Palantir replicas. The lock is held until the caller's transaction ends.
func (r *GatewayTopUpRepo) LockOperation(ctx context.Context, tx *sql.Tx, operationKey string) error {
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, operationKey); err != nil {
		return fmt.Errorf("repo: lock top-up operation: %w", err)
	}
	return nil
}

// InsertTopUp writes a new gateway_topups row in the caller's transaction.
// The palantir_ref is derived from the donation_id for idempotency (D-012).
func (r *GatewayTopUpRepo) InsertTopUp(ctx context.Context, tx *sql.Tx, t GatewayTopUp) error {
	ref := t.PalantirRef
	if ref == "" {
		ref = keys.PalantirTopUpKey(t.SarumanDonationID.String())
	}
	_, err := tx.ExecContext(ctx, `
		INSERT INTO gateway_topups (id, palantir_ref, saruman_donation_id, streamer_id,
		    amount_idr, mdr_idr, gross_charged_idr, provider_name, provider_request_id,
		    request_hash, payment_method, provider_charge_id, payment_url, expires_at,
		    status, created_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15, NOW())
	`, uuid.New(), ref, t.SarumanDonationID, t.StreamerID, t.AmountIDR, t.MDRIDR,
		t.GrossChargedIDR, t.ProviderName, t.ProviderRequestID, t.RequestHash,
		t.PaymentMethod, t.ProviderChargeID, t.PaymentURL, t.ExpiresAt, t.Status)
	if err != nil {
		return fmt.Errorf("repo: insert gateway_topups: %w", err)
	}
	return nil
}

// MarkCaptured transitions a gateway_topup from PENDING to CAPTURED inside
// the caller's transaction.
func (r *GatewayTopUpRepo) MarkCaptured(ctx context.Context, tx *sql.Tx, palantirRef string, providerChargeID, status string) error {
	res, err := tx.ExecContext(ctx, `
		UPDATE gateway_topups
		SET status = $2, provider_charge_id = $3, captured_at = NOW()
		WHERE palantir_ref = $1 AND status NOT IN ('CAPTURED', 'SETTLED')
	`, palantirRef, status, providerChargeID)
	if err != nil {
		return fmt.Errorf("repo: mark captured: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return fmt.Errorf("repo: gateway_topup %s already terminal", palantirRef)
	}
	return nil
}
