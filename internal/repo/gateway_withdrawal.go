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

// GatewayWithdrawal is the in-process projection of a gateway_withdrawals row.
// The 00014 provider-mirror columns are nullable in the schema (pre-00014 rows
// were written before the provider columns existed), so they surface as
// pointers here.
type GatewayWithdrawal struct {
	ID                      uuid.UUID
	PalantirRef             string
	SarumanPayoutID         uuid.UUID
	SarumanBatchID          *uuid.UUID
	StreamerID              uuid.UUID
	AmountIDR               int64
	Status                  string
	CreatedAt               time.Time
	SettledAt               *time.Time
	ProviderName            *string
	ProviderPayoutID        *string
	BankAccountRef          *string
	StatusCheckedAt         *time.Time
	LastKnownProviderStatus *string
}

// GatewayWithdrawalRepo is the SQL adapter for the provider-withdrawal
// mirror. Palantir writes, Saruman reads (D-050 s6).
type GatewayWithdrawalRepo struct {
	DB *sql.DB
}

// NewGatewayWithdrawalRepo constructs a repository.
func NewGatewayWithdrawalRepo(db *sql.DB) *GatewayWithdrawalRepo {
	return &GatewayWithdrawalRepo{DB: db}
}

const gatewayWithdrawalColumns = `id, palantir_ref, saruman_payout_id, saruman_batch_id, streamer_id,
	amount_idr, status, created_at, settled_at,
	provider_name, provider_payout_id, bank_account_ref,
	status_checked_at, last_known_provider_status`

func scanGatewayWithdrawal(row *sql.Row) (GatewayWithdrawal, error) {
	var gw GatewayWithdrawal
	var batchID sql.NullString
	var settledAt, checkedAt sql.NullTime
	var providerName, providerPayoutID, bankRef, lastStatus sql.NullString
	err := row.Scan(
		&gw.ID, &gw.PalantirRef, &gw.SarumanPayoutID, &batchID, &gw.StreamerID,
		&gw.AmountIDR, &gw.Status, &gw.CreatedAt, &settledAt,
		&providerName, &providerPayoutID, &bankRef,
		&checkedAt, &lastStatus,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return GatewayWithdrawal{}, ErrNotFound
	}
	if err != nil {
		return GatewayWithdrawal{}, fmt.Errorf("repo: scan gateway_withdrawals: %w", err)
	}
	if batchID.Valid {
		id, parseErr := uuid.Parse(batchID.String)
		if parseErr == nil {
			gw.SarumanBatchID = &id
		}
	}
	if settledAt.Valid {
		gw.SettledAt = &settledAt.Time
	}
	if provided(providerName) {
		gw.ProviderName = &providerName.String
	}
	if provided(providerPayoutID) {
		gw.ProviderPayoutID = &providerPayoutID.String
	}
	if provided(bankRef) {
		gw.BankAccountRef = &bankRef.String
	}
	if checkedAt.Valid {
		gw.StatusCheckedAt = &checkedAt.Time
	}
	if provided(lastStatus) {
		gw.LastKnownProviderStatus = &lastStatus.String
	}
	return gw, nil
}

func provided(s sql.NullString) bool { return s.Valid && s.String != "" }

// LockForUpdate fetches a row by palantir_ref inside the caller's transaction.
func (r *GatewayWithdrawalRepo) LockForUpdate(ctx context.Context, tx *sql.Tx, palantirRef string) (GatewayWithdrawal, error) {
	row := tx.QueryRowContext(ctx, `
		SELECT `+gatewayWithdrawalColumns+`
		FROM gateway_withdrawals WHERE palantir_ref = $1 FOR UPDATE
	`, palantirRef)
	gw, err := scanGatewayWithdrawal(row)
	if err != nil {
		return GatewayWithdrawal{}, fmt.Errorf("repo: lock gateway_withdrawals: %w", err)
	}
	return gw, nil
}

// LockForSettlement finds a withdrawal using provider identifiers from a
// verified webhook. Providers may echo the provider payout id or the original
// (saruman payout) id, so both are accepted, preferring the provider id.
func (r *GatewayWithdrawalRepo) LockForSettlement(ctx context.Context, tx *sql.Tx, providerName, providerPayoutID string, sarumanPayoutID uuid.UUID) (GatewayWithdrawal, error) {
	row := tx.QueryRowContext(ctx, `
		SELECT `+gatewayWithdrawalColumns+`
		FROM gateway_withdrawals
		WHERE provider_name = $1 AND (
			provider_payout_id = $2 OR saruman_payout_id = $3 OR palantir_ref = $3::text
		)
		ORDER BY CASE WHEN provider_payout_id = $2 THEN 0 ELSE 1 END
		LIMIT 1 FOR UPDATE
	`, providerName, providerPayoutID, sarumanPayoutID)
	gw, err := scanGatewayWithdrawal(row)
	if err != nil {
		return GatewayWithdrawal{}, fmt.Errorf("repo: lock gateway_withdrawal for settlement: %w", err)
	}
	return gw, nil
}

// FindByPayoutOrProvider reads (without locking) the newest mirror row for a
// payout identified by saruman payout id or provider payout id. Used by the
// D-055 status probe.
func (r *GatewayWithdrawalRepo) FindByPayoutOrProvider(ctx context.Context, payoutID uuid.UUID, providerPayoutID string) (GatewayWithdrawal, error) {
	row := r.DB.QueryRowContext(ctx, `
		SELECT `+gatewayWithdrawalColumns+`
		FROM gateway_withdrawals
		WHERE saruman_payout_id = $1 OR (provider_payout_id IS NOT NULL AND provider_payout_id = $2 AND $2 <> '')
		ORDER BY created_at DESC LIMIT 1
	`, payoutID, providerPayoutID)
	gw, err := scanGatewayWithdrawal(row)
	if err != nil {
		return GatewayWithdrawal{}, fmt.Errorf("repo: find gateway_withdrawals: %w", err)
	}
	return gw, nil
}

// TransitionStatus applies a legal provider terminal transition while the row
// is locked. Statuses outside the withdrawal set are rejected.
func (r *GatewayWithdrawalRepo) TransitionStatus(ctx context.Context, tx *sql.Tx, id uuid.UUID, status string) error {
	var query string
	switch status {
	case "COMPLETED":
		query = `UPDATE gateway_withdrawals
			SET status=$2, settled_at=COALESCE(settled_at,NOW()),
			    status_checked_at=NOW(), last_known_provider_status=$2
			WHERE id=$1`
	case "FAILED":
		query = `UPDATE gateway_withdrawals
			SET status=$2, settled_at=COALESCE(settled_at,NOW()),
			    status_checked_at=NOW(), last_known_provider_status=$2
			WHERE id=$1`
	default:
		return fmt.Errorf("repo: unsupported withdrawal status %q", status)
	}
	if _, err := tx.ExecContext(ctx, query, id, status); err != nil {
		return fmt.Errorf("repo: transition gateway_withdrawal: %w", err)
	}
	return nil
}

// UpdateProviderState refreshes only the provider-truth mirror columns
// (D-055 probe). It never changes the local status — settlement is Saruman's
// decision, and Saruman reads last_known_provider_status.
func (r *GatewayWithdrawalRepo) UpdateProviderState(ctx context.Context, tx *sql.Tx, id uuid.UUID, providerStatus, providerPayoutID, bankReference string) error {
	if _, err := tx.ExecContext(ctx, `
		UPDATE gateway_withdrawals
		SET status_checked_at=NOW(), last_known_provider_status=$2,
		    provider_payout_id=COALESCE(provider_payout_id,NULLIF($3,'')),
		    bank_account_ref=COALESCE(bank_account_ref,NULLIF($4,''))
		WHERE id=$1
	`, id, providerStatus, providerPayoutID, bankReference); err != nil {
		return fmt.Errorf("repo: update gateway_withdrawal provider state: %w", err)
	}
	return nil
}

// LockOperation serializes creation for a stable operation key across all
// Palantir replicas. The lock is held until the caller's transaction ends.
func (r *GatewayWithdrawalRepo) LockOperation(ctx context.Context, tx *sql.Tx, operationKey string) error {
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, operationKey); err != nil {
		return fmt.Errorf("repo: lock withdrawal operation: %w", err)
	}
	return nil
}

// InsertWithdrawal writes a new mirror row in the caller's transaction. The
// palantir_ref is derived from the saruman payout id for idempotency: a
// duplicate payout id lands on the same row (unique palantir_ref) and never
// triggers a second provider call (the advisory lock + existence check run
// first). Returns (false, existing row) without error when the row already
// exists.
func (r *GatewayWithdrawalRepo) InsertWithdrawal(ctx context.Context, tx *sql.Tx, w GatewayWithdrawal) (bool, GatewayWithdrawal, error) {
	if w.PalantirRef == "" {
		w.PalantirRef = keys.PalantirWithdrawalKey(w.SarumanPayoutID.String())
	}
	var id uuid.UUID
	err := tx.QueryRowContext(ctx, `
		INSERT INTO gateway_withdrawals (id, palantir_ref, saruman_payout_id, saruman_batch_id,
		    streamer_id, amount_idr, status, created_at,
		    provider_name, provider_payout_id, bank_account_ref,
		    status_checked_at, last_known_provider_status)
		VALUES ($1,$2,$3,$4,$5,$6,$7,NOW(),$8,$9,$10,$11,$12)
		ON CONFLICT (palantir_ref) DO NOTHING
		RETURNING id
	`, uuid.New(), w.PalantirRef, w.SarumanPayoutID, w.SarumanBatchID, w.StreamerID,
		w.AmountIDR, w.Status,
		w.ProviderName, w.ProviderPayoutID, w.BankAccountRef,
		w.StatusCheckedAt, w.LastKnownProviderStatus,
	).Scan(&id)
	if err == nil {
		w.ID = id
		return true, w, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return false, GatewayWithdrawal{}, fmt.Errorf("repo: insert gateway_withdrawals: %w", err)
	}
	// Conflict: someone else inserted first. Surface the existing row so the
	// caller can replay the idempotent response.
	existing, lockErr := r.LockForUpdate(ctx, tx, w.PalantirRef)
	if lockErr != nil {
		return false, GatewayWithdrawal{}, fmt.Errorf("repo: reload gateway_withdrawals: %w", lockErr)
	}
	return false, existing, nil
}