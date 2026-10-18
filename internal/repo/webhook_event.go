package repo

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/google/uuid"
)

var ErrWebhookPayloadConflict = errors.New("repo: webhook event payload conflict")

// InsertWebhookEvent establishes the durable provider-name/event-ID
// idempotency boundary. It returns duplicate=true only for an identical replay.
func InsertWebhookEvent(ctx context.Context, tx *sql.Tx, providerName, providerEventID, payloadHash string, payload []byte) (id uuid.UUID, duplicate bool, err error) {
	id = uuid.New()
	var inserted uuid.UUID
	err = tx.QueryRowContext(ctx, `
		INSERT INTO webhook_events (id, provider_name, provider_event_id, payload_hash, payload, received_at)
		VALUES ($1,$2,$3,$4,$5::jsonb,NOW())
		ON CONFLICT (provider_name, provider_event_id) DO NOTHING
		RETURNING id
	`, id, providerName, providerEventID, payloadHash, string(payload)).Scan(&inserted)
	if err == nil {
		return inserted, false, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return uuid.Nil, false, fmt.Errorf("repo: insert webhook_event: %w", err)
	}
	var existingHash string
	if err = tx.QueryRowContext(ctx, `
		SELECT id, payload_hash FROM webhook_events
		WHERE provider_name=$1 AND provider_event_id=$2
	`, providerName, providerEventID).Scan(&id, &existingHash); err != nil {
		return uuid.Nil, false, fmt.Errorf("repo: read duplicate webhook_event: %w", err)
	}
	if existingHash != payloadHash {
		return uuid.Nil, false, ErrWebhookPayloadConflict
	}
	return id, true, nil
}

func MarkWebhookProcessed(ctx context.Context, tx *sql.Tx, id uuid.UUID) error {
	if _, err := tx.ExecContext(ctx, `UPDATE webhook_events SET processed_at=COALESCE(processed_at,NOW()) WHERE id=$1`, id); err != nil {
		return fmt.Errorf("repo: mark webhook processed: %w", err)
	}
	return nil
}
