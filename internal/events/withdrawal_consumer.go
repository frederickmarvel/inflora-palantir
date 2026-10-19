// Package events hosts Palantir's durable inbox consumer: the
// 'palantir-payout-requested' subscription turns Saruman's payout.requested.v1
// events into provider withdrawal creation (T16).
package events

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	palantirgrpc "github.com/frederickmarvel/inflora-palantir/internal/grpc"
	"github.com/frederickmarvel/inflora-shared/events"
	pb "github.com/frederickmarvel/inflora-shared/gen/go/palantir/v1"
)

const (
	payoutRequestedSubject = "payout.requested.v1.>"
	// Durable names are NATS consumer names: dots are invalid there (the
	// original 'palantir.payout-requested' failed subscription with
	// "nats: invalid consumer name"), so it is dash-separated.
	payoutRequestedDurable = "palantir-payout-requested"
)

// WithdrawalWorker consumes payout.requested.v1 and executes provider
// withdrawals. Messages are acked only after the handler succeeds; the
// CreateWithdrawal rpc is idempotent (palantir_ref derived from the payout
// id), so redeliveries land on the same mirror row without a second provider
// call.
type WithdrawalWorker struct {
	Subscriber *events.NATSSubscriber
	Server     *palantirgrpc.WithdrawalServer
}

func (w *WithdrawalWorker) Name() string { return "palantir-payout-requested" }

func (w *WithdrawalWorker) Run(ctx context.Context) error {
	if w.Subscriber == nil || w.Server == nil {
		return errors.New("events: subscriber and withdrawal server are required")
	}
	return w.Subscriber.Subscribe(ctx, payoutRequestedSubject, payoutRequestedDurable, w.handle)
}

func (w *WithdrawalWorker) handle(ctx context.Context, env events.Envelope) error {
	if env.EventType != events.PayoutRequested {
		// Not a payout request; tolerate spurious messages on the wildcard.
		return nil
	}
	var requested events.PayoutRequestedEvent
	if err := json.Unmarshal(env.Payload, &requested); err != nil {
		return fmt.Errorf("events: malformed payout.requested.v1 payload: %w", err)
	}
	if requested.PayoutID == "" || requested.StreamerID == "" {
		return errors.New("events: payout.requested.v1 payload missing payout_id or streamer_id")
	}
	_, err := w.Server.CreateWithdrawal(ctx, &pb.CreateWithdrawalRequest{
		SarumanPayoutId: requested.PayoutID,
		StreamerId:      requested.StreamerID,
		AmountIdr:       requested.AmountIDR,
		BankAccountRef:  requested.BankAccountID,
		RequestedAtUnix: requested.RequestedAt.Unix(),
	})
	return err
}