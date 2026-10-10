// Package grpc implements Palantir's gRPC services: TopUpService and
// HealthService. Settlement (SettleTopUp) is implemented in settle.go.
package grpc

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/frederickmarvel/inflora-palantir/internal/repo"
	"github.com/frederickmarvel/inflora-shared/auth"
	pb "github.com/frederickmarvel/inflora-shared/gen/go/palantir/v1"
	"github.com/frederickmarvel/inflora-shared/keys"
	"github.com/frederickmarvel/inflora-shared/outbox"
	"github.com/frederickmarvel/inflora-shared/provider"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TopUpServer implements the TopUpService gRPC interface.
type TopUpServer struct {
	pb.UnimplementedTopUpServiceServer
	DB           *sql.DB
	Provider     provider.Provider
	ProviderName string
	Repo         *repo.GatewayTopUpRepo
}

// NewTopUpServer constructs a TopUpServer with its dependencies.
func NewTopUpServer(db *sql.DB, p provider.Provider, providerName string, r *repo.GatewayTopUpRepo) *TopUpServer {
	return &TopUpServer{DB: db, Provider: p, ProviderName: providerName, Repo: r}
}

// CreateTopUp creates a gateway top-up. The same idempotency_key returns the
// same result (D-012). The donation_id is used as the stable key per Phase 0
// §11-operation-keys.md.
func (s *TopUpServer) CreateTopUp(ctx context.Context, req *pb.CreateTopUpRequest) (*pb.CreateTopUpResponse, error) {
	if req.AmountIdr <= 0 {
		return nil, status.Error(codes.InvalidArgument, "amount_idr must be positive")
	}
	if req.Currency != "" && req.Currency != "IDR" {
		return nil, status.Error(codes.InvalidArgument, "only IDR is supported")
	}
	if req.SarumanDonationId == "" || req.StreamerId == "" {
		return nil, status.Error(codes.InvalidArgument, "saruman_donation_id and streamer_id required")
	}
	donationID, err := uuid.Parse(req.SarumanDonationId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid saruman_donation_id: %v", err)
	}
	streamerID, err := uuid.Parse(req.StreamerId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid streamer_id: %v", err)
	}
	if req.IdempotencyKey == "" {
		req.IdempotencyKey = keys.PalantirTopUpKey(req.SarumanDonationId)
	}
	// 1. Idempotency: check for existing gateway_topup first.
	ref := keys.PalantirTopUpKey(req.SarumanDonationId)
	requestHash := topUpRequestHash(req)
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
		if existing.RequestHash != "" && existing.RequestHash != requestHash {
			return nil, status.Error(codes.AlreadyExists, "idempotency key reused with different request")
		}
		// Reuse existing row.
		_ = tx.Commit()
		committed = true
		return &pb.CreateTopUpResponse{
			ChargeId:      existing.ProviderChargeID,
			PaymentUrl:    existing.PaymentURL,
			ProviderName:  existing.ProviderName,
			ExpiresAtUnix: unixTime(existing.ExpiresAt),
		}, nil
	}
	if !errors.Is(err, repo.ErrNotFound) {
		return nil, status.Errorf(codes.Internal, "lookup gateway: %v", err)
	}
	// 2. Call provider (Pivot by default) for the payment URL.
	res, err := s.Provider.CreateTopUp(ctx, provider.CreateTopUpRequest{
		ExternalID:       req.SarumanDonationId,
		AmountIDR:        req.AmountIdr,
		DonorName:        req.DonorDisplayName,
		DonorEmail:       req.DonorEmail,
		RequestID:        ref,
		SuccessReturnURL: req.SuccessReturnUrl,
		FailureReturnURL: req.FailureReturnUrl,
		ExpirationURL:    req.ExpirationReturnUrl,
		Description:      req.Description,
	})
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "provider: %v", err)
	}
	paymentMethod := res.PaymentMethod
	if paymentMethod == "" {
		paymentMethod = string(provider.QRIS)
	}
	// 3. Persist gateway_topups.
	if err := s.Repo.InsertTopUp(ctx, tx, repo.GatewayTopUp{
		PalantirRef:       ref,
		SarumanDonationID: donationID,
		StreamerID:        streamerID,
		AmountIDR:         req.AmountIdr,
		MDRIDR:            0,
		GrossChargedIDR:   req.AmountIdr,
		ProviderName:      s.ProviderName,
		ProviderRequestID: ref,
		RequestHash:       requestHash,
		PaymentMethod:     paymentMethod,
		ProviderChargeID:  res.ChargeID,
		PaymentURL:        res.PaymentURL,
		ExpiresAt:         timePtr(res.ExpiresAt),
		Status:            res.Status,
	}); err != nil {
		return nil, status.Errorf(codes.Internal, "insert gateway: %v", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, status.Errorf(codes.Internal, "commit: %v", err)
	}
	committed = true
	return &pb.CreateTopUpResponse{
		ChargeId:      res.ChargeID,
		PaymentUrl:    res.PaymentURL,
		ProviderName:  s.ProviderName,
		ExpiresAtUnix: unixTime(timePtr(res.ExpiresAt)),
	}, nil
}

func topUpRequestHash(req *pb.CreateTopUpRequest) string {
	raw := fmt.Sprintf("%s\x00%s\x00%d\x00%s\x00%s", req.SarumanDonationId, req.StreamerId, req.AmountIdr, req.Currency, req.DonorDisplayName)
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}

func timePtr(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	u := t.UTC()
	return &u
}

func unixTime(t *time.Time) int64 {
	if t == nil || t.IsZero() {
		return 0
	}
	return t.Unix()
}

// MustParseUUID is a small helper for tests.
func MustParseUUID(s string) uuid.UUID {
	id, err := uuid.Parse(s)
	if err != nil {
		panic(fmt.Sprintf("invalid uuid %q: %v", s, err))
	}
	return id
}

// Compile-time guard: ensure auth.GRPCBearerAuth is usable.
var _ = auth.GRPCBearerAuth

// Compile-time guard: ensure outbox import is reachable.
var _ = outbox.ErrTxRequired

// Compile-time guard: ensure grpc import is reachable.
var _ grpc.ServerStream
