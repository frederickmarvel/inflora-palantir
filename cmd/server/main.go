// Package main is the Palantir composition root. Phase 4 wires:
//   - PostgreSQL connection (shared DB)
//   - Pivot provider (default for local dev) plus optional Midtrans
//   - TopUp gRPC service on :7001
//   - Health gRPC service
//   - Graceful shutdown via shared runtime helpers
package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	sharedauth "github.com/frederickmarvel/inflora-shared/auth"
	sharedconfig "github.com/frederickmarvel/inflora-shared/config"
	shareddb "github.com/frederickmarvel/inflora-shared/db"
	"github.com/frederickmarvel/inflora-shared/events"
	"github.com/frederickmarvel/inflora-shared/observability"
	"github.com/frederickmarvel/inflora-shared/outbox"
	sharedruntime "github.com/frederickmarvel/inflora-shared/runtime"

	"github.com/frederickmarvel/inflora-shared/provider"
	"github.com/frederickmarvel/inflora-shared/provider/midtrans"
	"github.com/frederickmarvel/inflora-shared/provider/pivot"

	"github.com/frederickmarvel/inflora-palantir/internal/config"
	palantirevents "github.com/frederickmarvel/inflora-palantir/internal/events"
	palantirgrpc "github.com/frederickmarvel/inflora-palantir/internal/grpc"
	"github.com/frederickmarvel/inflora-palantir/internal/repo"
	pb "github.com/frederickmarvel/inflora-shared/gen/go/palantir/v1"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	healthsrv "google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintln(os.Stderr, "config:", err)
		os.Exit(2)
	}
	logger, err := observability.NewLogger(cfg.ServiceName, cfg.LogLevel, cfg.LogFormat)
	if err != nil {
		fmt.Fprintln(os.Stderr, "logger:", err)
		os.Exit(2)
	}
	defer observability.Sync(logger)
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	database, err := shareddb.Open(ctx, shareddb.Config{DSN: cfg.Database.DSN, MaxOpenConns: cfg.Database.MaxOpenConns, MaxIdleConns: cfg.Database.MaxIdleConns, ConnMaxLifetime: cfg.Database.ConnMaxLifetime, PingTimeout: 5 * time.Second})
	if err != nil {
		logger.Fatal("connect database", zap.Error(err))
	}
	defer func() { _ = database.Close() }()
	prov, providerName := buildProvider(cfg, logger)
	repoGateway := repo.NewGatewayTopUpRepo(database)
	topUp := palantirgrpc.NewTopUpServer(database, prov, providerName, repoGateway)
	topUp.DevTopupAutosettle = cfg.DevTopupAutosettle
	adminSrv := palantirgrpc.NewAdminServer(database, prov, providerName, repoGateway)
	repoWithdrawal := repo.NewGatewayWithdrawalRepo(database)
	withdrawal := palantirgrpc.NewWithdrawalServer(database, prov, providerName, repoWithdrawal, cfg.DevWithdrawalAutosettle)
	if cfg.NATS.URL == "" {
		logger.Fatal("NATS_URL is required")
	}
	natsConfig := events.NATSConfig{URL: cfg.NATS.URL, Name: cfg.ServiceName, Stream: cfg.NATS.StreamName, Subjects: []string{">"}, AckWait: cfg.NATS.AckWait, MaxDeliver: cfg.NATS.MaxDeliver}
	publisher, err := events.NewNATSPublisher(natsConfig)
	if err != nil {
		logger.Fatal("connect nats", zap.Error(err))
	}
	defer func() { _ = publisher.Close() }()
	natsSubscriber, err := events.NewNATSSubscriber(natsConfig)
	if err != nil {
		logger.Fatal("connect nats subscriber", zap.Error(err))
	}
	defer func() { _ = natsSubscriber.Close() }()
	dispatcher, err := outbox.NewDispatcher(database, publisher, outbox.DispatcherConfig{WorkerName: "palantir-outbox", Producer: "palantir"})
	if err != nil {
		logger.Fatal("configure outbox", zap.Error(err))
	}
	healthServer := healthsrv.NewServer()
	healthServer.SetServingStatus("palantir.TopUpService", healthpb.HealthCheckResponse_SERVING)
	lis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		logger.Fatal("listen gRPC", zap.Error(err))
	}
	gsrv := grpc.NewServer(
		grpc.UnaryInterceptor(sharedauth.GRPCBearerAuth(cfg.EngineAPIKey)),
	)
	pb.RegisterTopUpServiceServer(gsrv, topUp)
	pb.RegisterAdminServiceServer(gsrv, adminSrv)
	pb.RegisterWithdrawalServiceServer(gsrv, withdrawal)
	pb.RegisterHealthServiceServer(gsrv, palantirgrpc.NewHealthServer())
	logger.Info("palantir listening", zap.String("addr", cfg.GRPCAddr), zap.String("version", cfg.Version), zap.String("default_provider", cfg.DefaultProv))
	probe := func(context.Context) (bool, error) { return database.PingContext(context.Background()) == nil, nil }
	worker := &grpcServer{lis: lis, gsrv: gsrv, logger: logger, grace: cfg.ShutdownGrace}
	withdrawalWorker := &palantirevents.WithdrawalWorker{Subscriber: natsSubscriber, Server: withdrawal}
	err = sharedruntime.Run(ctx, 5*time.Second, cfg.ShutdownGrace, probe, worker, dispatcher, withdrawalWorker)
	if err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("shutdown", zap.Error(err))
	}
}

func buildProvider(cfg config.Config, logger *zap.Logger) (provider.Provider, string) {
	switch cfg.DefaultProv {
	case "midtrans":
		if cfg.MidtransKey == "" {
			logger.Warn("MIDTRANS_SERVER_KEY missing; falling back to pivot stub")
			return pivot.NewStub(getenv("PIVOT_SECRET", "pivot-dev-secret")), "pivot"
		}
		base := "https://api.sandbox.midtrans.com"
		if cfg.MidtransEnv == "production" {
			base = "https://api.midtrans.com"
		}
		return midtrans.New(base, cfg.MidtransKey, nil), "midtrans"
	default:
		return pivot.NewStub(getenv("PIVOT_SECRET", "pivot-dev-secret")), "pivot"
	}
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

type grpcServer struct {
	lis    net.Listener
	gsrv   *grpc.Server
	logger *zap.Logger
	grace  time.Duration
}

func (g *grpcServer) Name() string { return "palantir-grpc" }
func (g *grpcServer) Run(ctx context.Context) error {
	errs := make(chan error, 1)
	go func() { errs <- g.gsrv.Serve(g.lis) }()
	select {
	case <-ctx.Done():
		stopped := make(chan struct{})
		go func() {
			g.gsrv.GracefulStop()
			close(stopped)
		}()
		grace := g.grace
		if grace <= 0 {
			grace = 10 * time.Second
		}
		select {
		case <-stopped:
		case <-time.After(grace):
			g.gsrv.Stop()
			<-stopped
		}
		return nil
	case err := <-errs:
		if err != nil {
			g.logger.Error("grpc serve", zap.Error(err))
			return err
		}
		return nil
	}
}

// Compile-time guards.
var (
	_ sharedconfig.Config
	_ events.Envelope
)
