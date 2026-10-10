// Package grpc — Health service for Palantir.
package grpc

import (
	"context"
	"time"

	pb "github.com/frederickmarvel/inflora-shared/gen/go/palantir/v1"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

// HealthServer implements HealthService.Check.
type HealthServer struct {
	pb.UnimplementedHealthServiceServer
	StartedAt time.Time
}

// NewHealthServer constructs a HealthServer.
func NewHealthServer() *HealthServer {
	return &HealthServer{StartedAt: time.Now()}
}

// Check returns SERVING with uptime.
func (s *HealthServer) Check(ctx context.Context, _ *emptypb.Empty) (*pb.HealthCheckResponse, error) {
	_ = ctx
	uptime := int64(time.Since(s.StartedAt).Seconds())
	return &pb.HealthCheckResponse{Status: pb.HealthCheckResponse_SERVING, UptimeSeconds: uptime}, nil
}
