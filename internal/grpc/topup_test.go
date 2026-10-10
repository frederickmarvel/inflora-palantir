package grpc_test

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	shareddb "github.com/frederickmarvel/inflora-shared/db"
	"github.com/frederickmarvel/inflora-shared/keys"
	"github.com/frederickmarvel/inflora-shared/provider"
	"github.com/frederickmarvel/inflora-shared/provider/pivot"

	"github.com/frederickmarvel/inflora-palantir/internal/grpc"
	"github.com/frederickmarvel/inflora-palantir/internal/repo"
	pb "github.com/frederickmarvel/inflora-shared/gen/go/palantir/v1"
	"github.com/google/uuid"
)

// countingStub wraps the pivot stub and counts how many times the provider
// is called for CreateTopUp. Phase 4 requires concurrent duplicates must NOT
// call the provider more than once logically.
type countingStub struct {
	provider.Provider
	mu    sync.Mutex
	calls int
	stub  *pivot.Stub
}

func newCountingStub() *countingStub {
	return &countingStub{stub: pivot.NewStub("pivot-dev-secret")}
}

func (s *countingStub) CreateTopUp(ctx context.Context, r provider.CreateTopUpRequest) (provider.CreateTopUpResult, error) {
	s.mu.Lock()
	s.calls++
	s.mu.Unlock()
	return s.stub.CreateTopUp(ctx, r)
}

func (s *countingStub) Calls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func TestCreateTopUpIdempotent(t *testing.T) {
	if os.Getenv("TEST_DATABASE_URL") == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	db := applyMigrations(t, openTestDB(t))
	streamerID := seedStreamer(t, db)

	stub := newCountingStub()
	repoGateway := repo.NewGatewayTopUpRepo(db)
	server := grpc.NewTopUpServer(db, stub, "pivot", repoGateway)
	donationID := uuid.New().String()
	req := &pb.CreateTopUpRequest{
		SarumanDonationId: donationID,
		StreamerId:        streamerID,
		AmountIdr:         50000,
		Currency:          "IDR",
		DonorDisplayName:  "Phase Four",
	}

	first, err := server.CreateTopUp(context.Background(), req)
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	second, err := server.CreateTopUp(context.Background(), req)
	if err != nil {
		t.Fatalf("second: %v", err)
	}
	if first.ChargeId != second.ChargeId ||
		first.PaymentUrl != second.PaymentUrl ||
		first.ProviderName != second.ProviderName ||
		first.ExpiresAtUnix != second.ExpiresAtUnix {
		t.Fatalf("replay mismatch: first=%v second=%v", first, second)
	}
	if first.PaymentUrl == "" || first.ExpiresAtUnix == 0 {
		t.Fatalf("incomplete provider response: %v", first)
	}
	if stub.Calls() != 1 {
		t.Fatalf("provider calls = %d, want 1", stub.Calls())
	}
}

func TestCreateTopUpRejectsNonIDR(t *testing.T) {
	if os.Getenv("TEST_DATABASE_URL") == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	db := applyMigrations(t, openTestDB(t))
	streamerID := seedStreamer(t, db)

	server := grpc.NewTopUpServer(db, newCountingStub(), "pivot", repo.NewGatewayTopUpRepo(db))
	_, err := server.CreateTopUp(context.Background(), &pb.CreateTopUpRequest{
		SarumanDonationId: uuid.NewString(),
		StreamerId:        streamerID,
		AmountIdr:         50000,
		Currency:          "USD",
	})
	if err == nil {
		t.Fatalf("expected error for non-IDR currency")
	}
}

func TestCreateTopUpValidatesAmount(t *testing.T) {
	if os.Getenv("TEST_DATABASE_URL") == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	db := applyMigrations(t, openTestDB(t))
	streamerID := seedStreamer(t, db)

	server := grpc.NewTopUpServer(db, newCountingStub(), "pivot", repo.NewGatewayTopUpRepo(db))
	_, err := server.CreateTopUp(context.Background(), &pb.CreateTopUpRequest{
		SarumanDonationId: uuid.NewString(),
		StreamerId:        streamerID,
		AmountIdr:         0,
		Currency:          "IDR",
	})
	if err == nil {
		t.Fatalf("expected error for non-positive amount")
	}
}

func TestCreateTopUpConcurrentDuplicates(t *testing.T) {
	if os.Getenv("TEST_DATABASE_URL") == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	db := applyMigrations(t, openTestDB(t))
	streamerID := seedStreamer(t, db)

	stub := newCountingStub()
	server := grpc.NewTopUpServer(db, stub, "pivot", repo.NewGatewayTopUpRepo(db))
	donationID := uuid.New().String()
	req := &pb.CreateTopUpRequest{
		SarumanDonationId: donationID,
		StreamerId:        streamerID,
		AmountIdr:         50000,
		Currency:          "IDR",
	}
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := server.CreateTopUp(context.Background(), req)
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("err: %v", err)
		}
	}
	if stub.Calls() != 1 {
		t.Fatalf("provider calls = %d, want 1", stub.Calls())
	}
	ref := keys.PalantirTopUpKey(donationID)
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM gateway_topups WHERE palantir_ref = $1`, ref).Scan(&count); err != nil {
		t.Fatalf("count gateway_topups: %v", err)
	}
	if count != 1 {
		t.Fatalf("gateway_topups rows = %d, want 1", count)
	}
}

// --- helpers ---

func openTestDB(t *testing.T) *sql.DB {
	t.Helper()
	database, err := sql.Open("pgx", os.Getenv("TEST_DATABASE_URL"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return database
}

func applyMigrations(t *testing.T, db *sql.DB) *sql.DB {
	t.Helper()
	// Per-test isolated schema. The connection startup option applies the
	// search_path to every connection in the pool.
	name := "test_p4_" + sanitize(t.Name())
	rootDSN := os.Getenv("TEST_DATABASE_URL")
	q := rootDSN
	sep := "?"
	if strings.Contains(q, "?") {
		sep = "&"
	}
	q = q + sep + "options=-c search_path%3D" + name
	scoped, err := sql.Open("pgx", q)
	if err != nil {
		t.Fatalf("open scoped db: %v", err)
	}
	// Use independent PostgreSQL connections for concurrent requests.
	scoped.SetMaxOpenConns(16)
	scoped.SetMaxIdleConns(16)
	if _, err := scoped.Exec(`DROP SCHEMA IF EXISTS ` + name + ` CASCADE; CREATE SCHEMA ` + name); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	dir := os.Getenv("SARUMAN_SCHEMA_DIR")
	if dir == "" {
		_, file, _, _ := runtime.Caller(0)
		dir = filepath.Join(filepath.Dir(file), "..", "..", "..", "inflora-saruman", "migrations")
	}
	for _, n := range []string{"00001_init.up.sql", "00003_reliability.up.sql", "00004_palantir_topup_idempotency.up.sql"} {
		path := filepath.Join(dir, n)
		schema, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", n, err)
		}
		if _, err := scoped.Exec(string(schema)); err != nil {
			t.Fatalf("apply %s: %v", n, err)
		}
	}
	t.Cleanup(func() {
		_, _ = scoped.Exec(`DROP SCHEMA IF EXISTS ` + name + ` CASCADE`)
		_ = scoped.Close()
	})
	return scoped
}

func seedStreamer(t *testing.T, db *sql.DB) string {
	t.Helper()
	id := uuid.NewString()
	if _, err := db.Exec(
		`INSERT INTO streamers (id, email, display_name, password_hash) VALUES ($1, $2, $3, $4)`,
		id, id+"@example.com", "Phase Four Streamer", "$2a$12$placeholder",
	); err != nil {
		t.Fatalf("seed streamer: %v", err)
	}
	return id
}

// Compile-time guard: ensure shareddb is reachable from this test build.
var _ = shareddb.NewPostgres
var _ = time.Second

func sanitize(s string) string {
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		if (s[i] >= 'a' && s[i] <= 'z') || (s[i] >= 'A' && s[i] <= 'Z') || (s[i] >= '0' && s[i] <= '9') {
			out = append(out, s[i])
		} else {
			out = append(out, '_')
		}
	}
	return string(out)
}
