// Package config loads Palantir's environment configuration.
package config

import (
	"fmt"
	"os"
	"strconv"
	"time"

	sharedconfig "github.com/frederickmarvel/inflora-shared/config"
)

// Config is the resolved Palantir configuration.
type Config struct {
	sharedconfig.Config
	GRPCAddr      string
	Database      sharedconfig.DatabaseConfig
	NATS          sharedconfig.NATSConfig
	EngineAPIKey  string
	MidtransKey   string
	MidtransEnv   string // "sandbox" | "production"
	DefaultProv   string // "pivot" | "midtrans"
	ProviderStubs map[string]string
	ShutdownGrace time.Duration
	// Dev-only autosettle flags. DEFAULT ON for the dev stack, MUST be
	// turned off in production: the pivot stub holds no provider state, so
	// these short circuits are what close the dev top-up settlement (D-052)
	// and the dev payout loop (T13). Production traffic goes through real
	// provider webhooks and the D-055 status probe instead.
	DevTopupAutosettle      bool
	DevWithdrawalAutosettle bool
}

// Load reads the configuration from os.Getenv, applies defaults, and returns
// the resolved Config or an error.
func Load() (Config, error) {
	base, err := sharedconfig.Load("palantir")
	if err != nil {
		return Config{}, err
	}
	cfg := Config{
		Config:        base,
		GRPCAddr:      getenv("GRPC_ADDR", ":7001"),
		Database:      base.Database,
		NATS:          base.NATS,
		EngineAPIKey:  os.Getenv("SM_GATEWAY_ENGINE_API_KEY"),
		MidtransKey:   os.Getenv("MIDTRANS_SERVER_KEY"),
		MidtransEnv:   getenv("MIDTRANS_ENV", "sandbox"),
		DefaultProv:   getenv("PROVIDER_DEFAULT", "pivot"),
		ShutdownGrace: parseDuration("SHUTDOWN_GRACE_SECONDS", 15*time.Second),
		// Dev defaults; PIVOT_*_AUTOSETTLE=false restores webhook/status-probe
		// semantics (set explicitly in production).
		DevTopupAutosettle:      getenvBool("DEV_TOPUP_AUTOSETTLE", true),
		DevWithdrawalAutosettle: getenvBool("PIVOT_WITHDRAWAL_AUTOSETTLE", true),
	}
	if cfg.EngineAPIKey == "" {
		return Config{}, fmt.Errorf("config: SM_GATEWAY_ENGINE_API_KEY is required")
	}
	if cfg.Database.DSN == "" {
		return Config{}, fmt.Errorf("config: DB_DSN is required")
	}
	return cfg, nil
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func parseDuration(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	if n, err := strconv.Atoi(v); err == nil {
		return time.Duration(n) * time.Second
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return def
	}
	return d
}

func getenvBool(key string, def bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return def
	}
	return b
}
