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
	// Pivot (the default provider). MerchantID/Secret authenticate OAuth;
	// CallbackKey verifies webhooks. Env selects the API base URL unless
	// PivotBaseURL overrides it. RedirectURL is the donor-facing return base.
	PivotMerchantID     string
	PivotMerchantSecret string
	PivotCallbackKey    string
	PivotEnv            string
	PivotBaseURL        string
	PivotRedirectURL    string
	ShutdownGrace       time.Duration
}

// Load reads the configuration from os.Getenv, applies defaults, and returns
// the resolved Config or an error.
func Load() (Config, error) {
	base, err := sharedconfig.Load("palantir")
	if err != nil {
		return Config{}, err
	}
	cfg := Config{
		Config:              base,
		GRPCAddr:            getenv("GRPC_ADDR", ":7001"),
		Database:            base.Database,
		NATS:                base.NATS,
		EngineAPIKey:        os.Getenv("SM_GATEWAY_ENGINE_API_KEY"),
		MidtransKey:         os.Getenv("MIDTRANS_SERVER_KEY"),
		MidtransEnv:         getenv("MIDTRANS_ENV", "sandbox"),
		DefaultProv:         getenv("PROVIDER_DEFAULT", "pivot"),
		PivotMerchantID:     base.Payment.PivotMerchantID,
		PivotMerchantSecret: base.Payment.PivotMerchantSecret,
		PivotCallbackKey:    base.Payment.PivotCallbackKey,
		PivotEnv:            getenv("PIVOT_ENV", "sandbox"),
		PivotBaseURL:        base.Payment.PivotBaseURL,
		PivotRedirectURL:    base.Payment.PivotRedirectURL,
		ShutdownGrace:       parseDuration("SHUTDOWN_GRACE_SECONDS", 15*time.Second),
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
