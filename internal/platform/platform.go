// Package platform holds the small amount of plumbing every service shares:
// config from env, structured logging, the Postgres pool and the admin HTTP server.
package platform

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Env returns the environment variable key, or def if it is unset.
func Env(key, def string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return def
}

// NewLogger returns a JSON logger tagged with the service name, and installs it as the default.
func NewLogger(service string) *slog.Logger {
	l := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", service)
	slog.SetDefault(l)
	return l
}

// DefaultKafkaBrokers points at the Redpanda started by `make up`.
const DefaultKafkaBrokers = "localhost:19092"

// DefaultDatabaseURL points at the Postgres started by `make up`.
const DefaultDatabaseURL = "postgres://vault:vault@localhost:5432/vault?sslmode=disable"

// NewPool connects to Postgres and fails fast if the database is unreachable,
// so a misconfigured service crashes at startup instead of on the first request.
func NewPool(ctx context.Context, url string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("parse database url: %w", err)
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}
	return pool, nil
}

// Route is an extra endpoint on the admin port.
type Route struct {
	Pattern string // e.g. "GET /audit/verify"
	Handler http.HandlerFunc
}

// StartAdmin serves /healthz (liveness), /readyz (can we reach our dependencies?) and any extra
// internal routes on a separate port from business traffic, one that's never exposed publicly.
func StartAdmin(addr string, ready func(context.Context) error, routes ...Route) *http.Server {
	mux := http.NewServeMux()
	for _, r := range routes {
		mux.HandleFunc(r.Pattern, r.Handler)
	}
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte("ok"))
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), time.Second)
		defer cancel()
		if err := ready(ctx); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		w.Write([]byte("ok"))
	})
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("admin server stopped", "err", err)
		}
	}()
	return srv
}
