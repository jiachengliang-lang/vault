package platform

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// TestPool connects to the dev Postgres for integration tests, skipping the test if it isn't running.
func TestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool, err := NewPool(context.Background(), Env("DATABASE_URL", DefaultDatabaseURL))
	if err != nil {
		t.Skipf("postgres unavailable, run `make up` first: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}
