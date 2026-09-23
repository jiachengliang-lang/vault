package platform

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// TestPool connects to the dev Postgres for integration tests.
func TestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool, err := NewPool(context.Background(), Env("DATABASE_URL", DefaultDatabaseURL))
	if err != nil {
		SkipUnlessRequired(t, "postgres unavailable, run `make up` first: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// SkipUnlessRequired skips a test whose infrastructure is missing, so `go test` works on a laptop
// without Docker. With VAULT_INTEGRATION=1 (set in CI) it fails instead: a CI run that quietly
// skipped every database test would look green while testing almost nothing.
func SkipUnlessRequired(t *testing.T, format string, args ...any) {
	t.Helper()
	if os.Getenv("VAULT_INTEGRATION") == "1" {
		t.Fatalf(format, args...)
	}
	t.Skipf(format, args...)
}
