package order

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/google/uuid"

	"vault/internal/platform"
)

func countEvents(t *testing.T, s *Store, orderID uuid.UUID) int {
	t.Helper()
	var n int
	err := s.db.QueryRow(context.Background(),
		`SELECT count(*) FROM outbox WHERE payload->>'order_id' = $1`, orderID.String()).Scan(&n)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// The core guarantee: N concurrent retries of one checkout create exactly one order and one event.
func TestCreateConcurrentRetriesCreateOneOrder(t *testing.T) {
	s := NewStore(platform.TestPool(t))
	ctx := context.Background()
	user := uuid.New()

	const n = 20
	var wg sync.WaitGroup
	ids := make([]uuid.UUID, n)
	replays := make([]bool, n)
	errs := make([]error, n)
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var o Order
			o, replays[i], errs[i] = s.Create(ctx, user, 500, "checkout-1")
			ids[i] = o.ID
		}()
	}
	wg.Wait()

	fresh := 0
	for i := range n {
		if errs[i] != nil {
			t.Fatalf("call %d: %v", i, errs[i])
		}
		if ids[i] != ids[0] {
			t.Fatalf("call %d got order %s, want %s", i, ids[i], ids[0])
		}
		if !replays[i] {
			fresh++
		}
	}
	if fresh != 1 {
		t.Errorf("%d calls created an order, want exactly 1", fresh)
	}
	if got := countEvents(t, s, ids[0]); got != 1 {
		t.Errorf("outbox has %d events for the order, want 1", got)
	}
}

func TestCreateKeyReuseWithDifferentAmount(t *testing.T) {
	s := NewStore(platform.TestPool(t))
	ctx := context.Background()
	user := uuid.New()

	if _, _, err := s.Create(ctx, user, 500, "k"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Create(ctx, user, 999, "k"); !errors.Is(err, ErrKeyReuse) {
		t.Fatalf("got %v, want ErrKeyReuse", err)
	}
}

func TestKeysAreScopedPerUser(t *testing.T) {
	s := NewStore(platform.TestPool(t))
	ctx := context.Background()

	a, _, err := s.Create(ctx, uuid.New(), 500, "same-key")
	if err != nil {
		t.Fatal(err)
	}
	b, replayed, err := s.Create(ctx, uuid.New(), 500, "same-key")
	if err != nil {
		t.Fatal(err)
	}
	if replayed || a.ID == b.ID {
		t.Fatal("two different users with the same key must get two different orders")
	}
}

func TestUpdateStatusTransitions(t *testing.T) {
	s := NewStore(platform.TestPool(t))
	ctx := context.Background()
	o, _, err := s.Create(ctx, uuid.New(), 500, "k")
	if err != nil {
		t.Fatal(err)
	}

	if o, err = s.UpdateStatus(ctx, o.ID, StatusPaid); err != nil || o.Status != StatusPaid {
		t.Fatalf("PENDING -> PAID: status %q, err %v", o.Status, err)
	}
	// Repeating the same update (a gateway retry) is a no-op and emits no second event.
	if _, err = s.UpdateStatus(ctx, o.ID, StatusPaid); err != nil {
		t.Fatalf("PAID -> PAID should be a no-op, got %v", err)
	}
	if got := countEvents(t, s, o.ID); got != 2 {
		t.Errorf("got %d events, want 2 (created + paid)", got)
	}
	if _, err = s.UpdateStatus(ctx, o.ID, StatusFailed); !errors.Is(err, ErrInvalidTransition) {
		t.Errorf("PAID -> FAILED: got %v, want ErrInvalidTransition", err)
	}
	if _, err = s.UpdateStatus(ctx, uuid.New(), StatusPaid); !errors.Is(err, ErrNotFound) {
		t.Errorf("unknown order: got %v, want ErrNotFound", err)
	}
}
