package payment

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"vault/internal/platform"
)

// countingPSP wraps a PSP and counts calls, so tests can assert "charged exactly once".
type countingPSP struct {
	PSP
	calls atomic.Int32
	fail  atomic.Int32 // fail this many calls before succeeding
}

func (p *countingPSP) Charge(ctx context.Context, key string, amount int64) (string, error) {
	p.calls.Add(1)
	if p.fail.Load() > 0 {
		p.fail.Add(-1)
		return "", ErrPSPUnavailable
	}
	return p.PSP.Charge(ctx, key, amount)
}

func newTestService(t *testing.T, latency time.Duration) (*Service, *countingPSP) {
	psp := &countingPSP{PSP: NewMockPSP(latency, 0)}
	return NewService(platform.TestPool(t), psp), psp
}

func TestConcurrentRetriesChargeOnce(t *testing.T) {
	svc, psp := newTestService(t, 50*time.Millisecond)
	ctx := context.Background()
	orderID := uuid.New()
	key := orderID.String()

	const n = 20
	var wg sync.WaitGroup
	var fresh, inProgress atomic.Int32
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, replayed, err := svc.Charge(ctx, orderID, 500, key)
			switch {
			case errors.Is(err, ErrInProgress):
				inProgress.Add(1)
			case err != nil:
				t.Error(err)
			case !replayed:
				fresh.Add(1)
			}
		}()
	}
	wg.Wait()

	if got := psp.calls.Load(); got != 1 {
		t.Errorf("PSP called %d times, want 1", got)
	}
	if got := fresh.Load(); got != 1 {
		t.Errorf("%d calls performed a charge, want 1", got)
	}
	// Once the first charge finishes, a retry replays the result.
	p, replayed, err := svc.Charge(ctx, orderID, 500, key)
	if err != nil || !replayed || p.Status != StatusSucceeded {
		t.Fatalf("retry after completion: status %q replayed %v err %v", p.Status, replayed, err)
	}
	t.Logf("%d of %d concurrent callers were told to retry (in progress)", inProgress.Load(), n)
}

func TestDeclinedAboveLimit(t *testing.T) {
	svc, _ := newTestService(t, 0)
	orderID := uuid.New()
	p, _, err := svc.Charge(context.Background(), orderID, 2_000_000, orderID.String())
	if err != nil || p.Status != StatusDeclined {
		t.Fatalf("status %q err %v, want DECLINED", p.Status, err)
	}
}

func TestKeyReuseForDifferentAmount(t *testing.T) {
	svc, _ := newTestService(t, 0)
	ctx := context.Background()
	orderID := uuid.New()
	if _, _, err := svc.Charge(ctx, orderID, 500, orderID.String()); err != nil {
		t.Fatal(err)
	}
	if _, _, err := svc.Charge(ctx, orderID, 600, orderID.String()); !errors.Is(err, ErrKeyReuse) {
		t.Fatalf("got %v, want ErrKeyReuse", err)
	}
}

// If the PSP fails mid-charge, the payment stays PENDING. Retries are refused until the lease
// expires, then the next caller re-asks the (idempotent) PSP and finishes the payment.
func TestStalePendingIsTakenOver(t *testing.T) {
	svc, psp := newTestService(t, 0)
	ctx := context.Background()
	orderID := uuid.New()
	key := orderID.String()

	psp.fail.Store(1)
	if _, _, err := svc.Charge(ctx, orderID, 500, key); !errors.Is(err, ErrPSPUnavailable) {
		t.Fatalf("first attempt: got %v, want ErrPSPUnavailable", err)
	}
	if _, _, err := svc.Charge(ctx, orderID, 500, key); !errors.Is(err, ErrInProgress) {
		t.Fatalf("retry within lease: got %v, want ErrInProgress", err)
	}

	svc.PendingLease = 0
	p, replayed, err := svc.Charge(ctx, orderID, 500, key)
	if err != nil || !replayed || p.Status != StatusSucceeded {
		t.Fatalf("takeover: status %q replayed %v err %v", p.Status, replayed, err)
	}
}
