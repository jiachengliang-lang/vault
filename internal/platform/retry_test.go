package platform

import (
	"context"
	"errors"
	"testing"

	"github.com/cloudwego/kitex/pkg/kerrors"
)

func TestRetryOnlyRetriesTimeouts(t *testing.T) {
	cases := map[string]struct {
		err       error
		wantCalls int
	}{
		"timeout is retried twice":    {kerrors.ErrRPCTimeout, 3},
		"business error is an answer": {errors.New("order not found"), 1},
		"open breaker is not retried": {kerrors.ErrServiceCircuitBreak, 1},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			calls := 0
			_, err := Retry(context.Background(), NewRetryBudget(0.1, 10), func() (int, error) {
				calls++
				return 0, tc.err
			})
			if calls != tc.wantCalls || err == nil {
				t.Fatalf("calls = %d, err = %v; want %d calls and the error", calls, err, tc.wantCalls)
			}
		})
	}
}

func TestRetryStopsWhenBudgetIsEmpty(t *testing.T) {
	b := NewRetryBudget(0.1, 3) // three retries banked, and each call earns a tenth of one
	calls := 0
	for range 10 { // an outage: every call times out
		Retry(context.Background(), b, func() (int, error) {
			calls++
			return 0, kerrors.ErrRPCTimeout
		})
	}
	// Without a budget: 10 calls x 3 attempts = 30. With it: 10 first attempts + about 4 retries.
	if calls > 15 {
		t.Fatalf("%d calls during the outage; the retry budget should cap retries near 10%% of traffic", calls)
	}
}

func TestRetrySucceedsAfterTransientTimeout(t *testing.T) {
	calls := 0
	got, err := Retry(context.Background(), NewRetryBudget(0.1, 10), func() (string, error) {
		calls++
		if calls == 1 {
			return "", kerrors.ErrRPCTimeout
		}
		return "ok", nil
	})
	if err != nil || got != "ok" || calls != 2 {
		t.Fatalf("got %q, %v after %d calls", got, err, calls)
	}
}
