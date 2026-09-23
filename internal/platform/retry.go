package platform

import (
	"context"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/cloudwego/kitex/pkg/kerrors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var retriesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "rpc_client_retries_total",
	Help: "RPC retries, by result: attempted, or denied because the retry budget was empty.",
}, []string{"result"})

// RetryBudget caps retries at a fraction of traffic (the approach gRPC uses for retry throttling).
// Every call deposits `ratio` tokens, up to `max`; every retry spends one. While a dependency is
// healthy there are always tokens; during an outage the budget empties after a burst, and calls stop
// retrying instead of multiplying load on the service that is already failing (a retry storm).
type RetryBudget struct {
	mu     sync.Mutex
	tokens float64
	ratio  float64
	max    float64
}

func NewRetryBudget(ratio, max float64) *RetryBudget {
	return &RetryBudget{tokens: max, ratio: ratio, max: max}
}

func (b *RetryBudget) deposit() {
	b.mu.Lock()
	b.tokens = min(b.tokens+b.ratio, b.max)
	b.mu.Unlock()
}

func (b *RetryBudget) withdraw() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// Retry runs call, retrying up to 2 more times with 10-50 ms of jitter when it times out, but never
// starting a retry more than 2.5 s after the first attempt: with 2 s RPC timeouts, that caps the
// worst case at about 4.5 s instead of 6.
//
// Only timeouts are retried: a business error is an answer, and an open circuit breaker means
// "don't call". Callers must only retry idempotent calls. In Vault every RPC is (idempotency keys,
// conditional updates); retrying a non-idempotent call can apply it twice.
func Retry[T any](ctx context.Context, b *RetryBudget, call func() (T, error)) (T, error) {
	b.deposit()
	start := time.Now()
	res, err := call()
	for attempt := 1; attempt <= 2 && err != nil && kerrors.IsTimeoutError(err); attempt++ {
		if time.Since(start) > 2500*time.Millisecond {
			break
		}
		if !b.withdraw() {
			retriesTotal.WithLabelValues("budget_exhausted").Inc()
			break
		}
		select {
		case <-ctx.Done():
			return res, err
		case <-time.After(time.Duration(10+rand.IntN(41)) * time.Millisecond):
		}
		retriesTotal.WithLabelValues("attempted").Inc()
		res, err = call()
	}
	return res, err
}
