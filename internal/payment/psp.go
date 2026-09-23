// Package payment charges orders through a payment service provider (PSP) exactly once per order.
package payment

import (
	"context"
	"errors"
	"math/rand/v2"
	"sync"
	"time"
)

const (
	StatusPending   = "PENDING"
	StatusSucceeded = "SUCCEEDED"
	StatusDeclined  = "DECLINED"
)

// ErrPSPUnavailable is a transient failure: the charge may or may not have happened.
var ErrPSPUnavailable = errors.New("payment provider unavailable")

// PSP is the external payment provider. Like Stripe, it accepts an idempotency key:
// calling Charge twice with the same key returns the first result instead of charging twice.
// That property is what lets us safely retry after a timeout.
type PSP interface {
	Charge(ctx context.Context, key string, amountCents int64) (status string, err error)
}

// MockPSP stands in for a real provider. It declines charges above DeclineAbove cents,
// adds Latency to every call, and fails a FailureRate fraction of calls to exercise retries.
type MockPSP struct {
	Latency      time.Duration
	FailureRate  float64
	DeclineAbove int64

	mu      sync.Mutex
	results map[string]string // idempotency key -> result; unbounded, fine for a mock
}

func NewMockPSP(latency time.Duration, failureRate float64) *MockPSP {
	return &MockPSP{
		Latency:      latency,
		FailureRate:  failureRate,
		DeclineAbove: 1_000_000, // $10,000
		results:      make(map[string]string),
	}
}

func (p *MockPSP) Charge(ctx context.Context, key string, amountCents int64) (string, error) {
	select {
	case <-time.After(p.Latency):
	case <-ctx.Done():
		return "", ctx.Err()
	}
	if rand.Float64() < p.FailureRate {
		return "", ErrPSPUnavailable
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if status, ok := p.results[key]; ok {
		return status, nil
	}
	status := StatusSucceeded
	if amountCents > p.DeclineAbove {
		status = StatusDeclined
	}
	p.results[key] = status
	return status, nil
}
