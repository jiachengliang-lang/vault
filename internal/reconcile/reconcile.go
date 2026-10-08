// Package reconcile finishes checkouts that were cut off part way, the way a client retrying with
// the same Idempotency-Key would.
//
// A checkout is three steps: create the order, charge it, mark the order PAID or FAILED. A crash
// or timeout between them leaves the order PENDING. Clients are told to retry with the same key,
// which finishes the job, but one that never retries leaves it PENDING forever, possibly with the
// customer already charged. A chaos test on AWS left 11 such checkouts out of 39,411.
//
// The reconciler goes through the order and payment services rather than writing to the database
// itself, so the same rules apply (no double charge, valid transitions only) and the order
// service still emits order.paid / order.failed for analytics.
package reconcile

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/cloudwego/kitex/client/callopt"
	"github.com/jackc/pgx/v5/pgxpool"

	orderapi "vault/kitex_gen/order"
	paymentapi "vault/kitex_gen/payment"
)

// The parts of the order and payment clients the reconciler uses.
type orders interface {
	UpdateStatus(ctx context.Context, req *orderapi.UpdateStatusRequest, opts ...callopt.Option) (*orderapi.UpdateStatusResponse, error)
}

type payments interface {
	Charge(ctx context.Context, req *paymentapi.ChargeRequest, opts ...callopt.Option) (*paymentapi.ChargeResponse, error)
}

type Reconciler struct {
	db       *pgxpool.Pool
	orders   orders
	payments payments
	// MinAge leaves younger orders alone: their checkout may still be running.
	MinAge time.Duration
	// AbandonAfter is how long an order with no payment at all waits before it's marked FAILED.
	// Until then the client may still retry; after that nothing was charged and it gave up.
	AbandonAfter time.Duration
	// BatchSize caps how many orders one run looks at.
	BatchSize int
}

func New(db *pgxpool.Pool, o orders, p payments) *Reconciler {
	return &Reconciler{db: db, orders: o, payments: p, MinAge: 2 * time.Minute, AbandonAfter: time.Hour, BatchSize: 500}
}

// Result counts what one run did with the stuck orders it found.
type Result struct {
	Found     int // PENDING orders older than MinAge
	Paid      int // marked PAID: the charge had succeeded, or succeeded when retried
	Failed    int // marked FAILED: the charge had been declined
	Abandoned int // marked FAILED: never charged, older than AbandonAfter
	Waiting   int // left for now: never charged but not abandoned yet, or a charge in progress
	Errors    int // couldn't be finished this run; the next run tries again
}

type stuck struct {
	orderID   string
	amount    int64
	payment   *string // payment status, nil if the order was never charged
	abandoned bool
}

// Run finds stuck orders and finishes what it can. Each order is handled independently: one that
// fails is counted in Errors and retried on the next run.
func (r *Reconciler) Run(ctx context.Context) (Result, error) {
	// The database decides ages with its own clock (see payment.Service for why).
	rows, err := r.db.Query(ctx, `
		SELECT o.order_id, o.amount_cents, p.status, now() - o.created_at > $2 * interval '1 second'
		FROM orders o LEFT JOIN payments p ON p.order_id = o.order_id
		WHERE o.status = 'PENDING' AND o.created_at < now() - $1 * interval '1 second'
		ORDER BY o.created_at
		LIMIT $3`, r.MinAge.Seconds(), r.AbandonAfter.Seconds(), r.BatchSize)
	if err != nil {
		return Result{}, fmt.Errorf("find stuck orders: %w", err)
	}
	var found []stuck
	for rows.Next() {
		var s stuck
		if err := rows.Scan(&s.orderID, &s.amount, &s.payment, &s.abandoned); err != nil {
			return Result{}, err
		}
		found = append(found, s)
	}
	if err := rows.Err(); err != nil {
		return Result{}, err
	}

	res := Result{Found: len(found)}
	for _, s := range found {
		outcome, err := r.finish(ctx, s)
		if err != nil {
			res.Errors++
			slog.ErrorContext(ctx, "reconcile order failed", "order_id", s.orderID, "err", err)
			continue
		}
		switch outcome {
		case outcomePaid:
			res.Paid++
		case outcomeFailed:
			res.Failed++
		case outcomeAbandoned:
			res.Abandoned++
		case outcomeWaiting:
			res.Waiting++
		}
		if outcome != outcomeWaiting {
			slog.InfoContext(ctx, "reconciled order", "order_id", s.orderID, "payment", paymentStatus(s), "outcome", outcome)
		}
	}
	return res, nil
}

const (
	outcomePaid      = "paid"
	outcomeFailed    = "failed"
	outcomeAbandoned = "abandoned"
	outcomeWaiting   = "waiting"
)

func (r *Reconciler) finish(ctx context.Context, s stuck) (string, error) {
	switch paymentStatus(s) {
	case "SUCCEEDED":
		// Charged, but the order was never marked: the crash came between the last two steps.
		return outcomePaid, r.mark(ctx, s.orderID, "PAID")
	case "DECLINED":
		return outcomeFailed, r.mark(ctx, s.orderID, "FAILED")
	case "PENDING":
		// Cut off mid-charge: we don't know whether the provider charged. Charging again with the
		// same key is what a client retry does; the payment service takes over the reservation
		// and the provider, which is idempotent, returns the original result.
		resp, err := r.payments.Charge(ctx, &paymentapi.ChargeRequest{
			OrderId: s.orderID, AmountCents: s.amount, IdempotencyKey: s.orderID,
		})
		var perr *paymentapi.PaymentError
		if errors.As(err, &perr) && perr.Code == 409 {
			return outcomeWaiting, nil // someone else is mid-charge right now
		}
		if err != nil {
			return "", fmt.Errorf("charge: %w", err)
		}
		if resp.Payment.Status == "SUCCEEDED" {
			return outcomePaid, r.mark(ctx, s.orderID, "PAID")
		}
		return outcomeFailed, r.mark(ctx, s.orderID, "FAILED")
	default: // never charged
		if !s.abandoned {
			return outcomeWaiting, nil
		}
		return outcomeAbandoned, r.mark(ctx, s.orderID, "FAILED")
	}
}

func (r *Reconciler) mark(ctx context.Context, orderID, status string) error {
	_, err := r.orders.UpdateStatus(ctx, &orderapi.UpdateStatusRequest{OrderId: orderID, Status: status})
	if err != nil {
		return fmt.Errorf("mark %s: %w", status, err)
	}
	return nil
}

func paymentStatus(s stuck) string {
	if s.payment == nil {
		return "none"
	}
	return *s.payment
}
