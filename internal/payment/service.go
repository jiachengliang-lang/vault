package payment

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"vault/internal/platform"
)

var (
	pspCalls = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "psp_calls_total",
		Help: "Calls to the payment provider, by result: SUCCEEDED, DECLINED or error.",
	}, []string{"result"})
	pspDuration = platform.NewHistogram("psp_call_duration_seconds",
		"Payment provider latency. Usually the slowest step in checkout.")
)

func init() {
	for _, r := range []string{StatusSucceeded, StatusDeclined, "error"} {
		pspCalls.WithLabelValues(r) // start at 0 so the first provider error shows up
	}
}

var (
	ErrKeyReuse   = errors.New("idempotency key already used for a different order or amount")
	ErrInProgress = errors.New("a charge with this idempotency key is already in progress")
)

type Payment struct {
	ID        uuid.UUID
	OrderID   uuid.UUID
	Amount    int64
	Status    string
	CreatedAt time.Time
}

// Service charges orders so that no retry, crash or race ever charges an order twice.
//
// The flow is "reserve, then call out":
//  1. INSERT a PENDING row keyed by the idempotency key. Only one caller can win this insert.
//  2. The winner calls the PSP (outside any DB transaction, so a slow PSP never holds locks).
//  3. The winner records the result.
//
// Losers see the existing row: a finished payment is returned as-is (replay); a PENDING one
// means someone else is mid-charge, so they get ErrInProgress and should retry shortly.
// If the winner crashed between steps 1 and 3, the row stays PENDING. Once it is older than
// PendingLease, the next caller takes over and re-calls the PSP with the same key, which is
// safe because the PSP itself is idempotent.
type Service struct {
	db           *pgxpool.Pool
	psp          PSP
	PendingLease time.Duration
}

func NewService(db *pgxpool.Pool, psp PSP) *Service {
	return &Service{db: db, psp: psp, PendingLease: 10 * time.Second}
}

const paymentColumns = "payment_id, order_id, amount_cents, status, created_at"

func (s *Service) Charge(ctx context.Context, orderID uuid.UUID, amount int64, key string) (p Payment, replayed bool, err error) {
	p, err = scanPayment(s.db.QueryRow(ctx, `
		INSERT INTO payments (payment_id, order_id, idempotency_key, amount_cents, status)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (idempotency_key) DO NOTHING
		RETURNING `+paymentColumns,
		uuid.New(), orderID, key, amount, StatusPending))
	if err == nil {
		return s.callPSP(ctx, p, key)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return Payment{}, false, fmt.Errorf("reserve payment: %w", err)
	}

	// The key was used before.
	p, err = scanPayment(s.db.QueryRow(ctx,
		`SELECT `+paymentColumns+` FROM payments WHERE idempotency_key = $1`, key))
	if err != nil {
		return Payment{}, false, fmt.Errorf("load payment for replayed key: %w", err)
	}
	if p.OrderID != orderID || p.Amount != amount {
		return Payment{}, false, ErrKeyReuse
	}
	if p.Status != StatusPending {
		return p, true, nil
	}
	if time.Since(p.CreatedAt) < s.PendingLease {
		return Payment{}, false, ErrInProgress
	}
	p, _, err = s.callPSP(ctx, p, key)
	return p, true, err
}

func (s *Service) callPSP(ctx context.Context, p Payment, key string) (Payment, bool, error) {
	start := time.Now()
	status, err := s.psp.Charge(ctx, key, p.Amount)
	pspDuration.WithLabelValues().Observe(time.Since(start).Seconds())
	if err != nil {
		pspCalls.WithLabelValues("error").Inc()
		// Leave the row PENDING: we don't know whether the PSP charged. A retry after the
		// lease expires re-asks the PSP with the same key and gets the real answer.
		return Payment{}, false, err
	}
	pspCalls.WithLabelValues(status).Inc()
	// WHERE status = PENDING: if two takeovers race, both got the same answer from the
	// idempotent PSP, so whichever update lands second is simply a no-op.
	_, err = s.db.Exec(ctx,
		`UPDATE payments SET status = $2 WHERE payment_id = $1 AND status = $3`, p.ID, status, StatusPending)
	if err != nil {
		return Payment{}, false, fmt.Errorf("record payment result: %w", err)
	}
	p.Status = status
	return p, false, nil
}

func scanPayment(row pgx.Row) (Payment, error) {
	var p Payment
	err := row.Scan(&p.ID, &p.OrderID, &p.Amount, &p.Status, &p.CreatedAt)
	return p, err
}
