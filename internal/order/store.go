// Package order owns orders and their lifecycle: PENDING -> PAID | FAILED.
package order

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"vault/internal/outbox"
)

const (
	StatusPending = "PENDING"
	StatusPaid    = "PAID"
	StatusFailed  = "FAILED"

	TopicOrderEvents = "order.events"
)

var (
	ErrNotFound          = errors.New("order not found")
	ErrKeyReuse          = errors.New("idempotency key already used with a different amount")
	ErrInvalidTransition = errors.New("invalid status transition")
)

type Order struct {
	ID        uuid.UUID
	UserID    uuid.UUID
	Amount    int64
	Status    string
	CreatedAt time.Time
}

// Event is what lands in the outbox and, via the relay, in Kafka.
type Event struct {
	EventID    uuid.UUID `json:"event_id"`
	Type       string    `json:"type"` // order.created | order.paid | order.failed
	OrderID    uuid.UUID `json:"order_id"`
	UserID     uuid.UUID `json:"user_id"`
	Amount     int64     `json:"amount_cents"`
	Status     string    `json:"status"`
	OccurredAt time.Time `json:"occurred_at"`
}

type Store struct {
	db *pgxpool.Pool
}

func NewStore(db *pgxpool.Pool) *Store {
	return &Store{db: db}
}

const orderColumns = "order_id, user_id, amount_cents, status, created_at"

// Create inserts a new order, or returns the existing one if this (user, key) pair was seen before.
// The order row and its order.created event are written in one transaction (transactional outbox),
// so an order can never exist without its event, or the other way around.
func (s *Store) Create(ctx context.Context, userID uuid.UUID, amount int64, key string) (o Order, replayed bool, err error) {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return Order{}, false, err
	}
	defer tx.Rollback(ctx)

	// ON CONFLICT DO NOTHING makes concurrent retries safe: the second insert blocks until the
	// first transaction commits, then inserts nothing and falls through to the lookup below.
	o, err = scanOrder(tx.QueryRow(ctx, `
		INSERT INTO orders (order_id, user_id, idempotency_key, amount_cents)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (user_id, idempotency_key) DO NOTHING
		RETURNING `+orderColumns,
		uuid.New(), userID, key, amount))
	if errors.Is(err, pgx.ErrNoRows) {
		o, err = scanOrder(tx.QueryRow(ctx,
			`SELECT `+orderColumns+` FROM orders WHERE user_id = $1 AND idempotency_key = $2`, userID, key))
		if err != nil {
			return Order{}, false, fmt.Errorf("load order for replayed key: %w", err)
		}
		if o.Amount != amount {
			return Order{}, false, ErrKeyReuse
		}
		return o, true, nil
	}
	if err != nil {
		return Order{}, false, fmt.Errorf("insert order: %w", err)
	}

	if err := writeEvent(ctx, tx, "order.created", o); err != nil {
		return Order{}, false, err
	}
	return o, false, tx.Commit(ctx)
}

func (s *Store) Get(ctx context.Context, id uuid.UUID) (Order, error) {
	o, err := scanOrder(s.db.QueryRow(ctx, `SELECT `+orderColumns+` FROM orders WHERE order_id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return Order{}, ErrNotFound
	}
	return o, err
}

// UpdateStatus moves a PENDING order to PAID or FAILED. Setting the status it already has
// is a no-op (so gateway retries are safe); any other transition is rejected.
func (s *Store) UpdateStatus(ctx context.Context, id uuid.UUID, status string) (Order, error) {
	if status != StatusPaid && status != StatusFailed {
		return Order{}, ErrInvalidTransition
	}
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return Order{}, err
	}
	defer tx.Rollback(ctx)

	// FOR UPDATE locks the row so two concurrent updates can't both see PENDING.
	o, err := scanOrder(tx.QueryRow(ctx,
		`SELECT `+orderColumns+` FROM orders WHERE order_id = $1 FOR UPDATE`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return Order{}, ErrNotFound
	}
	if err != nil {
		return Order{}, err
	}
	if o.Status == status {
		return o, nil
	}
	if o.Status != StatusPending {
		return Order{}, ErrInvalidTransition
	}

	o, err = scanOrder(tx.QueryRow(ctx,
		`UPDATE orders SET status = $2 WHERE order_id = $1 RETURNING `+orderColumns, id, status))
	if err != nil {
		return Order{}, fmt.Errorf("update order: %w", err)
	}
	eventType := "order.paid"
	if status == StatusFailed {
		eventType = "order.failed"
	}
	if err := writeEvent(ctx, tx, eventType, o); err != nil {
		return Order{}, err
	}
	return o, tx.Commit(ctx)
}

func writeEvent(ctx context.Context, tx pgx.Tx, eventType string, o Order) error {
	// Keyed by user_id so all of one user's events land in the same partition, in order.
	return outbox.Write(ctx, tx, TopicOrderEvents, o.UserID.String(), Event{
		EventID:    uuid.New(),
		Type:       eventType,
		OrderID:    o.ID,
		UserID:     o.UserID,
		Amount:     o.Amount,
		Status:     o.Status,
		OccurredAt: time.Now().UTC(),
	})
}

func scanOrder(row pgx.Row) (Order, error) {
	var o Order
	err := row.Scan(&o.ID, &o.UserID, &o.Amount, &o.Status, &o.CreatedAt)
	return o, err
}
