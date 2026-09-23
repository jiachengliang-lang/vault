// Package outbox implements the transactional outbox: services write events into the outbox table
// in the same transaction as their data change, and the Relay publishes them to Kafka.
package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

var (
	publishedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "outbox_published_total",
		Help: "Events published from the outbox to Kafka.",
	})
	backlog = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "outbox_backlog",
		Help: "Events written but not yet published. Should hover near 0; growth means Kafka or the relay is stuck.",
	})
	oldestAge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "outbox_oldest_unpublished_age_seconds",
		Help: "Age of the oldest unpublished event: how stale downstream consumers' view is.",
	})
)

// relayLockID is an arbitrary constant shared by every relay instance, in every service.
const relayLockID = 7_700_001

// Write adds an event to the outbox inside the caller's transaction.
// key picks the Kafka partition: events with the same key stay in order.
//
// It also saves the current trace context. The relay publishes later, from a background loop
// with no link to this request; the saved context travels as Kafka headers, so the consumer's
// span joins the original request's trace.
func Write(ctx context.Context, tx pgx.Tx, topic, key string, event any) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	headers := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, headers)
	_, err = tx.Exec(ctx, `INSERT INTO outbox (topic, key, payload, headers) VALUES ($1, $2, $3, $4)`,
		topic, key, payload, map[string]string(headers))
	if err != nil {
		return fmt.Errorf("write outbox event: %w", err)
	}
	return nil
}

// Relay publishes outbox rows to Kafka, oldest first, and marks them published.
//
// Delivery is at-least-once: if we crash after Kafka acks but before the UPDATE commits,
// the same rows are published again on restart. Consumers dedupe on event_id.
//
// Only one relay publishes at a time, across all instances and services (a Postgres advisory lock).
// Two relays working in parallel could publish a user's order.paid before their
// order.created; a single publisher keeps each user's events in order.
type Relay struct {
	db        *pgxpool.Pool
	kafka     *kgo.Client
	BatchSize int
	Interval  time.Duration
	Retention time.Duration // published rows are deleted after this long
}

func NewRelay(db *pgxpool.Pool, kafka *kgo.Client) *Relay {
	return &Relay{db: db, kafka: kafka, BatchSize: 500, Interval: 200 * time.Millisecond, Retention: time.Hour}
}

// Run publishes until ctx is cancelled. A full batch means there's a backlog, so it
// loops immediately; otherwise it sleeps for Interval.
func (r *Relay) Run(ctx context.Context) {
	lastCleanup, lastStats := time.Now(), time.Time{}
	for {
		n, err := r.PublishBatch(ctx)
		if err != nil && ctx.Err() == nil {
			slog.Warn("outbox relay: publish failed, will retry", "err", err)
		}
		if time.Since(lastStats) > 5*time.Second {
			r.recordStats(ctx)
			lastStats = time.Now()
		}
		if time.Since(lastCleanup) > time.Minute {
			r.cleanup(ctx)
			lastCleanup = time.Now()
		}
		if err == nil && n == r.BatchSize {
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(r.Interval):
		}
	}
}

// PublishBatch publishes up to BatchSize unpublished rows and returns how many it published.
// It returns 0 without error if another instance holds the relay lock.
func (r *Relay) PublishBatch(ctx context.Context) (int, error) {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)

	// Transaction-scoped: released automatically on commit, rollback, or if we crash.
	var locked bool
	if err := tx.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock($1)`, relayLockID).Scan(&locked); err != nil {
		return 0, err
	}
	if !locked {
		return 0, nil
	}

	rows, err := tx.Query(ctx, `
		SELECT id, topic, key, payload, headers FROM outbox
		WHERE published_at IS NULL ORDER BY id LIMIT $1`, r.BatchSize)
	if err != nil {
		return 0, err
	}
	var ids []int64
	var records []*kgo.Record
	for rows.Next() {
		var id int64
		var topic, key string
		var payload []byte
		var headers map[string]string
		if err := rows.Scan(&id, &topic, &key, &payload, &headers); err != nil {
			return 0, err
		}
		rec := &kgo.Record{Topic: topic, Key: []byte(key), Value: payload}
		for k, v := range headers {
			rec.Headers = append(rec.Headers, kgo.RecordHeader{Key: k, Value: []byte(v)})
		}
		ids = append(ids, id)
		records = append(records, rec)
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if len(records) == 0 {
		return 0, nil
	}

	// ProduceSync waits for broker acks. The producer is idempotent by default, so records
	// sharing a key (user_id) keep their order within the partition, even across retries.
	if err := r.kafka.ProduceSync(ctx, records...).FirstErr(); err != nil {
		return 0, fmt.Errorf("produce: %w", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE outbox SET published_at = now() WHERE id = ANY($1)`, ids); err != nil {
		return 0, fmt.Errorf("mark published: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	publishedTotal.Add(float64(len(records)))
	return len(records), nil
}

func (r *Relay) recordStats(ctx context.Context) {
	var n int64
	var age float64
	err := r.db.QueryRow(ctx, `
		SELECT count(*), COALESCE(EXTRACT(EPOCH FROM now() - min(created_at)), 0)
		FROM outbox WHERE published_at IS NULL`).Scan(&n, &age)
	if err != nil {
		return
	}
	backlog.Set(float64(n))
	oldestAge.Set(age)
}

func (r *Relay) cleanup(ctx context.Context) {
	_, err := r.db.Exec(ctx, `DELETE FROM outbox WHERE published_at < now() - $1::interval`,
		fmt.Sprintf("%d seconds", int(r.Retention.Seconds())))
	if err != nil && ctx.Err() == nil {
		slog.Warn("outbox relay: cleanup failed", "err", err)
	}
}
