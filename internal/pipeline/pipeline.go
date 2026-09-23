// Package pipeline consumes order events and writes de-identified copies to the analytics store.
//
// Privacy rule: analytics only ever sees an allowlist of fields, and user IDs are replaced
// with a keyed token. A new field added to the event later is dropped unless someone
// deliberately adds it here (allowlist, not blocklist).
package pipeline

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

var (
	recordsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pipeline_records_total",
		Help: "Records handled, by outcome: stored or dead_lettered.",
	}, []string{"outcome"})
	retriesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pipeline_retries_total",
		Help: "Retries of transient failures, by operation. A steady rate means a dependency is unhealthy.",
	}, []string{"op"})
	// From the business event (order paid) to the row existing in analytics: how fresh analytics is.
	endToEnd = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "pipeline_event_end_to_end_seconds",
		Help:    "Time from the event occurring to it landing in analytics.",
		Buckets: []float64{.05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600, 1800},
	})
)

func init() {
	// Start at 0 so the first dead letter shows up as an increase (see audit.go).
	recordsTotal.WithLabelValues("stored")
	recordsTotal.WithLabelValues("dead_lettered")
	retriesTotal.WithLabelValues("analytics insert")
	retriesTotal.WithLabelValues("dlq send")
}

// ErrPoison marks a message that can never be processed (bad JSON, missing fields).
// Retrying won't help, so it goes to the dead-letter queue.
var ErrPoison = errors.New("poison message")

var knownTypes = map[string]bool{"order.created": true, "order.paid": true, "order.failed": true}

// Row is one analytics record. It holds no raw user identifiers.
type Row struct {
	EventID   uuid.UUID
	UserToken string
	EventType string
	Amount    int64
	TS        time.Time
}

// Tokenizer maps a user ID to a stable pseudonym: the same user always gets the same token,
// so analysts can still count unique buyers, but can't recover who the user is.
// It is a keyed hash (HMAC), not a plain SHA-256: with a plain hash, anyone can hash
// a list of known IDs or emails and match them. Without the key, they can't.
type Tokenizer struct {
	key []byte
}

func NewTokenizer(key []byte) Tokenizer {
	return Tokenizer{key: key}
}

func (t Tokenizer) Token(userID string) string {
	mac := hmac.New(sha256.New, t.key)
	mac.Write([]byte(userID))
	return hex.EncodeToString(mac.Sum(nil))
}

// event lists the only fields the pipeline reads from an order event.
type event struct {
	EventID    uuid.UUID `json:"event_id"`
	Type       string    `json:"type"`
	UserID     uuid.UUID `json:"user_id"`
	Amount     int64     `json:"amount_cents"`
	OccurredAt time.Time `json:"occurred_at"`
}

// Transform validates an event and turns it into a de-identified Row.
func Transform(value []byte, tok Tokenizer) (Row, error) {
	var e event
	if err := json.Unmarshal(value, &e); err != nil {
		return Row{}, fmt.Errorf("%w: decode: %v", ErrPoison, err)
	}
	switch {
	case e.EventID == uuid.Nil:
		return Row{}, fmt.Errorf("%w: missing event_id", ErrPoison)
	case !knownTypes[e.Type]:
		return Row{}, fmt.Errorf("%w: unknown type %q", ErrPoison, e.Type)
	case e.UserID == uuid.Nil:
		return Row{}, fmt.Errorf("%w: missing user_id", ErrPoison)
	case e.OccurredAt.IsZero():
		return Row{}, fmt.Errorf("%w: missing occurred_at", ErrPoison)
	}
	return Row{
		EventID:   e.EventID,
		UserToken: tok.Token(e.UserID.String()),
		EventType: e.Type,
		Amount:    e.Amount,
		TS:        e.OccurredAt,
	}, nil
}

type Sink interface {
	// InsertBatch stores rows in one statement and one commit. It must be idempotent:
	// a redelivered batch (or a duplicate within one) stores each event once.
	InsertBatch(ctx context.Context, rows []Row) error
}

type DLQ interface {
	Send(ctx context.Context, rec *kgo.Record, reason error) error
}

// Processor handles a batch of records per poll. The two failure kinds are treated differently:
//   - permanent (ErrPoison): send to the DLQ and move on, so one bad message can't block the partition.
//   - transient (DB down): retry with backoff until it works. Sending these to the DLQ would
//     dump every message during an outage; blocking instead applies backpressure, and Kafka
//     keeps the backlog until we recover.
type Processor struct {
	tok  Tokenizer
	sink Sink
	dlq  DLQ

	MinBackoff, MaxBackoff time.Duration
}

func NewProcessor(tok Tokenizer, sink Sink, dlq DLQ) *Processor {
	return &Processor{tok: tok, sink: sink, dlq: dlq, MinBackoff: 100 * time.Millisecond, MaxBackoff: 5 * time.Second}
}

// Handle processes a single record. See HandleBatch.
func (p *Processor) Handle(ctx context.Context, rec *kgo.Record) error {
	return p.HandleBatch(ctx, []*kgo.Record{rec})
}

// HandleBatch processes one poll's worth of records. It returns an error only if ctx is cancelled
// before every record is handled; the caller must then not commit offsets, so the batch is redelivered.
//
// Valid records are written in ONE insert and ONE commit. Postgres makes every commit wait for its
// write-ahead log to reach disk, so under load the commit rate, not query speed, is the limit;
// one commit per event made the pipeline a third of all commits and it fell minutes behind.
func (p *Processor) HandleBatch(ctx context.Context, recs []*kgo.Record) error {
	rows := make([]Row, 0, len(recs))
	links := make([]trace.Link, 0, len(recs))
	for _, rec := range recs {
		row, ok, err := p.prepare(ctx, rec)
		if err != nil {
			return err
		}
		if ok {
			rows = append(rows, row.Row)
			links = append(links, trace.Link{SpanContext: row.span})
		}
	}
	if len(rows) == 0 {
		return nil
	}

	// One span for the batch write, linked to every event's own span: many traces fan in here.
	ctx, span := otel.Tracer("pipeline").Start(ctx, "analytics batch insert",
		trace.WithLinks(links...), trace.WithAttributes(attribute.Int("batch.size", len(rows))))
	defer span.End()
	if err := p.retry(ctx, "analytics insert", func() error { return p.sink.InsertBatch(ctx, rows) }); err != nil {
		return err
	}
	recordsTotal.WithLabelValues("stored").Add(float64(len(rows)))
	for _, r := range rows {
		endToEnd.Observe(time.Since(r.TS).Seconds())
	}
	return nil
}

type preparedRow struct {
	Row
	span trace.SpanContext
}

// prepare validates one record inside a span that continues the trace of the request that produced
// it (see outbox.Write). Poison records go to the DLQ here; ok reports whether there's a row to store.
func (p *Processor) prepare(ctx context.Context, rec *kgo.Record) (preparedRow, bool, error) {
	ctx = otel.GetTextMapPropagator().Extract(ctx, headerCarrier(rec.Headers))
	ctx, span := otel.Tracer("pipeline").Start(ctx, "process "+rec.Topic,
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.Int("messaging.kafka.partition", int(rec.Partition)),
			attribute.Int64("messaging.kafka.offset", rec.Offset),
		))
	defer span.End()

	row, reason := Transform(rec.Value, p.tok)
	if reason != nil {
		slog.WarnContext(ctx, "sending to DLQ", "topic", rec.Topic, "partition", rec.Partition, "offset", rec.Offset, "err", reason)
		span.SetStatus(codes.Error, reason.Error())
		if err := p.retry(ctx, "dlq send", func() error { return p.dlq.Send(ctx, rec, reason) }); err != nil {
			return preparedRow{}, false, err
		}
		recordsTotal.WithLabelValues("dead_lettered").Inc()
		return preparedRow{}, false, nil
	}
	span.SetAttributes(attribute.String("event.type", row.EventType))
	return preparedRow{Row: row, span: span.SpanContext()}, true, nil
}

// headerCarrier lets OpenTelemetry read trace context from Kafka record headers.
type headerCarrier []kgo.RecordHeader

func (h headerCarrier) Get(key string) string {
	for _, kv := range h {
		if kv.Key == key {
			return string(kv.Value)
		}
	}
	return ""
}

func (h headerCarrier) Set(string, string) {}

func (h headerCarrier) Keys() []string {
	keys := make([]string, len(h))
	for i, kv := range h {
		keys[i] = kv.Key
	}
	return keys
}

// retry runs fn until it succeeds or ctx is cancelled, with exponential backoff and full jitter.
// Jitter spreads retries out so many consumers don't hit a recovering database at the same instant.
func (p *Processor) retry(ctx context.Context, op string, fn func() error) error {
	backoff := p.MinBackoff
	for attempt := 1; ; attempt++ {
		err := fn()
		if err == nil {
			return nil
		}
		slog.WarnContext(ctx, "retrying", "op", op, "attempt", attempt, "err", err)
		retriesTotal.WithLabelValues(op).Inc()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(rand.Int64N(int64(backoff) + 1))):
		}
		backoff = min(backoff*2, p.MaxBackoff)
	}
}

// PostgresSink writes to analytics_events. The event_id primary key plus ON CONFLICT DO NOTHING
// makes redelivered events harmless: at-least-once delivery, but each event is stored once.
type PostgresSink struct {
	db *pgxpool.Pool
}

func NewPostgresSink(db *pgxpool.Pool) *PostgresSink {
	return &PostgresSink{db: db}
}

// InsertBatch sends the rows as five arrays and lets Postgres unnest them into rows: one statement,
// one round trip and one commit however many rows there are.
func (s *PostgresSink) InsertBatch(ctx context.Context, rows []Row) error {
	ids := make([]uuid.UUID, len(rows))
	tokens := make([]string, len(rows))
	types := make([]string, len(rows))
	amounts := make([]int64, len(rows))
	times := make([]time.Time, len(rows))
	for i, r := range rows {
		ids[i], tokens[i], types[i], amounts[i], times[i] = r.EventID, r.UserToken, r.EventType, r.Amount, r.TS
	}
	_, err := s.db.Exec(ctx, `
		INSERT INTO analytics_events (event_id, user_token, event_type, amount_cents, ts)
		SELECT * FROM unnest($1::uuid[], $2::text[], $3::text[], $4::bigint[], $5::timestamptz[])
		ON CONFLICT (event_id) DO NOTHING`,
		ids, tokens, types, amounts, times)
	return err
}

// KafkaDLQ republishes a poison record to the dead-letter topic with the reason and its
// original position, so someone can inspect it, fix the bug, and replay it.
type KafkaDLQ struct {
	client *kgo.Client
	topic  string
}

func NewKafkaDLQ(client *kgo.Client, topic string) *KafkaDLQ {
	return &KafkaDLQ{client: client, topic: topic}
}

func (d *KafkaDLQ) Send(ctx context.Context, rec *kgo.Record, reason error) error {
	return d.client.ProduceSync(ctx, &kgo.Record{
		Topic: d.topic,
		Key:   rec.Key,
		Value: rec.Value,
		Headers: []kgo.RecordHeader{
			{Key: "dlq.reason", Value: []byte(reason.Error())},
			{Key: "dlq.source", Value: []byte(fmt.Sprintf("%s/%d/%d", rec.Topic, rec.Partition, rec.Offset))},
		},
	}).FirstErr()
}

// Consume polls, processes and commits until ctx is cancelled.
//
// Offsets are committed only after every record in the batch is stored. A crash mid-batch
// means those records are redelivered and deduplicated by the sink, never skipped.
// The client must be created with DisableAutoCommit and BlockRebalanceOnPoll, so partitions
// can't be reassigned between processing a batch and committing it.
func Consume(ctx context.Context, cl *kgo.Client, p *Processor) {
	// With BlockRebalanceOnPoll, every poll must be followed by AllowRebalance, including the one
	// interrupted by shutdown. Without this, cl.Close() waits for it forever and the process
	// never exits (found when stopped pipelines kept piling up).
	defer cl.AllowRebalance()
	for {
		fetches := cl.PollRecords(ctx, 500)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			return
		}
		fetches.EachError(func(topic string, partition int32, err error) {
			slog.Error("fetch error", "topic", topic, "partition", partition, "err", err)
		})

		var recs []*kgo.Record
		fetches.EachRecord(func(rec *kgo.Record) { recs = append(recs, rec) })
		if err := p.HandleBatch(ctx, recs); err != nil {
			return // shutting down mid-batch: don't commit
		}
		if err := cl.CommitUncommittedOffsets(ctx); err != nil {
			slog.Error("commit offsets", "err", err)
		}
		cl.AllowRebalance()
	}
}
