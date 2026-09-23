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
	"github.com/twmb/franz-go/pkg/kgo"
)

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
	Insert(ctx context.Context, r Row) error
}

type DLQ interface {
	Send(ctx context.Context, rec *kgo.Record, reason error) error
}

// Processor handles one record at a time. The two failure kinds are treated differently:
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

// Handle returns an error only if ctx is cancelled before the record is fully handled.
// The caller must then not commit the offset, so the record is redelivered.
func (p *Processor) Handle(ctx context.Context, rec *kgo.Record) error {
	row, err := Transform(rec.Value, p.tok)
	if err != nil {
		slog.Warn("sending to DLQ", "topic", rec.Topic, "partition", rec.Partition, "offset", rec.Offset, "err", err)
		return p.retry(ctx, "dlq send", func() error { return p.dlq.Send(ctx, rec, err) })
	}
	return p.retry(ctx, "analytics insert", func() error { return p.sink.Insert(ctx, row) })
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
		slog.Warn("retrying", "op", op, "attempt", attempt, "err", err)
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

func (s *PostgresSink) Insert(ctx context.Context, r Row) error {
	_, err := s.db.Exec(ctx, `
		INSERT INTO analytics_events (event_id, user_token, event_type, amount_cents, ts)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (event_id) DO NOTHING`,
		r.EventID, r.UserToken, r.EventType, r.Amount, r.TS)
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
	for {
		fetches := cl.PollRecords(ctx, 500)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			return
		}
		fetches.EachError(func(topic string, partition int32, err error) {
			slog.Error("fetch error", "topic", topic, "partition", partition, "err", err)
		})

		var stopped bool
		fetches.EachRecord(func(rec *kgo.Record) {
			if !stopped && p.Handle(ctx, rec) != nil {
				stopped = true
			}
		})
		if stopped {
			return // shutting down mid-batch: don't commit
		}
		if err := cl.CommitUncommittedOffsets(ctx); err != nil {
			slog.Error("commit offsets", "err", err)
		}
		cl.AllowRebalance()
	}
}
