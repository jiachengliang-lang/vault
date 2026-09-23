package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"

	"vault/internal/platform"
)

var tok = NewTokenizer([]byte("test-key"))

func validEvent(t *testing.T) (map[string]any, []byte) {
	t.Helper()
	e := map[string]any{
		"event_id":     uuid.NewString(),
		"type":         "order.paid",
		"order_id":     uuid.NewString(),
		"user_id":      uuid.NewString(),
		"amount_cents": 1999,
		"status":       "PAID",
		"occurred_at":  time.Now().UTC().Format(time.RFC3339Nano),
	}
	b, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	return e, b
}

func TestTransformDeidentifies(t *testing.T) {
	e, b := validEvent(t)
	row, err := Transform(b, tok)
	if err != nil {
		t.Fatal(err)
	}
	userID := e["user_id"].(string)
	if strings.Contains(row.UserToken, userID) {
		t.Fatal("raw user_id leaked into the analytics row")
	}
	if row.UserToken != tok.Token(userID) {
		t.Error("token must be stable for the same user, so analysts can count unique buyers")
	}
	if NewTokenizer([]byte("other-key")).Token(userID) == row.UserToken {
		t.Error("a different key must give a different token")
	}
}

func TestTransformRejectsPoison(t *testing.T) {
	cases := map[string]func(e map[string]any){
		"missing event_id": func(e map[string]any) { delete(e, "event_id") },
		"unknown type":     func(e map[string]any) { e["type"] = "order.teleported" },
		"bad user_id":      func(e map[string]any) { e["user_id"] = "not-a-uuid" },
		"missing time":     func(e map[string]any) { delete(e, "occurred_at") },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			e, _ := validEvent(t)
			mutate(e)
			b, _ := json.Marshal(e)
			if _, err := Transform(b, tok); !errors.Is(err, ErrPoison) {
				t.Fatalf("got %v, want ErrPoison", err)
			}
		})
	}
	if _, err := Transform([]byte("{not json"), tok); !errors.Is(err, ErrPoison) {
		t.Fatalf("bad JSON: got %v, want ErrPoison", err)
	}
}

type fakeSink struct {
	mu       sync.Mutex
	failNext int
	rows     []Row
}

func (s *fakeSink) Insert(_ context.Context, r Row) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failNext > 0 {
		s.failNext--
		return errors.New("connection refused")
	}
	s.rows = append(s.rows, r)
	return nil
}

type fakeDLQ struct{ sent []error }

func (d *fakeDLQ) Send(_ context.Context, _ *kgo.Record, reason error) error {
	d.sent = append(d.sent, reason)
	return nil
}

func newTestProcessor(sink Sink, dlq DLQ) *Processor {
	p := NewProcessor(tok, sink, dlq)
	p.MinBackoff, p.MaxBackoff = time.Millisecond, 5*time.Millisecond
	return p
}

func TestTransientErrorsAreRetriedNotDeadLettered(t *testing.T) {
	sink, dlq := &fakeSink{failNext: 3}, &fakeDLQ{}
	_, b := validEvent(t)
	if err := newTestProcessor(sink, dlq).Handle(context.Background(), &kgo.Record{Value: b}); err != nil {
		t.Fatal(err)
	}
	if len(sink.rows) != 1 || len(dlq.sent) != 0 {
		t.Fatalf("stored %d rows, dead-lettered %d; want 1 stored after retries, 0 dead-lettered", len(sink.rows), len(dlq.sent))
	}
}

func TestPoisonGoesToDLQ(t *testing.T) {
	sink, dlq := &fakeSink{}, &fakeDLQ{}
	if err := newTestProcessor(sink, dlq).Handle(context.Background(), &kgo.Record{Value: []byte("garbage")}); err != nil {
		t.Fatal(err)
	}
	if len(sink.rows) != 0 || len(dlq.sent) != 1 {
		t.Fatalf("stored %d rows, dead-lettered %d; want 0 stored, 1 dead-lettered", len(sink.rows), len(dlq.sent))
	}
}

// On shutdown during an outage, Handle gives up so the offset isn't committed.
func TestHandleStopsOnShutdown(t *testing.T) {
	sink := &fakeSink{failNext: 1 << 30}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, b := validEvent(t)
	if err := newTestProcessor(sink, &fakeDLQ{}).Handle(ctx, &kgo.Record{Value: b}); err == nil {
		t.Fatal("want an error so the caller doesn't commit the offset")
	}
}

// Redelivered events (at-least-once) must not create duplicate analytics rows.
func TestPostgresSinkIsIdempotent(t *testing.T) {
	pool := platform.TestPool(t)
	sink := NewPostgresSink(pool)
	ctx := context.Background()
	_, b := validEvent(t)
	row, err := Transform(b, tok)
	if err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if err := sink.Insert(ctx, row); err != nil {
			t.Fatal(err)
		}
	}
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM analytics_events WHERE event_id = $1`, row.EventID).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("got %d rows, want 1", n)
	}
}
