package outbox

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"

	"vault/internal/platform"
)

func testKafka(t *testing.T, opts ...kgo.Opt) *kgo.Client {
	t.Helper()
	cl, err := kgo.NewClient(append([]kgo.Opt{kgo.SeedBrokers(platform.DefaultKafkaBrokers)}, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := cl.Ping(ctx); err != nil {
		cl.Close()
		platform.SkipUnlessRequired(t, "kafka unavailable, run `make up` first: %v", err)
	}
	t.Cleanup(cl.Close)
	return cl
}

const testTopic = "order.events"

type testEvent struct {
	EventID uuid.UUID `json:"event_id"`
	UserID  uuid.UUID `json:"user_id"`
}

// An outbox event must reach Kafka, keyed as written, and leave the outbox.
func TestRelayPublishesOutboxEvents(t *testing.T) {
	pool := platform.TestPool(t)
	producer := testKafka(t)
	relay := NewRelay(pool, producer)
	ctx := context.Background()

	start := time.Now().Add(-time.Second)
	ev := testEvent{EventID: uuid.New(), UserID: uuid.New()}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := Write(ctx, tx, testTopic, ev.UserID.String(), ev); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	// Another relay (e.g. a running order service) may hold the lock and publish it
	// instead, so keep going until the row has been published by someone.
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := relay.PublishBatch(ctx); err != nil {
			t.Fatal(err)
		}
		var remaining int
		err := pool.QueryRow(ctx, `SELECT count(*) FROM outbox WHERE payload->>'event_id' = $1`,
			ev.EventID.String()).Scan(&remaining)
		if err != nil {
			t.Fatal(err)
		}
		if remaining == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("outbox row was never published")
		}
	}

	// Start from records produced after this test began: the topic may hold millions of older ones.
	consumer := testKafka(t, kgo.ConsumeTopics(testTopic), kgo.ConsumeResetOffset(kgo.NewOffset().AfterMilli(start.UnixMilli())))
	pollCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	for {
		fetches := consumer.PollFetches(pollCtx)
		if pollCtx.Err() != nil {
			t.Fatal("event not found in Kafka")
		}
		var found *kgo.Record
		fetches.EachRecord(func(r *kgo.Record) {
			var e testEvent
			if json.Unmarshal(r.Value, &e) == nil && e.EventID == ev.EventID {
				found = r
			}
		})
		if found != nil {
			if string(found.Key) != ev.UserID.String() {
				t.Errorf("record key %q, want user_id %s (keeps each user's events in order)", found.Key, ev.UserID)
			}
			return
		}
	}
}
