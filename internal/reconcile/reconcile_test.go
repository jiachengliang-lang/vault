package reconcile

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/cloudwego/kitex/client/callopt"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"vault/internal/platform"
	orderapi "vault/kitex_gen/order"
	paymentapi "vault/kitex_gen/payment"
)

// fakeOrders records which status each order was set to, and can fail chosen orders.
type fakeOrders struct {
	mu     sync.Mutex
	marked map[string]string
	fail   map[string]bool
}

func (f *fakeOrders) UpdateStatus(_ context.Context, req *orderapi.UpdateStatusRequest, _ ...callopt.Option) (*orderapi.UpdateStatusResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail[req.OrderId] {
		return nil, errors.New("order service unavailable")
	}
	f.marked[req.OrderId] = req.Status
	return &orderapi.UpdateStatusResponse{Order: &orderapi.Order{OrderId: req.OrderId, Status: req.Status}}, nil
}

// fakePayments answers Charge per order: a status, or a PaymentError code.
type fakePayments struct {
	mu      sync.Mutex
	answers map[string]any // order ID -> "SUCCEEDED" | "DECLINED" | int32 error code
	keys    map[string]string
}

func (f *fakePayments) Charge(_ context.Context, req *paymentapi.ChargeRequest, _ ...callopt.Option) (*paymentapi.ChargeResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.keys[req.OrderId] = req.IdempotencyKey
	switch a := f.answers[req.OrderId].(type) {
	case string:
		return &paymentapi.ChargeResponse{Payment: &paymentapi.Payment{OrderId: req.OrderId, Status: a}}, nil
	case int32:
		return nil, &paymentapi.PaymentError{Code: a, Message: "test"}
	}
	return nil, errors.New("unexpected charge")
}

// order inserts a PENDING order created age ago, with a payment in paymentStatus ("" for none).
func order(t *testing.T, db *pgxpool.Pool, age time.Duration, paymentStatus string) string {
	t.Helper()
	ctx := context.Background()
	id := uuid.New()
	if _, err := db.Exec(ctx, `
		INSERT INTO orders (order_id, user_id, idempotency_key, amount_cents, status, created_at)
		VALUES ($1, $2, $3, 1999, 'PENDING', now() - $4 * interval '1 second')`,
		id, uuid.New(), "reconcile-test-"+id.String(), age.Seconds()); err != nil {
		t.Fatal(err)
	}
	if paymentStatus != "" {
		if _, err := db.Exec(ctx, `
			INSERT INTO payments (payment_id, order_id, idempotency_key, amount_cents, status, created_at)
			VALUES ($1, $2, $3, 1999, $4, now() - $5 * interval '1 second')`,
			uuid.New(), id, id.String(), paymentStatus, age.Seconds()); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		db.Exec(context.Background(), `DELETE FROM payments WHERE order_id = $1`, id)
		db.Exec(context.Background(), `DELETE FROM orders WHERE order_id = $1`, id)
	})
	return id.String()
}

func TestReconcileFinishesStuckCheckouts(t *testing.T) {
	db := platform.TestPool(t)
	old := 10 * time.Minute

	charged := order(t, db, old, "SUCCEEDED")          // crashed between charging and marking the order
	declined := order(t, db, old, "DECLINED")          // same, but the charge was declined
	midCharge := order(t, db, old, "PENDING")          // crashed mid-charge; retrying finds it went through
	inProgress := order(t, db, old, "PENDING")         // someone else is mid-charge right now
	neverCharged := order(t, db, old, "")              // client may still retry: leave it
	abandoned := order(t, db, 2*time.Hour, "")         // nothing charged, client long gone
	young := order(t, db, 30*time.Second, "SUCCEEDED") // checkout may still be running
	broken := order(t, db, old, "SUCCEEDED")           // the order service fails for this one

	o := &fakeOrders{marked: map[string]string{}, fail: map[string]bool{broken: true}}
	p := &fakePayments{
		answers: map[string]any{midCharge: "SUCCEEDED", inProgress: int32(409)},
		keys:    map[string]string{},
	}
	r := New(db, o, p)
	r.BatchSize = 100_000 // other tests' leftovers in the dev database mustn't crowd these out

	if _, err := r.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	want := map[string]string{
		charged:      "PAID",
		declined:     "FAILED",
		midCharge:    "PAID",
		inProgress:   "",
		neverCharged: "",
		abandoned:    "FAILED",
		young:        "",
		broken:       "",
	}
	names := map[string]string{charged: "charged", declined: "declined", midCharge: "mid-charge", inProgress: "in progress",
		neverCharged: "never charged", abandoned: "abandoned", young: "young", broken: "broken"}
	for id, status := range want {
		if got := o.marked[id]; got != status {
			t.Errorf("%s: marked %q, want %q", names[id], got, status)
		}
	}
	// Retrying a charge must reuse the original key, or the provider would charge again.
	for _, id := range []string{midCharge, inProgress} {
		if p.keys[id] != id {
			t.Errorf("%s: charged with key %q, want the order ID", names[id], p.keys[id])
		}
	}
	for _, id := range []string{charged, declined, neverCharged, abandoned, young} {
		if _, called := p.keys[id]; called {
			t.Errorf("%s: shouldn't have called the payment service", names[id])
		}
	}
}
