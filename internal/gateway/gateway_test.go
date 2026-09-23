package gateway

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cloudwego/hertz/pkg/common/config"
	"github.com/cloudwego/hertz/pkg/common/ut"
	"github.com/cloudwego/hertz/pkg/route"
	"github.com/cloudwego/kitex/client/callopt"
	"github.com/google/uuid"

	orderapi "vault/kitex_gen/order"
	paymentapi "vault/kitex_gen/payment"
	userapi "vault/kitex_gen/user"
)

var secret = []byte("test-secret")

// fakeOrders is an in-memory OrderService with the same idempotency semantics as the real one.
type fakeOrders struct {
	mu     sync.Mutex
	byKey  map[string]*orderapi.Order
	byID   map[string]*orderapi.Order
	failed bool // simulate the order service being down
}

func newFakeOrders() *fakeOrders {
	return &fakeOrders{byKey: map[string]*orderapi.Order{}, byID: map[string]*orderapi.Order{}}
}

func (f *fakeOrders) CreateOrder(_ context.Context, req *orderapi.CreateOrderRequest, _ ...callopt.Option) (*orderapi.CreateOrderResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failed {
		return nil, errors.New("connection refused")
	}
	k := req.UserId + "/" + req.IdempotencyKey
	if o, ok := f.byKey[k]; ok {
		return &orderapi.CreateOrderResponse{Order: clone(o), Replayed: true}, nil
	}
	o := &orderapi.Order{OrderId: uuid.NewString(), UserId: req.UserId, AmountCents: req.AmountCents, Status: "PENDING"}
	f.byKey[k], f.byID[o.OrderId] = o, o
	return &orderapi.CreateOrderResponse{Order: clone(o)}, nil
}

func (f *fakeOrders) GetOrder(_ context.Context, req *orderapi.GetOrderRequest, _ ...callopt.Option) (*orderapi.GetOrderResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	o, ok := f.byID[req.OrderId]
	if !ok {
		return nil, &orderapi.OrderError{Code: 404, Message: "order not found"}
	}
	return &orderapi.GetOrderResponse{Order: clone(o)}, nil
}

func (f *fakeOrders) UpdateStatus(_ context.Context, req *orderapi.UpdateStatusRequest, _ ...callopt.Option) (*orderapi.UpdateStatusResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	o := f.byID[req.OrderId]
	o.Status = req.Status
	return &orderapi.UpdateStatusResponse{Order: clone(o)}, nil
}

func clone(o *orderapi.Order) *orderapi.Order { c := *o; return &c }

// fakePayments succeeds up to $10,000, declines above, and can be made unavailable.
type fakePayments struct {
	mu      sync.Mutex
	charges map[string]*paymentapi.Payment
	calls   int
	down    bool
}

func (f *fakePayments) Charge(_ context.Context, req *paymentapi.ChargeRequest, _ ...callopt.Option) (*paymentapi.ChargeResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.down {
		return nil, errors.New("rpc timeout")
	}
	if p, ok := f.charges[req.IdempotencyKey]; ok {
		return &paymentapi.ChargeResponse{Payment: p, Replayed: true}, nil
	}
	f.calls++
	status := "SUCCEEDED"
	if req.AmountCents > 1_000_000 {
		status = "DECLINED"
	}
	p := &paymentapi.Payment{PaymentId: uuid.NewString(), OrderId: req.OrderId, AmountCents: req.AmountCents, Status: status}
	f.charges[req.IdempotencyKey] = p
	return &paymentapi.ChargeResponse{Payment: p}, nil
}

// fakeUsers records the accessor of each call so tests can check what gets audited.
type fakeUsers struct {
	mu        sync.Mutex
	profiles  map[string]*userapi.Profile
	accessors []*userapi.Accessor
}

func (f *fakeUsers) UpsertProfile(_ context.Context, req *userapi.UpsertProfileRequest, _ ...callopt.Option) (*userapi.UpsertProfileResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.accessors = append(f.accessors, req.Accessor)
	p := &userapi.Profile{UserId: req.UserId, Email: req.Email, Address: req.GetAddress()}
	f.profiles[req.UserId] = p
	return &userapi.UpsertProfileResponse{Profile: p}, nil
}

func (f *fakeUsers) GetProfile(_ context.Context, req *userapi.GetProfileRequest, _ ...callopt.Option) (*userapi.GetProfileResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.accessors = append(f.accessors, req.Accessor)
	p, ok := f.profiles[req.UserId]
	if !ok {
		return nil, &userapi.UserError{Code: 404, Message: "user not found"}
	}
	return &userapi.GetProfileResponse{Profile: p}, nil
}

func (f *fakeUsers) DeleteUser(_ context.Context, req *userapi.DeleteUserRequest, _ ...callopt.Option) (*userapi.DeleteUserResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.accessors = append(f.accessors, req.Accessor)
	if _, ok := f.profiles[req.UserId]; !ok {
		return nil, &userapi.UserError{Code: 404, Message: "user not found"}
	}
	delete(f.profiles, req.UserId)
	return &userapi.DeleteUserResponse{}, nil
}

type harness struct {
	engine   *route.Engine
	orders   *fakeOrders
	payments *fakePayments
	users    *fakeUsers
}

func newHarness(t *testing.T, limiter *RateLimiter) *harness {
	t.Helper()
	if limiter == nil {
		limiter = NewRateLimiter(1000, 1000)
	}
	h := &harness{
		orders:   newFakeOrders(),
		payments: &fakePayments{charges: map[string]*paymentapi.Payment{}},
		users:    &fakeUsers{profiles: map[string]*userapi.Profile{}},
	}
	h.engine = route.NewEngine(config.NewOptions(nil))
	New(h.orders, h.payments, h.users).Register(h.engine, secret, limiter)
	return h
}

func token(t *testing.T, user uuid.UUID) string {
	t.Helper()
	tok, err := NewToken(secret, user, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

func (h *harness) checkout(tok, key string, amount int64) *ut.ResponseRecorder {
	body := fmt.Sprintf(`{"amount_cents": %d}`, amount)
	headers := []ut.Header{{Key: "Content-Type", Value: "application/json"}}
	if tok != "" {
		headers = append(headers, ut.Header{Key: "Authorization", Value: "Bearer " + tok})
	}
	if key != "" {
		headers = append(headers, ut.Header{Key: "Idempotency-Key", Value: key})
	}
	return ut.PerformRequest(h.engine, "POST", "/v1/checkout",
		&ut.Body{Body: strings.NewReader(body), Len: len(body)}, headers...)
}

func TestCheckoutHappyPathAndReplay(t *testing.T) {
	h := newHarness(t, nil)
	tok := token(t, uuid.New())

	first := h.checkout(tok, "k1", 1999)
	if first.Code != 201 || !strings.Contains(first.Body.String(), `"status":"PAID"`) {
		t.Fatalf("first checkout: %d %s", first.Code, first.Body.String())
	}
	if first.Header().Get(headerRequestID) == "" {
		t.Error("response is missing X-Request-ID")
	}

	retry := h.checkout(tok, "k1", 1999)
	if retry.Code != 200 || retry.Header().Get("Idempotent-Replayed") != "true" {
		t.Fatalf("retry: %d, replayed header %q", retry.Code, retry.Header().Get("Idempotent-Replayed"))
	}
	if h.payments.calls != 1 {
		t.Errorf("payment charged %d times, want 1", h.payments.calls)
	}
}

func TestCheckoutDeclined(t *testing.T) {
	h := newHarness(t, nil)
	resp := h.checkout(token(t, uuid.New()), "k1", 2_000_000)
	if resp.Code != 402 || !strings.Contains(resp.Body.String(), `"status":"FAILED"`) {
		t.Fatalf("got %d %s, want 402 FAILED", resp.Code, resp.Body.String())
	}
}

// A payment outage returns 503; retrying with the same key after recovery completes
// the same order instead of creating a new one.
func TestCheckoutRecoversAfterPaymentOutage(t *testing.T) {
	h := newHarness(t, nil)
	tok := token(t, uuid.New())

	h.payments.down = true
	resp := h.checkout(tok, "k1", 1999)
	if resp.Code != 503 || resp.Header().Get("Retry-After") == "" {
		t.Fatalf("during outage: got %d, want 503 with Retry-After", resp.Code)
	}

	h.payments.down = false
	resp = h.checkout(tok, "k1", 1999)
	if resp.Code != 200 || !strings.Contains(resp.Body.String(), `"status":"PAID"`) {
		t.Fatalf("after recovery: %d %s", resp.Code, resp.Body.String())
	}
	if n := len(h.orders.byID); n != 1 {
		t.Errorf("%d orders created, want 1", n)
	}
}

func TestCheckoutRejectsBadRequests(t *testing.T) {
	h := newHarness(t, nil)
	tok := token(t, uuid.New())
	expired, _ := NewToken(secret, uuid.New(), -time.Minute)
	forged, _ := NewToken([]byte("wrong-secret"), uuid.New(), time.Hour)

	cases := []struct {
		name   string
		tok    string
		key    string
		amount int64
		want   int
	}{
		{"no token", "", "k", 100, 401},
		{"expired token", expired, "k", 100, 401},
		{"forged token", forged, "k", 100, 401},
		{"no idempotency key", tok, "", 100, 400},
		{"zero amount", tok, "k", 0, 400},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := h.checkout(tc.tok, tc.key, tc.amount).Code; got != tc.want {
				t.Errorf("got %d, want %d", got, tc.want)
			}
		})
	}
}

func TestGetOrderHidesOtherUsersOrders(t *testing.T) {
	h := newHarness(t, nil)
	owner, stranger := token(t, uuid.New()), token(t, uuid.New())
	h.checkout(owner, "k1", 1999)
	var id string
	for id = range h.orders.byID {
	}

	get := func(tok string) int {
		return ut.PerformRequest(h.engine, "GET", "/v1/orders/"+id, nil,
			ut.Header{Key: "Authorization", Value: "Bearer " + tok}).Code
	}
	if got := get(owner); got != 200 {
		t.Errorf("owner: got %d, want 200", got)
	}
	if got := get(stranger); got != 404 {
		t.Errorf("stranger: got %d, want 404", got)
	}
}

func TestRateLimitIsPerUser(t *testing.T) {
	h := newHarness(t, NewRateLimiter(0.001, 1)) // one request, then empty for a long time
	alice, bob := token(t, uuid.New()), token(t, uuid.New())

	if got := h.checkout(alice, "k1", 100).Code; got != 201 {
		t.Fatalf("alice first: got %d", got)
	}
	if got := h.checkout(alice, "k2", 100).Code; got != 429 {
		t.Fatalf("alice second: got %d, want 429", got)
	}
	if got := h.checkout(bob, "k1", 100).Code; got != 201 {
		t.Fatalf("bob should have his own bucket: got %d", got)
	}
}

func (h *harness) do(method, path, tok, body string) *ut.ResponseRecorder {
	headers := []ut.Header{{Key: "Authorization", Value: "Bearer " + tok}, {Key: "Content-Type", Value: "application/json"}}
	var b *ut.Body
	if body != "" {
		b = &ut.Body{Body: strings.NewReader(body), Len: len(body)}
	}
	return ut.PerformRequest(h.engine, method, path, b, headers...)
}

func TestProfileLifecycle(t *testing.T) {
	h := newHarness(t, nil)
	tok := token(t, uuid.New())

	if r := h.do("PUT", "/v1/me/profile", tok, `{"email":"a@example.com","address":"Berkeley"}`); r.Code != 200 {
		t.Fatalf("put: %d %s", r.Code, r.Body.String())
	}
	if r := h.do("GET", "/v1/me/profile", tok, ""); r.Code != 200 || !strings.Contains(r.Body.String(), "a@example.com") {
		t.Fatalf("get: %d %s", r.Code, r.Body.String())
	}
	if r := h.do("DELETE", "/v1/me", tok, ""); r.Code != 204 {
		t.Fatalf("delete: %d", r.Code)
	}
	if r := h.do("GET", "/v1/me/profile", tok, ""); r.Code != 404 {
		t.Fatalf("get after delete: %d, want 404", r.Code)
	}
}

func TestSupportAccessNeedsRoleAndReason(t *testing.T) {
	h := newHarness(t, nil)
	customer := uuid.New()
	h.do("PUT", "/v1/me/profile", token(t, customer), `{"email":"c@example.com"}`)
	path := "/v1/support/users/" + customer.String() + "/profile"

	staff, err := NewTokenWithRole(secret, uuid.New(), RoleSupport, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if r := h.do("GET", path+"?reason=ticket-4821-refund", token(t, uuid.New()), ""); r.Code != 403 {
		t.Errorf("customer token: got %d, want 403", r.Code)
	}
	if r := h.do("GET", path, staff, ""); r.Code != 400 {
		t.Errorf("staff without reason: got %d, want 400", r.Code)
	}
	if r := h.do("GET", path+"?reason=ticket-4821-refund", staff, ""); r.Code != 200 {
		t.Fatalf("staff with reason: got %d", r.Code)
	}
	last := h.users.accessors[len(h.users.accessors)-1]
	if !strings.HasPrefix(last.Actor, "support:") || last.Reason != "ticket-4821-refund" {
		t.Errorf("audited as %+v, want the staff identity and their reason", last)
	}
}
