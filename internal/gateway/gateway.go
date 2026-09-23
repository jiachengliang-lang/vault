// Package gateway is the public HTTP edge: it authenticates, rate-limits,
// orchestrates checkout across the order and payment services, and fronts the user service.
package gateway

import (
	"context"
	"errors"
	"log/slog"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/route"
	"github.com/cloudwego/kitex/pkg/kerrors"

	orderapi "vault/kitex_gen/order"
	"vault/kitex_gen/order/orderservice"
	paymentapi "vault/kitex_gen/payment"
	"vault/kitex_gen/payment/paymentservice"
	userapi "vault/kitex_gen/user"
	"vault/kitex_gen/user/userservice"
)

const maxKeyLen = 128

type Gateway struct {
	orders   orderservice.Client
	payments paymentservice.Client
	users    userservice.Client
}

func New(orders orderservice.Client, payments paymentservice.Client, users userservice.Client) *Gateway {
	return &Gateway{orders: orders, payments: payments, users: users}
}

// Register mounts the API. Health checks stay outside auth so load balancers can reach them.
func (g *Gateway) Register(r route.IRouter, secret []byte, limiter *RateLimiter) {
	r.Use(Metrics(), RequestID())
	r.GET("/healthz", func(ctx context.Context, c *app.RequestContext) { c.String(200, "ok") })

	v1 := r.Group("/v1", Auth(secret), limiter.Middleware())
	v1.POST("/checkout", g.Checkout)
	v1.GET("/orders/:id", g.GetOrder)
	v1.PUT("/me/profile", g.PutProfile)
	v1.GET("/me/profile", g.GetProfile)
	v1.DELETE("/me", g.DeleteMe)
	v1.GET("/support/users/:id/profile", RequireRole(RoleSupport), g.SupportGetProfile)
}

type checkoutRequest struct {
	AmountCents int64 `json:"amount_cents"`
}

type orderView struct {
	OrderID     string `json:"order_id"`
	Status      string `json:"status"`
	AmountCents int64  `json:"amount_cents"`
	CreatedAt   string `json:"created_at"`
}

// Checkout creates an order, charges it, and records the outcome.
//
// Every step is idempotent, so a client that times out can simply retry with the same
// Idempotency-Key: the order step returns the same order, the charge step returns the same
// payment (the key for the charge is the order ID, so an order is charged at most once),
// and the status update is a no-op if already applied.
func (g *Gateway) Checkout(ctx context.Context, c *app.RequestContext) {
	userID := c.GetString(ctxUserID)
	key := string(c.GetHeader("Idempotency-Key"))
	if key == "" || len(key) > maxKeyLen {
		abort(c, 400, errMissingKey.Error())
		return
	}
	var req checkoutRequest
	if err := c.BindJSON(&req); err != nil || req.AmountCents <= 0 {
		abort(c, 400, "body must be {\"amount_cents\": <positive integer>}")
		return
	}

	created, err := g.orders.CreateOrder(ctx, &orderapi.CreateOrderRequest{
		UserId: userID, AmountCents: req.AmountCents, IdempotencyKey: key,
	})
	if err != nil {
		g.checkoutFailed(ctx, c, "create order", err)
		return
	}
	o := created.Order
	if created.Replayed {
		c.Header("Idempotent-Replayed", "true")
	}
	if o.Status != "PENDING" {
		// A retry of a checkout that already finished: return the original outcome.
		code := 200
		if o.Status == "FAILED" {
			code = 402
		}
		checkoutOutcomes.WithLabelValues("replayed").Inc()
		g.writeOrder(c, code, o)
		return
	}

	charged, err := g.payments.Charge(ctx, &paymentapi.ChargeRequest{
		OrderId: o.OrderId, AmountCents: o.AmountCents, IdempotencyKey: o.OrderId,
	})
	if err != nil {
		g.checkoutFailed(ctx, c, "charge", err)
		return
	}

	status := "PAID"
	if charged.Payment.Status != "SUCCEEDED" {
		status = "FAILED"
	}
	updated, err := g.orders.UpdateStatus(ctx, &orderapi.UpdateStatusRequest{OrderId: o.OrderId, Status: status})
	if err != nil {
		g.checkoutFailed(ctx, c, "update order", err)
		return
	}

	code, outcome := 201, "paid"
	switch {
	case status == "FAILED":
		code, outcome = 402, "declined"
	case created.Replayed:
		code, outcome = 200, "replayed"
	}
	checkoutOutcomes.WithLabelValues(outcome).Inc()
	g.writeOrder(c, code, updated.Order)
}

// GetOrder returns 404 for other users' orders instead of 403, so callers can't
// probe which order IDs exist.
func (g *Gateway) GetOrder(ctx context.Context, c *app.RequestContext) {
	resp, err := g.orders.GetOrder(ctx, &orderapi.GetOrderRequest{OrderId: c.Param("id")})
	if err != nil {
		g.writeUpstreamError(ctx, c, "get order", err)
		return
	}
	if resp.Order.UserId != c.GetString(ctxUserID) {
		abort(c, 404, "order not found")
		return
	}
	g.writeOrder(c, 200, resp.Order)
}

// checkoutFailed writes the error and counts the checkout as unavailable if it was a 5xx.
func (g *Gateway) checkoutFailed(ctx context.Context, c *app.RequestContext, op string, err error) {
	g.writeUpstreamError(ctx, c, op, err)
	if c.Response.StatusCode() >= 500 {
		checkoutOutcomes.WithLabelValues("unavailable").Inc()
	}
}

func (g *Gateway) writeOrder(c *app.RequestContext, code int, o *orderapi.Order) {
	c.JSON(code, orderView{OrderID: o.OrderId, Status: o.Status, AmountCents: o.AmountCents, CreatedAt: o.CreatedAt})
}

// writeUpstreamError maps business errors from downstream services to their HTTP code.
// Anything else (timeout, connection refused, DB down) becomes 503: the client should
// retry with the same Idempotency-Key, which is always safe.
func (g *Gateway) writeUpstreamError(ctx context.Context, c *app.RequestContext, op string, err error) {
	var oe *orderapi.OrderError
	var pe *paymentapi.PaymentError
	var ue *userapi.UserError
	switch {
	case errors.As(err, &oe):
		abort(c, int(oe.Code), oe.Message)
	case errors.As(err, &ue):
		abort(c, int(ue.Code), ue.Message)
	case errors.As(err, &pe):
		if pe.Code == 409 {
			c.Header("Retry-After", "1")
		}
		abort(c, int(pe.Code), pe.Message)
	default:
		reason := "other"
		switch {
		case errors.Is(err, kerrors.ErrCircuitBreak):
			reason = "circuit_open"
		case kerrors.IsTimeoutError(err):
			reason = "timeout"
		}
		upstreamFailures.WithLabelValues(op, reason).Inc()
		slog.ErrorContext(ctx, "upstream call failed", "op", op, "reason", reason, "request_id", c.GetString(ctxRequestID), "err", err)
		c.Header("Retry-After", "1")
		abort(c, 503, "temporarily unavailable, retry with the same Idempotency-Key")
	}
}
