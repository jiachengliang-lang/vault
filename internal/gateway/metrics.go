package gateway

import (
	"context"
	"log/slog"
	"strconv"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"vault/internal/platform"
)

var (
	httpRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "http_requests_total",
		Help: "HTTP requests, by route, method and status code.",
	}, []string{"route", "method", "code"})
	httpDuration = platform.NewHistogram("http_request_duration_seconds",
		"HTTP request latency, as the client sees it (includes downstream RPCs).", "route", "method")
	upstreamFailures = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gateway_upstream_failures_total",
		Help: "Failed calls to downstream services, by operation and reason: circuit_open (failed fast), timeout, other.",
	}, []string{"op", "reason"})
	checkoutOutcomes = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "checkout_outcomes_total",
		Help: "Checkouts by outcome: paid, declined, replayed, unavailable.",
	}, []string{"outcome"})
)

func init() {
	for _, o := range []string{"paid", "declined", "replayed", "unavailable"} {
		checkoutOutcomes.WithLabelValues(o) // start at 0; see user/audit.go
	}
}

// Metrics records RED metrics for every request, including ones rejected by auth or rate limiting.
//
// The route label is the route pattern ("/v1/orders/:id"), never the raw path. Raw paths would
// create one time series per order ID, and unbounded label values like that are the classic way
// to take down a Prometheus server (a "cardinality explosion").
//
// It also logs the requests worth alerting on: every 5xx, and every request slower than slow.
// On AWS, where there's no Prometheus, CloudWatch alarms count these lines. Logging only the
// exceptions keeps the log volume (and its cost) independent of traffic.
func Metrics(slow time.Duration) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		start := time.Now()
		c.Next(ctx)
		elapsed := time.Since(start)
		route := c.FullPath()
		if route == "" {
			route = "unmatched"
		}
		method := string(c.Method())
		status := c.Response.StatusCode()
		httpRequests.WithLabelValues(route, method, strconv.Itoa(status)).Inc()
		httpDuration.WithLabelValues(route, method).Observe(elapsed.Seconds())

		attrs := []any{"route", route, "method", method, "status", status, "duration_ms", elapsed.Milliseconds()}
		switch {
		case status >= 500:
			slog.ErrorContext(ctx, "request failed", attrs...)
		case elapsed > slow:
			slog.WarnContext(ctx, "slow request", attrs...)
		}
	}
}

// tagSpan adds the request ID to the current trace span, linking the two ways of finding a request.
func tagSpan(ctx context.Context, requestID string) {
	trace.SpanFromContext(ctx).SetAttributes(attribute.String("request.id", requestID))
}
