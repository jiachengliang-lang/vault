package gateway

import (
	"context"
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
func Metrics() app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		start := time.Now()
		c.Next(ctx)
		route := c.FullPath()
		if route == "" {
			route = "unmatched"
		}
		method := string(c.Method())
		httpRequests.WithLabelValues(route, method, strconv.Itoa(c.Response.StatusCode())).Inc()
		httpDuration.WithLabelValues(route, method).Observe(time.Since(start).Seconds())
	}
}

// tagSpan adds the request ID to the current trace span, linking the two ways of finding a request.
func tagSpan(ctx context.Context, requestID string) {
	trace.SpanFromContext(ctx).SetAttributes(attribute.String("request.id", requestID))
}
