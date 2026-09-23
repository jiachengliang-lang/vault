package platform

import (
	"context"
	"log/slog"
	"time"

	"github.com/cloudwego/kitex/pkg/endpoint"
	"github.com/cloudwego/kitex/pkg/rpcinfo"
	"github.com/cloudwego/kitex/server"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

// ---------- Tracing ----------
//
// A trace is the full journey of one request; each step (an HTTP handler, an RPC, a SQL query,
// a Kafka message) is a span. Spans carry the trace ID across process boundaries in headers
// (W3C traceparent), so Jaeger can stitch gateway -> order -> payment -> Kafka -> pipeline
// into one timeline.

// InitTracing sends this service's spans to the OTLP endpoint (Jaeger, via `make up`).
// The exporter connects lazily, so a missing Jaeger never stops the service from starting.
// Call the returned function on shutdown to flush buffered spans.
func InitTracing(ctx context.Context, service string) (shutdown func(context.Context) error, err error) {
	exporter, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(Env("OTEL_EXPORTER_OTLP_ENDPOINT", "localhost:4317")),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		return nil, err
	}
	tp := sdktrace.NewTracerProvider(
		// Spans are sent in batches in the background, so tracing adds almost no request latency.
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(resource.NewSchemaless(semconv.ServiceName(service))),
		// Every request is traced here. At production volume you'd sample (say 1%), and keep
		// every trace that has an error.
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.AlwaysSample())),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	return tp.Shutdown, nil
}

// traceHandler adds trace_id and span_id to every log line written with a context,
// so you can jump from a log line straight to its trace in Jaeger.
type traceHandler struct {
	slog.Handler
}

func (h traceHandler) Handle(ctx context.Context, r slog.Record) error {
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		r.AddAttrs(slog.String("trace_id", sc.TraceID().String()), slog.String("span_id", sc.SpanID().String()))
	}
	return h.Handler.Handle(ctx, r)
}

func (h traceHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return traceHandler{h.Handler.WithAttrs(attrs)}
}

func (h traceHandler) WithGroup(name string) slog.Handler {
	return traceHandler{h.Handler.WithGroup(name)}
}

// ---------- Metrics ----------
//
// Metrics are numbers over time, scraped by Prometheus from each service's /metrics.
// Every service reports RED metrics for the requests it serves:
//   Rate (requests/sec), Errors (failure rate), Duration (latency histogram, for p50/p95/p99).
// The Go client also exports runtime metrics for free: goroutines, heap size, GC pauses.

// latencyBuckets span 1ms to 5s. Percentiles are interpolated within a bucket, so a p99 can never be
// more precise than the bucket it lands in: the buckets are densest around the 200 ms SLO, where
// the answer matters. (With a 25-50 ms bucket, p95 and p99 both read "50 ms" regardless of reality.)
var latencyBuckets = []float64{.001, .0025, .005, .0075, .01, .015, .02, .03, .04, .05, .075, .1, .15, .2, .3, .5, 1, 2.5, 5}

var (
	rpcRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "rpc_server_requests_total",
		Help: "RPC requests handled, by method and outcome (ok, business_error, error).",
	}, []string{"service", "method", "outcome"})

	rpcDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "rpc_server_duration_seconds",
		Help:    "RPC handling time.",
		Buckets: latencyBuckets,
	}, []string{"service", "method"})
)

// NewHistogram is a latency histogram with the shared buckets.
func NewHistogram(name, help string, labels ...string) *prometheus.HistogramVec {
	return promauto.NewHistogramVec(prometheus.HistogramOpts{Name: name, Help: help, Buckets: latencyBuckets}, labels)
}

// errResult is implemented by every Kitex-generated result type. Declared exceptions
// (OrderError, PaymentError...) are stored in the result, not returned as err.
type errResult interface {
	IsSetErr() bool
}

// RPCMetrics records RED metrics for every method a Kitex server handles.
//
// Outcomes are split three ways on purpose: "business_error" is an expected answer (not found,
// declined, key reused) and shouldn't page anyone; "error" is the service failing (DB down,
// panic, timeout), which is what alerts fire on.
func RPCMetrics(service string) server.Option {
	return server.WithMiddleware(func(next endpoint.Endpoint) endpoint.Endpoint {
		return func(ctx context.Context, req, resp any) error {
			start := time.Now()
			err := next(ctx, req, resp)

			method := "unknown"
			if ri := rpcinfo.GetRPCInfo(ctx); ri != nil {
				method = ri.To().Method()
			}
			outcome := "ok"
			if err != nil {
				outcome = "error"
			} else if r, ok := resp.(errResult); ok && r.IsSetErr() {
				outcome = "business_error"
			}
			rpcRequests.WithLabelValues(service, method, outcome).Inc()
			rpcDuration.WithLabelValues(service, method).Observe(time.Since(start).Seconds())
			return err
		}
	})
}
