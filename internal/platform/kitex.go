package platform

import (
	"context"
	"log/slog"
	"net"
	"time"

	"github.com/bytedance/gopkg/cloud/circuitbreaker"
	"github.com/cloudwego/kitex/client"
	"github.com/cloudwego/kitex/pkg/circuitbreak"
	"github.com/cloudwego/kitex/pkg/kerrors"
	"github.com/cloudwego/kitex/pkg/retry"
	"github.com/cloudwego/kitex/pkg/rpcinfo"
	"github.com/cloudwego/kitex/server"
	"github.com/kitex-contrib/obs-opentelemetry/tracing"
)

// ServerOptions is the standard setup for a Kitex server: its name, address,
// trace propagation (reads the caller's trace context from TTHeader) and RED metrics.
func ServerOptions(service string, addr *net.TCPAddr) []server.Option {
	return []server.Option{
		server.WithServiceAddr(addr),
		server.WithServerBasicInfo(&rpcinfo.EndpointBasicInfo{ServiceName: service}),
		server.WithSuite(tracing.NewServerSuite()),
		RPCMetrics(service),
	}
}

// ClientOptions is the standard setup for calling another service:
//
//   - A deadline on every call. Without one, a hung downstream ties up the caller's goroutines
//     until it falls over too.
//   - Retries on timeouts, at most 2, with jittered backoff and a 3 s total budget. Safe here only
//     because every RPC in Vault is idempotent (keys, conditional updates). Retrying a
//     non-idempotent call can double-apply it.
//   - A retry breaker: once more than 10% of calls are failing, stop retrying. During an outage,
//     retries would multiply load on a service that's already struggling (a retry storm).
//   - A circuit breaker per method (see circuitBreaker).
//     RPC_CIRCUIT_BREAKER=off disables it, for comparing behaviour in chaos tests.
func ClientOptions(caller, hostPort string) []client.Option {
	policy := retry.NewFailurePolicy()
	policy.WithMaxRetryTimes(2)
	policy.WithRandomBackOff(10, 50)
	policy.WithMaxDurationMS(3000)
	policy.WithRetryBreaker(0.1)

	opts := []client.Option{
		client.WithHostPorts(hostPort),
		client.WithClientBasicInfo(&rpcinfo.EndpointBasicInfo{ServiceName: caller}),
		client.WithSuite(tracing.NewClientSuite()),
		client.WithRPCTimeout(2 * time.Second),
		client.WithConnectTimeout(500 * time.Millisecond),
		client.WithFailureRetry(policy),
	}
	if Env("RPC_CIRCUIT_BREAKER", "on") != "off" {
		opts = append(opts, circuitBreaker())
	}
	return opts
}

// circuitBreaker trips per method (e.g. gateway/payment/Charge) once 50%+ of at least 100 calls in
// 10 s fail: calls then fail immediately instead of each waiting out the timeout. Business errors
// (not found, declined) don't count as failures.
//
// After CoolingTimeout it lets a few calls through to test for recovery. Kitex's built-in suite
// fixes that at 5 s; in a chaos test that kept checkout failing for ~9 s after the payment service
// was already back, so this builds the same breaker with 2 s. Shorter means faster recovery but
// more probe traffic at a service that may still be down.
func circuitBreaker() client.Option {
	panel, _ := circuitbreaker.NewPanel(
		func(key string, from, to circuitbreaker.State, _ circuitbreaker.Metricer) {
			slog.Warn("circuit breaker state changed", "key", key, "from", from.String(), "to", to.String())
		},
		circuitbreaker.Options{
			CoolingTimeout: 2 * time.Second,
			ShouldTrip:     circuitbreaker.RateTripFunc(0.5, 100),
		})
	return client.WithMiddleware(circuitbreak.NewCircuitBreakerMW(circuitbreak.Control{
		GetKey: func(ctx context.Context, _ any) (string, bool) {
			return circuitbreak.RPCInfo2Key(rpcinfo.GetRPCInfo(ctx)), true
		},
		GetErrorType: circuitbreak.ErrorTypeOnServiceLevel,
		DecorateError: func(context.Context, any, error) error {
			return kerrors.ErrServiceCircuitBreak
		},
	}, panel))
}
