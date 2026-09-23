// Gateway: public HTTP API (Hertz) that calls order, payment and user over Kitex RPC.
// Health checks and /metrics are on the internal ADMIN_ADDR, never on the public port.
package main

import (
	"context"
	"os"
	"strconv"
	"time"

	"github.com/cloudwego/hertz/pkg/app/server"
	hertztracing "github.com/hertz-contrib/obs-opentelemetry/tracing"

	"vault/internal/gateway"
	"vault/internal/platform"
	"vault/kitex_gen/order/orderservice"
	"vault/kitex_gen/payment/paymentservice"
	"vault/kitex_gen/user/userservice"
)

func main() {
	log := platform.NewLogger("gateway")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	shutdownTracing, err := platform.InitTracing(ctx, "gateway")
	if err != nil {
		log.Error("tracing", "err", err)
		os.Exit(1)
	}
	defer shutdownTracing(context.Background())

	secret := []byte(platform.Env("JWT_SECRET", "dev-secret-change-me"))
	if _, ok := os.LookupEnv("JWT_SECRET"); !ok {
		log.Warn("JWT_SECRET not set, using the dev default")
	}

	orders, err := orderservice.NewClient("order",
		platform.ClientOptions("gateway", platform.Env("ORDER_ADDR", "localhost:8881"))...)
	if err != nil {
		log.Error("order client", "err", err)
		os.Exit(1)
	}
	payments, err := paymentservice.NewClient("payment",
		platform.ClientOptions("gateway", platform.Env("PAYMENT_ADDR", "localhost:8882"))...)
	if err != nil {
		log.Error("payment client", "err", err)
		os.Exit(1)
	}
	users, err := userservice.NewClient("user",
		platform.ClientOptions("gateway", platform.Env("USER_ADDR", "localhost:8883"))...)
	if err != nil {
		log.Error("user client", "err", err)
		os.Exit(1)
	}

	rps, _ := strconv.ParseFloat(platform.Env("RATE_LIMIT_RPS", "20"), 64)
	burst, _ := strconv.Atoi(platform.Env("RATE_LIMIT_BURST", "40"))
	limiter := gateway.NewRateLimiter(rps, burst)
	go limiter.RunEvictor(ctx)

	admin := platform.StartAdmin(platform.Env("ADMIN_ADDR", ":8090"), func(context.Context) error { return nil })
	defer admin.Shutdown(context.Background())

	// The tracer starts a span for each request, or continues the caller's trace if it sent a traceparent header.
	tracer, tracingCfg := hertztracing.NewServerTracer()
	h := server.Default(
		server.WithHostPorts(platform.Env("HTTP_ADDR", ":8080")),
		server.WithReadTimeout(5*time.Second),
		server.WithExitWaitTime(5*time.Second), // drain in-flight requests on SIGTERM
		tracer,
	)
	h.Use(hertztracing.ServerMiddleware(tracingCfg))
	gateway.New(orders, payments, users).Register(h, secret, limiter)
	h.Spin()
}
