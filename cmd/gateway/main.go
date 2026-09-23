// Gateway: public HTTP API (Hertz) that calls order and payment over Kitex RPC.
package main

import (
	"context"
	"os"
	"strconv"
	"time"

	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/kitex/client"

	"vault/internal/gateway"
	"vault/internal/platform"
	"vault/kitex_gen/order/orderservice"
	"vault/kitex_gen/payment/paymentservice"
	"vault/kitex_gen/user/userservice"
)

func main() {
	log := platform.NewLogger("gateway")

	secret := []byte(platform.Env("JWT_SECRET", "dev-secret-change-me"))
	if _, ok := os.LookupEnv("JWT_SECRET"); !ok {
		log.Warn("JWT_SECRET not set, using the dev default")
	}

	// Every RPC gets a deadline. Without one, a hung downstream ties up gateway
	// goroutines until the whole gateway falls over.
	rpcOpts := []client.Option{
		client.WithRPCTimeout(2 * time.Second),
		client.WithConnectTimeout(500 * time.Millisecond),
	}
	orders, err := orderservice.NewClient("order",
		append(rpcOpts, client.WithHostPorts(platform.Env("ORDER_ADDR", "localhost:8881")))...)
	if err != nil {
		log.Error("order client", "err", err)
		os.Exit(1)
	}
	payments, err := paymentservice.NewClient("payment",
		append(rpcOpts, client.WithHostPorts(platform.Env("PAYMENT_ADDR", "localhost:8882")))...)
	if err != nil {
		log.Error("payment client", "err", err)
		os.Exit(1)
	}

	users, err := userservice.NewClient("user",
		append(rpcOpts, client.WithHostPorts(platform.Env("USER_ADDR", "localhost:8883")))...)
	if err != nil {
		log.Error("user client", "err", err)
		os.Exit(1)
	}

	rps, _ := strconv.ParseFloat(platform.Env("RATE_LIMIT_RPS", "20"), 64)
	burst, _ := strconv.Atoi(platform.Env("RATE_LIMIT_BURST", "40"))
	limiter := gateway.NewRateLimiter(rps, burst)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go limiter.RunEvictor(ctx)

	h := server.Default(
		server.WithHostPorts(platform.Env("HTTP_ADDR", ":8080")),
		server.WithReadTimeout(5*time.Second),
		server.WithExitWaitTime(5*time.Second), // drain in-flight requests on SIGTERM
	)
	gateway.New(orders, payments, users).Register(h, secret, limiter)
	h.Spin()
}
