// Payment service: Kitex RPC on RPC_ADDR, health checks on ADMIN_ADDR.
package main

import (
	"context"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/cloudwego/kitex/pkg/rpcinfo"
	"github.com/cloudwego/kitex/server"

	"vault/internal/payment"
	"vault/internal/platform"
	"vault/kitex_gen/payment/paymentservice"
)

func main() {
	log := platform.NewLogger("payment")
	ctx := context.Background()

	pool, err := platform.NewPool(ctx, platform.Env("DATABASE_URL", platform.DefaultDatabaseURL))
	if err != nil {
		log.Error("startup failed", "err", err)
		os.Exit(1)
	}
	defer pool.Close()
	admin := platform.StartAdmin(platform.Env("ADMIN_ADDR", ":8082"), pool.Ping)
	defer admin.Shutdown(ctx)

	// PSP_LATENCY_MS and PSP_FAILURE_RATE let you simulate a slow or flaky provider.
	latencyMS, _ := strconv.Atoi(platform.Env("PSP_LATENCY_MS", "20"))
	failureRate, _ := strconv.ParseFloat(platform.Env("PSP_FAILURE_RATE", "0"), 64)
	psp := payment.NewMockPSP(time.Duration(latencyMS)*time.Millisecond, failureRate)

	addr, err := net.ResolveTCPAddr("tcp", platform.Env("RPC_ADDR", ":8882"))
	if err != nil {
		log.Error("bad RPC_ADDR", "err", err)
		os.Exit(1)
	}
	svr := paymentservice.NewServer(
		payment.NewHandler(payment.NewService(pool, psp)),
		server.WithServiceAddr(addr),
		server.WithServerBasicInfo(&rpcinfo.EndpointBasicInfo{ServiceName: "payment"}),
	)
	log.Info("payment service starting", "rpc", addr.String(), "psp_latency_ms", latencyMS, "psp_failure_rate", failureRate)
	if err := svr.Run(); err != nil {
		log.Error("server stopped", "err", err)
	}
}
