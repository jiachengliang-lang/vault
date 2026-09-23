// Order service: Kitex RPC on RPC_ADDR, health checks on ADMIN_ADDR,
// plus the outbox relay that publishes order events to Kafka.
package main

import (
	"context"
	"net"
	"os"
	"strings"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"

	"vault/internal/order"
	"vault/internal/outbox"
	"vault/internal/platform"
	"vault/kitex_gen/order/orderservice"
)

func main() {
	log := platform.NewLogger("order")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	shutdownTracing, err := platform.InitTracing(ctx, "order")
	if err != nil {
		log.Error("tracing", "err", err)
		os.Exit(1)
	}
	defer shutdownTracing(context.Background())

	pool, err := platform.NewPool(ctx, platform.Env("DATABASE_URL", platform.DefaultDatabaseURL))
	if err != nil {
		log.Error("startup failed", "err", err)
		os.Exit(1)
	}
	defer pool.Close()
	platform.RegisterPoolMetrics(pool)
	admin, err := platform.StartAdmin(platform.Env("ADMIN_ADDR", ":8081"), pool.Ping)
	if err != nil {
		log.Error("startup failed", "err", err)
		os.Exit(1)
	}
	defer admin.Shutdown(context.Background())

	// Kafka being down must not stop orders from being taken: the relay just falls behind
	// and the outbox table buffers events until the broker is back.
	producer, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(platform.Env("KAFKA_BROKERS", platform.DefaultKafkaBrokers), ",")...))
	if err != nil {
		log.Error("kafka client", "err", err)
		os.Exit(1)
	}
	defer producer.Close()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		outbox.NewRelay(pool, producer).Run(ctx)
	}()

	addr, err := net.ResolveTCPAddr("tcp", platform.Env("RPC_ADDR", ":8881"))
	if err != nil {
		log.Error("bad RPC_ADDR", "err", err)
		os.Exit(1)
	}
	svr := orderservice.NewServer(
		order.NewHandler(order.NewStore(pool)),
		platform.ServerOptions("order", addr)...,
	)
	log.Info("order service starting", "rpc", addr.String())
	// Run blocks until SIGINT/SIGTERM, then drains in-flight requests before returning.
	if err := svr.Run(); err != nil {
		log.Error("server stopped", "err", err)
	}
	cancel()
	wg.Wait() // let the relay finish its current batch before closing the pool
}
