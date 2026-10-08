// Reconcile finishes checkouts that were cut off part way (see internal/reconcile), once, then
// exits. On AWS it runs every 5 minutes as a scheduled ECS task; locally, `make reconcile`.
// It exits non-zero if any order couldn't be finished, so a failed run is visible.
package main

import (
	"context"
	"os"
	"time"

	"vault/internal/platform"
	"vault/internal/reconcile"
	"vault/kitex_gen/order/orderservice"
	"vault/kitex_gen/payment/paymentservice"
)

func main() {
	log := platform.NewLogger("reconcile")
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute) // well inside the 5-minute schedule
	defer cancel()

	pool, err := platform.NewPool(ctx, platform.Env("DATABASE_URL", platform.DefaultDatabaseURL))
	if err != nil {
		log.Error("startup failed", "err", err)
		os.Exit(1)
	}
	defer pool.Close()
	orders, err := orderservice.NewClient("order", platform.ClientOptions("reconcile", platform.Env("ORDER_ADDR", "localhost:8881"))...)
	if err != nil {
		log.Error("order client", "err", err)
		os.Exit(1)
	}
	payments, err := paymentservice.NewClient("payment", platform.ClientOptions("reconcile", platform.Env("PAYMENT_ADDR", "localhost:8882"))...)
	if err != nil {
		log.Error("payment client", "err", err)
		os.Exit(1)
	}

	res, err := reconcile.New(pool, orders, payments).Run(ctx)
	if err != nil {
		log.Error("reconcile run failed", "err", err)
		os.Exit(1)
	}
	log.Info("reconcile run", "found", res.Found, "paid", res.Paid, "failed", res.Failed,
		"abandoned", res.Abandoned, "waiting", res.Waiting, "errors", res.Errors)
	if res.Errors > 0 {
		os.Exit(1)
	}
}
