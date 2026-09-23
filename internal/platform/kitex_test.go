package platform

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	orderapi "vault/kitex_gen/order"
	"vault/kitex_gen/order/orderservice"
)

// stubOrders answers every call with a business error, the way the real order service
// answers a reused idempotency key.
type stubOrders struct{}

func (*stubOrders) CreateOrder(context.Context, *orderapi.CreateOrderRequest) (*orderapi.CreateOrderResponse, error) {
	return nil, &orderapi.OrderError{Code: 422, Message: "idempotency key already used with a different amount"}
}
func (*stubOrders) GetOrder(context.Context, *orderapi.GetOrderRequest) (*orderapi.GetOrderResponse, error) {
	return nil, &orderapi.OrderError{Code: 404, Message: "order not found"}
}
func (*stubOrders) UpdateStatus(context.Context, *orderapi.UpdateStatusRequest) (*orderapi.UpdateStatusResponse, error) {
	return nil, &orderapi.OrderError{Code: 409, Message: "invalid status transition"}
}

// Regression test through a real Kitex client and server with the production options. Business
// errors must reach the caller as typed errors. With Kitex's built-in retry enabled they came back
// as (nil, nil) and the gateway crashed; tests with fake clients couldn't see it.
func TestBusinessErrorsSurviveClientOptions(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().(*net.TCPAddr)
	ln.Close()

	svr := orderservice.NewServer(&stubOrders{}, ServerOptions("order", addr)...)
	go svr.Run()
	t.Cleanup(func() { svr.Stop() })

	cli, err := orderservice.NewClient("order", ClientOptions("test", addr.String())...)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	var resp *orderapi.CreateOrderResponse
	deadline := time.Now().Add(5 * time.Second)
	for { // the server starts asynchronously
		resp, err = cli.CreateOrder(ctx, &orderapi.CreateOrderRequest{UserId: "u", AmountCents: 1, IdempotencyKey: "k"})
		var oe *orderapi.OrderError
		if errors.As(err, &oe) {
			if oe.Code != 422 {
				t.Fatalf("code %d, want 422", oe.Code)
			}
			return
		}
		if err == nil {
			t.Fatalf("business error lost: got response %v and nil error", resp)
		}
		if time.Now().After(deadline) {
			t.Fatalf("server never answered: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
