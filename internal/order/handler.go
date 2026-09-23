package order

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/google/uuid"

	orderapi "vault/kitex_gen/order"
)

const maxKeyLen = 128

// Handler adapts Store to the Kitex OrderService interface: it validates input
// and translates domain errors into OrderError codes the gateway maps to HTTP.
type Handler struct {
	store *Store
}

var _ orderapi.OrderService = (*Handler)(nil)

func NewHandler(store *Store) *Handler {
	return &Handler{store: store}
}

func (h *Handler) CreateOrder(ctx context.Context, req *orderapi.CreateOrderRequest) (*orderapi.CreateOrderResponse, error) {
	userID, err := uuid.Parse(req.UserId)
	if err != nil {
		return nil, badRequest("user_id must be a UUID")
	}
	if req.AmountCents <= 0 {
		return nil, badRequest("amount_cents must be positive")
	}
	if req.IdempotencyKey == "" || len(req.IdempotencyKey) > maxKeyLen {
		return nil, badRequest("idempotency_key must be 1-128 characters")
	}
	o, replayed, err := h.store.Create(ctx, userID, req.AmountCents, req.IdempotencyKey)
	if err != nil {
		return nil, toAPIError(err)
	}
	return &orderapi.CreateOrderResponse{Order: toAPI(o), Replayed: replayed}, nil
}

func (h *Handler) GetOrder(ctx context.Context, req *orderapi.GetOrderRequest) (*orderapi.GetOrderResponse, error) {
	id, err := uuid.Parse(req.OrderId)
	if err != nil {
		return nil, &orderapi.OrderError{Code: 404, Message: ErrNotFound.Error()}
	}
	o, err := h.store.Get(ctx, id)
	if err != nil {
		return nil, toAPIError(err)
	}
	return &orderapi.GetOrderResponse{Order: toAPI(o)}, nil
}

func (h *Handler) UpdateStatus(ctx context.Context, req *orderapi.UpdateStatusRequest) (*orderapi.UpdateStatusResponse, error) {
	id, err := uuid.Parse(req.OrderId)
	if err != nil {
		return nil, &orderapi.OrderError{Code: 404, Message: ErrNotFound.Error()}
	}
	o, err := h.store.UpdateStatus(ctx, id, req.Status)
	if err != nil {
		return nil, toAPIError(err)
	}
	return &orderapi.UpdateStatusResponse{Order: toAPI(o)}, nil
}

func toAPIError(err error) error {
	switch {
	case errors.Is(err, ErrNotFound):
		return &orderapi.OrderError{Code: 404, Message: err.Error()}
	case errors.Is(err, ErrInvalidTransition):
		return &orderapi.OrderError{Code: 409, Message: err.Error()}
	case errors.Is(err, ErrKeyReuse):
		return &orderapi.OrderError{Code: 422, Message: err.Error()}
	default:
		// Unexpected (DB down, etc.): log the detail here, return a plain error so
		// Kitex reports it as a transport-level failure the gateway treats as retryable.
		slog.Error("order store", "err", err)
		return err
	}
}

func badRequest(msg string) error {
	return &orderapi.OrderError{Code: 400, Message: msg}
}

func toAPI(o Order) *orderapi.Order {
	return &orderapi.Order{
		OrderId:     o.ID.String(),
		UserId:      o.UserID.String(),
		AmountCents: o.Amount,
		Status:      o.Status,
		CreatedAt:   o.CreatedAt.UTC().Format(time.RFC3339),
	}
}
