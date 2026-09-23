package payment

import (
	"context"
	"errors"
	"log/slog"

	"github.com/google/uuid"

	paymentapi "vault/kitex_gen/payment"
)

// Handler adapts Service to the Kitex PaymentService interface.
type Handler struct {
	svc *Service
}

var _ paymentapi.PaymentService = (*Handler)(nil)

func NewHandler(svc *Service) *Handler {
	return &Handler{svc: svc}
}

func (h *Handler) Charge(ctx context.Context, req *paymentapi.ChargeRequest) (*paymentapi.ChargeResponse, error) {
	orderID, err := uuid.Parse(req.OrderId)
	if err != nil {
		return nil, &paymentapi.PaymentError{Code: 400, Message: "order_id must be a UUID"}
	}
	if req.AmountCents <= 0 {
		return nil, &paymentapi.PaymentError{Code: 400, Message: "amount_cents must be positive"}
	}
	if req.IdempotencyKey == "" {
		return nil, &paymentapi.PaymentError{Code: 400, Message: "idempotency_key is required"}
	}

	p, replayed, err := h.svc.Charge(ctx, orderID, req.AmountCents, req.IdempotencyKey)
	switch {
	case errors.Is(err, ErrKeyReuse):
		return nil, &paymentapi.PaymentError{Code: 422, Message: err.Error()}
	case errors.Is(err, ErrInProgress):
		return nil, &paymentapi.PaymentError{Code: 409, Message: err.Error()}
	case err != nil:
		// PSP outage or DB error: a plain error, which the gateway treats as retryable.
		slog.Error("charge failed", "order_id", orderID, "err", err)
		return nil, err
	}
	return &paymentapi.ChargeResponse{
		Payment: &paymentapi.Payment{
			PaymentId:   p.ID.String(),
			OrderId:     p.OrderID.String(),
			AmountCents: p.Amount,
			Status:      p.Status,
		},
		Replayed: replayed,
	}, nil
}
