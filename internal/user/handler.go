package user

import (
	"context"
	"errors"
	"log/slog"

	"github.com/google/uuid"

	userapi "vault/kitex_gen/user"
)

// Handler adapts Store to the Kitex UserService interface.
type Handler struct {
	store *Store
}

var _ userapi.UserService = (*Handler)(nil)

func NewHandler(store *Store) *Handler {
	return &Handler{store: store}
}

func (h *Handler) UpsertProfile(ctx context.Context, req *userapi.UpsertProfileRequest) (*userapi.UpsertProfileResponse, error) {
	id, who, err := parse(req.UserId, req.Accessor)
	if err != nil {
		return nil, err
	}
	p := Profile{UserID: id, Email: req.Email, Address: req.GetAddress()}
	if err := h.store.Upsert(ctx, p, who); err != nil {
		return nil, toAPIError(err)
	}
	return &userapi.UpsertProfileResponse{Profile: toAPI(p)}, nil
}

func (h *Handler) GetProfile(ctx context.Context, req *userapi.GetProfileRequest) (*userapi.GetProfileResponse, error) {
	id, who, err := parse(req.UserId, req.Accessor)
	if err != nil {
		return nil, err
	}
	p, err := h.store.Get(ctx, id, who)
	if err != nil {
		return nil, toAPIError(err)
	}
	return &userapi.GetProfileResponse{Profile: toAPI(p)}, nil
}

func (h *Handler) DeleteUser(ctx context.Context, req *userapi.DeleteUserRequest) (*userapi.DeleteUserResponse, error) {
	id, who, err := parse(req.UserId, req.Accessor)
	if err != nil {
		return nil, err
	}
	if err := h.store.Delete(ctx, id, who); err != nil {
		return nil, toAPIError(err)
	}
	return &userapi.DeleteUserResponse{}, nil
}

// parse rejects any PII request that doesn't say who is asking and why.
func parse(userID string, acc *userapi.Accessor) (uuid.UUID, Accessor, error) {
	id, err := uuid.Parse(userID)
	if err != nil {
		return uuid.Nil, Accessor{}, &userapi.UserError{Code: 404, Message: ErrNotFound.Error()}
	}
	if acc == nil || acc.Actor == "" || acc.Reason == "" {
		return uuid.Nil, Accessor{}, &userapi.UserError{Code: 400, Message: "accessor actor and reason are required"}
	}
	return id, Accessor{Actor: acc.Actor, Reason: acc.Reason}, nil
}

func toAPIError(err error) error {
	switch {
	case errors.Is(err, ErrNotFound):
		return &userapi.UserError{Code: 404, Message: err.Error()}
	case errors.Is(err, ErrInvalidEmail):
		return &userapi.UserError{Code: 400, Message: err.Error()}
	case errors.Is(err, ErrEmailTaken):
		return &userapi.UserError{Code: 409, Message: err.Error()}
	default:
		slog.Error("user store", "err", err)
		return err
	}
}

func toAPI(p Profile) *userapi.Profile {
	return &userapi.Profile{UserId: p.UserID.String(), Email: p.Email, Address: p.Address}
}
