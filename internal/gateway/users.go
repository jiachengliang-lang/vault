package gateway

import (
	"context"

	"github.com/cloudwego/hertz/pkg/app"

	userapi "vault/kitex_gen/user"
)

const minReasonLen = 10

type profileRequest struct {
	Email   string `json:"email"`
	Address string `json:"address"`
}

type profileView struct {
	UserID  string `json:"user_id"`
	Email   string `json:"email"`
	Address string `json:"address"`
}

// PutProfile creates or replaces the caller's own profile.
func (g *Gateway) PutProfile(ctx context.Context, c *app.RequestContext) {
	var req profileRequest
	if err := c.BindJSON(&req); err != nil || req.Email == "" {
		abort(c, 400, `body must be {"email": "...", "address": "..."}`)
		return
	}
	userID := c.GetString(ctxUserID)
	resp, err := g.users.UpsertProfile(ctx, &userapi.UpsertProfileRequest{
		UserId: userID, Email: req.Email, Address: &req.Address,
		Accessor: &userapi.Accessor{Actor: "user:" + userID, Reason: "self-service profile update"},
	})
	if err != nil {
		g.writeUpstreamError(ctx, c, "upsert profile", err)
		return
	}
	writeProfile(c, resp.Profile)
}

// GetProfile returns the caller's own profile.
func (g *Gateway) GetProfile(ctx context.Context, c *app.RequestContext) {
	userID := c.GetString(ctxUserID)
	resp, err := g.users.GetProfile(ctx, &userapi.GetProfileRequest{
		UserId:   userID,
		Accessor: &userapi.Accessor{Actor: "user:" + userID, Reason: "self-service profile view"},
	})
	if err != nil {
		g.writeUpstreamError(ctx, c, "get profile", err)
		return
	}
	writeProfile(c, resp.Profile)
}

// DeleteMe crypto-shreds the caller's account ("right to be forgotten").
func (g *Gateway) DeleteMe(ctx context.Context, c *app.RequestContext) {
	userID := c.GetString(ctxUserID)
	_, err := g.users.DeleteUser(ctx, &userapi.DeleteUserRequest{
		UserId:   userID,
		Accessor: &userapi.Accessor{Actor: "user:" + userID, Reason: "user-requested deletion"},
	})
	if err != nil {
		g.writeUpstreamError(ctx, c, "delete user", err)
		return
	}
	c.Status(204)
}

// SupportGetProfile lets support staff view a customer's profile. Staff must give a reason
// (e.g. a ticket number); it's written to the audit log with their identity.
// This is "justified access": access is allowed, but never silent.
func (g *Gateway) SupportGetProfile(ctx context.Context, c *app.RequestContext) {
	reason := c.Query("reason")
	if len(reason) < minReasonLen {
		abort(c, 400, "a reason of at least 10 characters is required, e.g. ?reason=ticket-4821")
		return
	}
	resp, err := g.users.GetProfile(ctx, &userapi.GetProfileRequest{
		UserId:   c.Param("id"),
		Accessor: &userapi.Accessor{Actor: "support:" + c.GetString(ctxUserID), Reason: reason},
	})
	if err != nil {
		g.writeUpstreamError(ctx, c, "support get profile", err)
		return
	}
	writeProfile(c, resp.Profile)
}

func writeProfile(c *app.RequestContext, p *userapi.Profile) {
	c.JSON(200, profileView{UserID: p.UserId, Email: p.Email, Address: p.Address})
}
