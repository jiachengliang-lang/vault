package gateway

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"golang.org/x/time/rate"
)

const (
	headerRequestID = "X-Request-ID"
	ctxUserID       = "user_id"
	ctxRole         = "role"
	ctxRequestID    = "request_id"

	RoleSupport = "support"
)

// Claims is what the gateway trusts from a token: who the caller is, and optionally a staff role.
type Claims struct {
	jwt.RegisteredClaims
	Role string `json:"role,omitempty"`
}

// RequestID tags every request with an ID (reusing the caller's if present) and echoes it back,
// so one checkout can be followed through every service's logs.
func RequestID() app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		id := string(c.GetHeader(headerRequestID))
		if id == "" || len(id) > 64 {
			id = uuid.NewString()
		}
		c.Set(ctxRequestID, id)
		c.Header(headerRequestID, id)
		c.Next(ctx)
	}
}

// Auth accepts only HS256 JWTs signed with secret, with an expiry, whose subject is a user UUID.
// Pinning the algorithm blocks the classic "alg: none" / algorithm-confusion attacks.
func Auth(secret []byte) app.HandlerFunc {
	parser := jwt.NewParser(jwt.WithValidMethods([]string{"HS256"}), jwt.WithExpirationRequired())
	keyFunc := func(*jwt.Token) (any, error) { return secret, nil }

	return func(ctx context.Context, c *app.RequestContext) {
		raw, ok := strings.CutPrefix(string(c.GetHeader("Authorization")), "Bearer ")
		if !ok {
			abort(c, 401, "missing bearer token")
			return
		}
		var claims Claims
		if _, err := parser.ParseWithClaims(raw, &claims, keyFunc); err != nil {
			abort(c, 401, "invalid token")
			return
		}
		userID, err := uuid.Parse(claims.Subject)
		if err != nil {
			abort(c, 401, "invalid token subject")
			return
		}
		c.Set(ctxUserID, userID.String())
		c.Set(ctxRole, claims.Role)
		c.Next(ctx)
	}
}

// RequireRole rejects callers whose token doesn't carry role. Runs after Auth.
func RequireRole(role string) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		if c.GetString(ctxRole) != role {
			abort(c, 403, "requires role "+role)
			return
		}
		c.Next(ctx)
	}
}

// NewToken mints a token for userID. Used by cmd/tokengen and tests; a real system gets these from an identity provider.
func NewToken(secret []byte, userID uuid.UUID, ttl time.Duration) (string, error) {
	return NewTokenWithRole(secret, userID, "", ttl)
}

// NewTokenWithRole mints a token carrying a staff role, e.g. RoleSupport.
func NewTokenWithRole(secret []byte, userID uuid.UUID, role string, ttl time.Duration) (string, error) {
	return jwt.NewWithClaims(jwt.SigningMethodHS256, Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   userID.String(),
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(ttl)),
		},
		Role: role,
	}).SignedString(secret)
}

// RateLimiter gives each user a token bucket: `rps` sustained, `burst` short spikes.
// Limiting per user (not globally) means one noisy client can't starve everyone else.
// It is in-memory, so with N gateway replicas each user effectively gets N x rps;
// a shared limit would need Redis. Fine for one instance.
type RateLimiter struct {
	rps   rate.Limit
	burst int

	mu      sync.Mutex
	buckets map[string]*bucket
}

type bucket struct {
	limiter  *rate.Limiter
	lastSeen time.Time
}

func NewRateLimiter(rps float64, burst int) *RateLimiter {
	return &RateLimiter{rps: rate.Limit(rps), burst: burst, buckets: make(map[string]*bucket)}
}

func (rl *RateLimiter) Allow(key string) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	b, ok := rl.buckets[key]
	if !ok {
		b = &bucket{limiter: rate.NewLimiter(rl.rps, rl.burst)}
		rl.buckets[key] = b
	}
	b.lastSeen = time.Now()
	return b.limiter.Allow()
}

// Evict drops buckets idle for longer than idle, so memory doesn't grow with every user ever seen.
func (rl *RateLimiter) Evict(idle time.Duration) {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	for k, b := range rl.buckets {
		if time.Since(b.lastSeen) > idle {
			delete(rl.buckets, k)
		}
	}
}

// RunEvictor evicts idle buckets every minute until ctx is cancelled.
func (rl *RateLimiter) RunEvictor(ctx context.Context) {
	t := time.NewTicker(time.Minute)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			rl.Evict(5 * time.Minute)
		}
	}
}

// Middleware must run after Auth, since it limits by user.
func (rl *RateLimiter) Middleware() app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		if !rl.Allow(c.GetString(ctxUserID)) {
			c.Header("Retry-After", "1")
			abort(c, 429, "rate limit exceeded")
			return
		}
		c.Next(ctx)
	}
}

func abort(c *app.RequestContext, status int, msg string) {
	c.AbortWithStatusJSON(status, errorBody{Error: msg})
}

type errorBody struct {
	Error string `json:"error"`
}

var errMissingKey = errors.New("Idempotency-Key header is required (1-128 chars)")
