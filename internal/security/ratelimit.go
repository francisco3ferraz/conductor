package security

import (
	"context"
	"sync"
	"time"

	"github.com/francisco3ferraz/conductor/internal/config"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// RateLimiter manages per-client rate limiting using token bucket algorithm
type RateLimiter struct {
	config   *config.RateLimitConfig
	logger   *zap.Logger
	mu       sync.RWMutex
	limiters map[string]*clientLimiter // client ID/IP -> limiter
	ctx      context.Context
	cancel   context.CancelFunc
}

// clientLimiter wraps a rate limiter with last access time for cleanup
type clientLimiter struct {
	limiter    *rate.Limiter
	lastAccess time.Time
}

// NewRateLimiter creates a new rate limiter
func NewRateLimiter(cfg *config.RateLimitConfig, logger *zap.Logger) *RateLimiter {
	// Set defaults if not configured
	if cfg.CleanupInterval == 0 {
		cfg.CleanupInterval = 5 * time.Minute
	}
	if cfg.ClientTTL == 0 {
		cfg.ClientTTL = 30 * time.Minute
	}

	ctx, cancel := context.WithCancel(context.Background())

	rl := &RateLimiter{
		config:   cfg,
		logger:   logger,
		limiters: make(map[string]*clientLimiter),
		ctx:      ctx,
		cancel:   cancel,
	}

	// Start cleanup goroutine
	go rl.cleanupInactiveLimiters()

	return rl
}

// getLimiter gets or creates a rate limiter for a client
func (rl *RateLimiter) getLimiter(clientID string) *rate.Limiter {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	cl, exists := rl.limiters[clientID]
	if !exists {
		// Create new limiter for this client
		limiter := rate.NewLimiter(rate.Limit(rl.config.RequestsPerSec), rl.config.Burst)
		cl = &clientLimiter{
			limiter:    limiter,
			lastAccess: time.Now(),
		}
		rl.limiters[clientID] = cl
		rl.logger.Debug("Created rate limiter for client",
			zap.String("client_id", clientID),
			zap.Float64("requests_per_sec", rl.config.RequestsPerSec),
			zap.Int("burst", rl.config.Burst))
	} else {
		// Update last access time
		cl.lastAccess = time.Now()
	}

	return cl.limiter
}

// Allow checks if a request should be allowed
func (rl *RateLimiter) Allow(clientID string) bool {
	if !rl.config.Enabled {
		return true
	}

	limiter := rl.getLimiter(clientID)
	return limiter.Allow()
}

// Wait waits until the request can be allowed (blocking)
func (rl *RateLimiter) Wait(ctx context.Context, clientID string) error {
	if !rl.config.Enabled {
		return nil
	}

	limiter := rl.getLimiter(clientID)
	return limiter.Wait(ctx)
}

// cleanupInactiveLimiters removes rate limiters for inactive clients
func (rl *RateLimiter) cleanupInactiveLimiters() {
	ticker := time.NewTicker(rl.config.CleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-rl.ctx.Done():
			return
		case <-ticker.C:
			rl.cleanup()
		}
	}
}

// cleanup removes inactive limiters
func (rl *RateLimiter) cleanup() {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := time.Now()
	removed := 0

	for clientID, cl := range rl.limiters {
		if now.Sub(cl.lastAccess) > rl.config.ClientTTL {
			delete(rl.limiters, clientID)
			removed++
		}
	}

	if removed > 0 {
		rl.logger.Debug("Cleaned up inactive rate limiters",
			zap.Int("removed", removed),
			zap.Int("remaining", len(rl.limiters)))
	}
}

// Shutdown stops the rate limiter cleanup goroutine
func (rl *RateLimiter) Shutdown() {
	rl.cancel()
}

// GetStats returns current rate limiter statistics
func (rl *RateLimiter) GetStats() map[string]interface{} {
	rl.mu.RLock()
	defer rl.mu.RUnlock()

	return map[string]interface{}{
		"enabled":          rl.config.Enabled,
		"active_clients":   len(rl.limiters),
		"requests_per_sec": rl.config.RequestsPerSec,
		"burst":            rl.config.Burst,
	}
}

// RateLimitInterceptor creates a gRPC unary interceptor for rate limiting
func RateLimitInterceptor(rateLimiter *RateLimiter, logger *zap.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		// Skip rate limiting if disabled
		if !rateLimiter.config.Enabled {
			return handler(ctx, req)
		}

		// Extract client identifier (prefer user ID from metadata, fallback to IP)
		clientID := extractClientID(ctx)

		// Check rate limit
		if !rateLimiter.Allow(clientID) {
			logger.Warn("Rate limit exceeded",
				zap.String("client_id", clientID),
				zap.String("method", info.FullMethod))

			// Return rate limit error
			return nil, status.Errorf(codes.ResourceExhausted,
				"Rate limit exceeded. Please slow down your requests. (limit: %.1f req/s, burst: %d)",
				rateLimiter.config.RequestsPerSec,
				rateLimiter.config.Burst)
		}

		// Allow request
		return handler(ctx, req)
	}
}

// extractClientID extracts a unique client identifier from the context
// Priority: user ID from JWT > client IP
func extractClientID(ctx context.Context) string {
	// Try to get user ID from metadata (set by auth interceptor)
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if userIDs := md.Get("user_id"); len(userIDs) > 0 {
			return "user:" + userIDs[0]
		}
	}

	// Fallback to client IP address
	if p, ok := peer.FromContext(ctx); ok {
		return "ip:" + p.Addr.String()
	}

	// Last resort: unknown client
	return "unknown"
}
