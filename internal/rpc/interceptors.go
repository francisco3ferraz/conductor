package rpc

import (
	"context"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// JWTConfig holds JWT validation configuration
type JWTConfig struct {
	SecretKey  string         // For HMAC signing
	PublicKey  *rsa.PublicKey // For RSA verification
	Issuer     string
	Audience   string
	SkipExpiry bool // For development/testing
}

// LoggingInterceptor logs all incoming gRPC requests
func LoggingInterceptor(logger *zap.Logger) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (interface{}, error) {
		start := time.Now()

		// Call the handler
		resp, err := handler(ctx, req)

		// Log the request
		duration := time.Since(start)
		if err != nil {
			logger.Error("RPC call failed",
				zap.String("method", info.FullMethod),
				zap.Duration("duration", duration),
				zap.Error(err),
			)
		} else {
			logger.Info("RPC call",
				zap.String("method", info.FullMethod),
				zap.Duration("duration", duration),
			)
		}

		return resp, err
	}
}

// AuthInterceptor validates authentication tokens using JWT
func AuthInterceptor(logger *zap.Logger) grpc.UnaryServerInterceptor {
	return AuthInterceptorWithConfig(logger, &JWTConfig{
		// Development configuration - allows all tokens
		SecretKey:  "dev-secret-key-conductor-2026", // Change in production!
		Issuer:     "conductor-system",
		Audience:   "conductor-api",
		SkipExpiry: true, // Skip expiry check in development
	})
}

// AuthInterceptorWithConfig validates authentication tokens with custom JWT config
func AuthInterceptorWithConfig(logger *zap.Logger, jwtConfig *JWTConfig) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (interface{}, error) {
		// Skip auth for health check endpoints
		if info.FullMethod == "/grpc.health.v1.Health/Check" {
			return handler(ctx, req)
		}

		// Get metadata from context
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			logger.Warn("Missing metadata in request",
				zap.String("method", info.FullMethod),
			)
			// In development mode, allow requests without auth
			// In production, uncomment the following line:
			// return nil, status.Error(codes.Unauthenticated, "missing metadata")
		}

		// Validate authorization token
		tokens := md.Get("authorization")
		if len(tokens) == 0 {
			logger.Warn("Missing authorization token",
				zap.String("method", info.FullMethod),
			)
			// In development mode, allow requests without token
			// In production, uncomment the following line:
			// return nil, status.Error(codes.Unauthenticated, "missing authorization token")
		}

		// JWT token validation implemented
		// Production: Configure proper JWT secrets/keys and validation rules
		if len(tokens) > 0 {
			token := tokens[0]

			// Validate JWT token
			jwtToken, err := validateJWT(token, jwtConfig, logger)
			if err != nil {
				logger.Warn("JWT token validation failed",
					zap.String("method", info.FullMethod),
					zap.Error(err),
					zap.String("token_prefix", token[:min(20, len(token))]),
				)
				// In development mode, allow requests with invalid tokens
				// In production, uncomment the following line:
				// return nil, status.Errorf(codes.Unauthenticated, "JWT validation failed: %v", err)
			} else {
				// Log successful authentication with user info from JWT
				if claims, ok := jwtToken.Claims.(jwt.MapClaims); ok {
					logger.Debug("JWT token validated successfully",
						zap.String("method", info.FullMethod),
						zap.Any("user_id", claims["sub"]),
						zap.Any("issuer", claims["iss"]),
					)
				}
			}
		}

		// Call the handler
		return handler(ctx, req)
	}
}

// tokenBucket implements a simple token bucket for rate limiting
type tokenBucket struct {
	mu         sync.Mutex
	tokens     int
	capacity   int
	refillRate int
	lastRefill time.Time
}

func newBucket(capacity, refillRate int) *tokenBucket {
	return &tokenBucket{
		tokens:     capacity,
		capacity:   capacity,
		refillRate: refillRate,
		lastRefill: time.Now(),
	}
}

func (tb *tokenBucket) takeToken() bool {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	// Refill tokens based on time elapsed
	now := time.Now()
	elapsed := now.Sub(tb.lastRefill).Seconds()
	tokensToAdd := int(elapsed * float64(tb.refillRate))

	if tokensToAdd > 0 {
		tb.tokens = min(tb.capacity, tb.tokens+tokensToAdd)
		tb.lastRefill = now
	}

	// Check if we have tokens available
	if tb.tokens > 0 {
		tb.tokens--
		return true
	}
	return false
}

// RateLimitInterceptor implements basic rate limiting
func RateLimitInterceptor(logger *zap.Logger, maxRequestsPerSecond int) grpc.UnaryServerInterceptor {
	// Create a token bucket for rate limiting
	bucket := newBucket(maxRequestsPerSecond*2, maxRequestsPerSecond) // 2x capacity for bursts

	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (interface{}, error) {
		// Check rate limit
		if !bucket.takeToken() {
			logger.Warn("Rate limit exceeded",
				zap.String("method", info.FullMethod),
				zap.Int("rate_limit", maxRequestsPerSecond),
			)
			return nil, status.Error(codes.ResourceExhausted, "rate limit exceeded")
		}

		return handler(ctx, req)
	}
}

// RecoveryInterceptor recovers from panics in gRPC handlers
func RecoveryInterceptor(logger *zap.Logger) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (resp interface{}, err error) {
		defer func() {
			if r := recover(); r != nil {
				logger.Error("Panic recovered in gRPC handler",
					zap.String("method", info.FullMethod),
					zap.Any("panic", r),
					zap.Stack("stack"),
				)
				err = status.Errorf(codes.Internal, "internal server error")
			}
		}()

		return handler(ctx, req)
	}
}

// ChainInterceptors combines multiple interceptors into one
func ChainInterceptors(interceptors ...grpc.UnaryServerInterceptor) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (interface{}, error) {
		// Build the handler chain in reverse order
		chained := handler
		for i := len(interceptors) - 1; i >= 0; i-- {
			interceptor := interceptors[i]
			next := chained
			chained = func(ctx context.Context, req interface{}) (interface{}, error) {
				return interceptor(ctx, req, info, next)
			}
		}
		return chained(ctx, req)
	}
}

// validateJWT validates a JWT token with comprehensive checks
func validateJWT(tokenString string, config *JWTConfig, logger *zap.Logger) (*jwt.Token, error) {
	// Remove Bearer prefix if present
	if strings.HasPrefix(tokenString, "Bearer ") {
		tokenString = strings.TrimPrefix(tokenString, "Bearer ")
	}

	// Parse options to handle expiry skipping
	var options []jwt.ParserOption
	if config.SkipExpiry {
		options = append(options, jwt.WithoutClaimsValidation())
	}

	// Parse and validate the token
	token, err := jwt.ParseWithClaims(tokenString, jwt.MapClaims{}, func(token *jwt.Token) (interface{}, error) {
		// Validate signing method
		switch token.Method.(type) {
		case *jwt.SigningMethodHMAC:
			if config.SecretKey == "" {
				return nil, fmt.Errorf("HMAC secret key not configured")
			}
			return []byte(config.SecretKey), nil
		case *jwt.SigningMethodRSA:
			if config.PublicKey == nil {
				return nil, fmt.Errorf("RSA public key not configured")
			}
			return config.PublicKey, nil
		default:
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
	}, options...)

	if err != nil {
		return nil, fmt.Errorf("failed to parse token: %w", err)
	}

	if !token.Valid {
		return nil, fmt.Errorf("invalid token")
	}

	// Manual claims validation when we skip automatic validation
	if claims, ok := token.Claims.(jwt.MapClaims); ok {
		// Check issuer
		if config.Issuer != "" {
			if iss, ok := claims["iss"]; !ok || iss != config.Issuer {
				return nil, fmt.Errorf("invalid issuer")
			}
		}

		// Check audience
		if config.Audience != "" {
			if aud, ok := claims["aud"]; !ok || aud != config.Audience {
				return nil, fmt.Errorf("invalid audience")
			}
		}

		// Check expiry only if not skipped
		if !config.SkipExpiry {
			if exp, ok := claims["exp"]; ok {
				if expFloat, ok := exp.(float64); ok {
					if time.Now().Unix() > int64(expFloat) {
						return nil, fmt.Errorf("token expired")
					}
				}
			}
		}
	}

	return token, nil
}

// GenerateJWT creates a JWT token for testing purposes
func GenerateJWT(userID, issuer, audience string, secretKey string, expiry time.Duration) (string, error) {
	claims := jwt.MapClaims{
		"sub": userID,
		"iss": issuer,
		"aud": audience,
		"iat": time.Now().Unix(),
		"exp": time.Now().Add(expiry).Unix(),
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString([]byte(secretKey))
}

// ParseRSAPublicKeyFromPEM parses PEM-encoded RSA public key
func ParseRSAPublicKeyFromPEM(pemStr string) (*rsa.PublicKey, error) {
	block, _ := pem.Decode([]byte(pemStr))
	if block == nil {
		return nil, fmt.Errorf("failed to parse PEM block")
	}

	pub, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse public key: %w", err)
	}

	rsaPub, ok := pub.(*rsa.PublicKey)
	if !ok {
		return nil, fmt.Errorf("not an RSA public key")
	}

	return rsaPub, nil
}

// min returns the minimum of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
