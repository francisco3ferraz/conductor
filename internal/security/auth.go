package security

import (
	"context"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// AuthManager handles JWT authentication
type AuthManager struct {
	config *JWTConfig
	logger *zap.Logger
}

// JWTConfig holds JWT configuration
type JWTConfig struct {
	SecretKey  string
	Issuer     string
	Audience   string
	SkipExpiry bool
}

// NewAuthManager creates a new authentication manager
func NewAuthManager(config *JWTConfig, logger *zap.Logger) *AuthManager {
	return &AuthManager{
		config: config,
		logger: logger,
	}
}

// GenerateToken generates a new JWT token for a user
func (am *AuthManager) GenerateToken(userID string, roles []string, expiry time.Duration) (string, error) {
	now := time.Now()
	claims := jwt.MapClaims{
		"sub":   userID,
		"iss":   am.config.Issuer,
		"aud":   am.config.Audience,
		"iat":   now.Unix(),
		"exp":   now.Add(expiry).Unix(),
		"roles": roles,
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tokenString, err := token.SignedString([]byte(am.config.SecretKey))
	if err != nil {
		return "", fmt.Errorf("failed to sign token: %w", err)
	}

	am.logger.Info("Generated JWT token",
		zap.String("user_id", userID),
		zap.Strings("roles", roles),
		zap.Duration("expiry", expiry),
	)

	return tokenString, nil
}

// ValidateToken validates a JWT token and extracts claims
func (am *AuthManager) ValidateToken(tokenString string) (*UserClaims, error) {
	// Remove "Bearer " prefix if present
	if len(tokenString) > 7 && tokenString[:7] == "Bearer " {
		tokenString = tokenString[7:]
	}

	// Parse token
	token, err := jwt.ParseWithClaims(tokenString, &UserClaims{}, func(token *jwt.Token) (interface{}, error) {
		// Validate signing method
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return []byte(am.config.SecretKey), nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to parse token: %w", err)
	}

	// Extract claims
	claims, ok := token.Claims.(*UserClaims)
	if !ok || !token.Valid {
		return nil, fmt.Errorf("invalid token")
	}

	// Manual expiry check if not skipped
	if !am.config.SkipExpiry && claims.ExpiresAt != nil {
		if time.Now().After(claims.ExpiresAt.Time) {
			return nil, fmt.Errorf("token expired")
		}
	}

	// Validate issuer
	if claims.Issuer != am.config.Issuer {
		return nil, fmt.Errorf("invalid issuer: expected %s, got %s", am.config.Issuer, claims.Issuer)
	}

	// Validate audience
	if len(claims.Audience) > 0 && claims.Audience[0] != am.config.Audience {
		return nil, fmt.Errorf("invalid audience")
	}

	return claims, nil
}

// UserClaims extends JWT standard claims with user information
type UserClaims struct {
	jwt.RegisteredClaims
	Roles []string `json:"roles,omitempty"`
}

// AuthInterceptor creates a gRPC interceptor for JWT authentication
func (am *AuthManager) AuthInterceptor() grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (interface{}, error) {
		// Skip auth for health checks
		if info.FullMethod == "/grpc.health.v1.Health/Check" {
			return handler(ctx, req)
		}

		// Extract metadata
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			am.logger.Warn("Missing metadata", zap.String("method", info.FullMethod))
			// In development mode (SkipExpiry=true), allow requests without metadata
			if am.config.SkipExpiry {
				return handler(ctx, req)
			}
			return nil, status.Error(codes.Unauthenticated, "missing metadata")
		}

		// Extract authorization token
		tokens := md.Get("authorization")
		if len(tokens) == 0 {
			am.logger.Warn("Missing authorization token", zap.String("method", info.FullMethod))
			// In development mode, allow requests without token
			if am.config.SkipExpiry {
				return handler(ctx, req)
			}
			return nil, status.Error(codes.Unauthenticated, "missing authorization token")
		}

		// Validate token
		claims, err := am.ValidateToken(tokens[0])
		if err != nil {
			am.logger.Warn("Token validation failed",
				zap.String("method", info.FullMethod),
				zap.Error(err),
			)
			// In development mode, allow invalid tokens
			if am.config.SkipExpiry {
				am.logger.Warn("Allowing invalid token in development mode")
				return handler(ctx, req)
			}
			return nil, status.Errorf(codes.Unauthenticated, "invalid token: %v", err)
		}

		am.logger.Debug("Token validated",
			zap.String("user_id", claims.Subject),
			zap.Strings("roles", claims.Roles),
			zap.String("method", info.FullMethod),
		)

		// Add user info to context for downstream handlers
		ctx = metadata.AppendToOutgoingContext(ctx, "user-id", claims.Subject)
		if len(claims.Roles) > 0 {
			ctx = metadata.AppendToOutgoingContext(ctx, "user-roles", claims.Roles[0])
		}

		return handler(ctx, req)
	}
}
