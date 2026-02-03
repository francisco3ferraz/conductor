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
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// AuthManager handles JWT authentication
type AuthManager struct {
	config       *JWTConfig
	logger       *zap.Logger
	auditLogger  *zap.Logger
	refreshCache map[string]*RefreshToken // userID -> refresh token
}

// JWTConfig holds JWT configuration
type JWTConfig struct {
	SecretKey       string
	Issuer          string
	Audience        string
	SkipExpiry      bool
	DevelopmentMode bool          // Explicit dev mode flag
	AccessTokenTTL  time.Duration // Default: 15 minutes
	RefreshTokenTTL time.Duration // Default: 7 days
}

// RefreshToken represents a refresh token for token renewal
type RefreshToken struct {
	Token     string
	UserID    string
	ExpiresAt time.Time
	CreatedAt time.Time
}

// NewAuthManager creates a new authentication manager
func NewAuthManager(config *JWTConfig, logger *zap.Logger) *AuthManager {
	// Create dedicated audit logger
	auditLogger := logger.Named("audit")

	// Set default TTLs if not specified
	if config.AccessTokenTTL == 0 {
		config.AccessTokenTTL = 15 * time.Minute
	}
	if config.RefreshTokenTTL == 0 {
		config.RefreshTokenTTL = 7 * 24 * time.Hour
	}

	return &AuthManager{
		config:       config,
		logger:       logger,
		auditLogger:  auditLogger,
		refreshCache: make(map[string]*RefreshToken),
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

// GenerateTokenPair generates both access and refresh tokens
func (am *AuthManager) GenerateTokenPair(userID string, roles []string) (accessToken, refreshToken string, err error) {
	// Generate access token
	accessToken, err = am.GenerateToken(userID, roles, am.config.AccessTokenTTL)
	if err != nil {
		return "", "", err
	}

	// Generate refresh token
	now := time.Now()
	refreshClaims := jwt.MapClaims{
		"sub":  userID,
		"iss":  am.config.Issuer,
		"type": "refresh",
		"iat":  now.Unix(),
		"exp":  now.Add(am.config.RefreshTokenTTL).Unix(),
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, refreshClaims)
	refreshToken, err = token.SignedString([]byte(am.config.SecretKey))
	if err != nil {
		return "", "", fmt.Errorf("failed to sign refresh token: %w", err)
	}

	// Store refresh token
	am.refreshCache[userID] = &RefreshToken{
		Token:     refreshToken,
		UserID:    userID,
		ExpiresAt: now.Add(am.config.RefreshTokenTTL),
		CreatedAt: now,
	}

	am.auditLogger.Info("Generated token pair",
		zap.String("user_id", userID),
		zap.Strings("roles", roles),
		zap.Time("access_expires", now.Add(am.config.AccessTokenTTL)),
		zap.Time("refresh_expires", now.Add(am.config.RefreshTokenTTL)),
	)

	return accessToken, refreshToken, nil
}

// RefreshAccessToken generates a new access token using a refresh token
func (am *AuthManager) RefreshAccessToken(refreshToken string) (string, error) {
	// Parse refresh token
	token, err := jwt.Parse(refreshToken, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return []byte(am.config.SecretKey), nil
	})

	if err != nil {
		am.auditLogger.Warn("Refresh token validation failed", zap.Error(err))
		return "", fmt.Errorf("invalid refresh token: %w", err)
	}

	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok || !token.Valid {
		am.auditLogger.Warn("Invalid refresh token claims")
		return "", fmt.Errorf("invalid refresh token")
	}

	// Verify token type
	if tokenType, _ := claims["type"].(string); tokenType != "refresh" {
		return "", fmt.Errorf("not a refresh token")
	}

	userID, _ := claims["sub"].(string)
	if userID == "" {
		return "", fmt.Errorf("missing user ID in refresh token")
	}

	// Verify refresh token exists in cache
	cachedToken, ok := am.refreshCache[userID]
	if !ok || cachedToken.Token != refreshToken {
		am.auditLogger.Warn("Refresh token not found or mismatch",
			zap.String("user_id", userID))
		return "", fmt.Errorf("refresh token revoked or invalid")
	}

	// Check expiry
	if time.Now().After(cachedToken.ExpiresAt) {
		delete(am.refreshCache, userID)
		am.auditLogger.Warn("Refresh token expired", zap.String("user_id", userID))
		return "", fmt.Errorf("refresh token expired")
	}

	// Get user's current roles (in production, fetch from database)
	// For now, we'll generate a token with empty roles
	// TODO: Fetch user roles from user service/database
	roles := []string{}

	// Generate new access token
	accessToken, err := am.GenerateToken(userID, roles, am.config.AccessTokenTTL)
	if err != nil {
		return "", err
	}

	am.auditLogger.Info("Refreshed access token",
		zap.String("user_id", userID))

	return accessToken, nil
}

// RevokeRefreshToken revokes a user's refresh token
func (am *AuthManager) RevokeRefreshToken(userID string) {
	delete(am.refreshCache, userID)
	am.auditLogger.Info("Revoked refresh token", zap.String("user_id", userID))
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
		start := time.Now()

		// Extract client address for audit logging
		clientAddr := "unknown"
		if p, ok := peer.FromContext(ctx); ok {
			clientAddr = p.Addr.String()
		}

		// Skip auth for health checks
		if info.FullMethod == "/grpc.health.v1.Health/Check" {
			return handler(ctx, req)
		}

		// Extract metadata
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			am.auditLogger.Warn("Authentication failed: missing metadata",
				zap.String("method", info.FullMethod),
				zap.String("client", clientAddr),
				zap.String("reason", "no_metadata"),
			)

			// Development mode: log warning but allow
			if am.config.DevelopmentMode {
				am.logger.Warn("DEV MODE: Allowing request without metadata")
				return handler(ctx, req)
			}
			return nil, status.Error(codes.Unauthenticated, "missing metadata")
		}

		// Extract authorization token
		tokens := md.Get("authorization")
		if len(tokens) == 0 {
			am.auditLogger.Warn("Authentication failed: missing token",
				zap.String("method", info.FullMethod),
				zap.String("client", clientAddr),
				zap.String("reason", "no_token"),
			)

			// Development mode: log warning but allow
			if am.config.DevelopmentMode {
				am.logger.Warn("DEV MODE: Allowing request without token")
				return handler(ctx, req)
			}
			return nil, status.Error(codes.Unauthenticated, "missing authorization token")
		}

		// Validate token
		claims, err := am.ValidateToken(tokens[0])
		if err != nil {
			am.auditLogger.Warn("Authentication failed: token validation error",
				zap.String("method", info.FullMethod),
				zap.String("client", clientAddr),
				zap.String("reason", "invalid_token"),
				zap.Error(err),
				zap.Duration("latency", time.Since(start)),
			)

			// Development mode: log warning but allow
			if am.config.DevelopmentMode {
				am.logger.Warn("DEV MODE: Allowing request with invalid token",
					zap.Error(err))
				return handler(ctx, req)
			}
			return nil, status.Errorf(codes.Unauthenticated, "invalid token")
		}

		// Successful authentication - audit log
		am.auditLogger.Info("Authentication successful",
			zap.String("user_id", claims.Subject),
			zap.Strings("roles", claims.Roles),
			zap.String("method", info.FullMethod),
			zap.String("client", clientAddr),
			zap.Duration("latency", time.Since(start)),
		)

		// Add user info to context for downstream handlers
		ctx = metadata.AppendToOutgoingContext(ctx, "user-id", claims.Subject)
		if len(claims.Roles) > 0 {
			ctx = metadata.AppendToOutgoingContext(ctx, "user-roles", claims.Roles[0])
		}

		return handler(ctx, req)
	}
}
