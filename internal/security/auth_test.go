package security

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

func TestGenerateTokenPair(t *testing.T) {
	logger := zaptest.NewLogger(t)
	cfg := &JWTConfig{
		SecretKey:       "test-secret",
		Issuer:          "conductor-test",
		Audience:        "conductor",
		AccessTokenTTL:  15 * time.Minute,
		RefreshTokenTTL: 7 * 24 * time.Hour,
		DevelopmentMode: false,
	}

	auth := NewAuthManager(cfg, logger)

	access, refresh, err := auth.GenerateTokenPair("user123", []string{"admin"})
	require.NoError(t, err)
	assert.NotEmpty(t, access)
	assert.NotEmpty(t, refresh)

	// Verify access token
	claims, err := auth.ValidateToken(access)
	require.NoError(t, err)
	assert.Equal(t, "user123", claims.Subject)

	// Verify refresh token is cached
	cachedToken, exists := auth.refreshCache["user123"]
	require.True(t, exists)
	assert.Equal(t, "user123", cachedToken.UserID)
}

func TestRefreshAccessToken(t *testing.T) {
	logger := zaptest.NewLogger(t)
	cfg := &JWTConfig{
		SecretKey:       "test-secret",
		Issuer:          "conductor-test",
		Audience:        "conductor",
		AccessTokenTTL:  1 * time.Second,
		RefreshTokenTTL: 10 * time.Second,
		DevelopmentMode: false,
	}

	auth := NewAuthManager(cfg, logger)

	// Generate initial token pair
	_, refreshToken, err := auth.GenerateTokenPair("user123", []string{"admin"})
	require.NoError(t, err)

	// Wait for access token to expire
	time.Sleep(2 * time.Second)

	// Refresh access token
	newAccessToken, err := auth.RefreshAccessToken(refreshToken)
	require.NoError(t, err)
	assert.NotEmpty(t, newAccessToken)

	// Verify new access token
	claims, err := auth.ValidateToken(newAccessToken)
	require.NoError(t, err)
	assert.Equal(t, "user123", claims.Subject)
}

func TestRefreshAccessToken_InvalidToken(t *testing.T) {
	logger := zaptest.NewLogger(t)
	cfg := &JWTConfig{
		SecretKey:       "test-secret",
		Issuer:          "conductor-test",
		Audience:        "conductor",
		AccessTokenTTL:  15 * time.Minute,
		RefreshTokenTTL: 7 * 24 * time.Hour,
		DevelopmentMode: false,
	}

	auth := NewAuthManager(cfg, logger)

	// Try to refresh with invalid token
	_, err := auth.RefreshAccessToken("invalid-token")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid refresh token")
}

func TestRefreshAccessToken_Revoked(t *testing.T) {
	logger := zaptest.NewLogger(t)
	cfg := &JWTConfig{
		SecretKey:       "test-secret",
		Issuer:          "conductor-test",
		Audience:        "conductor",
		AccessTokenTTL:  15 * time.Minute,
		RefreshTokenTTL: 7 * 24 * time.Hour,
		DevelopmentMode: false,
	}

	auth := NewAuthManager(cfg, logger)

	// Generate token pair
	_, refreshToken, err := auth.GenerateTokenPair("user123", []string{"admin"})
	require.NoError(t, err)

	// Revoke refresh token
	auth.RevokeRefreshToken("user123")

	// Try to refresh with revoked token
	_, err = auth.RefreshAccessToken(refreshToken)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "revoked or invalid")
}

func TestRevokeRefreshToken(t *testing.T) {
	logger := zaptest.NewLogger(t)
	cfg := &JWTConfig{
		SecretKey:       "test-secret",
		Issuer:          "conductor-test",
		Audience:        "conductor",
		AccessTokenTTL:  15 * time.Minute,
		RefreshTokenTTL: 7 * 24 * time.Hour,
		DevelopmentMode: false,
	}

	auth := NewAuthManager(cfg, logger)

	// Generate token pair
	_, _, err := auth.GenerateTokenPair("user123", []string{"admin"})
	require.NoError(t, err)

	// Verify token exists in cache
	_, exists := auth.refreshCache["user123"]
	require.True(t, exists)

	// Revoke token
	auth.RevokeRefreshToken("user123")

	// Verify token removed from cache
	_, exists = auth.refreshCache["user123"]
	assert.False(t, exists)
}

func TestRevokeRefreshToken_NotFound(t *testing.T) {
	logger := zaptest.NewLogger(t)
	cfg := &JWTConfig{
		SecretKey:       "test-secret",
		Issuer:          "conductor-test",
		Audience:        "conductor",
		AccessTokenTTL:  15 * time.Minute,
		RefreshTokenTTL: 7 * 24 * time.Hour,
		DevelopmentMode: false,
	}

	auth := NewAuthManager(cfg, logger)

	// Revoke non-existent token is OK (no-op)
	auth.RevokeRefreshToken("non-existent-user")
}

func TestAuthInterceptor_DevelopmentMode(t *testing.T) {
	logger := zaptest.NewLogger(t)
	cfg := &JWTConfig{
		SecretKey:       "test-secret",
		Issuer:          "conductor-test",
		Audience:        "conductor",
		AccessTokenTTL:  15 * time.Minute,
		RefreshTokenTTL: 7 * 24 * time.Hour,
		DevelopmentMode: true,
	}

	auth := NewAuthManager(cfg, logger)

	// In development mode, requests without tokens should be allowed
	// This would be tested through actual interceptor invocation
	// which requires setting up a full gRPC server context

	assert.True(t, auth.config.DevelopmentMode)
}

func TestTokenExpiry(t *testing.T) {
	logger := zaptest.NewLogger(t)
	cfg := &JWTConfig{
		SecretKey:       "test-secret",
		Issuer:          "conductor-test",
		Audience:        "conductor",
		AccessTokenTTL:  1 * time.Second,
		RefreshTokenTTL: 10 * time.Second,
		DevelopmentMode: false,
	}

	auth := NewAuthManager(cfg, logger)

	// Generate token
	accessToken, _, err := auth.GenerateTokenPair("user123", []string{"admin"})
	require.NoError(t, err)

	// Token should be valid initially
	_, err = auth.ValidateToken(accessToken)
	assert.NoError(t, err)

	// Wait for token to expire
	time.Sleep(2 * time.Second)

	// Token should be invalid now
	_, err = auth.ValidateToken(accessToken)
	assert.Error(t, err)
}
