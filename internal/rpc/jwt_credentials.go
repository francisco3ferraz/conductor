package rpc

import (
	"context"

	"google.golang.org/grpc/credentials"
)

// JWTCredentials implements grpc.PerRPCCredentials for JWT authentication
type JWTCredentials struct {
	token    string
	insecure bool // allow sending over non-TLS connection (dev only)
}

// NewJWTCredentials creates new JWT credentials for gRPC calls
func NewJWTCredentials(token string, allowInsecure bool) credentials.PerRPCCredentials {
	return &JWTCredentials{
		token:    token,
		insecure: allowInsecure,
	}
}

// GetRequestMetadata returns the authorization header with the JWT token
func (j *JWTCredentials) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	return map[string]string{
		"authorization": "Bearer " + j.token,
	}, nil
}

// RequireTransportSecurity returns whether TLS is required
func (j *JWTCredentials) RequireTransportSecurity() bool {
	return !j.insecure
}
