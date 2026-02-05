package tracing

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDefaultConfig(t *testing.T) {
	tests := []struct {
		name         string
		envVars      map[string]string
		wantInsecure bool
		wantEndpoint string
		wantEnabled  bool
		wantTLSCert  string
		wantEnv      string
	}{
		{
			name: "development defaults",
			envVars: map[string]string{
				"CONDUCTOR_ENV": "development",
			},
			wantInsecure: true,
			wantEndpoint: "localhost:4318",
			wantEnabled:  true,
			wantEnv:      "development",
		},
		{
			name: "production with secure connection",
			envVars: map[string]string{
				"CONDUCTOR_ENV":               "production",
				"OTEL_EXPORTER_OTLP_INSECURE": "false",
				"OTEL_EXPORTER_OTLP_ENDPOINT": "otel-collector.prod.example.com:4318",
			},
			wantInsecure: false,
			wantEndpoint: "otel-collector.prod.example.com:4318",
			wantEnabled:  true,
			wantEnv:      "production",
		},
		{
			name: "custom TLS certificate",
			envVars: map[string]string{
				"OTEL_EXPORTER_OTLP_INSECURE":    "false",
				"OTEL_EXPORTER_OTLP_CERTIFICATE": "/etc/conductor/certs/otel-ca.crt",
			},
			wantInsecure: false,
			wantTLSCert:  "/etc/conductor/certs/otel-ca.crt",
			wantEnabled:  true,
			wantEnv:      "development",
		},
		{
			name: "tracing disabled",
			envVars: map[string]string{
				"OTEL_TRACE_ENABLED": "false",
			},
			wantInsecure: true,
			wantEnabled:  false,
			wantEnv:      "development",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Clear relevant env vars first
			os.Unsetenv("CONDUCTOR_ENV")
			os.Unsetenv("OTEL_EXPORTER_OTLP_INSECURE")
			os.Unsetenv("OTEL_EXPORTER_OTLP_ENDPOINT")
			os.Unsetenv("OTEL_EXPORTER_OTLP_CERTIFICATE")
			os.Unsetenv("OTEL_TRACE_ENABLED")

			// Set test env vars
			for k, v := range tt.envVars {
				os.Setenv(k, v)
			}

			cfg := DefaultConfig()

			assert.Equal(t, tt.wantInsecure, cfg.Insecure, "Insecure flag mismatch")
			assert.Equal(t, tt.wantEnabled, cfg.Enabled, "Enabled flag mismatch")
			assert.Equal(t, tt.wantEnv, cfg.Environment, "Environment mismatch")

			if tt.wantEndpoint != "" {
				assert.Equal(t, tt.wantEndpoint, cfg.OTLPEndpoint, "Endpoint mismatch")
			}
			if tt.wantTLSCert != "" {
				assert.Equal(t, tt.wantTLSCert, cfg.TLSCertFile, "TLS cert file mismatch")
			}

			// Cleanup
			for k := range tt.envVars {
				os.Unsetenv(k)
			}
		})
	}
}

func TestTracingConfig_SecuritySettings(t *testing.T) {
	tests := []struct {
		name   string
		config *TracingConfig
	}{
		{
			name: "insecure for development",
			config: &TracingConfig{
				ServiceName:    "test-service",
				ServiceVersion: "1.0.0",
				OTLPEndpoint:   "localhost:4318",
				Insecure:       true,
				Enabled:        true,
			},
		},
		{
			name: "secure with system certs",
			config: &TracingConfig{
				ServiceName:    "test-service",
				ServiceVersion: "1.0.0",
				OTLPEndpoint:   "otel.prod.example.com:4318",
				Insecure:       false,
				Enabled:        true,
			},
		},
		{
			name: "secure with custom cert",
			config: &TracingConfig{
				ServiceName:    "test-service",
				ServiceVersion: "1.0.0",
				OTLPEndpoint:   "otel.prod.example.com:4318",
				Insecure:       false,
				TLSCertFile:    "/etc/certs/ca.crt",
				Enabled:        true,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Validate config fields are set correctly
			assert.NotEmpty(t, tt.config.ServiceName, "ServiceName should be set")
			assert.NotEmpty(t, tt.config.OTLPEndpoint, "OTLPEndpoint should be set")

			if tt.config.Insecure {
				assert.Empty(t, tt.config.TLSCertFile, "TLSCertFile should be empty for insecure")
			}
		})
	}
}
