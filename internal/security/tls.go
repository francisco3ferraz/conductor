package security

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/credentials"
)

// TLSConfig holds TLS configuration paths and settings
type TLSConfig struct {
	Enabled      bool
	CertFile     string
	KeyFile      string
	CAFile       string
	ServerName   string
	SkipVerify   bool // Only for development
	AutoGenerate bool // Auto-generate self-signed certs for development
}

// CertManager manages TLS certificates for mTLS
type CertManager struct {
	config *TLSConfig
	logger *zap.Logger
}

// NewCertManager creates a new certificate manager
func NewCertManager(config *TLSConfig, logger *zap.Logger) *CertManager {
	return &CertManager{
		config: config,
		logger: logger,
	}
}

// LoadOrGenerateServerTLS loads or generates server TLS credentials
func (cm *CertManager) LoadOrGenerateServerTLS() (credentials.TransportCredentials, error) {
	if !cm.config.Enabled {
		return nil, nil
	}

	// Auto-generate certificates if enabled and files don't exist
	if cm.config.AutoGenerate && (!fileExists(cm.config.CertFile) || !fileExists(cm.config.KeyFile)) {
		cm.logger.Info("Auto-generating self-signed certificates for development")
		if err := cm.GenerateSelfSignedCert(); err != nil {
			return nil, fmt.Errorf("failed to generate certificates: %w", err)
		}
	}

	// Load server certificate and key
	cert, err := tls.LoadX509KeyPair(cm.config.CertFile, cm.config.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load server certificate: %w", err)
	}

	// Create certificate pool for client verification (mTLS)
	certPool := x509.NewCertPool()
	if cm.config.CAFile != "" {
		ca, err := os.ReadFile(cm.config.CAFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA certificate: %w", err)
		}
		if !certPool.AppendCertsFromPEM(ca) {
			return nil, fmt.Errorf("failed to append CA certificate")
		}
	}

	// Configure mTLS
	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientCAs:    certPool,
		ClientAuth:   tls.RequireAndVerifyClientCert, // mTLS: require client certificates
		MinVersion:   tls.VersionTLS13,               // Use TLS 1.3
	}

	// For development, allow skipping client verification
	if cm.config.SkipVerify {
		cm.logger.Warn("TLS client verification disabled - NOT FOR PRODUCTION")
		tlsConfig.ClientAuth = tls.NoClientCert
	}

	return credentials.NewTLS(tlsConfig), nil
}

// LoadOrGenerateClientTLS loads or generates client TLS credentials
func (cm *CertManager) LoadOrGenerateClientTLS() (credentials.TransportCredentials, error) {
	if !cm.config.Enabled {
		return nil, nil
	}

	// Auto-generate certificates if enabled and files don't exist
	if cm.config.AutoGenerate && (!fileExists(cm.config.CertFile) || !fileExists(cm.config.KeyFile)) {
		cm.logger.Info("Auto-generating self-signed certificates for development")
		if err := cm.GenerateSelfSignedCert(); err != nil {
			return nil, fmt.Errorf("failed to generate certificates: %w", err)
		}
	}

	// Load client certificate and key
	cert, err := tls.LoadX509KeyPair(cm.config.CertFile, cm.config.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load client certificate: %w", err)
	}

	// Create certificate pool for server verification
	certPool := x509.NewCertPool()
	if cm.config.CAFile != "" {
		ca, err := os.ReadFile(cm.config.CAFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA certificate: %w", err)
		}
		if !certPool.AppendCertsFromPEM(ca) {
			return nil, fmt.Errorf("failed to append CA certificate")
		}
	}

	// Configure client TLS
	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      certPool,
		ServerName:   cm.config.ServerName,
		MinVersion:   tls.VersionTLS13,
	}

	// For development, allow insecure skip verify
	if cm.config.SkipVerify {
		cm.logger.Warn("TLS server verification disabled - NOT FOR PRODUCTION")
		tlsConfig.InsecureSkipVerify = true
	}

	return credentials.NewTLS(tlsConfig), nil
}

// GenerateSelfSignedCert generates a self-signed certificate for development
func (cm *CertManager) GenerateSelfSignedCert() error {
	// Ensure directory exists
	certDir := filepath.Dir(cm.config.CertFile)
	if err := os.MkdirAll(certDir, 0755); err != nil {
		return fmt.Errorf("failed to create cert directory: %w", err)
	}

	// Generate private key
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return fmt.Errorf("failed to generate private key: %w", err)
	}

	// Create certificate template
	serialNumber, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return fmt.Errorf("failed to generate serial number: %w", err)
	}

	template := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization: []string{"Conductor Development"},
			CommonName:   "localhost",
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(365 * 24 * time.Hour), // Valid for 1 year
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		DNSNames:              []string{"localhost", "conductor", "*.conductor.local"},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
	}

	// Create self-signed certificate
	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		return fmt.Errorf("failed to create certificate: %w", err)
	}

	// Write certificate to file
	certOut, err := os.Create(cm.config.CertFile)
	if err != nil {
		return fmt.Errorf("failed to create cert file: %w", err)
	}
	defer certOut.Close()

	if err := pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: certDER}); err != nil {
		return fmt.Errorf("failed to write certificate: %w", err)
	}

	cm.logger.Info("Generated certificate", zap.String("file", cm.config.CertFile))

	// Write private key to file
	keyOut, err := os.OpenFile(cm.config.KeyFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return fmt.Errorf("failed to create key file: %w", err)
	}
	defer keyOut.Close()

	privBytes, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		return fmt.Errorf("failed to marshal private key: %w", err)
	}

	if err := pem.Encode(keyOut, &pem.Block{Type: "EC PRIVATE KEY", Bytes: privBytes}); err != nil {
		return fmt.Errorf("failed to write private key: %w", err)
	}

	cm.logger.Info("Generated private key", zap.String("file", cm.config.KeyFile))

	// Also use cert as CA for development
	if cm.config.CAFile != "" {
		caOut, err := os.Create(cm.config.CAFile)
		if err != nil {
			return fmt.Errorf("failed to create CA file: %w", err)
		}
		defer caOut.Close()

		if err := pem.Encode(caOut, &pem.Block{Type: "CERTIFICATE", Bytes: certDER}); err != nil {
			return fmt.Errorf("failed to write CA certificate: %w", err)
		}

		cm.logger.Info("Generated CA certificate", zap.String("file", cm.config.CAFile))
	}

	return nil
}

// LoadRaftTLSConfig loads TLS configuration for Raft communication
func (cm *CertManager) LoadRaftTLSConfig() (*tls.Config, error) {
	if !cm.config.Enabled {
		return nil, nil
	}

	// Auto-generate if needed
	if cm.config.AutoGenerate && (!fileExists(cm.config.CertFile) || !fileExists(cm.config.KeyFile)) {
		if err := cm.GenerateSelfSignedCert(); err != nil {
			return nil, fmt.Errorf("failed to generate certificates: %w", err)
		}
	}

	// Load certificate
	cert, err := tls.LoadX509KeyPair(cm.config.CertFile, cm.config.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load Raft certificate: %w", err)
	}

	// Load CA for peer verification
	certPool := x509.NewCertPool()
	if cm.config.CAFile != "" {
		ca, err := os.ReadFile(cm.config.CAFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA certificate: %w", err)
		}
		if !certPool.AppendCertsFromPEM(ca) {
			return nil, fmt.Errorf("failed to append CA certificate")
		}
	}

	// Configure TLS for Raft
	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientCAs:    certPool,
		RootCAs:      certPool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS13,
	}

	if cm.config.SkipVerify {
		cm.logger.Warn("Raft TLS verification disabled - NOT FOR PRODUCTION")
		tlsConfig.ClientAuth = tls.NoClientCert
		tlsConfig.InsecureSkipVerify = true
	}

	return tlsConfig, nil
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
