package config

import (
	"fmt"
	"time"

	"github.com/spf13/viper"
)

// Config holds all configuration for the system
type Config struct {
	Cluster   ClusterConfig   `mapstructure:"cluster"`
	Raft      RaftConfig      `mapstructure:"raft"`
	GRPC      GRPCConfig      `mapstructure:"grpc"`
	Worker    WorkerConfig    `mapstructure:"worker"`
	Scheduler SchedulerConfig `mapstructure:"scheduler"`
	Log       LogConfig       `mapstructure:"log"`
	Security  SecurityConfig  `mapstructure:"security"`
}

type ClusterConfig struct {
	NodeID    string `mapstructure:"node_id"`
	BindAddr  string `mapstructure:"bind_addr"`
	RaftDir   string `mapstructure:"raft_dir"`
	DataDir   string `mapstructure:"data_dir"`
	Bootstrap bool   `mapstructure:"bootstrap"`
	JoinAddr  string `mapstructure:"join_addr"` // Address of existing cluster member to join
}

type RaftConfig struct {
	HeartbeatTimeout  time.Duration `mapstructure:"heartbeat_timeout"`
	ElectionTimeout   time.Duration `mapstructure:"election_timeout"`
	SnapshotInterval  time.Duration `mapstructure:"snapshot_interval"`
	SnapshotThreshold uint64        `mapstructure:"snapshot_threshold"`
}

type GRPCConfig struct {
	MasterPort int `mapstructure:"master_port"`
	WorkerPort int `mapstructure:"worker_port"`
	MaxMsgSize int `mapstructure:"max_msg_size"`
}

type WorkerConfig struct {
	WorkerID          string        `mapstructure:"worker_id"`
	MasterAddr        string        `mapstructure:"master_addr"`
	HeartbeatInterval time.Duration `mapstructure:"heartbeat_interval"`
	HeartbeatTimeout  time.Duration `mapstructure:"heartbeat_timeout"`
	MaxConcurrentJobs int           `mapstructure:"max_concurrent_jobs"`
}

type SchedulerConfig struct {
	SchedulingPolicy string        `mapstructure:"scheduling_policy"` // round-robin, least-loaded, priority, random, capacity-aware
	JobTimeout       time.Duration `mapstructure:"job_timeout"`
	MaxRetries       int           `mapstructure:"max_retries"`
}

type LogConfig struct {
	Level string `mapstructure:"level"`
}

// SecurityConfig holds all security-related configuration
type SecurityConfig struct {
	// TLS configuration for gRPC
	TLS TLSConfig `mapstructure:"tls"`

	// JWT configuration for authentication
	JWT JWTConfig `mapstructure:"jwt"`

	// RBAC configuration
	RBAC RBACConfig `mapstructure:"rbac"`

	// Raft TLS configuration
	RaftTLS TLSConfig `mapstructure:"raft_tls"`
}

type TLSConfig struct {
	Enabled      bool   `mapstructure:"enabled"`
	CertFile     string `mapstructure:"cert_file"`
	KeyFile      string `mapstructure:"key_file"`
	CAFile       string `mapstructure:"ca_file"`
	ServerName   string `mapstructure:"server_name"`
	SkipVerify   bool   `mapstructure:"skip_verify"`   // Only for development
	AutoGenerate bool   `mapstructure:"auto_generate"` // Auto-generate self-signed certs
}

type RBACConfig struct {
	Enabled bool `mapstructure:"enabled"`
}

type JWTConfig struct {
	SecretKey     string `mapstructure:"secret_key"`
	PublicKeyPath string `mapstructure:"public_key_path"`
	Issuer        string `mapstructure:"issuer"`
	Audience      string `mapstructure:"audience"`
	SkipExpiry    bool   `mapstructure:"skip_expiry"`
}

// Load loads configuration from file and environment variables
func Load(configPath string) (*Config, error) {
	v := viper.New()

	// Set defaults
	setDefaults(v)

	// Read from config file if provided
	if configPath != "" {
		v.SetConfigFile(configPath)
		if err := v.ReadInConfig(); err != nil {
			return nil, fmt.Errorf("failed to read config file: %w", err)
		}
	}

	// Environment variables override config file
	v.AutomaticEnv()
	v.SetEnvPrefix("") // No prefix for env vars

	// Bind specific environment variables
	v.BindEnv("worker.worker_id", "WORKER_ID")
	v.BindEnv("grpc.worker_port", "GRPC_WORKER_PORT")
	v.BindEnv("grpc.master_port", "GRPC_MASTER_PORT")
	v.BindEnv("worker.master_addr", "MASTER_ADDR")
	v.BindEnv("cluster.node_id", "NODE_ID")
	v.BindEnv("cluster.bind_addr", "BIND_ADDR")
	v.BindEnv("cluster.raft_dir", "RAFT_DIR")
	v.BindEnv("cluster.bootstrap", "BOOTSTRAP")
	v.BindEnv("cluster.join_addr", "JOIN_ADDR")

	// Security environment variables
	v.BindEnv("security.tls.enabled", "SECURITY_TLS_ENABLED")
	v.BindEnv("security.tls.auto_generate", "SECURITY_TLS_AUTO_GENERATE")
	v.BindEnv("security.tls.skip_verify", "SECURITY_TLS_SKIP_VERIFY")
	v.BindEnv("security.jwt.skip_expiry", "SECURITY_JWT_SKIP_EXPIRY")
	v.BindEnv("security.rbac.enabled", "SECURITY_RBAC_ENABLED")
	v.BindEnv("security.raft_tls.enabled", "SECURITY_RAFT_TLS_ENABLED")
	v.BindEnv("security.raft_tls.auto_generate", "SECURITY_RAFT_TLS_AUTO_GENERATE")
	v.BindEnv("security.raft_tls.skip_verify", "SECURITY_RAFT_TLS_SKIP_VERIFY")

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	// Validate configuration
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	return &cfg, nil
}

// setDefaults sets default configuration values
func setDefaults(v *viper.Viper) {
	// Cluster defaults
	v.SetDefault("cluster.node_id", "node-1")
	v.SetDefault("cluster.bind_addr", "127.0.0.1:7000")
	v.SetDefault("cluster.raft_dir", "/tmp/conductor/raft")
	v.SetDefault("cluster.data_dir", "/tmp/conductor/data")
	v.SetDefault("cluster.bootstrap", true)
	v.SetDefault("cluster.join_addr", "")

	// Raft defaults
	v.SetDefault("raft.heartbeat_timeout", "1s")
	v.SetDefault("raft.election_timeout", "1s")
	v.SetDefault("raft.snapshot_interval", "120s")
	v.SetDefault("raft.snapshot_threshold", 8192) // Snapshot after 8K log entries (optimized for production)

	// gRPC defaults
	v.SetDefault("grpc.master_port", 9000)
	v.SetDefault("grpc.worker_port", 9001)
	v.SetDefault("grpc.max_msg_size", 4194304) // 4MB

	// Worker defaults
	v.SetDefault("worker.worker_id", "worker-1")
	v.SetDefault("worker.master_addr", "localhost:9000")
	v.SetDefault("worker.heartbeat_interval", "3s")
	v.SetDefault("worker.heartbeat_timeout", "10s")
	v.SetDefault("worker.max_concurrent_jobs", 10)

	// Scheduler defaults
	v.SetDefault("scheduler.scheduling_policy", "least-loaded") // round-robin, least-loaded, priority, random, capacity-aware
	v.SetDefault("scheduler.job_timeout", "5m")
	v.SetDefault("scheduler.max_retries", 3)

	// Log defaults
	v.SetDefault("log.level", "info")

	// Security defaults
	// TLS for gRPC
	v.SetDefault("security.tls.enabled", false) // Disabled by default for development
	v.SetDefault("security.tls.cert_file", "/tmp/conductor/certs/server.crt")
	v.SetDefault("security.tls.key_file", "/tmp/conductor/certs/server.key")
	v.SetDefault("security.tls.ca_file", "/tmp/conductor/certs/ca.crt")
	v.SetDefault("security.tls.server_name", "localhost")
	v.SetDefault("security.tls.skip_verify", true) // Development mode
	v.SetDefault("security.tls.auto_generate", true)

	// JWT defaults
	v.SetDefault("security.jwt.secret_key", "dev-secret-key-conductor-2026-change-in-production")
	v.SetDefault("security.jwt.issuer", "conductor-system")
	v.SetDefault("security.jwt.audience", "conductor-api")
	v.SetDefault("security.jwt.skip_expiry", true) // Development mode

	// RBAC defaults
	v.SetDefault("security.rbac.enabled", false) // Disabled by default for development

	// Raft TLS defaults
	v.SetDefault("security.raft_tls.enabled", false) // Disabled by default
	v.SetDefault("security.raft_tls.cert_file", "/tmp/conductor/certs/raft.crt")
	v.SetDefault("security.raft_tls.key_file", "/tmp/conductor/certs/raft.key")
	v.SetDefault("security.raft_tls.ca_file", "/tmp/conductor/certs/ca.crt")
	v.SetDefault("security.raft_tls.skip_verify", true)
	v.SetDefault("security.raft_tls.auto_generate", true)
}

// Validate validates the configuration
func (c *Config) Validate() error {
	if c.Cluster.NodeID == "" {
		return fmt.Errorf("cluster.node_id is required")
	}
	if c.Cluster.BindAddr == "" {
		return fmt.Errorf("cluster.bind_addr is required")
	}
	if c.GRPC.MasterPort <= 0 || c.GRPC.MasterPort > 65535 {
		return fmt.Errorf("grpc.master_port must be between 1 and 65535")
	}
	if c.Worker.MaxConcurrentJobs <= 0 {
		return fmt.Errorf("worker.max_concurrent_jobs must be positive")
	}
	if c.Scheduler.MaxRetries < 0 {
		return fmt.Errorf("scheduler.max_retries must be non-negative")
	}
	return nil
}
