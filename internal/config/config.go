package config

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/spf13/viper"
)

// Profile represents the runtime environment
type Profile string

const (
	ProfileDevelopment Profile = "development"
	ProfileStaging     Profile = "staging"
	ProfileProduction  Profile = "production"
)

// Config holds all configuration for the system
type Config struct {
	Profile   Profile         `mapstructure:"profile"`
	Cluster   ClusterConfig   `mapstructure:"cluster"`
	Raft      RaftConfig      `mapstructure:"raft"`
	GRPC      GRPCConfig      `mapstructure:"grpc"`
	Worker    WorkerConfig    `mapstructure:"worker"`
	Scheduler SchedulerConfig `mapstructure:"scheduler"`
	Log       LogConfig       `mapstructure:"log"`
	Security  SecurityConfig  `mapstructure:"security"`
	Metrics   MetricsConfig   `mapstructure:"metrics"`

	mu    sync.RWMutex
	viper *viper.Viper
}

// ValidationError represents configuration validation errors
type ValidationError struct {
	Field   string
	Message string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("%s: %s", e.Field, e.Message)
}

type ValidationErrors []ValidationError

func (ve ValidationErrors) Error() string {
	var sb strings.Builder
	sb.WriteString("configuration validation failed:\n")
	for _, err := range ve {
		sb.WriteString(fmt.Sprintf("  - %s\n", err.Error()))
	}
	return sb.String()
}

type ClusterConfig struct {
	NodeID    string `mapstructure:"node_id"`
	BindAddr  string `mapstructure:"bind_addr"`
	RaftDir   string `mapstructure:"raft_dir"`
	DataDir   string `mapstructure:"data_dir"`
	Bootstrap bool   `mapstructure:"bootstrap"`
	JoinAddr  string `mapstructure:"join_addr"` // Address of existing cluster member to join
	HTTPPort  int    `mapstructure:"http_port"`
}

type RaftConfig struct {
	HeartbeatTimeout  time.Duration `mapstructure:"heartbeat_timeout"`
	ElectionTimeout   time.Duration `mapstructure:"election_timeout"`
	BarrierTimeout    time.Duration `mapstructure:"barrier_timeout"` // Timeout for Raft barrier operations
	SnapshotInterval  time.Duration `mapstructure:"snapshot_interval"`
	SnapshotThreshold uint64        `mapstructure:"snapshot_threshold"`
	ApplyTimeout      time.Duration `mapstructure:"apply_timeout"` // Timeout for Raft Apply operations
}

type GRPCConfig struct {
	MasterPort     int           `mapstructure:"master_port"`
	WorkerPort     int           `mapstructure:"worker_port"`
	MaxMsgSize     int           `mapstructure:"max_msg_size"`
	DialTimeout    time.Duration `mapstructure:"dial_timeout"`    // Timeout for establishing gRPC connections
	ConnectionWait time.Duration `mapstructure:"connection_wait"` // Timeout for waiting on connection state changes
}

type WorkerConfig struct {
	WorkerID          string        `mapstructure:"worker_id"`
	MasterAddr        string        `mapstructure:"master_addr"`
	HeartbeatInterval time.Duration `mapstructure:"heartbeat_interval"`
	HeartbeatTimeout  time.Duration `mapstructure:"heartbeat_timeout"`
	ResultTimeout     time.Duration `mapstructure:"result_timeout"` // Timeout for reporting job results
	MaxConcurrentJobs int           `mapstructure:"max_concurrent_jobs"`
	ShutdownDelay     time.Duration `mapstructure:"shutdown_delay"` // Grace period for in-flight jobs during shutdown
}

type SchedulerConfig struct {
	SchedulingPolicy  string        `mapstructure:"scheduling_policy"` // round-robin, least-loaded, priority, random, capacity-aware
	JobTimeout        time.Duration `mapstructure:"job_timeout"`
	AssignmentTimeout time.Duration `mapstructure:"assignment_timeout"` // Timeout for assigning job to worker
	MaxRetries        int           `mapstructure:"max_retries"`
	RetryDelay        time.Duration `mapstructure:"retry_delay"`
	RebalanceInterval time.Duration `mapstructure:"rebalance_interval"`
	MaxPendingJobs    int           `mapstructure:"max_pending_jobs"` // Maximum number of pending jobs allowed (backpressure)
}

type LogConfig struct {
	Level        string `mapstructure:"level"`
	Format       string `mapstructure:"format"`
	Output       string `mapstructure:"output"`
	AuditEnabled bool   `mapstructure:"audit_enabled"`
	AuditOutput  string `mapstructure:"audit_output"`
}

type MetricsConfig struct {
	Enabled         bool          `mapstructure:"enabled"`
	Port            int           `mapstructure:"port"`
	Path            string        `mapstructure:"path"`
	CollectInterval time.Duration `mapstructure:"collect_interval"`
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

	// Rate limiting configuration
	RateLimit RateLimitConfig `mapstructure:"rate_limit"`
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
	Enabled    bool   `mapstructure:"enabled"`
	PolicyFile string `mapstructure:"policy_file"`
}

type JWTConfig struct {
	SecretKey       string        `mapstructure:"secret_key"`
	PublicKeyPath   string        `mapstructure:"public_key_path"`
	Issuer          string        `mapstructure:"issuer"`
	Audience        string        `mapstructure:"audience"`
	AccessTokenTTL  time.Duration `mapstructure:"access_token_ttl"`
	RefreshTokenTTL time.Duration `mapstructure:"refresh_token_ttl"`
	SkipExpiry      bool          `mapstructure:"skip_expiry"`
}

type RateLimitConfig struct {
	Enabled         bool          `mapstructure:"enabled"`
	RequestsPerSec  float64       `mapstructure:"requests_per_sec"`
	Burst           int           `mapstructure:"burst"`
	CleanupInterval time.Duration `mapstructure:"cleanup_interval"` // How often to clean up inactive limiters
	ClientTTL       time.Duration `mapstructure:"client_ttl"`       // How long to keep inactive client limiters
}

// Load loads configuration with profile-based defaults
// Priority order (highest to lowest):
// 1. Environment variables (with CONDUCTOR_ prefix)
// 2. Config file (./config/{profile}.yaml or provided path)
// 3. Profile-specific defaults
func Load(configPath string) (*Config, error) {
	v := viper.New()

	// Determine profile from environment
	profile := Profile(os.Getenv("CONDUCTOR_PROFILE"))
	if profile == "" {
		profile = ProfileDevelopment
	}

	// Set profile-specific defaults first (lowest priority)
	setProfileDefaults(v, profile)

	// Auto-discover config file based on profile if not explicitly provided
	if configPath == "" {
		// Map profile to config filename
		configFile := "dev.yaml"
		switch profile {
		case ProfileDevelopment:
			configFile = "dev.yaml"
		case ProfileStaging:
			configFile = "staging.yaml"
		case ProfileProduction:
			configFile = "prod.yaml"
		}

		configPath = fmt.Sprintf("./config/%s", configFile)
	}

	// Read and expand configuration file
	content, err := os.ReadFile(configPath)
	if err != nil {
		// Only ignore if it's the auto-discovered path and it doesn't exist
		if os.IsNotExist(err) && configPath == fmt.Sprintf("./config/%s", "dev.yaml") {
			// Fallback to defaults
		} else {
			return nil, fmt.Errorf("failed to read config file '%s': %w", configPath, err)
		}
	} else {
		// Expand environment variables
		expandedContent := os.ExpandEnv(string(content))
		v.SetConfigType("yaml")
		if err := v.ReadConfig(strings.NewReader(expandedContent)); err != nil {
			return nil, fmt.Errorf("failed to parse config file '%s': %w", configPath, err)
		}
	}

	// Environment variables override everything (highest priority)
	v.SetEnvPrefix("CONDUCTOR")
	v.AutomaticEnv()
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	// Bind specific environment variables for backwards compatibility
	bindEnvVars(v)

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	cfg.Profile = profile
	cfg.viper = v

	// Validate configuration with detailed errors
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return &cfg, nil
}

// bindEnvVars binds all environment variables
func bindEnvVars(v *viper.Viper) {
	// Cluster
	v.BindEnv("cluster.node_id", "NODE_ID")
	v.BindEnv("cluster.bind_addr", "BIND_ADDR")
	v.BindEnv("cluster.http_port", "HTTP_PORT")
	v.BindEnv("cluster.raft_dir", "RAFT_DIR")
	v.BindEnv("cluster.data_dir", "DATA_DIR")
	v.BindEnv("cluster.bootstrap", "BOOTSTRAP")
	v.BindEnv("cluster.join_addr", "JOIN_ADDR")

	// Raft
	v.BindEnv("raft.heartbeat_timeout", "RAFT_HEARTBEAT_TIMEOUT")
	v.BindEnv("raft.election_timeout", "RAFT_ELECTION_TIMEOUT")
	v.BindEnv("raft.barrier_timeout", "RAFT_BARRIER_TIMEOUT")
	v.BindEnv("raft.snapshot_interval", "RAFT_SNAPSHOT_INTERVAL")
	v.BindEnv("raft.snapshot_threshold", "RAFT_SNAPSHOT_THRESHOLD")
	v.BindEnv("raft.apply_timeout", "RAFT_APPLY_TIMEOUT")

	// gRPC
	v.BindEnv("grpc.master_port", "GRPC_MASTER_PORT")
	v.BindEnv("grpc.worker_port", "GRPC_WORKER_PORT")
	v.BindEnv("grpc.max_msg_size", "GRPC_MAX_MSG_SIZE")
	v.BindEnv("grpc.dial_timeout", "GRPC_DIAL_TIMEOUT")
	v.BindEnv("grpc.connection_wait", "GRPC_CONNECTION_WAIT")

	// Worker
	v.BindEnv("worker.worker_id", "WORKER_ID")
	v.BindEnv("worker.master_addr", "MASTER_ADDR")
	v.BindEnv("worker.heartbeat_interval", "WORKER_HEARTBEAT_INTERVAL")
	v.BindEnv("worker.heartbeat_timeout", "WORKER_HEARTBEAT_TIMEOUT")
	v.BindEnv("worker.result_timeout", "WORKER_RESULT_TIMEOUT")
	v.BindEnv("worker.max_concurrent_jobs", "WORKER_MAX_CONCURRENT_JOBS")
	v.BindEnv("worker.shutdown_delay", "WORKER_SHUTDOWN_DELAY")

	// Scheduler
	v.BindEnv("scheduler.scheduling_policy", "SCHEDULER_POLICY")
	v.BindEnv("scheduler.job_timeout", "SCHEDULER_JOB_TIMEOUT")
	v.BindEnv("scheduler.assignment_timeout", "SCHEDULER_ASSIGNMENT_TIMEOUT")
	v.BindEnv("scheduler.max_retries", "SCHEDULER_MAX_RETRIES")
	v.BindEnv("scheduler.retry_delay", "SCHEDULER_RETRY_DELAY")
	v.BindEnv("scheduler.rebalance_interval", "SCHEDULER_REBALANCE_INTERVAL")
	v.BindEnv("scheduler.max_pending_jobs", "SCHEDULER_MAX_PENDING_JOBS")

	// Logging
	v.BindEnv("log.level", "LOG_LEVEL")
	v.BindEnv("log.format", "LOG_FORMAT")
	v.BindEnv("log.output", "LOG_OUTPUT")
	v.BindEnv("log.audit_enabled", "LOG_AUDIT_ENABLED")
	v.BindEnv("log.audit_output", "LOG_AUDIT_OUTPUT")

	// Metrics
	v.BindEnv("metrics.enabled", "METRICS_ENABLED")
	v.BindEnv("metrics.port", "METRICS_PORT")
	v.BindEnv("metrics.path", "METRICS_PATH")
	v.BindEnv("metrics.collect_interval", "METRICS_COLLECT_INTERVAL")

	// Security - TLS
	v.BindEnv("security.tls.enabled", "SECURITY_TLS_ENABLED")
	v.BindEnv("security.tls.auto_generate", "SECURITY_TLS_AUTO_GENERATE")
	v.BindEnv("security.tls.skip_verify", "SECURITY_TLS_SKIP_VERIFY")
	v.BindEnv("security.tls.cert_file", "SECURITY_TLS_CERT_FILE")
	v.BindEnv("security.tls.key_file", "SECURITY_TLS_KEY_FILE")
	v.BindEnv("security.tls.ca_file", "SECURITY_TLS_CA_FILE")
	v.BindEnv("security.tls.server_name", "SECURITY_TLS_SERVER_NAME")

	// Security - JWT
	v.BindEnv("security.jwt.secret_key", "JWT_SECRET_KEY")
	v.BindEnv("security.jwt.public_key_path", "JWT_PUBLIC_KEY_PATH")
	v.BindEnv("security.jwt.issuer", "JWT_ISSUER")
	v.BindEnv("security.jwt.audience", "JWT_AUDIENCE")
	v.BindEnv("security.jwt.access_token_ttl", "JWT_ACCESS_TOKEN_TTL")
	v.BindEnv("security.jwt.refresh_token_ttl", "JWT_REFRESH_TOKEN_TTL")
	v.BindEnv("security.jwt.skip_expiry", "JWT_SKIP_EXPIRY")

	// Security - RBAC
	v.BindEnv("security.rbac.enabled", "RBAC_ENABLED")
	v.BindEnv("security.rbac.policy_file", "RBAC_POLICY_FILE")

	// Security - Rate Limiting
	v.BindEnv("security.rate_limit.enabled", "SECURITY_RATE_LIMIT_ENABLED")
	v.BindEnv("security.rate_limit.requests_per_sec", "SECURITY_RATE_LIMIT_REQUESTS_PER_SEC")
	v.BindEnv("security.rate_limit.burst", "SECURITY_RATE_LIMIT_BURST")
	v.BindEnv("security.rate_limit.cleanup_interval", "SECURITY_RATE_LIMIT_CLEANUP_INTERVAL")
	v.BindEnv("security.rate_limit.client_ttl", "SECURITY_RATE_LIMIT_CLIENT_TTL")

	// Security - Raft TLS
	v.BindEnv("security.raft_tls.enabled", "SECURITY_RAFT_TLS_ENABLED")
	v.BindEnv("security.raft_tls.auto_generate", "SECURITY_RAFT_TLS_AUTO_GENERATE")
	v.BindEnv("security.raft_tls.skip_verify", "SECURITY_RAFT_TLS_SKIP_VERIFY")
	v.BindEnv("security.raft_tls.cert_file", "SECURITY_RAFT_TLS_CERT_FILE")
	v.BindEnv("security.raft_tls.key_file", "SECURITY_RAFT_TLS_KEY_FILE")
	v.BindEnv("security.raft_tls.ca_file", "SECURITY_RAFT_TLS_CA_FILE")
}

// setProfileDefaults sets profile-specific defaults
func setProfileDefaults(v *viper.Viper, profile Profile) {
	// Common defaults
	setCommonDefaults(v)

	// Profile-specific overrides
	switch profile {
	case ProfileDevelopment:
		setDevDefaults(v)
	case ProfileStaging:
		setStagingDefaults(v)
	case ProfileProduction:
		setProdDefaults(v)
	}
}

// setCommonDefaults sets defaults common to all profiles
func setCommonDefaults(v *viper.Viper) {
	// Cluster
	v.SetDefault("cluster.node_id", "node-1")
	v.SetDefault("cluster.bind_addr", "127.0.0.1:7000")
	v.SetDefault("cluster.bootstrap", true)
	v.SetDefault("cluster.join_addr", "")
	v.SetDefault("cluster.http_port", 8080)

	// gRPC
	v.SetDefault("grpc.master_port", 9000)
	v.SetDefault("grpc.worker_port", 9001)
	v.SetDefault("grpc.max_msg_size", 4*1024*1024) // 4MB
	v.SetDefault("grpc.dial_timeout", "2s")
	v.SetDefault("grpc.connection_wait", "2s")

	// Worker
	v.SetDefault("worker.worker_id", "worker-1")
	v.SetDefault("worker.master_addr", "localhost:9000")
	v.SetDefault("worker.heartbeat_interval", "3s")
	v.SetDefault("worker.heartbeat_timeout", "10s")
	v.SetDefault("worker.result_timeout", "10s")
	v.SetDefault("worker.max_concurrent_jobs", 10)
	v.SetDefault("worker.shutdown_delay", "2s")

	// Scheduler
	v.SetDefault("scheduler.scheduling_policy", "least-loaded")
	v.SetDefault("scheduler.job_timeout", "5m")
	v.SetDefault("scheduler.assignment_timeout", "5s")
	v.SetDefault("scheduler.max_retries", 3)
	v.SetDefault("scheduler.retry_delay", "30s")
	v.SetDefault("scheduler.rebalance_interval", "1m")
	v.SetDefault("scheduler.max_pending_jobs", 10000) // Backpressure limit

	// Logging
	v.SetDefault("log.level", "info")
	v.SetDefault("log.format", "json")
	v.SetDefault("log.output", "stdout")
	v.SetDefault("log.audit_enabled", true)
	v.SetDefault("log.audit_output", "stdout")

	// Metrics
	v.SetDefault("metrics.enabled", true)
	v.SetDefault("metrics.port", 9090)
	v.SetDefault("metrics.path", "/metrics")
	v.SetDefault("metrics.collect_interval", "15s")

	// JWT
	v.SetDefault("security.jwt.secret_key", "change-this-secret-key")
	v.SetDefault("security.jwt.issuer", "conductor")
	v.SetDefault("security.jwt.audience", "conductor")
	v.SetDefault("security.jwt.access_token_ttl", "15m")
	v.SetDefault("security.jwt.refresh_token_ttl", "168h") // 7 days

	// RBAC
	v.SetDefault("security.rbac.policy_file", "./config/rbac-policies.json")

	// Rate limiting defaults
	v.SetDefault("security.rate_limit.enabled", true)
	v.SetDefault("security.rate_limit.requests_per_sec", 100.0)
	v.SetDefault("security.rate_limit.burst", 50)
	v.SetDefault("security.rate_limit.cleanup_interval", "5m")
	v.SetDefault("security.rate_limit.client_ttl", "30m")
}

// setDevDefaults sets development-specific defaults
func setDevDefaults(v *viper.Viper) {
	// Use /tmp for dev
	v.SetDefault("cluster.raft_dir", "/tmp/conductor/raft")
	v.SetDefault("cluster.data_dir", "/tmp/conductor/data")

	// Fast timeouts for quick iteration
	v.SetDefault("raft.heartbeat_timeout", "500ms")
	v.SetDefault("raft.election_timeout", "1s")
	v.SetDefault("raft.barrier_timeout", "5s")
	v.SetDefault("raft.snapshot_interval", "30s")
	v.SetDefault("raft.snapshot_threshold", uint64(100)) // Snapshot frequently
	v.SetDefault("raft.apply_timeout", "5s")             // Faster for dev

	// Debug logging
	v.SetDefault("log.level", "debug")

	// Relaxed security
	v.SetDefault("security.tls.enabled", false)
	v.SetDefault("security.tls.skip_verify", true)
	v.SetDefault("security.tls.auto_generate", true)
	v.SetDefault("security.jwt.skip_expiry", true)
	v.SetDefault("security.rbac.enabled", false)
	v.SetDefault("security.raft_tls.enabled", false)
}

// setStagingDefaults sets staging-specific defaults
func setStagingDefaults(v *viper.Viper) {
	// Persistent storage
	v.SetDefault("cluster.raft_dir", "/var/lib/conductor/raft")
	v.SetDefault("cluster.data_dir", "/var/lib/conductor/data")

	// Production-like timeouts
	v.SetDefault("raft.heartbeat_timeout", "1s")
	v.SetDefault("raft.election_timeout", "3s")
	v.SetDefault("raft.barrier_timeout", "7s")
	v.SetDefault("raft.snapshot_interval", "120s")
	v.SetDefault("raft.snapshot_threshold", uint64(8192))
	v.SetDefault("raft.apply_timeout", "10s")
	// Info logging
	v.SetDefault("log.level", "info")

	// Security enabled but relaxed
	v.SetDefault("security.tls.enabled", true)
	v.SetDefault("security.tls.skip_verify", false)
	v.SetDefault("security.tls.auto_generate", false)
	v.SetDefault("security.tls.cert_file", "/etc/conductor/certs/server.crt")
	v.SetDefault("security.tls.key_file", "/etc/conductor/certs/server.key")
	v.SetDefault("security.tls.ca_file", "/etc/conductor/certs/ca.crt")
	v.SetDefault("security.jwt.skip_expiry", false)
	v.SetDefault("security.rbac.enabled", true)
	v.SetDefault("security.raft_tls.enabled", true)
	v.SetDefault("security.raft_tls.cert_file", "/etc/conductor/certs/raft.crt")
	v.SetDefault("security.raft_tls.key_file", "/etc/conductor/certs/raft.key")
	v.SetDefault("security.raft_tls.ca_file", "/etc/conductor/certs/ca.crt")
}

// setProdDefaults sets production-specific defaults
func setProdDefaults(v *viper.Viper) {
	// Production storage paths
	v.SetDefault("cluster.raft_dir", "/var/lib/conductor/raft")
	v.SetDefault("cluster.data_dir", "/var/lib/conductor/data")

	// Conservative timeouts for stability
	v.SetDefault("raft.heartbeat_timeout", "1s")
	v.SetDefault("raft.election_timeout", "5s")
	v.SetDefault("raft.barrier_timeout", "10s")
	v.SetDefault("raft.snapshot_interval", "300s")         // 5 minutes
	v.SetDefault("raft.snapshot_threshold", uint64(16384)) // 16K entries\tv.SetDefault("raft.apply_timeout", "10s")
	// Minimal logging
	v.SetDefault("log.level", "warn")

	// Full security
	v.SetDefault("security.tls.enabled", true)
	v.SetDefault("security.tls.skip_verify", false)
	v.SetDefault("security.tls.auto_generate", false)
	v.SetDefault("security.tls.cert_file", "/etc/conductor/certs/server.crt")
	v.SetDefault("security.tls.key_file", "/etc/conductor/certs/server.key")
	v.SetDefault("security.tls.ca_file", "/etc/conductor/certs/ca.crt")
	v.SetDefault("security.jwt.skip_expiry", false)
	v.SetDefault("security.rbac.enabled", true)
	v.SetDefault("security.raft_tls.enabled", true)
	v.SetDefault("security.raft_tls.cert_file", "/etc/conductor/certs/raft.crt")
	v.SetDefault("security.raft_tls.key_file", "/etc/conductor/certs/raft.key")
	v.SetDefault("security.raft_tls.ca_file", "/etc/conductor/certs/ca.crt")
}

// Validate validates the configuration with detailed error messages
func (c *Config) Validate() error {
	var errors ValidationErrors

	// Cluster validation
	if c.Cluster.NodeID == "" {
		errors = append(errors, ValidationError{"cluster.node_id", "required field is empty"})
	}
	if c.Cluster.BindAddr == "" {
		errors = append(errors, ValidationError{"cluster.bind_addr", "required field is empty"})
	}
	if c.Cluster.RaftDir == "" {
		errors = append(errors, ValidationError{"cluster.raft_dir", "required field is empty"})
	}

	// Port validation
	if c.GRPC.MasterPort <= 0 || c.GRPC.MasterPort > 65535 {
		errors = append(errors, ValidationError{"grpc.master_port", fmt.Sprintf("must be between 1 and 65535, got %d", c.GRPC.MasterPort)})
	}
	if c.GRPC.WorkerPort <= 0 || c.GRPC.WorkerPort > 65535 {
		errors = append(errors, ValidationError{"grpc.worker_port", fmt.Sprintf("must be between 1 and 65535, got %d", c.GRPC.WorkerPort)})
	}

	// Worker validation
	if c.Worker.MaxConcurrentJobs <= 0 {
		errors = append(errors, ValidationError{"worker.max_concurrent_jobs", fmt.Sprintf("must be positive, got %d", c.Worker.MaxConcurrentJobs)})
	}
	if c.Worker.HeartbeatInterval <= 0 {
		errors = append(errors, ValidationError{"worker.heartbeat_interval", "must be positive duration"})
	}
	if c.Worker.HeartbeatTimeout <= c.Worker.HeartbeatInterval {
		errors = append(errors, ValidationError{"worker.heartbeat_timeout", "must be greater than heartbeat_interval"})
	}

	// Scheduler validation
	if c.Scheduler.MaxRetries < 0 {
		errors = append(errors, ValidationError{"scheduler.max_retries", fmt.Sprintf("must be non-negative, got %d", c.Scheduler.MaxRetries)})
	}
	validPolicies := map[string]bool{
		"round-robin": true, "least-loaded": true, "priority": true,
		"random": true, "capacity-aware": true,
	}
	if !validPolicies[c.Scheduler.SchedulingPolicy] {
		errors = append(errors, ValidationError{"scheduler.scheduling_policy", fmt.Sprintf("invalid policy '%s'", c.Scheduler.SchedulingPolicy)})
	}

	// Raft validation
	if c.Raft.HeartbeatTimeout <= 0 {
		errors = append(errors, ValidationError{"raft.heartbeat_timeout", "must be positive duration"})
	}
	if c.Raft.ElectionTimeout <= c.Raft.HeartbeatTimeout {
		errors = append(errors, ValidationError{"raft.election_timeout", "must be greater than heartbeat_timeout"})
	}

	// Security validation for production
	if c.Profile == ProfileProduction {
		if c.Security.TLS.Enabled && c.Security.TLS.AutoGenerate {
			errors = append(errors, ValidationError{"security.tls.auto_generate", "cannot be enabled in production"})
		}
		if c.Security.TLS.Enabled && c.Security.TLS.SkipVerify {
			errors = append(errors, ValidationError{"security.tls.skip_verify", "cannot be enabled in production"})
		}
		if c.Security.JWT.SkipExpiry {
			errors = append(errors, ValidationError{"security.jwt.skip_expiry", "cannot be enabled in production"})
		}
		if !c.Security.RBAC.Enabled {
			errors = append(errors, ValidationError{"security.rbac.enabled", "must be enabled in production"})
		}
		// Require JWT secret to be set (not empty or placeholder)
		if c.Security.JWT.SecretKey == "" || c.Security.JWT.SecretKey == "${JWT_SECRET_KEY}" || c.Security.JWT.SecretKey == "${CONDUCTOR_JWT_SECRET}" {
			errors = append(errors, ValidationError{"security.jwt.secret_key", "must be set via JWT_SECRET_KEY environment variable in production"})
		}
	}

	if len(errors) > 0 {
		return errors
	}
	return nil
}

// Reload reloads non-critical configuration settings (hot-reload)
func (c *Config) Reload() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.viper == nil {
		return fmt.Errorf("cannot reload: viper not initialized")
	}

	if err := c.viper.ReadInConfig(); err != nil {
		return fmt.Errorf("failed to reload config: %w", err)
	}

	// Only reload safe-to-change settings
	var newCfg Config
	if err := c.viper.Unmarshal(&newCfg); err != nil {
		return fmt.Errorf("failed to unmarshal reloaded config: %w", err)
	}

	// Hot-reloadable settings (non-critical)
	c.Log.Level = newCfg.Log.Level
	c.Worker.MaxConcurrentJobs = newCfg.Worker.MaxConcurrentJobs
	c.Scheduler.JobTimeout = newCfg.Scheduler.JobTimeout

	return nil
}

// GetLogLevel returns current log level (thread-safe)
func (c *Config) GetLogLevel() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Log.Level
}

// GetMaxConcurrentJobs returns current max concurrent jobs (thread-safe)
func (c *Config) GetMaxConcurrentJobs() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Worker.MaxConcurrentJobs
}
