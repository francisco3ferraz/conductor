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
}

type ClusterConfig struct {
	NodeID   string `mapstructure:"node_id"`
	BindAddr string `mapstructure:"bind_addr"`
	RaftDir  string `mapstructure:"raft_dir"`
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
	AssignmentStrategy string        `mapstructure:"assignment_strategy"`
	JobTimeout         time.Duration `mapstructure:"job_timeout"`
	MaxRetries         int           `mapstructure:"max_retries"`
}

type LogConfig struct {
	Level string `mapstructure:"level"`
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

	// Raft defaults
	v.SetDefault("raft.heartbeat_timeout", "1s")
	v.SetDefault("raft.election_timeout", "1s")
	v.SetDefault("raft.snapshot_interval", "120s")
	v.SetDefault("raft.snapshot_threshold", 8192)

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
	v.SetDefault("scheduler.assignment_strategy", "least_loaded")
	v.SetDefault("scheduler.job_timeout", "5m")
	v.SetDefault("scheduler.max_retries", 3)

	// Log defaults
	v.SetDefault("log.level", "info")
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
