package config

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadDefaults(t *testing.T) {
	cfg, err := Load("")
	require.NoError(t, err)
	assert.NotNil(t, cfg)

	// Check defaults
	assert.Equal(t, "node-1", cfg.Cluster.NodeID)
	assert.Equal(t, "127.0.0.1:7000", cfg.Cluster.BindAddr)
	assert.Equal(t, 9000, cfg.GRPC.MasterPort)
	assert.Equal(t, "least-loaded", cfg.Scheduler.SchedulingPolicy)
}

func TestLoadFromFile(t *testing.T) {
	// Create a temporary config file
	configContent := `
cluster:
  node_id: "test-node"
  bind_addr: "0.0.0.0:8000"
  raft_dir: "/tmp/test-raft"

grpc:
  master_port: 9999

log:
  level: "debug"
`
	tmpFile, err := os.CreateTemp("", "config-*.yaml")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())

	_, err = tmpFile.WriteString(configContent)
	require.NoError(t, err)
	tmpFile.Close()

	cfg, err := Load(tmpFile.Name())
	require.NoError(t, err)

	assert.Equal(t, "test-node", cfg.Cluster.NodeID)
	assert.Equal(t, "0.0.0.0:8000", cfg.Cluster.BindAddr)
	assert.Equal(t, 9999, cfg.GRPC.MasterPort)
	assert.Equal(t, "debug", cfg.Log.Level)
}

func TestValidation(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		wantErr bool
	}{
		{
			name: "valid config",
			cfg: Config{
				Profile: ProfileDevelopment,
				Cluster: ClusterConfig{
					NodeID:   "node-1",
					BindAddr: "127.0.0.1:7000",
					RaftDir:  "/tmp/raft",
				},
				GRPC: GRPCConfig{
					MasterPort: 9000,
					WorkerPort: 9001,
				},
				Worker: WorkerConfig{
					MaxConcurrentJobs: 10,
					HeartbeatInterval: 3 * time.Second,
					HeartbeatTimeout:  10 * time.Second,
				},
				Scheduler: SchedulerConfig{
					SchedulingPolicy: "least-loaded",
					MaxRetries:       3,
				},
				Raft: RaftConfig{
					HeartbeatTimeout: 1 * time.Second,
					ElectionTimeout:  3 * time.Second,
				},
			},
			wantErr: false,
		},
		{
			name: "empty node ID",
			cfg: Config{
				Cluster: ClusterConfig{
					NodeID:   "",
					BindAddr: "127.0.0.1:7000",
				},
			},
			wantErr: true,
		},
		{
			name: "invalid port",
			cfg: Config{
				Cluster: ClusterConfig{
					NodeID:   "node-1",
					BindAddr: "127.0.0.1:7000",
				},
				GRPC: GRPCConfig{
					MasterPort: 99999,
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestRaftConfigParsing(t *testing.T) {
	// Development profile has different defaults
	os.Setenv("CONDUCTOR_PROFILE", "dev")
	defer os.Unsetenv("CONDUCTOR_PROFILE")

	cfg, err := Load("")
	require.NoError(t, err)

	// Dev profile has faster timeouts for quick iteration
	assert.Equal(t, 500*time.Millisecond, cfg.Raft.HeartbeatTimeout)
	assert.Equal(t, 1*time.Second, cfg.Raft.ElectionTimeout)
	assert.Equal(t, 30*time.Second, cfg.Raft.SnapshotInterval)
	assert.Equal(t, uint64(100), cfg.Raft.SnapshotThreshold)
}
