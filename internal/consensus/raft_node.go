package consensus

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"go.uber.org/zap"
)

// RaftNode wraps the HashiCorp Raft implementation
type RaftNode struct {
	raft      *raft.Raft
	fsm       *FSM
	config    *Config
	logger    *zap.Logger
	transport *raft.NetworkTransport
}

// Config holds Raft node configuration
type Config struct {
	NodeID    string
	BindAddr  string
	DataDir   string
	Bootstrap bool

	// Raft timeouts
	HeartbeatTimeout time.Duration
	ElectionTimeout  time.Duration

	// Snapshot config
	SnapshotInterval  time.Duration
	SnapshotThreshold uint64
}

// NewRaftNode creates a new Raft node
func NewRaftNode(cfg *Config, fsm *FSM, logger *zap.Logger) (*RaftNode, error) {
	if logger == nil {
		return nil, fmt.Errorf("logger is required")
	}

	rn := &RaftNode{
		fsm:    fsm,
		config: cfg,
		logger: logger,
	}

	// Set up Raft configuration
	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(cfg.NodeID)
	raftConfig.HeartbeatTimeout = cfg.HeartbeatTimeout
	raftConfig.ElectionTimeout = cfg.ElectionTimeout
	raftConfig.SnapshotInterval = cfg.SnapshotInterval
	raftConfig.SnapshotThreshold = cfg.SnapshotThreshold

	// Create data directory
	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	// Set up log store using BoltDB
	logStore, err := raftboltdb.NewBoltStore(filepath.Join(cfg.DataDir, "raft-log.db"))
	if err != nil {
		return nil, fmt.Errorf("failed to create log store: %w", err)
	}

	// Set up stable store (also BoltDB)
	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(cfg.DataDir, "raft-stable.db"))
	if err != nil {
		return nil, fmt.Errorf("failed to create stable store: %w", err)
	}

	// Set up snapshot store
	snapshotStore, err := raft.NewFileSnapshotStore(cfg.DataDir, 2, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("failed to create snapshot store: %w", err)
	}

	// Set up transport
	addr, err := net.ResolveTCPAddr("tcp", cfg.BindAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve bind address: %w", err)
	}

	transport, err := raft.NewTCPTransport(cfg.BindAddr, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("failed to create transport: %w", err)
	}
	rn.transport = transport

	// Create the Raft instance
	ra, err := raft.NewRaft(raftConfig, fsm, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		return nil, fmt.Errorf("failed to create raft: %w", err)
	}
	rn.raft = ra

	// Bootstrap cluster if this is the first node
	if cfg.Bootstrap {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      raft.ServerID(cfg.NodeID),
					Address: raft.ServerAddress(cfg.BindAddr),
				},
			},
		}
		rn.raft.BootstrapCluster(configuration)
		logger.Info("Bootstrapped Raft cluster", zap.String("node_id", cfg.NodeID))
	}

	return rn, nil
}

// Apply applies a command to the Raft log
func (rn *RaftNode) Apply(cmd []byte, timeout time.Duration) error {
	future := rn.raft.Apply(cmd, timeout)
	if err := future.Error(); err != nil {
		return fmt.Errorf("failed to apply command: %w", err)
	}
	return nil
}

// IsLeader returns true if this node is the leader
func (rn *RaftNode) IsLeader() bool {
	return rn.raft.State() == raft.Leader
}

// Leader returns the address of the current leader
func (rn *RaftNode) Leader() string {
	return string(rn.raft.Leader())
}

// Shutdown gracefully shuts down the Raft node
func (rn *RaftNode) Shutdown() error {
	rn.logger.Info("Shutting down Raft node")

	future := rn.raft.Shutdown()
	if err := future.Error(); err != nil {
		return fmt.Errorf("failed to shutdown raft: %w", err)
	}

	if rn.transport != nil {
		if err := rn.transport.Close(); err != nil {
			return fmt.Errorf("failed to close transport: %w", err)
		}
	}

	return nil
}

// Stats returns Raft statistics
func (rn *RaftNode) Stats() map[string]string {
	return rn.raft.Stats()
}

// WaitForLeader waits for a leader to be elected
func (rn *RaftNode) WaitForLeader(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		if rn.Leader() != "" {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}

	return fmt.Errorf("no leader elected within timeout")
}
