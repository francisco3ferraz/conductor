package consensus

import (
	"crypto/tls"
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
	JoinAddr  string // Address of existing node to join

	// Raft timeouts
	HeartbeatTimeout time.Duration
	ElectionTimeout  time.Duration

	// Snapshot config
	SnapshotInterval  time.Duration
	SnapshotThreshold uint64

	// TLS config for Raft communication
	TLSConfig *tls.Config
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

	// Set up transport with TLS support
	addr, err := net.ResolveTCPAddr("tcp", cfg.BindAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve bind address: %w", err)
	}

	var transport *raft.NetworkTransport
	if cfg.TLSConfig != nil {
		logger.Info("Raft: TLS encryption configured for cluster communication")

		// Create TLS stream layer
		streamLayer, err := newTLSStreamLayer(cfg.BindAddr, cfg.TLSConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to create TLS stream layer: %w", err)
		}

		// Create network transport with TLS stream layer
		transport = raft.NewNetworkTransport(streamLayer, 3, 10*time.Second, os.Stderr)
	} else {
		logger.Warn("Raft: TLS encryption DISABLED - NOT FOR PRODUCTION")

		// Create standard TCP transport
		transport, err = raft.NewTCPTransport(cfg.BindAddr, addr, 3, 10*time.Second, os.Stderr)
		if err != nil {
			return nil, fmt.Errorf("failed to create transport: %w", err)
		}
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
	} else if cfg.JoinAddr != "" {
		// Join existing cluster
		logger.Info("Attempting to join existing cluster",
			zap.String("node_id", cfg.NodeID),
			zap.String("join_addr", cfg.JoinAddr),
		)
		// The actual join happens via RPC call to the existing cluster
		// This is handled externally after node creation
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

// NodeID returns the node ID of this Raft node
func (rn *RaftNode) NodeID() string {
	return rn.config.NodeID
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

// AddVoter adds a new voting member to the Raft cluster
func (rn *RaftNode) AddVoter(id, address string, prevIndex uint64, timeout time.Duration) error {
	rn.logger.Info("Adding voter to cluster",
		zap.String("node_id", id),
		zap.String("address", address),
	)

	future := rn.raft.AddVoter(raft.ServerID(id), raft.ServerAddress(address), prevIndex, timeout)
	if err := future.Error(); err != nil {
		return fmt.Errorf("failed to add voter: %w", err)
	}

	return nil
}

// RemoveServer removes a server from the Raft cluster
func (rn *RaftNode) RemoveServer(id string, prevIndex uint64, timeout time.Duration) error {
	rn.logger.Info("Removing server from cluster",
		zap.String("node_id", id),
	)

	future := rn.raft.RemoveServer(raft.ServerID(id), prevIndex, timeout)
	if err := future.Error(); err != nil {
		return fmt.Errorf("failed to remove server: %w", err)
	}

	return nil
}

// GetConfiguration returns the current Raft cluster configuration
func (rn *RaftNode) GetConfiguration() (raft.Configuration, error) {
	future := rn.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		return raft.Configuration{}, fmt.Errorf("failed to get configuration: %w", err)
	}

	return future.Configuration(), nil
}

// State returns the current Raft state
func (rn *RaftNode) State() raft.RaftState {
	return rn.raft.State()
}

// Barrier ensures all preceding operations are applied before returning
// This is useful for read-your-writes consistency
func (rn *RaftNode) Barrier(timeout time.Duration) error {
	future := rn.raft.Barrier(timeout)
	if err := future.Error(); err != nil {
		return fmt.Errorf("barrier failed: %w", err)
	}
	return nil
}

// LastIndex returns the last applied index
func (rn *RaftNode) LastIndex() uint64 {
	return rn.raft.LastIndex()
}

// AppliedIndex returns the last index applied to the FSM
func (rn *RaftNode) AppliedIndex() uint64 {
	return rn.raft.AppliedIndex()
}

// tlsStreamLayer implements raft.StreamLayer with TLS support
type tlsStreamLayer struct {
	listener  net.Listener
	tlsConfig *tls.Config
}

// newTLSStreamLayer creates a new TLS-enabled stream layer
func newTLSStreamLayer(bindAddr string, tlsConfig *tls.Config) (*tlsStreamLayer, error) {
	listener, err := net.Listen("tcp", bindAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to create listener: %w", err)
	}

	// Wrap with TLS listener
	tlsListener := tls.NewListener(listener, tlsConfig)

	return &tlsStreamLayer{
		listener:  tlsListener,
		tlsConfig: tlsConfig,
	}, nil
}

// Accept waits for and returns the next connection to the listener
func (t *tlsStreamLayer) Accept() (net.Conn, error) {
	return t.listener.Accept()
}

// Close closes the listener
func (t *tlsStreamLayer) Close() error {
	return t.listener.Close()
}

// Addr returns the listener's network address
func (t *tlsStreamLayer) Addr() net.Addr {
	return t.listener.Addr()
}

// Dial creates an outgoing TLS connection
func (t *tlsStreamLayer) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: timeout}
	return tls.DialWithDialer(dialer, "tcp", string(address), t.tlsConfig)
}
