package rpc

import (
	"context"
	"fmt"
	"sync"
	"time"

	proto "github.com/francisco3ferraz/conductor/api/proto"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// connMetadata tracks connection state and lifecycle
type connMetadata struct {
	conn         *grpc.ClientConn
	lastUsed     time.Time
	createdAt    time.Time
	failureCount int
}

// ClientForwarder handles forwarding requests to the leader
type ClientForwarder struct {
	logger      *zap.Logger
	mu          sync.RWMutex
	connections map[string]*connMetadata
	dialTimeout time.Duration
	maxIdleTime time.Duration // Close connections idle for this duration
	cleanupDone chan struct{}
	ctx         context.Context
	cancel      context.CancelFunc
}

// NewClientForwarder creates a new request forwarder
func NewClientForwarder(logger *zap.Logger) *ClientForwarder {
	ctx, cancel := context.WithCancel(context.Background())
	f := &ClientForwarder{
		logger:      logger,
		connections: make(map[string]*connMetadata),
		dialTimeout: 5 * time.Second,
		maxIdleTime: 5 * time.Minute, // Close connections idle for 5 minutes
		cleanupDone: make(chan struct{}),
		ctx:         ctx,
		cancel:      cancel,
	}

	// Start periodic cleanup goroutine
	go f.cleanupIdleConnections()

	return f
}

// cleanupIdleConnections periodically removes idle connections to prevent memory leaks
func (f *ClientForwarder) cleanupIdleConnections() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()
	defer close(f.cleanupDone)

	for {
		select {
		case <-f.ctx.Done():
			return
		case <-ticker.C:
			f.mu.Lock()
			now := time.Now()
			for addr, meta := range f.connections {
				if now.Sub(meta.lastUsed) > f.maxIdleTime {
					f.logger.Debug("Closing idle connection",
						zap.String("addr", addr),
						zap.Duration("idle_time", now.Sub(meta.lastUsed)),
					)
					meta.conn.Close()
					delete(f.connections, addr)
				}
			}
			f.mu.Unlock()
		}
	}
}

// getOrCreateConnection gets or creates a gRPC connection to a server
func (f *ClientForwarder) getOrCreateConnection(addr string) (*grpc.ClientConn, error) {
	f.mu.RLock()
	meta, exists := f.connections[addr]
	f.mu.RUnlock()

	// Check if we already have a connection
	if exists {
		state := meta.conn.GetState()

		switch state.String() {
		case "READY", "IDLE":
			// Connection is healthy, update last used time
			f.mu.Lock()
			meta.lastUsed = time.Now()
			f.mu.Unlock()
			return meta.conn, nil

		case "CONNECTING":
			// Connection is in progress, wait for it
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			if meta.conn.WaitForStateChange(ctx, state) {
				// State changed, check if it's now usable
				newState := meta.conn.GetState()
				if newState.String() == "READY" || newState.String() == "IDLE" {
					f.mu.Lock()
					meta.lastUsed = time.Now()
					f.mu.Unlock()
					return meta.conn, nil
				}
			}
			// Timeout or failed to connect, close and recreate
			f.logger.Warn("Connection timeout while CONNECTING",
				zap.String("addr", addr),
				zap.String("state", meta.conn.GetState().String()),
			)
			f.mu.Lock()
			meta.conn.Close()
			delete(f.connections, addr)
			f.mu.Unlock()

		case "TRANSIENT_FAILURE":
			// Temporary failure, wait briefly for recovery
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel()

			if meta.conn.WaitForStateChange(ctx, state) {
				// State changed, check if recovered
				newState := meta.conn.GetState()
				if newState.String() == "READY" || newState.String() == "IDLE" {
					f.logger.Info("Connection recovered from TRANSIENT_FAILURE",
						zap.String("addr", addr),
					)
					f.mu.Lock()
					meta.lastUsed = time.Now()
					meta.failureCount = 0
					f.mu.Unlock()
					return meta.conn, nil
				}
			}
			// Failed to recover, close and recreate
			f.logger.Warn("Connection failed to recover from TRANSIENT_FAILURE",
				zap.String("addr", addr),
				zap.String("final_state", meta.conn.GetState().String()),
			)
			f.mu.Lock()
			meta.conn.Close()
			delete(f.connections, addr)
			f.mu.Unlock()

		default:
			// SHUTDOWN or unknown state - close and recreate
			f.logger.Warn("Connection in invalid state, recreating",
				zap.String("addr", addr),
				zap.String("state", state.String()),
			)
			f.mu.Lock()
			meta.conn.Close()
			delete(f.connections, addr)
			f.mu.Unlock()
		}
	}

	// Create new connection
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %s: %w", addr, err)
	}

	// Store connection with metadata
	f.mu.Lock()
	now := time.Now()
	f.connections[addr] = &connMetadata{
		conn:         conn,
		lastUsed:     now,
		createdAt:    now,
		failureCount: 0,
	}
	f.mu.Unlock()

	f.logger.Info("Created new connection",
		zap.String("addr", addr),
	)

	return conn, nil
}

// ForwardSubmitJob forwards a job submission request to the leader
func (f *ClientForwarder) ForwardSubmitJob(ctx context.Context, leaderAddr string, req *proto.SubmitJobRequest) (*proto.SubmitJobResponse, error) {
	f.logger.Info("Forwarding SubmitJob to leader",
		zap.String("leader", leaderAddr),
		zap.String("job_type", req.Type),
	)

	conn, err := f.getOrCreateConnection(leaderAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to leader %s for SubmitJob: %w", leaderAddr, err)
	}

	client := proto.NewMasterServiceClient(conn)
	resp, err := client.SubmitJob(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("leader SubmitJob request failed: %w", err)
	}
	return resp, nil
}

// ForwardCancelJob forwards a job cancellation request to the leader
func (f *ClientForwarder) ForwardCancelJob(ctx context.Context, leaderAddr string, req *proto.CancelJobRequest) (*proto.CancelJobResponse, error) {
	f.logger.Info("Forwarding CancelJob to leader",
		zap.String("leader", leaderAddr),
		zap.String("job_id", req.JobId),
	)

	conn, err := f.getOrCreateConnection(leaderAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to leader %s for CancelJob: %w", leaderAddr, err)
	}

	client := proto.NewMasterServiceClient(conn)
	resp, err := client.CancelJob(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("leader CancelJob request failed: %w", err)
	}
	return resp, nil
}

// Close closes all forwarding connections and stops cleanup goroutine
func (f *ClientForwarder) Close() error {
	// Cancel context to stop cleanup goroutine
	f.cancel()

	// Wait for cleanup goroutine to finish
	<-f.cleanupDone

	// Close all connections
	f.mu.Lock()
	defer f.mu.Unlock()

	var lastErr error
	for addr, meta := range f.connections {
		if err := meta.conn.Close(); err != nil {
			f.logger.Error("Failed to close connection",
				zap.String("addr", addr),
				zap.Error(err),
			)
			lastErr = err
		}
	}
	f.connections = make(map[string]*connMetadata)
	return lastErr
}
