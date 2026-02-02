package rpc

import (
	"context"
	"fmt"
	"time"

	proto "github.com/francisco3ferraz/conductor/api/proto"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ClientForwarder handles forwarding requests to the leader
type ClientForwarder struct {
	logger      *zap.Logger
	connections map[string]*grpc.ClientConn
	dialTimeout time.Duration
}

// NewClientForwarder creates a new request forwarder
func NewClientForwarder(logger *zap.Logger) *ClientForwarder {
	return &ClientForwarder{
		logger:      logger,
		connections: make(map[string]*grpc.ClientConn),
		dialTimeout: 5 * time.Second,
	}
}

// getOrCreateConnection gets or creates a gRPC connection to a server
func (f *ClientForwarder) getOrCreateConnection(addr string) (*grpc.ClientConn, error) {
	// Check if we already have a connection
	if conn, exists := f.connections[addr]; exists {
		// Check if connection is still healthy
		if conn.GetState().String() == "READY" || conn.GetState().String() == "IDLE" {
			return conn, nil
		}
		// Close stale connection
		conn.Close()
		delete(f.connections, addr)
	}

	// Create new connection
	ctx, cancel := context.WithTimeout(context.Background(), f.dialTimeout)
	defer cancel()

	conn, err := grpc.DialContext(ctx, addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %s: %w", addr, err)
	}

	f.connections[addr] = conn
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
		return nil, err
	}

	client := proto.NewMasterServiceClient(conn)
	return client.SubmitJob(ctx, req)
}

// ForwardCancelJob forwards a job cancellation request to the leader
func (f *ClientForwarder) ForwardCancelJob(ctx context.Context, leaderAddr string, req *proto.CancelJobRequest) (*proto.CancelJobResponse, error) {
	f.logger.Info("Forwarding CancelJob to leader",
		zap.String("leader", leaderAddr),
		zap.String("job_id", req.JobId),
	)

	conn, err := f.getOrCreateConnection(leaderAddr)
	if err != nil {
		return nil, err
	}

	client := proto.NewMasterServiceClient(conn)
	return client.CancelJob(ctx, req)
}

// Close closes all forwarding connections
func (f *ClientForwarder) Close() error {
	var lastErr error
	for addr, conn := range f.connections {
		if err := conn.Close(); err != nil {
			f.logger.Error("Failed to close connection",
				zap.String("addr", addr),
				zap.Error(err),
			)
			lastErr = err
		}
	}
	f.connections = make(map[string]*grpc.ClientConn)
	return lastErr
}
