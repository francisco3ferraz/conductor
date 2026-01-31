package rpc

import (
	"context"
	"fmt"
	"time"

	proto "github.com/francisco3ferraz/conductor/api/proto"
	"github.com/francisco3ferraz/conductor/internal/executor"
	"github.com/francisco3ferraz/conductor/internal/job"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// WorkerClient handles worker-to-master communication
type WorkerClient struct {
	workerID   string
	masterAddr string
	Client     proto.MasterServiceClient  // Exported for result reporting
	conn       *grpc.ClientConn
	executor   *executor.Executor
	logger     *zap.Logger

	activeJobs     int32
	totalCompleted int32
	totalFailed    int32
}

// NewWorkerClient creates a new worker client
func NewWorkerClient(workerID, masterAddr string, executor *executor.Executor, logger *zap.Logger) (*WorkerClient, error) {
	// Connect to master
	conn, err := grpc.NewClient(masterAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to master: %w", err)
	}

	client := proto.NewMasterServiceClient(conn)

	return &WorkerClient{
		workerID:   workerID,
		masterAddr: masterAddr,
		Client:     client,
		conn:       conn,
		executor:   executor,
		logger:     logger,
	}, nil
}

// Close closes the connection to master
func (w *WorkerClient) Close() error {
	return w.conn.Close()
}

// Register registers the worker with the master
func (w *WorkerClient) Register(ctx context.Context, maxJobs int32) error {
	w.logger.Info("Registering with master",
		zap.String("worker_id", w.workerID),
		zap.String("master_addr", w.masterAddr),
	)

	// We'll implement worker registration after we add the WorkerService RPC
	// For now, just log
	w.logger.Info("Worker registration successful", zap.String("worker_id", w.workerID))
	return nil
}

// StartHeartbeat starts sending periodic heartbeats to master
func (w *WorkerClient) StartHeartbeat(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			w.logger.Info("Stopping heartbeat", zap.String("worker_id", w.workerID))
			return
		case <-ticker.C:
			if err := w.sendHeartbeat(ctx); err != nil {
				w.logger.Error("Heartbeat failed", zap.Error(err))
			}
		}
	}
}

// sendHeartbeat sends a single heartbeat to master
func (w *WorkerClient) sendHeartbeat(ctx context.Context) error {
	// We'll implement this after adding WorkerService RPC
	// For now, just log
	w.logger.Debug("Heartbeat sent",
		zap.String("worker_id", w.workerID),
		zap.Int32("active_jobs", w.activeJobs),
	)
	return nil
}

// ExecuteJob executes a job and reports the result to master
func (w *WorkerClient) ExecuteJob(ctx context.Context, j *job.Job) error {
	w.logger.Info("Received job assignment",
		zap.String("job_id", j.ID),
		zap.String("type", j.Type.String()),
	)

	w.activeJobs++
	defer func() { w.activeJobs-- }()

	// Start the job
	j.Start()

	// Execute the job
	result := w.executor.Execute(ctx, j)

	// Report result to master
	if result.Success {
		w.totalCompleted++
		return w.reportSuccess(ctx, j.ID, result)
	} else {
		w.totalFailed++
		return w.reportFailure(ctx, j.ID, result.Error)
	}
}

// reportSuccess reports successful job completion to master
func (w *WorkerClient) reportSuccess(ctx context.Context, jobID string, result *job.Result) error {
	w.logger.Info("Reporting job success",
		zap.String("job_id", jobID),
		zap.Int64("duration_ms", result.DurationMs),
	)

	// We'll use the complete job RPC through master service
	// For now this is a placeholder - we need to add a CompleteJob RPC
	// or use the existing patterns

	return nil
}

// reportFailure reports job failure to master
func (w *WorkerClient) reportFailure(ctx context.Context, jobID string, errMsg string) error {
	w.logger.Warn("Reporting job failure",
		zap.String("job_id", jobID),
		zap.String("error", errMsg),
	)

	// Placeholder - will implement with proper RPC
	return nil
}

// GetStats returns worker statistics
func (w *WorkerClient) GetStats() (active, completed, failed int32) {
	return w.activeJobs, w.totalCompleted, w.totalFailed
}
