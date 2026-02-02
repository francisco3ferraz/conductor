package rpc

import (
	"context"
	"fmt"
	"time"

	proto "github.com/francisco3ferraz/conductor/api/proto"
	"github.com/francisco3ferraz/conductor/internal/job"
	"github.com/francisco3ferraz/conductor/internal/worker"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// WorkerClient handles worker-to-master communication
type WorkerClient struct {
	workerID   string
	masterAddr string
	Client     proto.MasterServiceClient // Exported for result reporting
	conn       *grpc.ClientConn
	executor   *worker.Executor
	heartbeat  *worker.HeartbeatSender
	logger     *zap.Logger

	activeJobs     int32
	totalCompleted int32
	totalFailed    int32
}

// NewWorkerClient creates a new worker client
func NewWorkerClient(workerID, masterAddr string, executor *worker.Executor, logger *zap.Logger) (*WorkerClient, error) {
	// Connect to master
	conn, err := grpc.NewClient(masterAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to master: %w", err)
	}

	client := proto.NewMasterServiceClient(conn)

	wc := &WorkerClient{
		workerID:   workerID,
		masterAddr: masterAddr,
		Client:     client,
		conn:       conn,
		executor:   executor,
		logger:     logger,
	}

	// Create heartbeat sender
	wc.heartbeat = worker.NewHeartbeatSender(
		workerID,
		conn,
		5*time.Second,
		wc.GetStats,
		logger,
	)

	return wc, nil
}

// Close closes the connection to master
func (w *WorkerClient) Close() error {
	return w.conn.Close()
}

// GetConn returns the gRPC connection (for advanced usage)
func (w *WorkerClient) GetConn() *grpc.ClientConn {
	return w.conn
}

// Register registers the worker with the master
func (w *WorkerClient) Register(ctx context.Context, maxJobs int32, address string) error {
	w.logger.Info("Registering with master",
		zap.String("worker_id", w.workerID),
		zap.String("master_addr", w.masterAddr),
		zap.String("address", address),
	)

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	resp, err := w.Client.RegisterWorker(ctx, &proto.RegisterWorkerRequest{
		WorkerId:          w.workerID,
		Address:           address,
		MaxConcurrentJobs: maxJobs,
	})
	if err != nil {
		return fmt.Errorf("registration failed: %w", err)
	}

	if !resp.Success {
		return fmt.Errorf("registration rejected: %s", resp.Message)
	}

	w.logger.Info("Worker registration successful",
		zap.String("worker_id", w.workerID),
		zap.String("message", resp.Message),
	)
	return nil
}

// StartHeartbeat starts sending periodic heartbeats to master
func (w *WorkerClient) StartHeartbeat(ctx context.Context, interval time.Duration) {
	w.heartbeat.Start(ctx)
}

// StopHeartbeat stops the heartbeat sender
func (w *WorkerClient) StopHeartbeat() {
	w.heartbeat.Stop()
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

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	resp, err := w.Client.ReportResult(ctx, &proto.ReportResultRequest{
		JobId:    jobID,
		WorkerId: w.workerID,
		Result: &proto.JobResult{
			Success:    true,
			Output:     result.Output,
			DurationMs: result.DurationMs,
		},
	})
	if err != nil {
		return fmt.Errorf("failed to report success: %w", err)
	}

	if !resp.Success {
		w.logger.Warn("Result report not successful", zap.String("message", resp.Message))
	}

	return nil
}

// reportFailure reports job failure to master
func (w *WorkerClient) reportFailure(ctx context.Context, jobID string, errMsg string) error {
	w.logger.Warn("Reporting job failure",
		zap.String("job_id", jobID),
		zap.String("error", errMsg),
	)

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	resp, err := w.Client.ReportResult(ctx, &proto.ReportResultRequest{
		JobId:    jobID,
		WorkerId: w.workerID,
		Result: &proto.JobResult{
			Success: false,
			Error:   errMsg,
		},
	})
	if err != nil {
		return fmt.Errorf("failed to report failure: %w", err)
	}

	if !resp.Success {
		w.logger.Warn("Result report not successful", zap.String("message", resp.Message))
	}

	return nil
}

// GetStats returns worker statistics
func (w *WorkerClient) GetStats() (active, completed, failed int32) {
	return w.activeJobs, w.totalCompleted, w.totalFailed
}
