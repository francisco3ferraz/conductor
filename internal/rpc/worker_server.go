package rpc

import (
	"context"
	"fmt"

	proto "github.com/francisco3ferraz/conductor/api/proto"
	"github.com/francisco3ferraz/conductor/internal/config"
	"github.com/francisco3ferraz/conductor/internal/job"
	"github.com/francisco3ferraz/conductor/internal/worker"
	"go.uber.org/zap"
)

// WorkerServer implements the gRPC WorkerService
type WorkerServer struct {
	proto.UnimplementedWorkerServiceServer
	workerID string
	executor *worker.Executor
	jobQueue *worker.JobQueue
	reporter worker.ResultReporter
	logger   *zap.Logger
	cfg      *config.Config
}

// NewWorkerServer creates a new worker gRPC server
func NewWorkerServer(workerID string, exec *worker.Executor, jobQueue *worker.JobQueue, reporter worker.ResultReporter, cfg *config.Config, logger *zap.Logger) *WorkerServer {
	return &WorkerServer{
		workerID: workerID,
		executor: exec,
		jobQueue: jobQueue,
		reporter: reporter,
		logger:   logger,
		cfg:      cfg,
	}
}

// AssignJob receives a job assignment from the master
func (w *WorkerServer) AssignJob(ctx context.Context, req *proto.AssignJobRequest) (*proto.AssignJobResponse, error) {
	w.logger.Info("Received job assignment",
		zap.String("job_id", req.Job.Id),
		zap.String("type", req.Job.Type),
		zap.Int32("priority", req.Job.Priority),
	)

	// Convert proto.Job to job.Job
	j := protoToJob(req.Job)

	// Enqueue job for priority-based execution
	// Use background context for job execution - the gRPC request context
	// will be cancelled when AssignJob returns, but jobs need to run longer
	w.jobQueue.Enqueue(context.Background(), j)

	return &proto.AssignJobResponse{
		Accepted: true,
		Message:  "Job accepted and queued for execution",
	}, nil
}

// CancelJob cancels a running job on this worker
func (w *WorkerServer) CancelJob(ctx context.Context, req *proto.WorkerCancelJobRequest) (*proto.WorkerCancelJobResponse, error) {
	w.logger.Info("Received job cancellation request",
		zap.String("job_id", req.JobId),
		zap.String("reason", req.Reason),
	)

	cancelled := w.executor.Cancel(req.JobId)

	if cancelled {
		return &proto.WorkerCancelJobResponse{
			Cancelled: true,
			Message:   fmt.Sprintf("Job %s cancelled: %s", req.JobId, req.Reason),
		}, nil
	}

	return &proto.WorkerCancelJobResponse{
		Cancelled: false,
		Message:   fmt.Sprintf("Job %s not found or already completed", req.JobId),
	}, nil
}

// Heartbeat handles heartbeat requests from master
func (w *WorkerServer) Heartbeat(ctx context.Context, req *proto.HeartbeatRequest) (*proto.HeartbeatResponse, error) {
	// This is called by master to check worker health
	// For now, just acknowledge
	return &proto.HeartbeatResponse{
		Ack: true,
	}, nil
}

// protoToJob converts proto.Job to internal job.Job
func protoToJob(pj *proto.Job) *job.Job {
	j := &job.Job{
		ID:             pj.Id,
		Type:           parseJobTypeFromString(pj.Type),
		Payload:        pj.Payload,
		Priority:       int(pj.Priority),
		Status:         parseStatusFromString(pj.Status),
		AssignedTo:     pj.AssignedTo,
		RetryCount:     int(pj.RetryCount),
		MaxRetries:     int(pj.MaxRetries),
		ErrorMessage:   pj.ErrorMessage,
		TimeoutSeconds: pj.TimeoutSeconds,
	}

	if pj.CreatedAt != nil {
		j.CreatedAt = pj.CreatedAt.AsTime()
	}
	if pj.StartedAt != nil {
		j.StartedAt = pj.StartedAt.AsTime()
	}
	if pj.CompletedAt != nil {
		j.CompletedAt = pj.CompletedAt.AsTime()
	}

	return j
}

func parseJobTypeFromString(s string) job.Type {
	switch s {
	case "image_processing":
		return job.TypeImageProcessing
	case "web_scraping":
		return job.TypeWebScraping
	case "data_analysis":
		return job.TypeDataAnalysis
	default:
		return job.TypeImageProcessing
	}
}

func parseStatusFromString(s string) job.Status {
	status, _ := job.ParseStatus(s)
	return status
}
