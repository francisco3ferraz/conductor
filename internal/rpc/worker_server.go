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

// ResultReporter defines the interface for reporting job results
type ResultReporter interface {
	ReportResult(ctx context.Context, jobID string, result *job.Result) error
}

// WorkerServer implements the gRPC WorkerService
type WorkerServer struct {
	proto.UnimplementedWorkerServiceServer
	workerID string
	executor *worker.Executor
	reporter ResultReporter
	logger   *zap.Logger
	cfg      *config.Config
}

// NewWorkerServer creates a new worker gRPC server
func NewWorkerServer(workerID string, exec *worker.Executor, reporter ResultReporter, cfg *config.Config, logger *zap.Logger) *WorkerServer {
	return &WorkerServer{
		workerID: workerID,
		executor: exec,
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
	)

	// Convert proto.Job to job.Job
	j := protoToJob(req.Job)

	// Execute job in goroutine with context propagation
	go func(jobCtx context.Context) {
		// Mark job as running (start it)
		j.Start()

		// Execute the job with parent context
		result := w.executor.Execute(jobCtx, j)

		// Report result back to master with timeout
		if w.reporter != nil {
			reportCtx, cancel := context.WithTimeout(context.Background(), w.cfg.Worker.ResultTimeout)
			defer cancel()
			if err := w.reporter.ReportResult(reportCtx, j.ID, result); err != nil {
				w.logger.Error("Failed to report job result",
					zap.String("job_id", j.ID),
					zap.Error(err),
				)
			}
		}
	}(ctx)

	return &proto.AssignJobResponse{
		Accepted: true,
		Message:  "Job accepted for execution",
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
