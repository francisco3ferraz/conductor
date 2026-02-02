package rpc

import (
	"context"

	proto "github.com/francisco3ferraz/conductor/api/proto"
	"github.com/francisco3ferraz/conductor/internal/job"
	"github.com/francisco3ferraz/conductor/internal/worker"
	"go.uber.org/zap"
)

// WorkerServer implements the gRPC WorkerService
type WorkerServer struct {
	proto.UnimplementedWorkerServiceServer
	workerID       string
	executor       *worker.Executor
	resultReporter func(context.Context, string, *job.Result) error
	logger         *zap.Logger
}

// NewWorkerServer creates a new worker gRPC server
func NewWorkerServer(workerID string, exec *worker.Executor, resultReporter func(context.Context, string, *job.Result) error, logger *zap.Logger) *WorkerServer {
	return &WorkerServer{
		workerID:       workerID,
		executor:       exec,
		resultReporter: resultReporter,
		logger:         logger,
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

	// Execute job in goroutine
	go func() {
		// Mark job as running (start it)
		j.Start()

		// Execute the job
		result := w.executor.Execute(context.Background(), j)

		// Report result back to master
		if w.resultReporter != nil {
			if err := w.resultReporter(context.Background(), j.ID, result); err != nil {
				w.logger.Error("Failed to report job result",
					zap.String("job_id", j.ID),
					zap.Error(err),
				)
			}
		}
	}()

	return &proto.AssignJobResponse{
		Accepted: true,
		Message:  "Job accepted for execution",
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
