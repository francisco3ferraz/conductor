package scheduler

import (
	"context"
	"fmt"

	"github.com/francisco3ferraz/conductor/api/proto"
	"github.com/francisco3ferraz/conductor/internal/job"
	"github.com/francisco3ferraz/conductor/internal/worker"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// sendJobToWorker sends a job assignment to a worker via gRPC
func (s *Scheduler) sendJobToWorker(parentCtx context.Context, j *job.Job, w *worker.WorkerInfo) error {
	// Connect to worker
	conn, err := grpc.NewClient(w.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()

	client := proto.NewWorkerServiceClient(conn)

	// Convert job to proto
	pbJob := &proto.Job{
		Id:           j.ID,
		Type:         j.Type.String(),
		Payload:      j.Payload,
		Priority:     int32(j.Priority),
		Status:       j.Status.String(),
		AssignedTo:   j.AssignedTo,
		CreatedAt:    timestamppb.New(j.CreatedAt),
		RetryCount:   int32(j.RetryCount),
		MaxRetries:   int32(j.MaxRetries),
		ErrorMessage: j.ErrorMessage,
	}

	// Send job to worker with timeout, derived from parent context for proper cancellation
	ctx, cancel := context.WithTimeout(parentCtx, s.cfg.Scheduler.AssignmentTimeout)
	defer cancel()

	resp, err := client.AssignJob(ctx, &proto.AssignJobRequest{
		WorkerId: w.ID,
		Job:      pbJob,
	})
	if err != nil {
		return err
	}

	if !resp.Accepted {
		return fmt.Errorf("worker rejected job: %s", resp.Message)
	}

	return nil
}

// cancelJobOnWorker sends a cancellation request to a worker via gRPC
func (s *Scheduler) cancelJobOnWorker(parentCtx context.Context, jobID, workerID, reason string) error {
	// Get worker info from registry
	w, exists := s.registry.Get(workerID)
	if !exists {
		return fmt.Errorf("worker %s not found in registry", workerID)
	}

	// Connect to worker
	conn, err := grpc.NewClient(w.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to connect to worker: %w", err)
	}
	defer conn.Close()

	client := proto.NewWorkerServiceClient(conn)

	// Send cancellation request with timeout
	ctx, cancel := context.WithTimeout(parentCtx, s.cfg.Scheduler.AssignmentTimeout)
	defer cancel()

	resp, err := client.CancelJob(ctx, &proto.WorkerCancelJobRequest{
		WorkerId: workerID,
		JobId:    jobID,
		Reason:   reason,
	})
	if err != nil {
		return fmt.Errorf("cancellation RPC failed: %w", err)
	}

	if !resp.Cancelled {
		return fmt.Errorf("worker failed to cancel job: %s", resp.Message)
	}

	s.logger.Info("Job cancelled on worker",
		zap.String("job_id", jobID),
		zap.String("worker_id", workerID),
		zap.String("reason", reason),
	)

	return nil
}
