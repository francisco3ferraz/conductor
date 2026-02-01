package scheduler

import (
	"context"
	"fmt"
	"time"

	"github.com/francisco3ferraz/conductor/api/proto"
	"github.com/francisco3ferraz/conductor/internal/job"
	"github.com/francisco3ferraz/conductor/internal/worker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// sendJobToWorker sends a job assignment to a worker via gRPC
func (s *Scheduler) sendJobToWorker(j *job.Job, w *worker.WorkerInfo) error {
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

	// Send job to worker
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
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

// findAvailableWorker finds a worker with available capacity
func (s *Scheduler) findAvailableWorker() *worker.WorkerInfo {
	workers := s.registry.List()

	// Simple strategy: find first worker with capacity
	for _, w := range workers {
		if w.Status == "active" && w.ActiveJobs < w.MaxConcurrentJobs {
			return w
		}
	}

	return nil
}
