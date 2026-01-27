package rpc

import (
	"context"
	"fmt"

	proto "github.com/francisco3ferraz/conductor/api/proto"
	"github.com/francisco3ferraz/conductor/internal/consensus"
	"github.com/francisco3ferraz/conductor/internal/job"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// MasterServer implements the gRPC MasterService
type MasterServer struct {
	proto.UnimplementedMasterServiceServer
	raftNode *consensus.RaftNode
	fsm      *consensus.FSM
	applier  *consensus.ApplyCommand
	logger   *zap.Logger
}

// NewMasterServer creates a new master gRPC server
func NewMasterServer(raftNode *consensus.RaftNode, fsm *consensus.FSM, logger *zap.Logger) *MasterServer {
	return &MasterServer{
		raftNode: raftNode,
		fsm:      fsm,
		applier:  consensus.NewApplyCommand(raftNode),
		logger:   logger,
	}
}

// SubmitJob handles job submission via gRPC
func (s *MasterServer) SubmitJob(ctx context.Context, req *proto.SubmitJobRequest) (*proto.SubmitJobResponse, error) {
	// Check if we're the leader
	if !s.raftNode.IsLeader() {
		leader := s.raftNode.Leader()
		return &proto.SubmitJobResponse{
			JobId:   "",
			Status:  "error",
			Message: fmt.Sprintf("not leader, redirect to %s", leader),
		}, nil
	}

	// Parse job type
	jobType := parseJobType(req.Type)

	// Create job
	j := job.New(jobType, req.Payload, int(req.Priority), int(req.MaxRetries))

	s.logger.Info("Submitting job via gRPC",
		zap.String("job_id", j.ID),
		zap.String("type", req.Type),
		zap.Int32("priority", req.Priority),
	)

	// Apply to Raft cluster
	if err := s.applier.SubmitJob(j); err != nil {
		s.logger.Error("Failed to submit job", zap.Error(err))
		return &proto.SubmitJobResponse{
			JobId:   "",
			Status:  "error",
			Message: fmt.Sprintf("failed to submit: %v", err),
		}, err
	}

	return &proto.SubmitJobResponse{
		JobId:   j.ID,
		Status:  "success",
		Message: "Job submitted successfully",
	}, nil
}

// GetJobStatus retrieves job status via gRPC
func (s *MasterServer) GetJobStatus(ctx context.Context, req *proto.GetJobStatusRequest) (*proto.GetJobStatusResponse, error) {
	// Read from local FSM (no Raft needed for reads)
	j, err := s.fsm.GetJob(req.JobId)
	if err != nil {
		return nil, fmt.Errorf("job not found: %w", err)
	}

	// Convert to proto Job
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

	// Add timestamps if set
	if !j.StartedAt.IsZero() {
		pbJob.StartedAt = timestamppb.New(j.StartedAt)
	}
	if !j.CompletedAt.IsZero() {
		pbJob.CompletedAt = timestamppb.New(j.CompletedAt)
	}

	// Add result if completed
	if j.Result != nil {
		pbJob.Result = &proto.JobResult{
			Success:    j.Result.Success,
			Output:     j.Result.Output,
			Error:      j.Result.Error,
			DurationMs: j.Result.DurationMs,
		}
	}

	return &proto.GetJobStatusResponse{
		Job: pbJob,
	}, nil
}

// ListJobs lists jobs with optional filtering
func (s *MasterServer) ListJobs(ctx context.Context, req *proto.ListJobsRequest) (*proto.ListJobsResponse, error) {
	// Read all jobs from FSM
	jobs := s.fsm.ListJobs()

	// Convert to protobuf format with filtering
	pbJobs := make([]*proto.Job, 0)
	for _, j := range jobs {
		// Apply status filter if specified
		if req.StatusFilter != "" && j.Status.String() != req.StatusFilter {
			continue
		}

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

		if !j.StartedAt.IsZero() {
			pbJob.StartedAt = timestamppb.New(j.StartedAt)
		}
		if !j.CompletedAt.IsZero() {
			pbJob.CompletedAt = timestamppb.New(j.CompletedAt)
		}

		pbJobs = append(pbJobs, pbJob)
	}

	// Simple pagination
	start := int(req.Offset)
	end := start + int(req.Limit)
	if req.Limit == 0 || end > len(pbJobs) {
		end = len(pbJobs)
	}
	if start > len(pbJobs) {
		start = len(pbJobs)
	}

	return &proto.ListJobsResponse{
		Jobs:  pbJobs[start:end],
		Total: int32(len(pbJobs)),
	}, nil
}

// CancelJob cancels a running or pending job
func (s *MasterServer) CancelJob(ctx context.Context, req *proto.CancelJobRequest) (*proto.CancelJobResponse, error) {
	// Check if we're the leader
	if !s.raftNode.IsLeader() {
		leader := s.raftNode.Leader()
		return &proto.CancelJobResponse{
			Success: false,
			Message: fmt.Sprintf("not leader, redirect to %s", leader),
		}, nil
	}

	// Verify job exists
	j, err := s.fsm.GetJob(req.JobId)
	if err != nil {
		return &proto.CancelJobResponse{
			Success: false,
			Message: "job not found",
		}, err
	}

	s.logger.Info("Cancelling job via gRPC", zap.String("job_id", req.JobId))

	// Mark as failed with cancellation message
	if err := s.applier.FailJob(j.ID, "cancelled by user"); err != nil {
		return &proto.CancelJobResponse{
			Success: false,
			Message: fmt.Sprintf("failed to cancel: %v", err),
		}, err
	}

	return &proto.CancelJobResponse{
		Success: true,
		Message: "Job cancelled successfully",
	}, nil
}

// Helper to parse job type string to enum
func parseJobType(typeStr string) job.Type {
	switch typeStr {
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
