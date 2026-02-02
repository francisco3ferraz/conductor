package rpc

import (
	"context"
	"fmt"
	"time"

	proto "github.com/francisco3ferraz/conductor/api/proto"
	"github.com/francisco3ferraz/conductor/internal/consensus"
	"github.com/francisco3ferraz/conductor/internal/failover"
	"github.com/francisco3ferraz/conductor/internal/job"
	"github.com/francisco3ferraz/conductor/internal/storage"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Scheduler interface for worker management
type Scheduler interface {
	RegisterWorker(id, address string, maxJobs int)
	UpdateHeartbeat(workerID string, activeJobs int)
	RecordWorkerHeartbeat(workerID string, stats *failover.WorkerStats)
	GetFailureDetector() *failover.FailureDetector
	GetRecoveryManager() *failover.RecoveryManager
}

// MasterServer implements the gRPC MasterService and WorkerService
type MasterServer struct {
	proto.UnimplementedMasterServiceServer
	proto.UnimplementedWorkerServiceServer
	raftNode  *consensus.RaftNode
	fsm       *consensus.FSM
	applier   *consensus.ApplyCommand
	scheduler Scheduler
	forwarder *ClientForwarder
	logger    *zap.Logger
}

// NewMasterServer creates a new master gRPC server
func NewMasterServer(raftNode *consensus.RaftNode, fsm *consensus.FSM, logger *zap.Logger) *MasterServer {
	return &MasterServer{
		raftNode:  raftNode,
		fsm:       fsm,
		applier:   consensus.NewApplyCommand(raftNode),
		forwarder: NewClientForwarder(logger),
		logger:    logger,
	}
}

// SetScheduler sets the scheduler reference (to avoid circular dependency)
func (s *MasterServer) SetScheduler(scheduler Scheduler) {
	s.scheduler = scheduler
}

// SubmitJob handles job submission via gRPC
func (s *MasterServer) SubmitJob(ctx context.Context, req *proto.SubmitJobRequest) (*proto.SubmitJobResponse, error) {
	// Check if we're the leader
	if !s.raftNode.IsLeader() {
		leader := s.raftNode.Leader()
		if leader == "" {
			return &proto.SubmitJobResponse{
				JobId:   "",
				Status:  "error",
				Message: "no leader available",
			}, fmt.Errorf("no leader available")
		}

		// Forward request to leader
		s.logger.Info("Forwarding SubmitJob to leader",
			zap.String("leader", leader),
		)
		return s.forwarder.ForwardSubmitJob(ctx, leader, req)
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
	// Ensure read-your-writes consistency: wait for all pending writes to apply
	// This guarantees clients see their own writes
	if err := s.raftNode.Barrier(5 * time.Second); err != nil {
		s.logger.Warn("barrier timeout during GetJobStatus, proceeding with potentially stale read",
			zap.Error(err))
		// Proceed anyway - stale reads are better than no reads
	}

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
	// Ensure read-your-writes consistency
	if err := s.raftNode.Barrier(5 * time.Second); err != nil {
		s.logger.Warn("barrier timeout during ListJobs, proceeding with potentially stale read",
			zap.Error(err))
	}

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
		if leader == "" {
			return &proto.CancelJobResponse{
				Success: false,
				Message: "no leader available",
			}, fmt.Errorf("no leader available")
		}

		// Forward request to leader
		s.logger.Info("Forwarding CancelJob to leader",
			zap.String("leader", leader),
			zap.String("job_id", req.JobId),
		)
		return s.forwarder.ForwardCancelJob(ctx, leader, req)
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

// JoinCluster handles cluster membership requests
func (s *MasterServer) JoinCluster(ctx context.Context, req *proto.JoinClusterRequest) (*proto.JoinClusterResponse, error) {
	// Only leader can handle join requests
	if !s.raftNode.IsLeader() {
		leader := s.raftNode.Leader()
		return &proto.JoinClusterResponse{
			Success: false,
			Message: fmt.Sprintf("not leader, redirect to %s", leader),
			Leader:  leader,
		}, nil
	}

	s.logger.Info("Cluster join request",
		zap.String("node_id", req.NodeId),
		zap.String("address", req.Address),
	)

	// Add node as voter
	if err := s.raftNode.AddVoter(req.NodeId, req.Address, 0, 10*time.Second); err != nil {
		s.logger.Error("Failed to add voter",
			zap.String("node_id", req.NodeId),
			zap.Error(err),
		)
		return &proto.JoinClusterResponse{
			Success: false,
			Message: fmt.Sprintf("failed to add node: %v", err),
			Leader:  s.raftNode.Leader(),
		}, err
	}

	s.logger.Info("Node added to cluster",
		zap.String("node_id", req.NodeId),
		zap.String("address", req.Address),
	)

	return &proto.JoinClusterResponse{
		Success: true,
		Message: "Successfully joined cluster",
		Leader:  s.raftNode.Leader(),
	}, nil
}

// RegisterWorker handles worker registration
func (s *MasterServer) RegisterWorker(ctx context.Context, req *proto.RegisterWorkerRequest) (*proto.RegisterWorkerResponse, error) {
	s.logger.Info("Worker registration request",
		zap.String("worker_id", req.WorkerId),
		zap.String("address", req.Address),
		zap.Int32("max_jobs", req.MaxConcurrentJobs),
	)

	// Register worker with scheduler (includes failover detector registration)
	if s.scheduler != nil {
		s.scheduler.RegisterWorker(req.WorkerId, req.Address, int(req.MaxConcurrentJobs))

		// Also register directly with failure detector
		if failureDetector := s.scheduler.GetFailureDetector(); failureDetector != nil {
			workerInfo := &storage.WorkerInfo{
				ID:                req.WorkerId,
				Address:           req.Address,
				MaxConcurrentJobs: int(req.MaxConcurrentJobs),
				Status:            "active",
			}
			failureDetector.RegisterWorker(workerInfo)
		}
	}

	return &proto.RegisterWorkerResponse{
		Success: true,
		Message: "Worker registered successfully",
	}, nil
}

// Heartbeat handles worker heartbeat messages
func (s *MasterServer) Heartbeat(ctx context.Context, req *proto.HeartbeatRequest) (*proto.HeartbeatResponse, error) {
	s.logger.Debug("Worker heartbeat",
		zap.String("worker_id", req.WorkerId),
		zap.Int32("active_jobs", req.Stats.ActiveJobs),
	)

	// Convert proto stats to failover stats
	stats := &failover.WorkerStats{
		ActiveJobs:    req.Stats.ActiveJobs,
		CompletedJobs: req.Stats.CompletedJobs,
		FailedJobs:    req.Stats.FailedJobs,
	}

	// Update worker heartbeat in scheduler (includes failover system)
	if s.scheduler != nil {
		s.scheduler.RecordWorkerHeartbeat(req.WorkerId, stats)
	}

	return &proto.HeartbeatResponse{
		Ack: true,
	}, nil
}

// ReportResult handles job result reporting from workers
func (s *MasterServer) ReportResult(ctx context.Context, req *proto.ReportResultRequest) (*proto.ReportResultResponse, error) {
	// Check if we're the leader
	if !s.raftNode.IsLeader() {
		leader := s.raftNode.Leader()
		return &proto.ReportResultResponse{
			Success: false,
			Message: fmt.Sprintf("not leader, redirect to %s", leader),
		}, nil
	}

	s.logger.Info("Job result reported",
		zap.String("job_id", req.JobId),
		zap.String("worker_id", req.WorkerId),
		zap.Bool("success", req.Result.Success),
	)

	// Convert proto result to internal result
	result := &job.Result{
		Success:    req.Result.Success,
		Output:     req.Result.Output,
		Error:      req.Result.Error,
		DurationMs: req.Result.DurationMs,
	}

	// Apply result to Raft cluster
	var err error
	if result.Success {
		err = s.applier.CompleteJob(req.JobId, result)
	} else {
		err = s.applier.FailJob(req.JobId, result.Error)
	}

	if err != nil {
		s.logger.Error("Failed to apply job result", zap.Error(err))
		return &proto.ReportResultResponse{
			Success: false,
			Message: fmt.Sprintf("failed to apply result: %v", err),
		}, err
	}

	return &proto.ReportResultResponse{
		Success: true,
		Message: "Result recorded successfully",
	}, nil
}

// AssignJob is a placeholder - workers receive jobs via active push from scheduler
func (s *MasterServer) AssignJob(ctx context.Context, req *proto.AssignJobRequest) (*proto.AssignJobResponse, error) {
	// This endpoint is not used in our design - scheduler pushes jobs to workers
	// Kept for API compatibility
	return &proto.AssignJobResponse{
		Accepted: false,
		Message:  "Use scheduler-initiated job assignment",
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
