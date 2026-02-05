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
	"github.com/francisco3ferraz/conductor/internal/tracing"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Scheduler interface for worker management
type Scheduler interface {
	RegisterWorker(id, address string, maxJobs int) error
	UpdateHeartbeat(workerID string, activeJobs int)
	RecordWorkerHeartbeat(workerID string, stats *failover.WorkerStats)
	GetFailureDetector() *failover.FailureDetector
	GetRecoveryManager() *failover.RecoveryManager
	CancelJobOnWorker(ctx context.Context, jobID, workerID, reason string) error
}

// MasterServer implements the gRPC MasterService
type MasterServer struct {
	proto.UnimplementedMasterServiceServer
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
	// Start tracing span for job submission
	tracer := tracing.GetTracer("conductor.master")
	ctx, span := tracing.StartSpan(ctx, tracer, "master.submit_job",
		trace.WithAttributes(
			attribute.String("job.type", req.Type),
			attribute.Int("job.priority", int(req.Priority)),
			attribute.Int("job.max_retries", int(req.MaxRetries)),
			attribute.Int("job.timeout_seconds", int(req.GetTimeoutSeconds())),
		),
	)
	defer span.End()

	// Check if we're the leader
	if !s.raftNode.IsLeader() {
		span.AddEvent("forwarding_to_leader")
		leader := s.raftNode.Leader()
		if leader == "" {
			span.RecordError(fmt.Errorf("no leader available"))
			span.SetAttributes(attribute.String("error.message", "no leader available"))
			return &proto.SubmitJobResponse{
				JobId:   "",
				Status:  "error",
				Message: "no leader available",
			}, fmt.Errorf("no leader available")
		}

		// Forward request to leader
		s.logger.Info("Forwarding SubmitJob to leader",
			zap.String("leader", leader),
			zap.String("trace_id", span.SpanContext().TraceID().String()),
		)
		span.SetAttributes(attribute.String("leader.address", leader))
		return s.forwarder.ForwardSubmitJob(ctx, leader, req)
	}

	// Parse job type
	jobType := parseJobType(req.Type)

	// Create job with node ID for distributed uniqueness
	j := job.New(jobType, req.Payload, int(req.Priority), int(req.MaxRetries), req.GetTimeoutSeconds(), s.raftNode.NodeID())

	// Add job attributes to span
	tracing.AddJobAttributes(span, j.ID, jobType.String())
	span.SetAttributes(attribute.String("job.id", j.ID))

	s.logger.Info("Submitting job via gRPC",
		zap.String("job_id", j.ID),
		zap.String("type", req.Type),
		zap.Int32("priority", req.Priority),
		zap.String("trace_id", span.SpanContext().TraceID().String()),
	)

	// Apply to Raft cluster
	span.AddEvent("submitting_to_raft")
	if err := s.applier.SubmitJob(j); err != nil {
		span.RecordError(err)
		span.SetAttributes(attribute.String("error.message", err.Error()))
		s.logger.Error("Failed to submit job", zap.Error(err))
		return &proto.SubmitJobResponse{
			JobId:   "",
			Status:  "error",
			Message: fmt.Sprintf("failed to submit: %v", err),
		}, err
	}

	span.AddEvent("job_submitted_successfully")
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
		Id:             j.ID,
		Type:           j.Type.String(),
		Payload:        j.Payload,
		Priority:       int32(j.Priority),
		Status:         j.Status.String(),
		AssignedTo:     j.AssignedTo,
		CreatedAt:      timestamppb.New(j.CreatedAt),
		RetryCount:     int32(j.RetryCount),
		MaxRetries:     int32(j.MaxRetries),
		ErrorMessage:   j.ErrorMessage,
		TimeoutSeconds: j.TimeoutSeconds,
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

	// Cancel job on worker if it's assigned
	if j.AssignedTo != "" && (j.Status == job.StatusAssigned || j.Status == job.StatusRunning) {
		if err := s.scheduler.CancelJobOnWorker(ctx, j.ID, j.AssignedTo, "user_cancelled"); err != nil {
			s.logger.Warn("Failed to cancel job on worker (will still mark as failed)",
				zap.String("job_id", j.ID),
				zap.String("worker_id", j.AssignedTo),
				zap.Error(err),
			)
			// Continue anyway - we'll mark it as failed in Raft
		}
	}

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
		if err := s.scheduler.RegisterWorker(req.WorkerId, req.Address, int(req.MaxConcurrentJobs)); err != nil {
			s.logger.Error("Failed to register worker",
				zap.String("worker_id", req.WorkerId),
				zap.Error(err),
			)
			return &proto.RegisterWorkerResponse{
				Success: false,
				Message: fmt.Sprintf("Failed to register worker: %v", err),
			}, nil
		}

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

// Shutdown gracefully shuts down the master server and cleans up resources
func (s *MasterServer) Shutdown() error {
	s.logger.Info("Shutting down master server")

	// Close forwarder connections
	if err := s.forwarder.Close(); err != nil {
		s.logger.Error("Failed to close forwarder connections", zap.Error(err))
		return err
	}

	s.logger.Info("Master server shutdown complete")
	return nil
}

// jobToProto converts a job.Job to proto.Job
func (s *MasterServer) jobToProto(j *job.Job) *proto.Job {
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

	return pbJob
}

// ListDLQ returns all jobs in the dead letter queue
func (s *MasterServer) ListDLQ(ctx context.Context, req *proto.ListDLQRequest) (*proto.ListDLQResponse, error) {
	s.logger.Info("Listing dead letter queue jobs",
		zap.Int32("limit", req.Limit),
		zap.Int32("offset", req.Offset))

	// Read from FSM (no Raft consensus needed for reads)
	dlqJobs := s.fsm.ListDLQJobs()

	// Apply pagination
	offset := int(req.GetOffset())
	limit := int(req.GetLimit())
	if limit == 0 {
		limit = 100 // Default limit
	}

	total := len(dlqJobs)
	start := offset
	if start >= total {
		return &proto.ListDLQResponse{
			Jobs:  []*proto.Job{},
			Total: int32(total),
		}, nil
	}

	end := start + limit
	if end > total {
		end = total
	}

	// Convert jobs to proto format
	protoJobs := make([]*proto.Job, 0, end-start)
	for i := start; i < end; i++ {
		protoJobs = append(protoJobs, s.jobToProto(dlqJobs[i]))
	}

	return &proto.ListDLQResponse{
		Jobs:  protoJobs,
		Total: int32(total),
	}, nil
}

// RetryFromDLQ retries a job from the dead letter queue
func (s *MasterServer) RetryFromDLQ(ctx context.Context, req *proto.RetryFromDLQRequest) (*proto.RetryFromDLQResponse, error) {
	// Check if we're the leader
	if !s.raftNode.IsLeader() {
		leader := s.raftNode.Leader()
		s.logger.Warn("Not leader, cannot retry DLQ job",
			zap.String("job_id", req.JobId),
			zap.String("leader", leader))
		return &proto.RetryFromDLQResponse{
			Success: false,
			Message: fmt.Sprintf("Not leader. Current leader: %s", leader),
		}, nil
	}

	s.logger.Info("Retrying job from dead letter queue", zap.String("job_id", req.JobId))

	// Verify job exists in DLQ
	if _, err := s.fsm.GetDLQJob(req.JobId); err != nil {
		return &proto.RetryFromDLQResponse{
			Success: false,
			Message: fmt.Sprintf("Job not found in DLQ: %v", err),
		}, nil
	}

	// Retry via Raft command
	if err := s.applier.RetryFromDLQ(req.JobId); err != nil {
		s.logger.Error("Failed to retry job from DLQ",
			zap.String("job_id", req.JobId),
			zap.Error(err))
		return &proto.RetryFromDLQResponse{
			Success: false,
			Message: fmt.Sprintf("Failed to retry: %v", err),
		}, nil
	}

	return &proto.RetryFromDLQResponse{
		Success: true,
		Message: "Job successfully retried from DLQ",
	}, nil
}
