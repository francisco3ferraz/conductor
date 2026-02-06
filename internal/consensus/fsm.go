package consensus

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/francisco3ferraz/conductor/internal/job"
	"github.com/francisco3ferraz/conductor/internal/storage"
	"github.com/francisco3ferraz/conductor/internal/tracing"
	"github.com/hashicorp/raft"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// FSM implements the Raft finite state machine
// It maintains the replicated state for jobs and workers
type FSM struct {
	mu              sync.RWMutex
	jobs            map[string]*job.Job
	workers         map[string]*storage.WorkerInfo
	deadLetterQueue map[string]*job.Job // Jobs that exhausted retries
	logger          *zap.Logger
	ctx             context.Context // Parent context for tracing propagation
}

// Command types for state machine operations
const (
	CommandSubmitJob      = "submit_job"
	CommandAssignJob      = "assign_job"
	CommandUnassignJob    = "unassign_job"
	CommandCompleteJob    = "complete_job"
	CommandFailJob        = "fail_job"
	CommandRetryJob       = "retry_job"
	CommandMoveToDLQ      = "move_to_dlq"
	CommandRetryFromDLQ   = "retry_from_dlq"
	CommandRegisterWorker = "register_worker"
	CommandRemoveWorker   = "remove_worker"
)

// Command represents a state machine command
type Command struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

// SubmitJobPayload is the payload for submitting a job
type SubmitJobPayload struct {
	Job *job.Job `json:"job"`
}

// AssignJobPayload is the payload for assigning a job
type AssignJobPayload struct {
	JobID    string `json:"job_id"`
	WorkerID string `json:"worker_id"`
}

// UnassignJobPayload is the payload for unassigning a job (rollback)
type UnassignJobPayload struct {
	JobID string `json:"job_id"`
}

// CompleteJobPayload is the payload for completing a job
type CompleteJobPayload struct {
	JobID  string      `json:"job_id"`
	Result *job.Result `json:"result"`
}

// FailJobPayload is the payload for failing a job
type FailJobPayload struct {
	JobID string `json:"job_id"`
	Error string `json:"error"`
}

// RetryJobPayload is the payload for retrying a job after worker failure
type RetryJobPayload struct {
	JobID string `json:"job_id"`
}

// MoveToDLQPayload is the payload for moving a job to DLQ
type MoveToDLQPayload struct {
	JobID  string `json:"job_id"`
	Reason string `json:"reason"`
}

// RetryFromDLQPayload is the payload for retrying a job from DLQ
type RetryFromDLQPayload struct {
	JobID string `json:"job_id"`
}

// RegisterWorkerPayload is the payload for registering a worker
type RegisterWorkerPayload struct {
	Worker *storage.WorkerInfo `json:"worker"`
}

// RemoveWorkerPayload is the payload for removing a worker
type RemoveWorkerPayload struct {
	WorkerID string `json:"worker_id"`
}

// NewFSM creates a new finite state machine
// ctx is used as the parent context for tracing operations in Apply
func NewFSM(ctx context.Context, logger *zap.Logger) *FSM {
	return &FSM{
		jobs:            make(map[string]*job.Job),
		workers:         make(map[string]*storage.WorkerInfo),
		deadLetterQueue: make(map[string]*job.Job),
		logger:          logger,
		ctx:             ctx,
	}
}

// Apply applies a Raft log entry to the FSM
func (f *FSM) Apply(log *raft.Log) interface{} {
	// Create tracing context for Raft operations, using FSM's parent context
	tracer := tracing.GetTracer("conductor.raft")
	parentCtx := f.ctx
	if parentCtx == nil {
		parentCtx = context.Background()
	}
	ctx, span := tracer.Start(parentCtx, "raft.apply_command",
		trace.WithAttributes(
			attribute.String("raft.index", fmt.Sprintf("%d", log.Index)),
			attribute.String("raft.term", fmt.Sprintf("%d", log.Term)),
		),
	)
	defer span.End()

	var cmd Command
	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		span.RecordError(err)
		span.SetAttributes(attribute.String("error.message", "failed to unmarshal command"))
		f.logger.Error("Failed to unmarshal command", zap.Error(err))
		return err
	}

	span.SetAttributes(attribute.String("raft.command_type", cmd.Type))

	f.mu.Lock()
	defer f.mu.Unlock()

	switch cmd.Type {
	case CommandSubmitJob:
		span.AddEvent("applying_submit_job")
		return f.applySubmitJob(ctx, cmd.Payload)
	case CommandAssignJob:
		span.AddEvent("applying_assign_job")
		return f.applyAssignJob(ctx, cmd.Payload)
	case CommandUnassignJob:
		span.AddEvent("applying_unassign_job")
		return f.applyUnassignJob(ctx, cmd.Payload)
	case CommandCompleteJob:
		span.AddEvent("applying_complete_job")
		return f.applyCompleteJob(ctx, cmd.Payload)
	case CommandFailJob:
		span.AddEvent("applying_fail_job")
		return f.applyFailJob(ctx, cmd.Payload)
	case CommandRetryJob:
		span.AddEvent("applying_retry_job")
		return f.applyRetryJob(ctx, cmd.Payload)
	case CommandMoveToDLQ:
		span.AddEvent("applying_move_to_dlq")
		return f.applyMoveToDLQ(ctx, cmd.Payload)
	case CommandRetryFromDLQ:
		span.AddEvent("applying_retry_from_dlq")
		return f.applyRetryFromDLQ(ctx, cmd.Payload)
	case CommandRegisterWorker:
		span.AddEvent("applying_register_worker")
		return f.applyRegisterWorker(ctx, cmd.Payload)
	case CommandRemoveWorker:
		span.AddEvent("applying_remove_worker")
		return f.applyRemoveWorker(ctx, cmd.Payload)
	default:
		err := fmt.Errorf("unknown command type: %s", cmd.Type)
		span.RecordError(err)
		span.SetAttributes(attribute.String("error.message", err.Error()))
		f.logger.Error("Unknown command", zap.Error(err))
		return err
	}
}

func (f *FSM) applySubmitJob(ctx context.Context, payload json.RawMessage) interface{} {
	var p SubmitJobPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal SubmitJobPayload: %w", err)
	}

	f.jobs[p.Job.ID] = p.Job
	f.logger.Debug("Job submitted", zap.String("job_id", p.Job.ID))
	return nil
}

func (f *FSM) applyAssignJob(ctx context.Context, payload json.RawMessage) interface{} {
	var p AssignJobPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal AssignJobPayload: %w", err)
	}

	job, exists := f.jobs[p.JobID]
	if !exists {
		return fmt.Errorf("AssignJob: job not found: %s", p.JobID)
	}

	if err := job.Assign(p.WorkerID); err != nil {
		return fmt.Errorf("failed to assign job %s to worker %s: %w", p.JobID, p.WorkerID, err)
	}

	f.logger.Debug("Job assigned",
		zap.String("job_id", p.JobID),
		zap.String("worker_id", p.WorkerID))
	return nil
}

func (f *FSM) applyUnassignJob(ctx context.Context, payload json.RawMessage) interface{} {
	var p UnassignJobPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal UnassignJobPayload: %w", err)
	}

	j, exists := f.jobs[p.JobID]
	if !exists {
		return fmt.Errorf("UnassignJob: job not found: %s", p.JobID)
	}

	// Only unassign if currently assigned (prevents double unassign)
	if j.Status != job.StatusAssigned {
		f.logger.Warn("Attempted to unassign job not in assigned status",
			zap.String("job_id", p.JobID),
			zap.String("current_status", j.Status.String()))
		return nil // Not an error, just a no-op
	}

	// Reset job to pending state for reassignment
	j.Status = job.StatusPending
	j.AssignedTo = ""

	f.logger.Debug("Job unassigned (rollback)",
		zap.String("job_id", p.JobID))
	return nil
}

func (f *FSM) applyCompleteJob(ctx context.Context, payload json.RawMessage) interface{} {
	var p CompleteJobPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal CompleteJobPayload: %w", err)
	}

	job, exists := f.jobs[p.JobID]
	if !exists {
		return fmt.Errorf("CompleteJob: job not found: %s", p.JobID)
	}

	if err := job.Complete(p.Result); err != nil {
		return fmt.Errorf("failed to complete job %s: %w", p.JobID, err)
	}

	f.logger.Debug("Job completed", zap.String("job_id", p.JobID))
	return nil
}

func (f *FSM) applyFailJob(ctx context.Context, payload json.RawMessage) interface{} {
	var p FailJobPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal FailJobPayload: %w", err)
	}

	j, exists := f.jobs[p.JobID]
	if !exists {
		return fmt.Errorf("FailJob: job not found: %s", p.JobID)
	}

	if err := j.Fail(p.Error); err != nil {
		return fmt.Errorf("failed to mark job %s as failed: %w", p.JobID, err)
	}

	// Check if job exceeded max retries - if so, it should be moved to DLQ
	// This is just marking as failed; DLQ move happens via separate command
	f.logger.Debug("Job failed",
		zap.String("job_id", p.JobID),
		zap.String("error", p.Error),
		zap.Int("retry_count", j.RetryCount),
		zap.Int("max_retries", j.MaxRetries))
	return nil
}

func (f *FSM) applyRetryJob(ctx context.Context, payload json.RawMessage) interface{} {
	var p RetryJobPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal RetryJobPayload: %w", err)
	}

	j, exists := f.jobs[p.JobID]
	if !exists {
		return fmt.Errorf("RetryJob: job not found: %s", p.JobID)
	}

	// Reset job state for retry
	j.Status = job.StatusPending
	j.AssignedTo = ""
	j.RetryCount++
	j.StartedAt = time.Time{}
	j.ErrorMessage = ""

	f.logger.Info("Job reset for retry",
		zap.String("job_id", p.JobID),
		zap.Int("retry_count", j.RetryCount),
		zap.Int("max_retries", j.MaxRetries))
	return nil
}

func (f *FSM) applyMoveToDLQ(ctx context.Context, payload json.RawMessage) interface{} {
	var p MoveToDLQPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal MoveToDLQPayload: %w", err)
	}

	j, exists := f.jobs[p.JobID]
	if !exists {
		return fmt.Errorf("MoveToDLQ: job not found: %s", p.JobID)
	}

	// Move job to DLQ (keep in jobs map but also add to DLQ for tracking)
	f.deadLetterQueue[p.JobID] = j

	f.logger.Info("Job moved to dead letter queue",
		zap.String("job_id", p.JobID),
		zap.String("reason", p.Reason),
		zap.Int("retry_count", j.RetryCount),
		zap.Int("max_retries", j.MaxRetries))
	return nil
}

func (f *FSM) applyRetryFromDLQ(ctx context.Context, payload json.RawMessage) interface{} {
	var p RetryFromDLQPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal RetryFromDLQPayload: %w", err)
	}

	j, exists := f.deadLetterQueue[p.JobID]
	if !exists {
		return fmt.Errorf("RetryFromDLQ: job not in DLQ: %s", p.JobID)
	}

	// Remove from DLQ
	delete(f.deadLetterQueue, p.JobID)

	// Reset job state for retry
	j.Status = job.StatusPending
	j.AssignedTo = ""
	j.ErrorMessage = ""
	j.Result = nil
	j.RetryCount = 0 // Reset retry count for manual retry

	f.logger.Info("Job removed from DLQ and reset for retry",
		zap.String("job_id", p.JobID))
	return nil
}

func (f *FSM) applyRegisterWorker(ctx context.Context, payload json.RawMessage) interface{} {
	var p RegisterWorkerPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal RegisterWorkerPayload: %w", err)
	}

	f.workers[p.Worker.ID] = p.Worker
	f.logger.Debug("Worker registered", zap.String("worker_id", p.Worker.ID))
	return nil
}

func (f *FSM) applyRemoveWorker(ctx context.Context, payload json.RawMessage) interface{} {
	var p RemoveWorkerPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal RemoveWorkerPayload: %w", err)
	}

	delete(f.workers, p.WorkerID)
	f.logger.Debug("Worker removed", zap.String("worker_id", p.WorkerID))
	return nil
}

// Snapshot creates a snapshot of the current state
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	return f.CreateSnapshot()
}

// Restore restores the FSM from a snapshot
func (f *FSM) Restore(rc io.ReadCloser) error {
	return f.RestoreFromSnapshot(rc)
}

// GetJob retrieves a job by ID (read-only)
func (f *FSM) GetJob(id string) (*job.Job, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	j, exists := f.jobs[id]
	if !exists {
		return nil, storage.ErrNotFound
	}
	return j, nil
}

// ListJobs lists all jobs (read-only)
func (f *FSM) ListJobs() []*job.Job {
	f.mu.RLock()
	defer f.mu.RUnlock()

	jobs := make([]*job.Job, 0, len(f.jobs))
	for _, j := range f.jobs {
		jobs = append(jobs, j)
	}
	return jobs
}

// AssignJob assigns a job to a worker (for JobStateMachine interface)
func (f *FSM) AssignJob(jobID, workerID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	job, exists := f.jobs[jobID]
	if !exists {
		return fmt.Errorf("job not found: %s", jobID)
	}

	return job.Assign(workerID)
}

// FailJob fails a job (for JobStateMachine interface)
func (f *FSM) FailJob(jobID, errorMsg string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	job, exists := f.jobs[jobID]
	if !exists {
		return fmt.Errorf("job not found: %s", jobID)
	}

	return job.Fail(errorMsg)
}

// GetWorker retrieves a worker by ID (read-only)
func (f *FSM) GetWorker(id string) (*storage.WorkerInfo, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	w, exists := f.workers[id]
	if !exists {
		return nil, storage.ErrNotFound
	}
	return w, nil
}

// ListWorkers lists all workers (read-only)
func (f *FSM) ListWorkers() []*storage.WorkerInfo {
	f.mu.RLock()
	defer f.mu.RUnlock()

	workers := make([]*storage.WorkerInfo, 0, len(f.workers))
	for _, w := range f.workers {
		workers = append(workers, w)
	}
	return workers
}

// ListDLQJobs lists all jobs in the dead letter queue (read-only)
func (f *FSM) ListDLQJobs() []*job.Job {
	f.mu.RLock()
	defer f.mu.RUnlock()

	jobs := make([]*job.Job, 0, len(f.deadLetterQueue))
	for _, j := range f.deadLetterQueue {
		jobs = append(jobs, j)
	}
	return jobs
}

// GetDLQJob retrieves a job from DLQ by ID (read-only)
func (f *FSM) GetDLQJob(id string) (*job.Job, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	j, exists := f.deadLetterQueue[id]
	if !exists {
		return nil, storage.ErrNotFound
	}
	return j, nil
}
