package scheduler

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/francisco3ferraz/conductor/internal/config"
	"github.com/francisco3ferraz/conductor/internal/consensus"
	"github.com/francisco3ferraz/conductor/internal/failover"
	"github.com/francisco3ferraz/conductor/internal/job"
	"github.com/francisco3ferraz/conductor/internal/storage"
	"github.com/francisco3ferraz/conductor/internal/worker"
	"go.uber.org/zap"
)

// Scheduler assigns jobs to workers
type Scheduler struct {
	raftNode *consensus.RaftNode
	fsm      *consensus.FSM
	applier  *consensus.ApplyCommand
	logger   *zap.Logger
	cfg      *config.Config

	registry *worker.Registry
	policy   SchedulingPolicy
	mu       sync.RWMutex

	// Failover components
	failureDetector *failover.FailureDetector
	recoveryManager *failover.RecoveryManager

	stopCh chan struct{}
}

// NewScheduler creates a new job scheduler
func NewScheduler(raftNode *consensus.RaftNode, fsm *consensus.FSM, cfg *config.Config, logger *zap.Logger) *Scheduler {
	s := &Scheduler{
		raftNode: raftNode,
		fsm:      fsm,
		applier:  consensus.NewApplyCommand(raftNode),
		logger:   logger,
		cfg:      cfg,
		registry: worker.NewRegistry(),
		policy:   NewLeastLoadedPolicy(), // Default to least-loaded policy
		stopCh:   make(chan struct{}),
	}

	// Initialize failover components
	onWorkerFailure := func(workerID string) {
		s.handleWorkerFailure(workerID)
	}

	s.failureDetector = failover.NewFailureDetector(
		cfg.Worker.HeartbeatTimeout,
		2*time.Second, // Check interval
		onWorkerFailure,
		logger,
	)

	// Recovery manager will be initialized when we have a job store
	// This happens in SetJobStore method

	return s
}

// Start starts the scheduler background loop
func (s *Scheduler) Start(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	// Start failover components
	if s.failureDetector != nil {
		go s.failureDetector.Start(ctx)
	}
	if s.recoveryManager != nil {
		go s.recoveryManager.Start()
	}

	s.logger.Info("Scheduler started")

	for {
		select {
		case <-ctx.Done():
			s.logger.Info("Scheduler stopped")
			return
		case <-s.stopCh:
			s.logger.Info("Scheduler stopped")
			return
		case <-ticker.C:
			// Wrap scheduler operations in panic recovery to prevent loop from dying
			func() {
				defer func() {
					if r := recover(); r != nil {
						s.logger.Error("Panic in scheduler loop",
							zap.Any("panic", r),
							zap.Stack("stack"))
					}
				}()

				// Only schedule if we're the leader
				if s.raftNode.IsLeader() {
					s.schedulePendingJobs(ctx)
					s.checkJobTimeouts(ctx)
				}
			}()
		}
	}
}

// Stop stops the scheduler
func (s *Scheduler) Stop() {
	s.logger.Info("Stopping scheduler")

	// Stop failure detector
	if s.failureDetector != nil {
		s.failureDetector.Stop()
	}

	// Stop recovery manager
	if s.recoveryManager != nil {
		s.recoveryManager.Stop()
	}

	close(s.stopCh)
	s.logger.Info("Scheduler stopped")
}

// checkJobTimeouts checks for timed out jobs and fails them
func (s *Scheduler) checkJobTimeouts(ctx context.Context) {
	jobs := s.fsm.ListJobs()

	for _, j := range jobs {
		if j.IsTimeout() {
			s.logger.Warn("Job timed out",
				zap.String("job_id", j.ID),
				zap.String("worker_id", j.AssignedTo),
				zap.Duration("timeout", j.TimeoutDuration()),
				zap.Duration("elapsed", time.Since(j.StartedAt)),
			)

			// Cancel job on worker BEFORE failing in Raft
			if j.AssignedTo != "" {
				if err := s.cancelJobOnWorker(ctx, j.ID, j.AssignedTo, "timeout"); err != nil {
					s.logger.Error("Failed to cancel job on worker",
						zap.String("job_id", j.ID),
						zap.String("worker_id", j.AssignedTo),
						zap.Error(err),
					)
					// Continue anyway - worker might be dead
				} else {
					s.logger.Info("Successfully cancelled job on worker",
						zap.String("job_id", j.ID),
						zap.String("worker_id", j.AssignedTo),
					)
				}
			}

			// Then fail the job in Raft FSM
			timeoutErr := fmt.Sprintf("job timed out after %v", j.TimeoutDuration())
			if err := s.applier.FailJob(j.ID, timeoutErr); err != nil {
				s.logger.Error("Failed to timeout job",
					zap.String("job_id", j.ID),
					zap.Error(err),
				)
			}
		}
	}
}

// schedulePendingJobs finds pending jobs and assigns them to workers
func (s *Scheduler) schedulePendingJobs(ctx context.Context) {
	// Get all jobs from FSM
	jobs := s.fsm.ListJobs()

	// Find pending jobs and add to priority queue
	queue := NewJobQueue()
	for _, j := range jobs {
		if j.Status == job.StatusPending {
			queue.Enqueue(j)
		}
	}

	if queue.IsEmpty() {
		return
	}

	s.logger.Debug("Scheduling pending jobs", zap.Int("count", queue.Size()))

	// Get available workers
	workers := s.registry.List()
	availableWorkers := make([]*worker.WorkerInfo, 0)
	for i := range workers {
		w := &workers[i] // Take address of copy
		if w.Status == "active" && w.ActiveJobs < w.MaxConcurrentJobs {
			availableWorkers = append(availableWorkers, w)
		}
	}

	if len(availableWorkers) == 0 {
		s.logger.Debug("No available workers")
		return
	}

	// Process jobs from priority queue using configured scheduling policy
	for !queue.IsEmpty() {
		j := queue.Dequeue()

		// Re-check available workers (capacity may have changed)
		availableWorkers = make([]*worker.WorkerInfo, 0)
		freshWorkers := s.registry.List()
		for i := range freshWorkers {
			w := &freshWorkers[i] // Take address of copy
			if w.Status == "active" && w.ActiveJobs < w.MaxConcurrentJobs {
				availableWorkers = append(availableWorkers, w)
			}
		}

		if len(availableWorkers) == 0 {
			s.logger.Debug("No more available workers")
			break
		}

		// Use configured policy to select best worker for this job
		// Acquire read lock to safely access policy field
		s.mu.RLock()
		policy := s.policy
		s.mu.RUnlock()

		worker := policy.SelectWorker(j, availableWorkers)
		if worker == nil {
			s.logger.Warn("Policy returned no worker for job", zap.String("job_id", j.ID))
			continue
		}

		// First mark as assigned in Raft so it won't be picked up again
		if err := s.applier.AssignJob(j.ID, worker.ID); err != nil {
			s.logger.Error("Failed to assign job in Raft",
				zap.String("job_id", j.ID),
				zap.String("worker_id", worker.ID),
				zap.Error(err),
			)
			continue
		}

		// Now actually send the job to the worker via gRPC
		if err := s.sendJobToWorker(ctx, j, worker); err != nil {
			s.logger.Error("Failed to send job to worker, rolling back assignment",
				zap.String("job_id", j.ID),
				zap.String("worker_id", worker.ID),
				zap.Error(err),
			)

			// CRITICAL: Rollback the Raft assignment to prevent orphaned jobs
			if rollbackErr := s.applier.UnassignJob(j.ID); rollbackErr != nil {
				s.logger.Error("Failed to rollback job assignment - job may be orphaned",
					zap.String("job_id", j.ID),
					zap.Error(rollbackErr),
				)
			} else {
				s.logger.Debug("Successfully rolled back job assignment",
					zap.String("job_id", j.ID),
				)
			}
			continue
		}

		// Update worker's active job count
		s.mu.Lock()
		worker.ActiveJobs++
		s.mu.Unlock()

		s.logger.Info("Job assigned to worker",
			zap.String("job_id", j.ID),
			zap.String("worker_id", worker.ID),
			zap.Int("priority", j.Priority),
		)
	}
}

const (
	// MinWorkerCapacity is the minimum allowed concurrent jobs per worker
	MinWorkerCapacity = 1
	// MaxWorkerCapacity is the maximum allowed concurrent jobs per worker
	MaxWorkerCapacity = 1000
)

// RegisterWorker registers a new worker with validation
func (s *Scheduler) RegisterWorker(id, address string, maxJobs int) error {
	// Validate worker capacity
	if maxJobs < MinWorkerCapacity {
		s.logger.Error("Worker capacity too low",
			zap.String("worker_id", id),
			zap.Int("max_jobs", maxJobs),
			zap.Int("minimum", MinWorkerCapacity),
		)
		return fmt.Errorf("worker capacity %d is below minimum %d", maxJobs, MinWorkerCapacity)
	}

	if maxJobs > MaxWorkerCapacity {
		s.logger.Error("Worker capacity too high",
			zap.String("worker_id", id),
			zap.Int("max_jobs", maxJobs),
			zap.Int("maximum", MaxWorkerCapacity),
		)
		return fmt.Errorf("worker capacity %d exceeds maximum %d", maxJobs, MaxWorkerCapacity)
	}

	// Register in local registry
	s.registry.Register(id, address, maxJobs)

	// Also register in Raft FSM
	workerInfo := &storage.WorkerInfo{
		ID:                id,
		Address:           address,
		MaxConcurrentJobs: maxJobs,
		Status:            "active",
	}
	if err := s.applier.RegisterWorker(workerInfo); err != nil {
		s.logger.Error("Failed to register worker in FSM", zap.Error(err))
		return err
	}

	s.logger.Info("Worker registered",
		zap.String("worker_id", id),
		zap.String("address", address),
		zap.Int("max_jobs", maxJobs),
	)

	return nil
}

// CancelJobOnWorker cancels a running job on a worker (exposed for master server)
func (s *Scheduler) CancelJobOnWorker(ctx context.Context, jobID, workerID, reason string) error {
	return s.cancelJobOnWorker(ctx, jobID, workerID, reason)
}

// UpdateHeartbeat updates worker's last heartbeat time
func (s *Scheduler) UpdateHeartbeat(workerID string, activeJobs int) {
	s.registry.UpdateHeartbeat(workerID, activeJobs)
}

// RemoveWorker removes a worker
func (s *Scheduler) RemoveWorker(workerID string) {
	s.registry.Unregister(workerID)

	// Remove from Raft FSM
	if err := s.applier.RemoveWorker(workerID); err != nil {
		s.logger.Error("Failed to remove worker from FSM", zap.Error(err))
	}

	s.logger.Info("Worker removed", zap.String("worker_id", workerID))
}

// ListWorkers returns all registered workers
// Returns copies to prevent external mutations
func (s *Scheduler) ListWorkers() []worker.WorkerInfo {
	return s.registry.List()
}

// CheckWorkerHealth checks for inactive workers and marks them
func (s *Scheduler) CheckWorkerHealth(timeout time.Duration) {
	inactive := s.registry.MarkInactive(timeout)
	for _, workerID := range inactive {
		s.logger.Warn("Worker marked inactive",
			zap.String("worker_id", workerID),
		)
	}
}

// SetSchedulingPolicy sets the scheduling policy for job assignment
func (s *Scheduler) SetSchedulingPolicy(policy SchedulingPolicy) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.policy = policy
	s.logger.Info("Scheduling policy updated", zap.String("policy", getPolicyName(policy)))
}

// getPolicyName returns a human-readable name for the policy
func getPolicyName(policy SchedulingPolicy) string {
	switch policy.(type) {
	case *RoundRobinPolicy:
		return "round-robin"
	case *LeastLoadedPolicy:
		return "least-loaded"
	case *PriorityPolicy:
		return "priority"
	case *RandomPolicy:
		return "random"
	case *CapacityAwarePolicy:
		return "capacity-aware"
	default:
		return "unknown"
	}
}

// SetJobStore initializes the recovery manager with a job store
func (s *Scheduler) SetJobStore(jobStore storage.Store) {
	if s.recoveryManager == nil {
		// Create context for recovery manager (could be improved to use parent context)
		ctx := context.Background()
		s.recoveryManager = failover.NewRecoveryManager(
			ctx,           // Parent context for shutdown propagation
			s,             // Pass scheduler reference
			jobStore,      // Job storage
			s.fsm,         // FSM for job state management
			3,             // Max retries
			5*time.Second, // Retry delay
			s.logger,
		)
	}
}

// TriggerAssignment triggers the job assignment process
func (s *Scheduler) TriggerAssignment(ctx context.Context) error {
	// This method is called by the recovery manager to reassign jobs
	// We'll trigger the assignment loop manually
	go s.schedulePendingJobs(ctx)
	return nil
}

// handleWorkerFailure handles worker failure detected by failure detector
func (s *Scheduler) handleWorkerFailure(workerID string) {
	s.logger.Warn("Worker failure detected by scheduler",
		zap.String("worker_id", workerID),
	)

	// Remove from registry
	s.RemoveWorker(workerID)

	// Trigger recovery if recovery manager is available
	if s.recoveryManager != nil {
		s.recoveryManager.HandleWorkerFailure(workerID)
	}
}

// RecordWorkerHeartbeat records a heartbeat from a worker
func (s *Scheduler) RecordWorkerHeartbeat(workerID string, stats *failover.WorkerStats) {
	if s.failureDetector != nil {
		s.failureDetector.RecordHeartbeat(workerID, stats)
	}

	// Also update the local registry
	s.registry.UpdateHeartbeat(workerID, int(stats.ActiveJobs))
}

// GetFailureDetector returns the failure detector (for master server integration)
func (s *Scheduler) GetFailureDetector() *failover.FailureDetector {
	return s.failureDetector
}

// GetRecoveryManager returns the recovery manager (for health checks)
func (s *Scheduler) GetRecoveryManager() *failover.RecoveryManager {
	return s.recoveryManager
}
