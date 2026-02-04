package failover

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/francisco3ferraz/conductor/internal/job"
	"github.com/francisco3ferraz/conductor/internal/storage"
	"go.uber.org/zap"
)

// SchedulerInterface defines the interface for scheduler interactions
// This breaks the circular import with the scheduler package
type SchedulerInterface interface {
	TriggerAssignment(ctx context.Context) error
}

// RecoveryManager handles job recovery and reassignment when workers fail
type RecoveryManager struct {
	scheduler     SchedulerInterface
	jobStore      storage.Store
	fsm           JobStateMachine
	logger        *zap.Logger
	mu            sync.RWMutex
	recoveryQueue chan string // Worker IDs that need recovery
	maxRetries    int
	retryDelay    time.Duration
	ctx           context.Context
	cancel        context.CancelFunc
	wg            sync.WaitGroup
}

// JobStateMachine interface for interacting with job state
type JobStateMachine interface {
	GetJob(id string) (*job.Job, error)
	ListJobs() []*job.Job
	AssignJob(jobID, workerID string) error
	FailJob(jobID, errorMsg string) error
}

// RecoveryStats tracks recovery operation statistics
type RecoveryStats struct {
	TotalRecoveries      int64
	SuccessfulRecoveries int64
	FailedRecoveries     int64
	JobsReassigned       int64
	JobsFailed           int64
	LastRecoveryTime     time.Time
}

// NewRecoveryManager creates a new recovery manager
// Accepts parent context for proper shutdown propagation
func NewRecoveryManager(
	ctx context.Context,
	scheduler SchedulerInterface,
	jobStore storage.Store,
	fsm JobStateMachine,
	maxRetries int,
	retryDelay time.Duration,
	logger *zap.Logger,
) *RecoveryManager {
	childCtx, cancel := context.WithCancel(ctx)

	return &RecoveryManager{
		scheduler:     scheduler,
		jobStore:      jobStore,
		fsm:           fsm,
		logger:        logger,
		recoveryQueue: make(chan string, 100), // Buffer for worker failures
		maxRetries:    maxRetries,
		retryDelay:    retryDelay,
		ctx:           childCtx,
		cancel:        cancel,
	}
}

// Start begins the recovery manager background processes
func (rm *RecoveryManager) Start() {
	rm.wg.Add(1)
	go rm.recoveryWorker()

	rm.logger.Info("Recovery manager started",
		zap.Int("max_retries", rm.maxRetries),
		zap.Duration("retry_delay", rm.retryDelay),
	)
}

// Stop stops the recovery manager
func (rm *RecoveryManager) Stop() {
	rm.cancel()
	close(rm.recoveryQueue)
	rm.wg.Wait()
	rm.logger.Info("Recovery manager stopped")
}

// HandleWorkerFailure is called when a worker is detected as failed
func (rm *RecoveryManager) HandleWorkerFailure(workerID string) {
	rm.logger.Warn("Handling worker failure",
		zap.String("worker_id", workerID),
	)

	select {
	case rm.recoveryQueue <- workerID:
		// Successfully queued for recovery
	default:
		rm.logger.Error("Recovery queue full, dropping worker failure",
			zap.String("worker_id", workerID),
		)
	}
}

// recoveryWorker processes worker failures from the recovery queue
func (rm *RecoveryManager) recoveryWorker() {
	defer rm.wg.Done()

	for {
		select {
		case <-rm.ctx.Done():
			return
		case workerID, ok := <-rm.recoveryQueue:
			if !ok {
				return // Channel closed
			}

			if err := rm.recoverWorkerJobs(workerID); err != nil {
				rm.logger.Error("Failed to recover worker jobs",
					zap.String("worker_id", workerID),
					zap.Error(err),
				)
			}
		}
	}
}

// recoverWorkerJobs recovers all jobs assigned to a failed worker
func (rm *RecoveryManager) recoverWorkerJobs(workerID string) error {
	rm.logger.Info("Starting job recovery for failed worker",
		zap.String("worker_id", workerID),
	)

	// Find all jobs assigned to the failed worker
	jobs := rm.fsm.ListJobs()
	var affectedJobs []*job.Job

	for _, j := range jobs {
		if j.AssignedTo == workerID && (j.Status == job.StatusAssigned || j.Status == job.StatusRunning) {
			affectedJobs = append(affectedJobs, j)
		}
	}

	if len(affectedJobs) == 0 {
		rm.logger.Info("No jobs to recover for worker",
			zap.String("worker_id", workerID),
		)
		return nil
	}

	rm.logger.Info("Found jobs to recover",
		zap.String("worker_id", workerID),
		zap.Int("job_count", len(affectedJobs)),
	)

	// Process each affected job
	var recoveredCount, failedCount int
	for _, j := range affectedJobs {
		if err := rm.recoverJob(j); err != nil {
			rm.logger.Error("Failed to recover job",
				zap.String("job_id", j.ID),
				zap.String("worker_id", workerID),
				zap.Error(err),
			)
			failedCount++
		} else {
			recoveredCount++
		}
	}

	rm.logger.Info("Job recovery completed",
		zap.String("worker_id", workerID),
		zap.Int("recovered", recoveredCount),
		zap.Int("failed", failedCount),
	)

	return nil
}

// recoverJob attempts to recover a single job
func (rm *RecoveryManager) recoverJob(j *job.Job) error {
	// Check if we should retry or fail the job
	if j.RetryCount >= rm.maxRetries {
		rm.logger.Error("Job exceeded max retries, permanently failing",
			zap.String("job_id", j.ID),
			zap.String("job_type", j.Type.String()),
			zap.Int("retry_count", j.RetryCount),
			zap.Int("max_retries", rm.maxRetries),
		)

		errMsg := fmt.Sprintf("Permanent failure: worker failures exceeded max retries (%d/%d)", j.RetryCount, rm.maxRetries)
		return rm.fsm.FailJob(j.ID, errMsg)
	}

	// Reset job for reassignment
	if err := rm.resetJobForReassignment(j); err != nil {
		return fmt.Errorf("failed to reset job for reassignment: %w", err)
	}

	// Add delay before reassignment to allow system to stabilize
	if rm.retryDelay > 0 {
		time.Sleep(rm.retryDelay)
	}

	// Check if there are any healthy workers available
	if !rm.hasHealthyWorkers() {
		rm.logger.Warn("No healthy workers available for job reassignment",
			zap.String("job_id", j.ID),
		)
		// Job remains in pending state until workers become available
		return nil
	}

	// Attempt to reassign the job to a different worker
	if err := rm.scheduler.TriggerAssignment(rm.ctx); err != nil {
		rm.logger.Error("Failed to trigger job reassignment",
			zap.String("job_id", j.ID),
			zap.Error(err),
		)
		return err
	}

	rm.logger.Info("Job queued for reassignment",
		zap.String("job_id", j.ID),
		zap.String("failed_worker", j.AssignedTo),
		zap.Int("retry_count", j.RetryCount+1),
	)

	return nil
}

// resetJobForReassignment resets a job's state so it can be reassigned
func (rm *RecoveryManager) resetJobForReassignment(j *job.Job) error {
	// This would typically interact with the FSM to reset job state
	// For now, we'll simulate this by updating the job directly
	j.Status = job.StatusPending
	j.AssignedTo = ""
	j.RetryCount++
	j.StartedAt = time.Time{}
	j.ErrorMessage = ""

	// Save the updated job state
	if err := rm.jobStore.SaveJob(j); err != nil {
		return fmt.Errorf("failed to save job state: %w", err)
	}

	return nil
}

// hasHealthyWorkers checks if there are any active workers available
func (rm *RecoveryManager) hasHealthyWorkers() bool {
	workers, err := rm.jobStore.ListWorkers()
	if err != nil {
		rm.logger.Error("Failed to list workers", zap.Error(err))
		return false
	}

	for _, w := range workers {
		if w.Status == "active" {
			return true
		}
	}
	return false
}

// RecoverOrphanedJobs finds and recovers jobs that may have been orphaned
func (rm *RecoveryManager) RecoverOrphanedJobs() error {
	rm.logger.Info("Starting orphaned job recovery scan")

	jobs := rm.fsm.ListJobs()
	var orphanedJobs []*job.Job
	timeout := 10 * time.Minute // Jobs running for more than 10 minutes without heartbeat

	for _, j := range jobs {
		if j.Status == job.StatusRunning {
			// Check if job has been running too long without update
			if time.Since(j.StartedAt) > timeout {
				orphanedJobs = append(orphanedJobs, j)
			}
		}
	}

	if len(orphanedJobs) == 0 {
		rm.logger.Info("No orphaned jobs found")
		return nil
	}

	rm.logger.Warn("Found potentially orphaned jobs",
		zap.Int("count", len(orphanedJobs)),
	)

	for _, j := range orphanedJobs {
		rm.logger.Warn("Recovering orphaned job",
			zap.String("job_id", j.ID),
			zap.String("assigned_to", j.AssignedTo),
			zap.Duration("running_for", time.Since(j.StartedAt)),
		)

		if err := rm.recoverJob(j); err != nil {
			rm.logger.Error("Failed to recover orphaned job",
				zap.String("job_id", j.ID),
				zap.Error(err),
			)
		}
	}

	return nil
}

// GetRecoveryStats returns recovery operation statistics
func (rm *RecoveryManager) GetRecoveryStats() RecoveryStats {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	// In a real implementation, these would be tracked properly
	return RecoveryStats{
		TotalRecoveries:      0, // TODO: Track these metrics
		SuccessfulRecoveries: 0,
		FailedRecoveries:     0,
		JobsReassigned:       0,
		JobsFailed:           0,
		LastRecoveryTime:     time.Now(),
	}
}

// ForceRecovery forces recovery of jobs for a specific worker (for testing/admin use)
func (rm *RecoveryManager) ForceRecovery(workerID string) error {
	rm.logger.Info("Force recovery requested",
		zap.String("worker_id", workerID),
	)

	return rm.recoverWorkerJobs(workerID)
}

// HealthCheck performs a health check on the recovery manager
func (rm *RecoveryManager) HealthCheck() error {
	select {
	case <-rm.ctx.Done():
		return fmt.Errorf("recovery manager is shut down")
	default:
		// Check if recovery queue is nearly full
		queueUsage := float64(len(rm.recoveryQueue)) / float64(cap(rm.recoveryQueue))
		if queueUsage > 0.8 {
			rm.logger.Warn("Recovery queue is nearly full",
				zap.Float64("usage", queueUsage),
			)
		}

		return nil
	}
}
