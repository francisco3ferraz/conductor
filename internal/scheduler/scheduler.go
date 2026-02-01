package scheduler

import (
	"context"
	"sync"
	"time"

	"github.com/francisco3ferraz/conductor/internal/consensus"
	"github.com/francisco3ferraz/conductor/internal/job"
	"github.com/francisco3ferraz/conductor/internal/storage"
	"go.uber.org/zap"
)

// WorkerInfo tracks worker state
type WorkerInfo struct {
	ID                string
	Address           string
	MaxConcurrentJobs int
	ActiveJobs        int
	LastHeartbeat     time.Time
	Status            string // "active", "inactive"
}

// Scheduler assigns jobs to workers
type Scheduler struct {
	raftNode *consensus.RaftNode
	fsm      *consensus.FSM
	applier  *consensus.ApplyCommand
	logger   *zap.Logger

	workers map[string]*WorkerInfo
	mu      sync.RWMutex

	stopCh chan struct{}
}

// NewScheduler creates a new job scheduler
func NewScheduler(raftNode *consensus.RaftNode, fsm *consensus.FSM, logger *zap.Logger) *Scheduler {
	return &Scheduler{
		raftNode: raftNode,
		fsm:      fsm,
		applier:  consensus.NewApplyCommand(raftNode),
		logger:   logger,
		workers:  make(map[string]*WorkerInfo),
		stopCh:   make(chan struct{}),
	}
}

// Start starts the scheduler background loop
func (s *Scheduler) Start(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

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
			// Only schedule if we're the leader
			if s.raftNode.IsLeader() {
				s.schedulePendingJobs()
			}
		}
	}
}

// Stop stops the scheduler
func (s *Scheduler) Stop() {
	close(s.stopCh)
}

// schedulePendingJobs finds pending jobs and assigns them to workers
func (s *Scheduler) schedulePendingJobs() {
	// Get all jobs from FSM
	jobs := s.fsm.ListJobs()

	// Find pending jobs
	var pendingJobs []*job.Job
	for _, j := range jobs {
		if j.Status == job.StatusPending {
			pendingJobs = append(pendingJobs, j)
		}
	}

	if len(pendingJobs) == 0 {
		return
	}

	s.logger.Debug("Scheduling pending jobs", zap.Int("count", len(pendingJobs)))

	// Assign each pending job to an available worker
	for _, j := range pendingJobs {
		worker := s.findAvailableWorker()
		if worker == nil {
			s.logger.Debug("No available workers", zap.String("job_id", j.ID))
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
		if err := s.sendJobToWorker(j, worker); err != nil {
			s.logger.Error("Failed to send job to worker",
				zap.String("job_id", j.ID),
				zap.String("worker_id", worker.ID),
				zap.Error(err),
			)
			// TODO: Mark job as pending again for retry
			continue
		}

		// Update worker's active job count
		s.mu.Lock()
		worker.ActiveJobs++
		s.mu.Unlock()

		s.logger.Info("Job assigned to worker",
			zap.String("job_id", j.ID),
			zap.String("worker_id", worker.ID),
		)
	}
}

// sendJobToWorker sends a job to a worker via gRPC
// RegisterWorker registers a new worker
func (s *Scheduler) RegisterWorker(id, address string, maxJobs int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.workers[id] = &WorkerInfo{
		ID:                id,
		Address:           address,
		MaxConcurrentJobs: maxJobs,
		ActiveJobs:        0,
		LastHeartbeat:     time.Now(),
		Status:            "active",
	}

	// Also register in Raft FSM
	workerInfo := &storage.WorkerInfo{
		ID:                id,
		Address:           address,
		MaxConcurrentJobs: maxJobs,
		Status:            "active",
	}
	if err := s.applier.RegisterWorker(workerInfo); err != nil {
		s.logger.Error("Failed to register worker in FSM", zap.Error(err))
	}

	s.logger.Info("Worker registered",
		zap.String("worker_id", id),
		zap.String("address", address),
		zap.Int("max_jobs", maxJobs),
	)
}

// UpdateHeartbeat updates worker's last heartbeat time
func (s *Scheduler) UpdateHeartbeat(workerID string, activeJobs int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if worker, ok := s.workers[workerID]; ok {
		worker.LastHeartbeat = time.Now()
		worker.ActiveJobs = activeJobs
		worker.Status = "active"
	}
}

// RemoveWorker removes a worker
func (s *Scheduler) RemoveWorker(workerID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.workers, workerID)

	// Remove from Raft FSM
	if err := s.applier.RemoveWorker(workerID); err != nil {
		s.logger.Error("Failed to remove worker from FSM", zap.Error(err))
	}

	s.logger.Info("Worker removed", zap.String("worker_id", workerID))
}

// ListWorkers returns all registered workers
func (s *Scheduler) ListWorkers() []*WorkerInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()

	workers := make([]*WorkerInfo, 0, len(s.workers))
	for _, w := range s.workers {
		workers = append(workers, w)
	}
	return workers
}

// CheckWorkerHealth checks for inactive workers and marks them
func (s *Scheduler) CheckWorkerHealth(timeout time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	for _, worker := range s.workers {
		if now.Sub(worker.LastHeartbeat) > timeout {
			worker.Status = "inactive"
			s.logger.Warn("Worker marked inactive",
				zap.String("worker_id", worker.ID),
				zap.Duration("since_heartbeat", now.Sub(worker.LastHeartbeat)),
			)
		}
	}
}
