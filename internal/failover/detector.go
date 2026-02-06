package failover

import (
	"context"
	"sync"
	"time"

	"github.com/francisco3ferraz/conductor/internal/storage"
	"go.uber.org/zap"
)

// FailureDetector monitors worker health and detects failures
type FailureDetector struct {
	registry      *WorkerHealthRegistry
	timeout       time.Duration
	checkInterval time.Duration
	onFailure     func(workerID string)
	logger        *zap.Logger
	stopCh        chan struct{}
	wg            sync.WaitGroup
}

// WorkerHealthRegistry tracks worker heartbeat status
type WorkerHealthRegistry struct {
	mu        sync.RWMutex
	workers   map[string]*WorkerHealth
	onFailure func(workerID string)
	onRecover func(workerID string)
	logger    *zap.Logger
}

// WorkerHealth tracks health status for a single worker
type WorkerHealth struct {
	WorkerID         string
	LastHeartbeat    time.Time
	Status           WorkerStatus
	FailureCount     int
	LastFailureTime  time.Time
	LastRecoveryTime time.Time
	Stats            *WorkerStats
}

// WorkerStats contains worker performance metrics
type WorkerStats struct {
	ActiveJobs    int32
	CompletedJobs int32
	FailedJobs    int32
}

// WorkerStatus represents the current status of a worker
type WorkerStatus string

const (
	WorkerStatusActive    WorkerStatus = "active"
	WorkerStatusSuspected WorkerStatus = "suspected"
	WorkerStatusFailed    WorkerStatus = "failed"
	WorkerStatusRecovered WorkerStatus = "recovered"
)

// NewFailureDetector creates a new failure detector
func NewFailureDetector(
	timeout time.Duration,
	checkInterval time.Duration,
	onFailure func(workerID string),
	logger *zap.Logger,
) *FailureDetector {
	registry := NewWorkerHealthRegistry(onFailure, nil, logger)

	return &FailureDetector{
		registry:      registry,
		timeout:       timeout,
		checkInterval: checkInterval,
		onFailure:     onFailure,
		logger:        logger,
		stopCh:        make(chan struct{}),
	}
}

// NewWorkerHealthRegistry creates a new worker health registry
func NewWorkerHealthRegistry(
	onFailure func(workerID string),
	onRecover func(workerID string),
	logger *zap.Logger,
) *WorkerHealthRegistry {
	return &WorkerHealthRegistry{
		workers:   make(map[string]*WorkerHealth),
		onFailure: onFailure,
		onRecover: onRecover,
		logger:    logger,
	}
}

// Start begins monitoring worker health
func (fd *FailureDetector) Start(ctx context.Context) {
	fd.wg.Add(1)
	go fd.monitorLoop(ctx)

	fd.logger.Info("Failure detector started",
		zap.Duration("timeout", fd.timeout),
		zap.Duration("check_interval", fd.checkInterval),
	)
}

// Stop stops the failure detector
func (fd *FailureDetector) Stop() {
	close(fd.stopCh)
	fd.wg.Wait()
	fd.logger.Info("Failure detector stopped")
}

// RecordHeartbeat records a heartbeat from a worker
func (fd *FailureDetector) RecordHeartbeat(workerID string, stats *WorkerStats) {
	fd.registry.RecordHeartbeat(workerID, stats)
}

// RegisterWorker adds a new worker to monitoring
func (fd *FailureDetector) RegisterWorker(worker *storage.WorkerInfo) {
	fd.registry.RegisterWorker(worker)
}

// UnregisterWorker removes a worker from monitoring
func (fd *FailureDetector) UnregisterWorker(workerID string) {
	fd.registry.UnregisterWorker(workerID)
}

// GetWorkerHealth returns the health status of a worker
func (fd *FailureDetector) GetWorkerHealth(workerID string) (*WorkerHealth, bool) {
	return fd.registry.GetWorkerHealth(workerID)
}

// ListActiveWorkers returns all active workers
func (fd *FailureDetector) ListActiveWorkers() []*WorkerHealth {
	return fd.registry.ListActiveWorkers()
}

// monitorLoop runs the periodic health check
func (fd *FailureDetector) monitorLoop(ctx context.Context) {
	defer fd.wg.Done()

	ticker := time.NewTicker(fd.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-fd.stopCh:
			return
		case <-ticker.C:
			fd.checkWorkerHealth()
		}
	}
}

// checkWorkerHealth examines all workers for failures
func (fd *FailureDetector) checkWorkerHealth() {
	now := time.Now()

	fd.registry.mu.Lock()
	defer fd.registry.mu.Unlock()

	for workerID, health := range fd.registry.workers {
		timeSinceLastHeartbeat := now.Sub(health.LastHeartbeat)

		switch health.Status {
		case WorkerStatusActive:
			if timeSinceLastHeartbeat > fd.timeout {
				fd.logger.Warn("Worker suspected of failure",
					zap.String("worker_id", workerID),
					zap.Duration("time_since_heartbeat", timeSinceLastHeartbeat),
				)
				health.Status = WorkerStatusSuspected
			}

		case WorkerStatusSuspected:
			if timeSinceLastHeartbeat > fd.timeout*2 { // Give extra time before declaring failed
				fd.logger.Error("Worker declared failed",
					zap.String("worker_id", workerID),
					zap.Duration("time_since_heartbeat", timeSinceLastHeartbeat),
				)
				health.Status = WorkerStatusFailed
				health.FailureCount++
				health.LastFailureTime = now

				// Notify failure handler
				if fd.onFailure != nil {
					go fd.onFailure(workerID)
				}
			}

		case WorkerStatusFailed:
			// Check if worker has recovered (received heartbeat recently)
			if timeSinceLastHeartbeat < fd.timeout {
				fd.logger.Info("Worker recovered from failure",
					zap.String("worker_id", workerID),
					zap.Duration("downtime", now.Sub(health.LastFailureTime)),
					zap.Int("failure_count", health.FailureCount),
				)
				health.Status = WorkerStatusRecovered
				health.LastRecoveryTime = now

				if fd.registry.onRecover != nil {
					go fd.registry.onRecover(workerID)
				}
			}

		case WorkerStatusRecovered:
			// Transition back to active after successful recovery
			if timeSinceLastHeartbeat < fd.timeout {
				fd.logger.Info("Worker fully operational after recovery",
					zap.String("worker_id", workerID),
				)
				health.Status = WorkerStatusActive
			}
		}
	}
}

// RecordHeartbeat records a heartbeat from a worker
func (whr *WorkerHealthRegistry) RecordHeartbeat(workerID string, stats *WorkerStats) {
	whr.mu.Lock()
	defer whr.mu.Unlock()

	now := time.Now()

	if health, exists := whr.workers[workerID]; exists {
		health.LastHeartbeat = now
		health.Stats = stats

		// If worker was failed/suspected, mark as recovered
		if health.Status == WorkerStatusFailed || health.Status == WorkerStatusSuspected {
			whr.logger.Info("Worker sending heartbeat after failure",
				zap.String("worker_id", workerID),
				zap.String("previous_status", string(health.Status)),
			)
			health.Status = WorkerStatusRecovered
		}
	} else {
		// Unknown worker - this might be a worker that reconnected
		whr.logger.Warn("Received heartbeat from unknown worker",
			zap.String("worker_id", workerID),
		)
		whr.workers[workerID] = &WorkerHealth{
			WorkerID:      workerID,
			LastHeartbeat: now,
			Status:        WorkerStatusActive,
			Stats:         stats,
		}
	}
}

// RegisterWorker adds a new worker to monitoring
func (whr *WorkerHealthRegistry) RegisterWorker(worker *storage.WorkerInfo) {
	whr.mu.Lock()
	defer whr.mu.Unlock()

	whr.workers[worker.ID] = &WorkerHealth{
		WorkerID:      worker.ID,
		LastHeartbeat: time.Now(),
		Status:        WorkerStatusActive,
		Stats: &WorkerStats{
			ActiveJobs:    0,
			CompletedJobs: 0,
			FailedJobs:    0,
		},
	}

	whr.logger.Info("Worker registered for health monitoring",
		zap.String("worker_id", worker.ID),
		zap.String("address", worker.Address),
	)
}

// UnregisterWorker removes a worker from monitoring
func (whr *WorkerHealthRegistry) UnregisterWorker(workerID string) {
	whr.mu.Lock()
	defer whr.mu.Unlock()

	delete(whr.workers, workerID)
	whr.logger.Info("Worker unregistered from health monitoring",
		zap.String("worker_id", workerID),
	)
}

// GetWorkerHealth returns the health status of a worker
func (whr *WorkerHealthRegistry) GetWorkerHealth(workerID string) (*WorkerHealth, bool) {
	whr.mu.RLock()
	defer whr.mu.RUnlock()

	health, exists := whr.workers[workerID]
	if !exists {
		return nil, false
	}

	// Return a copy to avoid race conditions
	healthCopy := *health
	return &healthCopy, true
}

// ListActiveWorkers returns all workers that are currently active
func (whr *WorkerHealthRegistry) ListActiveWorkers() []*WorkerHealth {
	whr.mu.RLock()
	defer whr.mu.RUnlock()

	var activeWorkers []*WorkerHealth
	for _, health := range whr.workers {
		if health.Status == WorkerStatusActive || health.Status == WorkerStatusRecovered {
			healthCopy := *health
			activeWorkers = append(activeWorkers, &healthCopy)
		}
	}

	return activeWorkers
}
