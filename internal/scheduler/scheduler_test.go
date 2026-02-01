package scheduler

import (
	"testing"
	"time"

	"github.com/francisco3ferraz/conductor/internal/consensus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestScheduler_NewScheduler(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	fsm := consensus.NewFSM(logger)

	scheduler := NewScheduler(nil, fsm, logger)

	assert.NotNil(t, scheduler)
	assert.NotNil(t, scheduler.registry)
	assert.NotNil(t, scheduler.stopCh)
	assert.NotNil(t, scheduler.logger)
	assert.Equal(t, fsm, scheduler.fsm)
}

func TestScheduler_RegisterWorker(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	fsm := consensus.NewFSM(logger)
	scheduler := NewScheduler(nil, fsm, logger)

	// Register directly in registry (RegisterWorker method requires Raft)
	scheduler.registry.Register("worker-test", "localhost:9999", 5)

	// Verify it's in registry
	workers := scheduler.ListWorkers()
	require.Len(t, workers, 1)
	assert.Equal(t, "worker-test", workers[0].ID)
	assert.Equal(t, "localhost:9999", workers[0].Address)
	assert.Equal(t, 5, workers[0].MaxConcurrentJobs)
	assert.Equal(t, "active", workers[0].Status)
}

func TestScheduler_ListWorkers(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	fsm := consensus.NewFSM(logger)
	scheduler := NewScheduler(nil, fsm, logger)

	// Register multiple workers directly in registry
	scheduler.registry.Register("worker-1", "localhost:9001", 10)
	scheduler.registry.Register("worker-2", "localhost:9002", 5)
	scheduler.registry.Register("worker-3", "localhost:9003", 8)

	// List all workers
	workers := scheduler.ListWorkers()
	assert.Len(t, workers, 3)

	// Verify all workers present
	ids := make(map[string]bool)
	for _, w := range workers {
		ids[w.ID] = true
	}
	assert.True(t, ids["worker-1"])
	assert.True(t, ids["worker-2"])
	assert.True(t, ids["worker-3"])
}

func TestScheduler_UpdateHeartbeat(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	fsm := consensus.NewFSM(logger)
	scheduler := NewScheduler(nil, fsm, logger)

	// Register worker directly
	scheduler.registry.Register("worker-1", "localhost:9001", 10)

	// Get initial state
	workers := scheduler.ListWorkers()
	require.Len(t, workers, 1)
	initialHeartbeat := workers[0].LastHeartbeat
	assert.Equal(t, 0, workers[0].ActiveJobs)

	// Wait and update heartbeat
	time.Sleep(10 * time.Millisecond)
	scheduler.UpdateHeartbeat("worker-1", 5)

	// Verify heartbeat was updated
	workers = scheduler.ListWorkers()
	require.Len(t, workers, 1)
	assert.True(t, workers[0].LastHeartbeat.After(initialHeartbeat))
	assert.Equal(t, 5, workers[0].ActiveJobs)
}

func TestScheduler_RemoveWorker(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	fsm := consensus.NewFSM(logger)
	scheduler := NewScheduler(nil, fsm, logger)

	// Register and then remove (use registry directly to avoid Raft calls)
	scheduler.registry.Register("worker-remove", "localhost:9999", 5)
	workers := scheduler.ListWorkers()
	require.Len(t, workers, 1)

	scheduler.registry.Unregister("worker-remove")
	workers = scheduler.ListWorkers()
	assert.Len(t, workers, 0)
}

func TestScheduler_CheckWorkerHealth(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	fsm := consensus.NewFSM(logger)
	scheduler := NewScheduler(nil, fsm, logger)

	// Register workers directly
	scheduler.registry.Register("worker-1", "localhost:9001", 10)
	scheduler.registry.Register("worker-2", "localhost:9002", 10)

	// Update heartbeat for worker-2
	time.Sleep(50 * time.Millisecond)
	scheduler.UpdateHeartbeat("worker-2", 0)

	// Check health with 30ms timeout
	scheduler.CheckWorkerHealth(30 * time.Millisecond)

	// Verify worker-1 is inactive, worker-2 is active
	workers := scheduler.ListWorkers()
	require.Len(t, workers, 2)

	for _, w := range workers {
		if w.ID == "worker-1" {
			assert.Equal(t, "inactive", w.Status)
		} else if w.ID == "worker-2" {
			assert.Equal(t, "active", w.Status)
		}
	}
}

func TestScheduler_FindAvailableWorker(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	fsm := consensus.NewFSM(logger)
	scheduler := NewScheduler(nil, fsm, logger)

	// No workers - should find none
	worker := scheduler.findAvailableWorker()
	assert.Nil(t, worker)

	// Register workers with different loads
	scheduler.registry.Register("worker-1", "localhost:9001", 10)
	scheduler.registry.Register("worker-2", "localhost:9002", 10)
	scheduler.registry.Register("worker-3", "localhost:9003", 10)

	// Update loads: worker-1 has 5 jobs, worker-2 has 0, worker-3 has 8
	scheduler.UpdateHeartbeat("worker-1", 5)
	scheduler.UpdateHeartbeat("worker-2", 0)
	scheduler.UpdateHeartbeat("worker-3", 8)

	// Should find an available worker (with capacity)
	worker = scheduler.findAvailableWorker()
	assert.NotNil(t, worker)
	assert.True(t, worker.ActiveJobs < worker.MaxConcurrentJobs)

	// Make all workers fully loaded
	scheduler.UpdateHeartbeat("worker-1", 10)
	scheduler.UpdateHeartbeat("worker-2", 10)
	scheduler.UpdateHeartbeat("worker-3", 10)

	// No available workers
	worker = scheduler.findAvailableWorker()
	assert.Nil(t, worker)
}

func TestScheduler_SchedulePendingJobs_NoWorkers(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	fsm := consensus.NewFSM(logger)
	scheduler := NewScheduler(nil, fsm, logger)

	// Schedule without workers - should not crash
	scheduler.schedulePendingJobs()

	// No jobs to schedule, just verifying it doesn't crash
}

func TestScheduler_Stop(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	fsm := consensus.NewFSM(logger)
	scheduler := NewScheduler(nil, fsm, logger)

	// Stop should not panic
	scheduler.Stop()
}

func TestScheduler_FindAvailableWorker_InactiveWorkersIgnored(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	fsm := consensus.NewFSM(logger)
	scheduler := NewScheduler(nil, fsm, logger)

	// Register workers
	scheduler.registry.Register("worker-active", "localhost:9001", 10)
	scheduler.registry.Register("worker-inactive", "localhost:9002", 10)

	// Mark one as inactive by not sending heartbeat
	time.Sleep(10 * time.Millisecond)
	scheduler.UpdateHeartbeat("worker-active", 0)
	scheduler.CheckWorkerHealth(5 * time.Millisecond)

	// Should only find active worker
	worker := scheduler.findAvailableWorker()
	assert.NotNil(t, worker)
	assert.Equal(t, "worker-active", worker.ID)
}
