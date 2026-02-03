package scheduler

import (
	"testing"
	"time"

	"github.com/francisco3ferraz/conductor/internal/config"
	"github.com/francisco3ferraz/conductor/internal/consensus"
	"github.com/francisco3ferraz/conductor/internal/job"
	"github.com/francisco3ferraz/conductor/internal/worker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// Helper function to create test config
func testConfig() *config.Config {
	return &config.Config{
		Scheduler: config.SchedulerConfig{
			AssignmentTimeout: 5 * time.Second,
		},
		Worker: config.WorkerConfig{
			HeartbeatTimeout: 10 * time.Second,
			ResultTimeout:    10 * time.Second,
		},
		Raft: config.RaftConfig{
			BarrierTimeout: 5 * time.Second,
		},
		GRPC: config.GRPCConfig{
			DialTimeout:    2 * time.Second,
			ConnectionWait: 2 * time.Second,
		},
	}
}

func TestScheduler_NewScheduler(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	fsm := consensus.NewFSM(logger)
	cfg := testConfig()

	scheduler := NewScheduler(nil, fsm, cfg, logger)

	assert.NotNil(t, scheduler)
	assert.NotNil(t, scheduler.registry)
	assert.NotNil(t, scheduler.stopCh)
	assert.NotNil(t, scheduler.logger)
	assert.Equal(t, fsm, scheduler.fsm)
}

func TestScheduler_RegisterWorker(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	fsm := consensus.NewFSM(logger)
	cfg := testConfig()
	scheduler := NewScheduler(nil, fsm, cfg, logger)

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
	cfg := testConfig()
	scheduler := NewScheduler(nil, fsm, cfg, logger)

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
	cfg := testConfig()
	scheduler := NewScheduler(nil, fsm, cfg, logger)

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
	cfg := testConfig()
	scheduler := NewScheduler(nil, fsm, cfg, logger)

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
	cfg := testConfig()
	scheduler := NewScheduler(nil, fsm, cfg, logger)

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

func TestScheduler_PolicyBasedWorkerSelection(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	fsm := consensus.NewFSM(logger)
	cfg := testConfig()
	scheduler := NewScheduler(nil, fsm, cfg, logger)

	// Set to LeastLoaded policy for predictable testing
	scheduler.SetSchedulingPolicy(NewLeastLoadedPolicy())

	// No workers - policy should return nil
	workers := scheduler.registry.List()
	assert.Empty(t, workers)

	// Register workers with different loads
	scheduler.registry.Register("worker-1", "localhost:9001", 10)
	scheduler.registry.Register("worker-2", "localhost:9002", 10)
	scheduler.registry.Register("worker-3", "localhost:9003", 10)

	// Update loads: worker-1 has 5 jobs, worker-2 has 0, worker-3 has 8
	scheduler.UpdateHeartbeat("worker-1", 5)
	scheduler.UpdateHeartbeat("worker-2", 0)
	scheduler.UpdateHeartbeat("worker-3", 8)

	// Get available workers
	allWorkers := scheduler.registry.List()
	availableWorkers := make([]*worker.WorkerInfo, 0)
	for i := range allWorkers {
		w := &allWorkers[i] // Take address of copy
		if w.Status == "active" && w.ActiveJobs < w.MaxConcurrentJobs {
			availableWorkers = append(availableWorkers, w)
		}
	}

	// Policy should select worker with least load (worker-2 with 0 jobs)
	dummyJob := &job.Job{Priority: 5}
	selected := scheduler.policy.SelectWorker(dummyJob, availableWorkers)
	assert.NotNil(t, selected)
	assert.Equal(t, "worker-2", selected.ID)
	assert.Equal(t, 0, selected.ActiveJobs)

	// Make all workers fully loaded
	scheduler.UpdateHeartbeat("worker-1", 10)
	scheduler.UpdateHeartbeat("worker-2", 10)
	scheduler.UpdateHeartbeat("worker-3", 10)

	// No available workers
	allWorkers = scheduler.registry.List()
	availableWorkers = make([]*worker.WorkerInfo, 0)
	for i := range allWorkers {
		w := &allWorkers[i] // Take address of copy
		if w.Status == "active" && w.ActiveJobs < w.MaxConcurrentJobs {
			availableWorkers = append(availableWorkers, w)
		}
	}
	assert.Empty(t, availableWorkers)
}

func TestScheduler_SchedulePendingJobs_NoWorkers(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	fsm := consensus.NewFSM(logger)
	cfg := testConfig()
	scheduler := NewScheduler(nil, fsm, cfg, logger)

	// Schedule without workers - should not crash
	scheduler.schedulePendingJobs()

	// No jobs to schedule, just verifying it doesn't crash
}

func TestScheduler_Stop(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	fsm := consensus.NewFSM(logger)
	cfg := testConfig()
	scheduler := NewScheduler(nil, fsm, cfg, logger)

	// Stop should not panic
	scheduler.Stop()
}

func TestScheduler_PolicyIgnoresInactiveWorkers(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	fsm := consensus.NewFSM(logger)
	cfg := testConfig()
	scheduler := NewScheduler(nil, fsm, cfg, logger)

	// Register workers
	scheduler.registry.Register("worker-active", "localhost:9001", 10)
	scheduler.registry.Register("worker-inactive", "localhost:9002", 10)

	// Mark one as inactive by not sending heartbeat
	time.Sleep(10 * time.Millisecond)
	scheduler.UpdateHeartbeat("worker-active", 0)
	scheduler.CheckWorkerHealth(5 * time.Millisecond)

	// Get available workers (should only include active ones)
	allWorkers := scheduler.registry.List()
	availableWorkers := make([]*worker.WorkerInfo, 0)
	for i := range allWorkers {
		w := &allWorkers[i] // Take address of copy
		if w.Status == "active" && w.ActiveJobs < w.MaxConcurrentJobs {
			availableWorkers = append(availableWorkers, w)
		}
	}

	// Should only have the active worker
	assert.Len(t, availableWorkers, 1)
	assert.Equal(t, "worker-active", availableWorkers[0].ID)

	// Policy should select the active worker
	dummyJob := &job.Job{Priority: 5}
	selected := scheduler.policy.SelectWorker(dummyJob, availableWorkers)
	assert.NotNil(t, selected)
	assert.Equal(t, "worker-active", selected.ID)
}
