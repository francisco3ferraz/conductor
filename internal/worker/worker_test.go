package worker

import (
	"context"
	"testing"
	"time"

	"github.com/francisco3ferraz/conductor/internal/job"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// ========== Executor Tests ==========

func TestExecutor_NewExecutor(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	executor := NewExecutor("worker-1", logger)

	assert.NotNil(t, executor)
	assert.Equal(t, "worker-1", executor.workerID)
}

func TestExecutor_Execute_ImageProcessing(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	executor := NewExecutor("worker-1", logger)

	j := job.New(job.TypeImageProcessing, []byte("test-image.jpg"), 5, 3, 0)
	ctx := context.Background()

	result := executor.Execute(ctx, j)

	assert.NotNil(t, result)
	assert.True(t, result.Success)
	assert.Greater(t, result.DurationMs, int64(0))
	assert.NotEmpty(t, result.Output)
}

func TestExecutor_Execute_WebScraping(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	executor := NewExecutor("worker-1", logger)

	j := job.New(job.TypeWebScraping, []byte("https://example.com"), 5, 3, 0)
	ctx := context.Background()

	result := executor.Execute(ctx, j)

	// Web scraping has random success (90% success rate in executor)
	// Just verify we get a result with duration
	assert.NotNil(t, result)
	assert.Greater(t, result.DurationMs, int64(0))
}

func TestExecutor_Execute_DataAnalysis(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	executor := NewExecutor("worker-1", logger)

	j := job.New(job.TypeDataAnalysis, []byte("data.csv"), 5, 3, 0)
	ctx := context.Background()

	result := executor.Execute(ctx, j)

	assert.NotNil(t, result)
	assert.True(t, result.Success)
	assert.Greater(t, result.DurationMs, int64(0))
}

// ========== Manager Tests ==========

func TestManager_NewManager(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	executor := NewExecutor("worker-1", logger)
	manager := NewManager("worker-1", executor, nil, logger)

	assert.NotNil(t, manager)
	assert.Equal(t, "worker-1", manager.id)
}

func TestManager_StartAndStop(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	executor := NewExecutor("worker-1", logger)
	manager := NewManager("worker-1", executor, nil, logger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := manager.Start(ctx)
	assert.NoError(t, err)

	err = manager.Stop()
	assert.NoError(t, err)
}

func TestManager_GetID(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	executor := NewExecutor("worker-1", logger)
	manager := NewManager("worker-123", executor, nil, logger)

	assert.Equal(t, "worker-123", manager.GetID())
}

func TestManager_GetExecutor(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	executor := NewExecutor("worker-1", logger)
	manager := NewManager("worker-1", executor, nil, logger)

	retrievedExecutor := manager.GetExecutor()
	assert.Equal(t, executor, retrievedExecutor)
}

// ========== Registry Tests ==========

func TestRegistry_RegisterAndGet(t *testing.T) {
	registry := NewRegistry()

	// Register a worker
	registry.Register("worker-1", "localhost:9001", 10)

	// Verify worker is registered
	worker, exists := registry.Get("worker-1")
	require.True(t, exists)
	assert.Equal(t, "worker-1", worker.ID)
	assert.Equal(t, "localhost:9001", worker.Address)
	assert.Equal(t, 10, worker.MaxConcurrentJobs)
	assert.Equal(t, 0, worker.ActiveJobs)
	assert.Equal(t, "active", worker.Status)
}

func TestRegistry_Unregister(t *testing.T) {
	registry := NewRegistry()

	// Register and then unregister a worker
	registry.Register("worker-1", "localhost:9001", 10)
	registry.Unregister("worker-1")

	// Verify worker is removed
	_, exists := registry.Get("worker-1")
	assert.False(t, exists)
}

func TestRegistry_List(t *testing.T) {
	registry := NewRegistry()

	// Register multiple workers
	registry.Register("worker-1", "localhost:9001", 10)
	registry.Register("worker-2", "localhost:9002", 5)
	registry.Register("worker-3", "localhost:9003", 8)

	// List all workers
	workers := registry.List()
	assert.Len(t, workers, 3)

	// Verify all IDs are present
	ids := make(map[string]bool)
	for _, w := range workers {
		ids[w.ID] = true
	}
	assert.True(t, ids["worker-1"])
	assert.True(t, ids["worker-2"])
	assert.True(t, ids["worker-3"])
}

func TestRegistry_UpdateHeartbeat(t *testing.T) {
	registry := NewRegistry()

	// Register a worker
	registry.Register("worker-1", "localhost:9001", 10)

	// Get initial heartbeat
	worker, _ := registry.Get("worker-1")
	initialHeartbeat := worker.LastHeartbeat

	// Wait a bit and update heartbeat
	time.Sleep(10 * time.Millisecond)
	registry.UpdateHeartbeat("worker-1", 3)

	// Verify heartbeat was updated
	worker, _ = registry.Get("worker-1")
	assert.True(t, worker.LastHeartbeat.After(initialHeartbeat))
	assert.Equal(t, 3, worker.ActiveJobs)
	assert.Equal(t, "active", worker.Status)
}

func TestRegistry_MarkInactive(t *testing.T) {
	registry := NewRegistry()

	// Register workers
	registry.Register("worker-1", "localhost:9001", 10)
	registry.Register("worker-2", "localhost:9002", 10)
	registry.Register("worker-3", "localhost:9003", 10)

	// Update heartbeat for worker-2 to keep it active
	time.Sleep(50 * time.Millisecond)
	registry.UpdateHeartbeat("worker-2", 0)

	// Mark workers as inactive if no heartbeat in last 30ms
	inactive := registry.MarkInactive(30 * time.Millisecond)

	// Worker-1 and worker-3 should be inactive, worker-2 should be active
	assert.Len(t, inactive, 2)
	assert.Contains(t, inactive, "worker-1")
	assert.Contains(t, inactive, "worker-3")

	// Verify statuses
	worker1, _ := registry.Get("worker-1")
	assert.Equal(t, "inactive", worker1.Status)

	worker2, _ := registry.Get("worker-2")
	assert.Equal(t, "active", worker2.Status)

	worker3, _ := registry.Get("worker-3")
	assert.Equal(t, "inactive", worker3.Status)
}

func TestRegistry_ListAvailableWorkers(t *testing.T) {
	registry := NewRegistry()

	// No workers - should return empty list
	workers := registry.List()
	assert.Empty(t, workers)

	// Register workers
	registry.Register("worker-1", "localhost:9001", 2)
	registry.Register("worker-2", "localhost:9002", 5)

	// Both workers should be listed
	workers = registry.List()
	assert.Len(t, workers, 2)

	// Check availability status
	var availableCount int
	for _, w := range workers {
		if w.Status == "active" && w.ActiveJobs < w.MaxConcurrentJobs {
			availableCount++
		}
	}
	assert.Equal(t, 2, availableCount)

	// Make worker-1 fully loaded
	registry.UpdateHeartbeat("worker-1", 2)

	// Check availability again
	workers = registry.List()
	availableCount = 0
	for _, w := range workers {
		if w.Status == "active" && w.ActiveJobs < w.MaxConcurrentJobs {
			availableCount++
		}
	}
	assert.Equal(t, 1, availableCount)

	// Make worker-2 fully loaded
	registry.UpdateHeartbeat("worker-2", 5)

	// No available workers
	workers = registry.List()
	availableCount = 0
	for _, w := range workers {
		if w.Status == "active" && w.ActiveJobs < w.MaxConcurrentJobs {
			availableCount++
		}
	}
	assert.Equal(t, 0, availableCount)
}

func TestWorkerInfo_ToStorageWorkerInfo(t *testing.T) {
	worker := &WorkerInfo{
		ID:                "worker-1",
		Address:           "localhost:9001",
		MaxConcurrentJobs: 10,
		ActiveJobs:        3,
		LastHeartbeat:     time.Now(),
		Status:            "active",
	}

	storageWorker := worker.ToStorageWorkerInfo()
	assert.Equal(t, worker.ID, storageWorker.ID)
	assert.Equal(t, worker.Address, storageWorker.Address)
	assert.Equal(t, worker.MaxConcurrentJobs, storageWorker.MaxConcurrentJobs)
	assert.Equal(t, worker.Status, storageWorker.Status)
}
