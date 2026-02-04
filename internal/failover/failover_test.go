package failover

import (
	"context"
	"testing"
	"time"

	"github.com/francisco3ferraz/conductor/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestFailureDetector_BasicFunctionality(t *testing.T) {
	logger := zap.NewNop()
	var failedWorkers []string
	onFailure := func(workerID string) {
		failedWorkers = append(failedWorkers, workerID)
	}

	detector := NewFailureDetector(5*time.Second, 1*time.Second, onFailure, logger)
	require.NotNil(t, detector)

	// Register a worker
	worker := &storage.WorkerInfo{
		ID:      "worker-1",
		Address: "localhost:9001",
		Status:  "active",
	}
	detector.RegisterWorker(worker)

	// Verify worker is registered and active
	health, exists := detector.GetWorkerHealth("worker-1")
	require.True(t, exists)
	assert.Equal(t, WorkerStatusActive, health.Status)
}

func TestWorkerHealthRegistry_BasicOperations(t *testing.T) {
	logger := zap.NewNop()
	registry := NewWorkerHealthRegistry(nil, nil, logger)

	// Register worker
	worker := &storage.WorkerInfo{ID: "worker-1", Address: "localhost:9001"}
	registry.RegisterWorker(worker)

	// Record heartbeat
	stats := &WorkerStats{ActiveJobs: 3}
	registry.RecordHeartbeat("worker-1", stats)

	// Verify heartbeat was recorded
	health, exists := registry.GetWorkerHealth("worker-1")
	require.True(t, exists)
	assert.Equal(t, int32(3), health.Stats.ActiveJobs)
	assert.True(t, time.Since(health.LastHeartbeat) < time.Second)
}

func TestRecoveryManager_Creation(t *testing.T) {
	logger := zap.NewNop()

	// Mock scheduler
	scheduler := &mockScheduler{}

	ctx := context.Background()
	recovery := NewRecoveryManager(ctx, scheduler, nil, nil, 3, 100*time.Millisecond, logger)
	require.NotNil(t, recovery)

	// Test health check
	err := recovery.HealthCheck()
	assert.NoError(t, err)
}

// Mock scheduler for testing
type mockScheduler struct {
	assignmentTriggered bool
}

func (m *mockScheduler) TriggerAssignment(ctx context.Context) error {
	m.assignmentTriggered = true
	return nil
}
