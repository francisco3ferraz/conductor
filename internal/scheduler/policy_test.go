package scheduler

import (
	"testing"
	"time"

	"github.com/francisco3ferraz/conductor/internal/job"
	"github.com/francisco3ferraz/conductor/internal/worker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJobQueue_PriorityOrdering(t *testing.T) {
	queue := NewJobQueue()

	// Create jobs with different priorities
	lowPriorityJob := job.New(job.TypeImageProcessing, []byte("low"), 1, 3, 0, "test-node")
	mediumPriorityJob := job.New(job.TypeWebScraping, []byte("medium"), 5, 3, 0, "test-node")
	highPriorityJob := job.New(job.TypeDataAnalysis, []byte("high"), 10, 3, 0, "test-node")

	// Enqueue in random order
	queue.Enqueue(mediumPriorityJob)
	queue.Enqueue(lowPriorityJob)
	queue.Enqueue(highPriorityJob)

	// Dequeue should return highest priority first
	j1 := queue.Dequeue()
	require.NotNil(t, j1)
	assert.Equal(t, 10, j1.Priority, "Should dequeue highest priority first")

	j2 := queue.Dequeue()
	require.NotNil(t, j2)
	assert.Equal(t, 5, j2.Priority, "Should dequeue medium priority second")

	j3 := queue.Dequeue()
	require.NotNil(t, j3)
	assert.Equal(t, 1, j3.Priority, "Should dequeue lowest priority last")
}

func TestJobQueue_SamePriority_FIFO(t *testing.T) {
	queue := NewJobQueue()

	// Create jobs with same priority but different creation times
	time.Sleep(1 * time.Millisecond)
	job1 := job.New(job.TypeImageProcessing, []byte("first"), 5, 3, 0, "test-node")

	time.Sleep(1 * time.Millisecond)
	job2 := job.New(job.TypeImageProcessing, []byte("second"), 5, 3, 0, "test-node")

	time.Sleep(1 * time.Millisecond)
	job3 := job.New(job.TypeImageProcessing, []byte("third"), 5, 3, 0, "test-node")

	// Enqueue in order
	queue.Enqueue(job1)
	queue.Enqueue(job2)
	queue.Enqueue(job3)

	// Should dequeue in FIFO order when priority is same
	assert.Equal(t, job1.ID, queue.Dequeue().ID)
	assert.Equal(t, job2.ID, queue.Dequeue().ID)
	assert.Equal(t, job3.ID, queue.Dequeue().ID)
}

func TestRoundRobinPolicy(t *testing.T) {
	policy := NewRoundRobinPolicy()

	workers := []*worker.WorkerInfo{
		{ID: "worker-1", Status: "active", ActiveJobs: 0, MaxConcurrentJobs: 10},
		{ID: "worker-2", Status: "active", ActiveJobs: 0, MaxConcurrentJobs: 10},
		{ID: "worker-3", Status: "active", ActiveJobs: 0, MaxConcurrentJobs: 10},
	}

	j := job.New(job.TypeImageProcessing, []byte("test"), 5, 3, 0, "test-node")

	// Should cycle through workers
	w1 := policy.SelectWorker(j, workers)
	w2 := policy.SelectWorker(j, workers)
	w3 := policy.SelectWorker(j, workers)
	w4 := policy.SelectWorker(j, workers) // Should wrap around

	assert.NotEqual(t, w1.ID, w2.ID)
	assert.NotEqual(t, w2.ID, w3.ID)
	assert.Equal(t, w1.ID, w4.ID, "Should wrap around to first worker")
}

func TestLeastLoadedPolicy(t *testing.T) {
	policy := NewLeastLoadedPolicy()

	workers := []*worker.WorkerInfo{
		{ID: "worker-1", Status: "active", ActiveJobs: 5, MaxConcurrentJobs: 10},
		{ID: "worker-2", Status: "active", ActiveJobs: 2, MaxConcurrentJobs: 10},
		{ID: "worker-3", Status: "active", ActiveJobs: 8, MaxConcurrentJobs: 10},
	}

	j := job.New(job.TypeImageProcessing, []byte("test"), 5, 3, 0, "test-node")

	// Should select worker with fewest active jobs
	selected := policy.SelectWorker(j, workers)
	assert.Equal(t, "worker-2", selected.ID, "Should select least loaded worker")
}

func TestPriorityPolicy_HighPriorityJob(t *testing.T) {
	policy := NewPriorityPolicy()

	workers := []*worker.WorkerInfo{
		{ID: "worker-1", Status: "active", ActiveJobs: 5, MaxConcurrentJobs: 10},
		{ID: "worker-2", Status: "active", ActiveJobs: 1, MaxConcurrentJobs: 10},
		{ID: "worker-3", Status: "active", ActiveJobs: 8, MaxConcurrentJobs: 10},
	}

	// High priority job should get least loaded worker
	highPriorityJob := job.New(job.TypeImageProcessing, []byte("urgent"), 9, 3, 0, "test-node")
	selected := policy.SelectWorker(highPriorityJob, workers)
	assert.Equal(t, "worker-2", selected.ID, "High priority job should get least loaded worker")
}

func TestCapacityAwarePolicy(t *testing.T) {
	policy := NewCapacityAwarePolicy()

	workers := []*worker.WorkerInfo{
		{ID: "worker-1", Status: "active", ActiveJobs: 5, MaxConcurrentJobs: 10}, // 5 available
		{ID: "worker-2", Status: "active", ActiveJobs: 2, MaxConcurrentJobs: 20}, // 18 available
		{ID: "worker-3", Status: "active", ActiveJobs: 1, MaxConcurrentJobs: 5},  // 4 available
	}

	j := job.New(job.TypeImageProcessing, []byte("test"), 5, 3, 0, "test-node")

	// Should select worker with most available capacity
	selected := policy.SelectWorker(j, workers)
	assert.Equal(t, "worker-2", selected.ID, "Should select worker with most available capacity")
}

func TestSchedulingPolicy_EmptyWorkers(t *testing.T) {
	policies := []SchedulingPolicy{
		NewRoundRobinPolicy(),
		NewLeastLoadedPolicy(),
		NewPriorityPolicy(),
		NewRandomPolicy(),
		NewCapacityAwarePolicy(),
	}

	j := job.New(job.TypeImageProcessing, []byte("test"), 5, 3, 0, "test-node")
	emptyWorkers := []*worker.WorkerInfo{}

	for _, policy := range policies {
		selected := policy.SelectWorker(j, emptyWorkers)
		assert.Nil(t, selected, "All policies should return nil for empty worker list")
	}
}
