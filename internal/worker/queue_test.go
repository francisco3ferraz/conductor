package worker

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/francisco3ferraz/conductor/internal/job"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// Mock result reporter for testing
type mockReporter struct {
	mu      sync.Mutex
	results map[string]*job.Result
}

func (m *mockReporter) ReportResult(ctx context.Context, jobID string, result *job.Result) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.results[jobID] = result
	return nil
}

func TestPriorityQueue_HighPriorityFirst(t *testing.T) {
	logger := zap.NewNop()
	exec := NewExecutor("test-worker", logger)
	reporter := &mockReporter{results: make(map[string]*job.Result)}

	queue := NewJobQueue(exec, reporter, 1, logger)
	queue.Start()
	defer queue.Stop()

	// Create jobs with different priorities
	lowPriorityJob := job.New(job.TypeImageProcessing, []byte("low"), 1, 3, 60, "test")
	medPriorityJob := job.New(job.TypeImageProcessing, []byte("med"), 5, 3, 60, "test")
	highPriorityJob := job.New(job.TypeImageProcessing, []byte("high"), 10, 3, 60, "test")

	// Enqueue in wrong order
	queue.Enqueue(context.Background(), lowPriorityJob)
	queue.Enqueue(context.Background(), highPriorityJob)
	queue.Enqueue(context.Background(), medPriorityJob)

	// Wait for processing (jobs take 1-3 seconds each)
	time.Sleep(10 * time.Second)

	// Verify all jobs were processed
	reporter.mu.Lock()
	defer reporter.mu.Unlock()

	assert.Equal(t, 3, len(reporter.results), "All jobs should be processed")
	assert.Contains(t, reporter.results, highPriorityJob.ID)
	assert.Contains(t, reporter.results, medPriorityJob.ID)
	assert.Contains(t, reporter.results, lowPriorityJob.ID)
}

func TestPriorityQueue_SamePriorityFIFO(t *testing.T) {
	logger := zap.NewNop()
	exec := NewExecutor("test-worker", logger)
	reporter := &mockReporter{results: make(map[string]*job.Result)}

	queue := NewJobQueue(exec, reporter, 1, logger)
	queue.Start()
	defer queue.Stop()

	// Create jobs with same priority
	job1 := job.New(job.TypeImageProcessing, []byte("first"), 5, 3, 60, "test")
	time.Sleep(10 * time.Millisecond) // Ensure different timestamps
	job2 := job.New(job.TypeImageProcessing, []byte("second"), 5, 3, 60, "test")
	time.Sleep(10 * time.Millisecond)
	job3 := job.New(job.TypeImageProcessing, []byte("third"), 5, 3, 60, "test")

	// Enqueue in order
	queue.Enqueue(context.Background(), job1)
	queue.Enqueue(context.Background(), job2)
	queue.Enqueue(context.Background(), job3)

	// Verify FIFO ordering by created time
	require.True(t, job1.CreatedAt.Before(job2.CreatedAt))
	require.True(t, job2.CreatedAt.Before(job3.CreatedAt))

	// Wait for processing (jobs take 1-3 seconds each)
	time.Sleep(10 * time.Second)

	// Verify all processed
	reporter.mu.Lock()
	defer reporter.mu.Unlock()
	assert.Equal(t, 3, len(reporter.results))
}

func TestPriorityQueue_Concurrency(t *testing.T) {
	logger := zap.NewNop()
	exec := NewExecutor("test-worker", logger)
	reporter := &mockReporter{results: make(map[string]*job.Result)}

	// Use 3 concurrent workers
	queue := NewJobQueue(exec, reporter, 3, logger)
	queue.Start()
	defer queue.Stop()

	// Create multiple jobs
	numJobs := 10
	for i := 0; i < numJobs; i++ {
		j := job.New(job.TypeImageProcessing, []byte("test"), i%5, 3, 60, "test")
		queue.Enqueue(context.Background(), j)
	}

	// Wait for all to process (jobs take 1-3 seconds, 3 workers concurrently)
	time.Sleep(15 * time.Second)

	// Verify all processed
	reporter.mu.Lock()
	defer reporter.mu.Unlock()
	assert.Equal(t, numJobs, len(reporter.results), "All jobs should be processed")
}

func TestPriorityQueue_Size(t *testing.T) {
	logger := zap.NewNop()
	exec := NewExecutor("test-worker", logger)
	reporter := &mockReporter{results: make(map[string]*job.Result)}

	queue := NewJobQueue(exec, reporter, 1, logger)
	// Don't start workers yet to test queue size

	assert.Equal(t, 0, queue.Size())

	j1 := job.New(job.TypeImageProcessing, []byte("test1"), 5, 3, 60, "test")
	queue.Enqueue(context.Background(), j1)
	assert.Equal(t, 1, queue.Size())

	j2 := job.New(job.TypeImageProcessing, []byte("test2"), 5, 3, 60, "test")
	queue.Enqueue(context.Background(), j2)
	assert.Equal(t, 2, queue.Size())

	// Now start and process
	queue.Start()
	defer queue.Stop()

	// Wait for processing
	time.Sleep(3 * time.Second)

	assert.Equal(t, 0, queue.Size(), "Queue should be empty after processing")
}
