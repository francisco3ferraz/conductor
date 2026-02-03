package job

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewJob(t *testing.T) {
	payload := []byte("test payload")
	j := New(TypeImageProcessing, payload, 5, 3, 0, "test-node")

	assert.NotEmpty(t, j.ID)
	assert.Equal(t, TypeImageProcessing, j.Type)
	assert.Equal(t, payload, j.Payload)
	assert.Equal(t, 5, j.Priority)
	assert.Equal(t, StatusPending, j.Status)
	assert.Equal(t, 3, j.MaxRetries)
	assert.Equal(t, 0, j.RetryCount)
}

func TestJobStateMachine(t *testing.T) {
	j := New(TypeDataAnalysis, []byte("data"), 1, 3, 0, "test-node")

	// Test assign
	err := j.Assign("worker-1")
	require.NoError(t, err)
	assert.Equal(t, StatusAssigned, j.Status)
	assert.Equal(t, "worker-1", j.AssignedTo)

	// Cannot assign again
	err = j.Assign("worker-2")
	assert.Error(t, err)

	// Test start
	err = j.Start()
	require.NoError(t, err)
	assert.Equal(t, StatusRunning, j.Status)
	assert.False(t, j.StartedAt.IsZero())

	// Test complete
	result := NewResult([]byte("output"), 100*time.Millisecond)
	err = j.Complete(result)
	require.NoError(t, err)
	assert.Equal(t, StatusCompleted, j.Status)
	assert.False(t, j.CompletedAt.IsZero())
	assert.Equal(t, result, j.Result)
}

func TestJobFailAndRetry(t *testing.T) {
	j := New(TypeWebScraping, []byte("url"), 1, 3, 0, "test-node")
	j.Assign("worker-1")
	j.Start()

	// First failure - should allow retry
	err := j.Fail("network error")
	require.NoError(t, err)
	assert.Equal(t, StatusPending, j.Status)
	assert.Equal(t, 1, j.RetryCount)
	assert.True(t, j.CanRetry())
	assert.Empty(t, j.AssignedTo)

	// Retry
	j.Assign("worker-2")
	j.Start()
	j.Fail("timeout")

	assert.Equal(t, 2, j.RetryCount)
	assert.True(t, j.CanRetry())

	// Third failure - should mark as failed
	j.Assign("worker-3")
	j.Start()
	j.Fail("persistent error")

	assert.Equal(t, 3, j.RetryCount)
	assert.Equal(t, StatusFailed, j.Status)
	assert.False(t, j.CanRetry())
}

func TestJobCancel(t *testing.T) {
	j := New(TypeImageProcessing, []byte("image"), 1, 3, 0, "test-node")

	err := j.Cancel()
	require.NoError(t, err)
	assert.Equal(t, StatusCancelled, j.Status)

	// Cannot cancel completed job
	j2 := New(TypeDataAnalysis, []byte("data"), 1, 3, 0, "test-node")
	j2.Assign("worker-1")
	j2.Start()
	j2.Complete(NewResult([]byte("result"), time.Second))

	err = j2.Cancel()
	assert.Error(t, err)
}

func TestJobDuration(t *testing.T) {
	j := New(TypeWebScraping, []byte("url"), 1, 3, 0, "test-node")
	j.Assign("worker-1")

	time.Sleep(10 * time.Millisecond)
	j.Start()

	time.Sleep(50 * time.Millisecond)
	j.Complete(NewResult([]byte("output"), 50*time.Millisecond))

	duration := j.Duration()
	assert.True(t, duration >= 50*time.Millisecond)
}

func TestJobTimeout(t *testing.T) {
	// Job with 100ms timeout
	j := New(TypeImageProcessing, []byte("test"), 1, 3, 0, "test-node") // 0 = no timeout
	j.Assign("worker-1")
	j.Start()

	// Should not timeout with 0 timeout
	assert.False(t, j.IsTimeout())
	assert.Equal(t, time.Duration(0), j.TimeoutDuration())
	assert.Equal(t, time.Duration(0), j.RemainingTimeout())

	// Job with actual timeout
	timeoutJob := New(TypeImageProcessing, []byte("test"), 1, 3, 1, "test-node") // 1 second timeout
	timeoutJob.Assign("worker-1")

	// Not timeout before starting
	assert.False(t, timeoutJob.IsTimeout())

	timeoutJob.Start()

	// Should not timeout immediately
	assert.False(t, timeoutJob.IsTimeout())
	assert.Equal(t, time.Second, timeoutJob.TimeoutDuration())
	assert.Greater(t, timeoutJob.RemainingTimeout(), time.Duration(0))

	// Simulate job running for over timeout period
	timeoutJob.StartedAt = time.Now().Add(-2 * time.Second) // Started 2 seconds ago

	// Should now be timed out
	assert.True(t, timeoutJob.IsTimeout())
	assert.Equal(t, time.Duration(0), timeoutJob.RemainingTimeout())

	// Completed job should not timeout
	timeoutJob.Complete(NewResult([]byte("done"), time.Second))
	assert.False(t, timeoutJob.IsTimeout())
}

func TestStatusStringConversion(t *testing.T) {
	tests := []struct {
		status Status
		str    string
	}{
		{StatusPending, "pending"},
		{StatusAssigned, "assigned"},
		{StatusRunning, "running"},
		{StatusCompleted, "completed"},
		{StatusFailed, "failed"},
		{StatusCancelled, "cancelled"},
	}

	for _, tt := range tests {
		t.Run(tt.str, func(t *testing.T) {
			assert.Equal(t, tt.str, tt.status.String())

			parsed, err := ParseStatus(tt.str)
			require.NoError(t, err)
			assert.Equal(t, tt.status, parsed)
		})
	}
}

func TestTypeStringConversion(t *testing.T) {
	tests := []struct {
		jobType Type
		str     string
	}{
		{TypeImageProcessing, "image_processing"},
		{TypeWebScraping, "web_scraping"},
		{TypeDataAnalysis, "data_analysis"},
	}

	for _, tt := range tests {
		t.Run(tt.str, func(t *testing.T) {
			assert.Equal(t, tt.str, tt.jobType.String())

			parsed, err := ParseType(tt.str)
			require.NoError(t, err)
			assert.Equal(t, tt.jobType, parsed)
		})
	}
}
