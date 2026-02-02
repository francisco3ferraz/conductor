package job

import (
	"fmt"
	"sync/atomic"
	"time"
)

// Status represents the current state of a job
type Status int

const (
	StatusPending Status = iota
	StatusAssigned
	StatusRunning
	StatusCompleted
	StatusFailed
	StatusCancelled
)

// String returns the string representation of the status
func (s Status) String() string {
	switch s {
	case StatusPending:
		return "pending"
	case StatusAssigned:
		return "assigned"
	case StatusRunning:
		return "running"
	case StatusCompleted:
		return "completed"
	case StatusFailed:
		return "failed"
	case StatusCancelled:
		return "cancelled"
	default:
		return "unknown"
	}
}

// ParseStatus parses a string into a Status
func ParseStatus(s string) (Status, error) {
	switch s {
	case "pending":
		return StatusPending, nil
	case "assigned":
		return StatusAssigned, nil
	case "running":
		return StatusRunning, nil
	case "completed":
		return StatusCompleted, nil
	case "failed":
		return StatusFailed, nil
	case "cancelled":
		return StatusCancelled, nil
	default:
		return 0, fmt.Errorf("unknown status: %s", s)
	}
}

// Job represents a job to be executed
type Job struct {
	ID             string
	Type           Type
	Payload        []byte
	Priority       int
	Status         Status
	AssignedTo     string
	CreatedAt      time.Time
	StartedAt      time.Time
	CompletedAt    time.Time
	Result         *Result
	RetryCount     int
	MaxRetries     int
	ErrorMessage   string
	TimeoutSeconds int64 // Job timeout in seconds (0 = no timeout)
}

// New creates a new job
func New(jobType Type, payload []byte, priority int, maxRetries int, timeoutSeconds int64) *Job {
	return &Job{
		ID:             generateID(),
		Type:           jobType,
		Payload:        payload,
		Priority:       priority,
		Status:         StatusPending,
		CreatedAt:      time.Now(),
		MaxRetries:     maxRetries,
		TimeoutSeconds: timeoutSeconds,
	}
}

// Assign assigns the job to a worker
func (j *Job) Assign(workerID string) error {
	if j.Status != StatusPending {
		return fmt.Errorf("cannot assign job in status %s", j.Status)
	}
	j.Status = StatusAssigned
	j.AssignedTo = workerID
	return nil
}

// Start marks the job as running
func (j *Job) Start() error {
	if j.Status != StatusAssigned {
		return fmt.Errorf("cannot start job in status %s", j.Status)
	}
	j.Status = StatusRunning
	j.StartedAt = time.Now()
	return nil
}

// Complete marks the job as completed
func (j *Job) Complete(result *Result) error {
	// Allow completion from either assigned or running status
	if j.Status != StatusRunning && j.Status != StatusAssigned {
		return fmt.Errorf("cannot complete job in status %s", j.Status)
	}
	j.Status = StatusCompleted
	j.CompletedAt = time.Now()
	j.Result = result
	return nil
}

// Fail marks the job as failed
func (j *Job) Fail(errMsg string) error {
	j.RetryCount++
	j.ErrorMessage = errMsg
	j.CompletedAt = time.Now()

	if j.RetryCount >= j.MaxRetries {
		j.Status = StatusFailed
		return nil
	}

	// Reset for retry
	j.Status = StatusPending
	j.AssignedTo = ""
	j.StartedAt = time.Time{}
	return nil
}

// Cancel marks the job as cancelled
func (j *Job) Cancel() error {
	if j.Status == StatusCompleted || j.Status == StatusFailed {
		return fmt.Errorf("cannot cancel job in status %s", j.Status)
	}
	j.Status = StatusCancelled
	j.CompletedAt = time.Now()
	return nil
}

// CanRetry returns true if the job can be retried
func (j *Job) CanRetry() bool {
	return j.RetryCount < j.MaxRetries
}

// Duration returns the total duration of the job
func (j *Job) Duration() time.Duration {
	if j.CompletedAt.IsZero() {
		return 0
	}
	if j.StartedAt.IsZero() {
		return j.CompletedAt.Sub(j.CreatedAt)
	}
	return j.CompletedAt.Sub(j.StartedAt)
}

// IsTimeout returns true if the job has exceeded its timeout duration
func (j *Job) IsTimeout() bool {
	if j.TimeoutSeconds == 0 {
		return false // No timeout configured
	}

	if j.Status != StatusRunning {
		return false // Only running jobs can timeout
	}

	if j.StartedAt.IsZero() {
		return false // Job hasn't started yet
	}

	elapsed := time.Since(j.StartedAt)
	timeout := time.Duration(j.TimeoutSeconds) * time.Second
	return elapsed > timeout
}

// TimeoutDuration returns the timeout duration for this job
func (j *Job) TimeoutDuration() time.Duration {
	return time.Duration(j.TimeoutSeconds) * time.Second
}

// RemainingTimeout returns the remaining timeout duration (0 if no timeout or expired)
func (j *Job) RemainingTimeout() time.Duration {
	if j.TimeoutSeconds == 0 || j.Status != StatusRunning || j.StartedAt.IsZero() {
		return 0
	}

	elapsed := time.Since(j.StartedAt)
	timeout := time.Duration(j.TimeoutSeconds) * time.Second
	remaining := timeout - elapsed

	if remaining < 0 {
		return 0
	}
	return remaining
}

// generateID generates a unique job ID
var idCounter int64

func generateID() string {
	id := atomic.AddInt64(&idCounter, 1)
	return fmt.Sprintf("job-%d-%d", time.Now().Unix(), id)
}
