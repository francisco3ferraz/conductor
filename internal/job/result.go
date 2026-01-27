package job

import "time"

// Result represents the result of a job execution
type Result struct {
	Success    bool
	Output     []byte
	Error      string
	DurationMs int64
}

// NewResult creates a new successful result
func NewResult(output []byte, duration time.Duration) *Result {
	return &Result{
		Success:    true,
		Output:     output,
		DurationMs: duration.Milliseconds(),
	}
}

// NewErrorResult creates a new error result
func NewErrorResult(err error, duration time.Duration) *Result {
	return &Result{
		Success:    false,
		Error:      err.Error(),
		DurationMs: duration.Milliseconds(),
	}
}
