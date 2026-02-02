package scheduler

import (
	"sort"

	"github.com/francisco3ferraz/conductor/internal/job"
)

// JobQueue manages pending jobs with priority support
type JobQueue struct {
	jobs []*job.Job
}

// NewJobQueue creates a new job queue
func NewJobQueue() *JobQueue {
	return &JobQueue{
		jobs: make([]*job.Job, 0),
	}
}

// Enqueue adds a job to the queue and maintains priority order
func (q *JobQueue) Enqueue(j *job.Job) {
	q.jobs = append(q.jobs, j)

	// Sort by priority (higher priority first), then by creation time (older first)
	sort.SliceStable(q.jobs, func(i, j int) bool {
		// Higher priority comes first
		if q.jobs[i].Priority != q.jobs[j].Priority {
			return q.jobs[i].Priority > q.jobs[j].Priority
		}
		// If same priority, older jobs (earlier CreatedAt) come first
		return q.jobs[i].CreatedAt.Before(q.jobs[j].CreatedAt)
	})
}

// Dequeue removes and returns the next job from the queue
func (q *JobQueue) Dequeue() *job.Job {
	if len(q.jobs) == 0 {
		return nil
	}
	j := q.jobs[0]
	q.jobs = q.jobs[1:]
	return j
}

// Peek returns the next job without removing it
func (q *JobQueue) Peek() *job.Job {
	if len(q.jobs) == 0 {
		return nil
	}
	return q.jobs[0]
}

// Size returns the number of jobs in the queue
func (q *JobQueue) Size() int {
	return len(q.jobs)
}

// IsEmpty returns true if the queue is empty
func (q *JobQueue) IsEmpty() bool {
	return len(q.jobs) == 0
}

// Clear removes all jobs from the queue
func (q *JobQueue) Clear() {
	q.jobs = make([]*job.Job, 0)
}

// List returns all jobs in the queue (for inspection)
func (q *JobQueue) List() []*job.Job {
	result := make([]*job.Job, len(q.jobs))
	copy(result, q.jobs)
	return result
}
