package scheduler

import (
	"container/heap"

	"github.com/francisco3ferraz/conductor/internal/job"
)

// JobHeap implements heap.Interface for job priority queue
// Higher priority jobs come first, with FIFO ordering for equal priorities
type JobHeap []*job.Job

func (h JobHeap) Len() int { return len(h) }

func (h JobHeap) Less(i, j int) bool {
	// Higher priority comes first
	if h[i].Priority != h[j].Priority {
		return h[i].Priority > h[j].Priority
	}
	// If same priority, older jobs (earlier CreatedAt) come first
	return h[i].CreatedAt.Before(h[j].CreatedAt)
}

func (h JobHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *JobHeap) Push(x interface{}) {
	*h = append(*h, x.(*job.Job))
}

func (h *JobHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil // Avoid memory leak
	*h = old[0 : n-1]
	return item
}

// JobQueue manages pending jobs with priority support using a heap
type JobQueue struct {
	heap JobHeap
}

// NewJobQueue creates a new job queue
func NewJobQueue() *JobQueue {
	q := &JobQueue{
		heap: make(JobHeap, 0),
	}
	heap.Init(&q.heap)
	return q
}

// Enqueue adds a job to the queue - O(log n) instead of O(n log n)
func (q *JobQueue) Enqueue(j *job.Job) {
	heap.Push(&q.heap, j)
}

// Dequeue removes and returns the next job from the queue - O(log n)
func (q *JobQueue) Dequeue() *job.Job {
	if q.heap.Len() == 0 {
		return nil
	}
	return heap.Pop(&q.heap).(*job.Job)
}

// Peek returns the next job without removing it
func (q *JobQueue) Peek() *job.Job {
	if q.heap.Len() == 0 {
		return nil
	}
	return q.heap[0]
}

// Size returns the number of jobs in the queue
func (q *JobQueue) Size() int {
	return q.heap.Len()
}

// IsEmpty returns true if the queue is empty
func (q *JobQueue) IsEmpty() bool {
	return q.heap.Len() == 0
}

// Clear removes all jobs from the queue
func (q *JobQueue) Clear() {
	q.heap = make(JobHeap, 0)
	heap.Init(&q.heap)
}

// List returns all jobs in the queue (for inspection)
// Note: Jobs are not guaranteed to be in priority order in the returned slice
func (q *JobQueue) List() []*job.Job {
	result := make([]*job.Job, len(q.heap))
	copy(result, q.heap)
	return result
}
