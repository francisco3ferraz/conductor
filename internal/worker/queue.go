package worker

import (
	"container/heap"
	"context"
	"fmt"
	"sync"

	"github.com/francisco3ferraz/conductor/internal/job"
	"go.uber.org/zap"
)

// ResultReporter defines the interface for reporting job results
type ResultReporter interface {
	ReportResult(ctx context.Context, jobID string, result *job.Result) error
}

// JobQueueItem wraps a job with its context for priority queue
type JobQueueItem struct {
	Job     *job.Job
	Context context.Context
	Index   int // heap index
}

// PriorityJobQueue implements a priority queue for jobs
type PriorityJobQueue []*JobQueueItem

func (pq PriorityJobQueue) Len() int { return len(pq) }

func (pq PriorityJobQueue) Less(i, j int) bool {
	// Higher priority comes first
	if pq[i].Job.Priority != pq[j].Job.Priority {
		return pq[i].Job.Priority > pq[j].Job.Priority
	}
	// If same priority, FIFO (older jobs first)
	return pq[i].Job.CreatedAt.Before(pq[j].Job.CreatedAt)
}

func (pq PriorityJobQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].Index = i
	pq[j].Index = j
}

func (pq *PriorityJobQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*JobQueueItem)
	item.Index = n
	*pq = append(*pq, item)
}

func (pq *PriorityJobQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // avoid memory leak
	item.Index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}

// JobQueue manages a priority queue of jobs for worker execution
type JobQueue struct {
	queue    PriorityJobQueue
	mu       sync.Mutex
	notEmpty *sync.Cond
	executor *Executor
	reporter ResultReporter
	logger   *zap.Logger
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	workers  int // number of concurrent workers
}

// NewJobQueue creates a new priority job queue
func NewJobQueue(executor *Executor, reporter ResultReporter, concurrency int, logger *zap.Logger) *JobQueue {
	ctx, cancel := context.WithCancel(context.Background())
	jq := &JobQueue{
		queue:    make(PriorityJobQueue, 0),
		executor: executor,
		reporter: reporter,
		logger:   logger,
		ctx:      ctx,
		cancel:   cancel,
		workers:  concurrency,
	}
	jq.notEmpty = sync.NewCond(&jq.mu)
	heap.Init(&jq.queue)
	return jq
}

// Start begins processing jobs from the queue
func (jq *JobQueue) Start() {
	jq.logger.Info("Starting job queue workers", zap.Int("concurrency", jq.workers))
	for i := 0; i < jq.workers; i++ {
		jq.wg.Add(1)
		go jq.worker(i)
	}
}

// Stop stops the job queue
func (jq *JobQueue) Stop() {
	jq.cancel()
	jq.notEmpty.Broadcast() // wake up all workers
	jq.wg.Wait()
	jq.logger.Info("Job queue stopped")
}

// Enqueue adds a job to the priority queue
func (jq *JobQueue) Enqueue(ctx context.Context, j *job.Job) {
	jq.mu.Lock()
	defer jq.mu.Unlock()

	item := &JobQueueItem{
		Job:     j,
		Context: ctx,
	}
	heap.Push(&jq.queue, item)

	jq.logger.Debug("Job enqueued",
		zap.String("job_id", j.ID),
		zap.Int("priority", j.Priority),
		zap.Int("queue_size", len(jq.queue)))

	jq.notEmpty.Signal() // wake up one worker
}

// worker processes jobs from the queue
func (jq *JobQueue) worker(id int) {
	defer jq.wg.Done()

	jq.logger.Debug("Job queue worker started", zap.Int("worker_id", id))

	for {
		jq.mu.Lock()

		// Wait for a job or shutdown
		for len(jq.queue) == 0 {
			select {
			case <-jq.ctx.Done():
				jq.mu.Unlock()
				jq.logger.Debug("Job queue worker stopped", zap.Int("worker_id", id))
				return
			default:
				jq.notEmpty.Wait()
				// Check again if we should shutdown
				if jq.ctx.Err() != nil {
					jq.mu.Unlock()
					return
				}
			}
		}

		// Get highest priority job
		item := heap.Pop(&jq.queue).(*JobQueueItem)
		jq.mu.Unlock()

		// Execute the job
		jq.logger.Info("Processing job from queue",
			zap.String("job_id", item.Job.ID),
			zap.Int("priority", item.Job.Priority),
			zap.Int("worker_id", id))

		// Mark job as running
		if err := item.Job.Start(); err != nil {
			jq.logger.Error("Failed to start job",
				zap.String("job_id", item.Job.ID),
				zap.Error(err))
			// Report failure since job is in invalid state
			if jq.reporter != nil {
				failResult := &job.Result{
					Success: false,
					Error:   fmt.Sprintf("failed to start job: %v", err),
				}
				if reportErr := jq.reporter.ReportResult(jq.ctx, item.Job.ID, failResult); reportErr != nil {
					jq.logger.Error("Failed to report job start failure", zap.Error(reportErr))
				}
			}
			continue
		}

		// Execute with the job's context
		result := jq.executor.Execute(item.Context, item.Job)

		// Report result
		if jq.reporter != nil {
			if err := jq.reporter.ReportResult(jq.ctx, item.Job.ID, result); err != nil {
				jq.logger.Error("Failed to report job result",
					zap.String("job_id", item.Job.ID),
					zap.Error(err))
			}
		}
	}
}

// Size returns the current queue size
func (jq *JobQueue) Size() int {
	jq.mu.Lock()
	defer jq.mu.Unlock()
	return len(jq.queue)
}
