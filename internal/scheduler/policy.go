package scheduler

import (
	"sort"

	"github.com/francisco3ferraz/conductor/internal/job"
	"github.com/francisco3ferraz/conductor/internal/worker"
)

// SchedulingPolicy defines how jobs are assigned to workers
type SchedulingPolicy interface {
	SelectWorker(job *job.Job, workers []*worker.WorkerInfo) *worker.WorkerInfo
}

// RoundRobinPolicy assigns jobs to workers in a round-robin fashion
type RoundRobinPolicy struct {
	lastWorkerIndex int
}

func NewRoundRobinPolicy() *RoundRobinPolicy {
	return &RoundRobinPolicy{lastWorkerIndex: -1}
}

func (p *RoundRobinPolicy) SelectWorker(j *job.Job, workers []*worker.WorkerInfo) *worker.WorkerInfo {
	if len(workers) == 0 {
		return nil
	}

	p.lastWorkerIndex = (p.lastWorkerIndex + 1) % len(workers)
	return workers[p.lastWorkerIndex]
}

// LeastLoadedPolicy assigns jobs to the worker with the fewest active jobs
type LeastLoadedPolicy struct{}

func NewLeastLoadedPolicy() *LeastLoadedPolicy {
	return &LeastLoadedPolicy{}
}

func (p *LeastLoadedPolicy) SelectWorker(j *job.Job, workers []*worker.WorkerInfo) *worker.WorkerInfo {
	if len(workers) == 0 {
		return nil
	}

	// Find worker with minimum active jobs
	minWorker := workers[0]
	for _, w := range workers[1:] {
		if w.ActiveJobs < minWorker.ActiveJobs {
			minWorker = w
		}
	}

	return minWorker
}

// PriorityPolicy assigns high-priority jobs to workers with lowest load
type PriorityPolicy struct{}

func NewPriorityPolicy() *PriorityPolicy {
	return &PriorityPolicy{}
}

func (p *PriorityPolicy) SelectWorker(j *job.Job, workers []*worker.WorkerInfo) *worker.WorkerInfo {
	if len(workers) == 0 {
		return nil
	}

	// For high priority jobs (>= 7), prefer workers with lowest load
	if j.Priority >= 7 {
		minWorker := workers[0]
		for _, w := range workers[1:] {
			if w.ActiveJobs < minWorker.ActiveJobs {
				minWorker = w
			}
		}
		return minWorker
	}

	// For normal/low priority jobs, use round-robin to distribute load evenly
	// Simple hash based on job ID for deterministic assignment
	hash := 0
	for _, c := range j.ID {
		hash += int(c)
	}
	return workers[hash%len(workers)]
}

// RandomPolicy assigns jobs randomly for load distribution
type RandomPolicy struct{}

func NewRandomPolicy() *RandomPolicy {
	return &RandomPolicy{}
}

func (p *RandomPolicy) SelectWorker(j *job.Job, workers []*worker.WorkerInfo) *worker.WorkerInfo {
	if len(workers) == 0 {
		return nil
	}

	// Use job ID hash for deterministic "random" selection
	hash := 0
	for _, c := range j.ID {
		hash += int(c)
	}
	return workers[hash%len(workers)]
}

// CapacityAwarePolicy considers worker capacity when assigning jobs
type CapacityAwarePolicy struct{}

func NewCapacityAwarePolicy() *CapacityAwarePolicy {
	return &CapacityAwarePolicy{}
}

func (p *CapacityAwarePolicy) SelectWorker(j *job.Job, workers []*worker.WorkerInfo) *worker.WorkerInfo {
	if len(workers) == 0 {
		return nil
	}

	// Sort workers by available capacity (max - active)
	type workerCapacity struct {
		worker    *worker.WorkerInfo
		available int
	}

	capacities := make([]workerCapacity, len(workers))
	for i, w := range workers {
		capacities[i] = workerCapacity{
			worker:    w,
			available: w.MaxConcurrentJobs - w.ActiveJobs,
		}
	}

	// Sort by available capacity descending
	sort.Slice(capacities, func(i, j int) bool {
		return capacities[i].available > capacities[j].available
	})

	// Return worker with most available capacity
	return capacities[0].worker
}
