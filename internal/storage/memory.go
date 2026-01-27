package storage

import (
	"sync"

	"github.com/francisco3ferraz/conductor/internal/job"
)

// Memory is an in-memory implementation of the Store interface
type Memory struct {
	mu      sync.RWMutex
	jobs    map[string]*job.Job
	workers map[string]*WorkerInfo
}

// NewMemory creates a new in-memory store
func NewMemory() *Memory {
	return &Memory{
		jobs:    make(map[string]*job.Job),
		workers: make(map[string]*WorkerInfo),
	}
}

// SaveJob saves a job to the store
func (m *Memory) SaveJob(j *job.Job) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.jobs[j.ID] = j
	return nil
}

// GetJob retrieves a job by ID
func (m *Memory) GetJob(id string) (*job.Job, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	j, ok := m.jobs[id]
	if !ok {
		return nil, ErrNotFound
	}
	return j, nil
}

// ListJobs lists jobs with optional filtering
func (m *Memory) ListJobs(filter JobFilter) ([]*job.Job, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var jobs []*job.Job
	for _, j := range m.jobs {
		// Apply status filter if specified
		if filter.HasStatusFilter && j.Status != filter.Status {
			continue
		}
		jobs = append(jobs, j)
	}

	// Apply pagination
	start := filter.Offset
	if start > len(jobs) {
		return []*job.Job{}, nil
	}

	end := start + filter.Limit
	if filter.Limit == 0 || end > len(jobs) {
		end = len(jobs)
	}

	return jobs[start:end], nil
}

// DeleteJob deletes a job by ID
func (m *Memory) DeleteJob(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.jobs[id]; !ok {
		return ErrNotFound
	}
	delete(m.jobs, id)
	return nil
}

// SaveWorker saves a worker to the store
func (m *Memory) SaveWorker(w *WorkerInfo) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.workers[w.ID] = w
	return nil
}

// GetWorker retrieves a worker by ID
func (m *Memory) GetWorker(id string) (*WorkerInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	w, ok := m.workers[id]
	if !ok {
		return nil, ErrNotFound
	}
	return w, nil
}

// ListWorkers lists all workers
func (m *Memory) ListWorkers() ([]*WorkerInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	workers := make([]*WorkerInfo, 0, len(m.workers))
	for _, w := range m.workers {
		workers = append(workers, w)
	}
	return workers, nil
}

// DeleteWorker deletes a worker by ID
func (m *Memory) DeleteWorker(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.workers[id]; !ok {
		return ErrNotFound
	}
	delete(m.workers, id)
	return nil
}

// Close closes the store (no-op for memory store)
func (m *Memory) Close() error {
	return nil
}
