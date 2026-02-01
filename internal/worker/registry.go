package worker

import (
	"sync"
	"time"

	"github.com/francisco3ferraz/conductor/internal/storage"
)

// Registry tracks registered workers and their health status
type Registry struct {
	mu         sync.RWMutex
	workers    map[string]*WorkerInfo
	heartbeats map[string]time.Time
}

// WorkerInfo contains worker metadata and current state
type WorkerInfo struct {
	ID                string
	Address           string
	MaxConcurrentJobs int
	ActiveJobs        int
	LastHeartbeat     time.Time
	Status            string // "active", "inactive", "dead"
}

// NewRegistry creates a new worker registry
func NewRegistry() *Registry {
	return &Registry{
		workers:    make(map[string]*WorkerInfo),
		heartbeats: make(map[string]time.Time),
	}
}

// Register adds a new worker to the registry
func (r *Registry) Register(id, address string, maxJobs int) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.workers[id] = &WorkerInfo{
		ID:                id,
		Address:           address,
		MaxConcurrentJobs: maxJobs,
		ActiveJobs:        0,
		LastHeartbeat:     time.Now(),
		Status:            "active",
	}
	r.heartbeats[id] = time.Now()
}

// Unregister removes a worker from the registry
func (r *Registry) Unregister(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.workers, id)
	delete(r.heartbeats, id)
}

// Get retrieves worker information by ID
func (r *Registry) Get(id string) (*WorkerInfo, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	worker, exists := r.workers[id]
	return worker, exists
}

// List returns all registered workers
func (r *Registry) List() []*WorkerInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()

	workers := make([]*WorkerInfo, 0, len(r.workers))
	for _, worker := range r.workers {
		workers = append(workers, worker)
	}
	return workers
}

// UpdateHeartbeat updates the last heartbeat time for a worker
func (r *Registry) UpdateHeartbeat(id string, activeJobs int) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if worker, exists := r.workers[id]; exists {
		worker.LastHeartbeat = time.Now()
		worker.ActiveJobs = activeJobs
		worker.Status = "active"
		r.heartbeats[id] = time.Now()
	}
}

// MarkInactive marks workers as inactive if they haven't sent heartbeats
func (r *Registry) MarkInactive(timeout time.Duration) []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now()
	inactive := []string{}

	for id, lastHeartbeat := range r.heartbeats {
		if now.Sub(lastHeartbeat) > timeout {
			if worker, exists := r.workers[id]; exists {
				worker.Status = "inactive"
				inactive = append(inactive, id)
			}
		}
	}

	return inactive
}

// FindAvailable finds an available worker with capacity
func (r *Registry) FindAvailable() *WorkerInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// Simple strategy: find first worker with capacity
	for _, worker := range r.workers {
		if worker.Status == "active" && worker.ActiveJobs < worker.MaxConcurrentJobs {
			return worker
		}
	}

	return nil
}

// ToStorageWorkerInfo converts WorkerInfo to storage.WorkerInfo
func (w *WorkerInfo) ToStorageWorkerInfo() *storage.WorkerInfo {
	return &storage.WorkerInfo{
		ID:                w.ID,
		Address:           w.Address,
		MaxConcurrentJobs: w.MaxConcurrentJobs,
		Status:            w.Status,
	}
}
