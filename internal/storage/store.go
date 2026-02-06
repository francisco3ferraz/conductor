package storage

import (
	"errors"

	"github.com/francisco3ferraz/conductor/internal/job"
)

var (
	// ErrNotFound is returned when a resource is not found
	ErrNotFound = errors.New("not found")
	// ErrAlreadyExists is returned when a resource already exists
	ErrAlreadyExists = errors.New("already exists")
)

// Store defines the interface for storing jobs and worker information
type Store interface {
	// Job operations
	SaveJob(j *job.Job) error
	GetJob(id string) (*job.Job, error)
	ListJobs(filter JobFilter) ([]*job.Job, error)
	DeleteJob(id string) error

	// Worker operations
	SaveWorker(w *WorkerInfo) error
	GetWorker(id string) (*WorkerInfo, error)
	ListWorkers() ([]*WorkerInfo, error)
	DeleteWorker(id string) error

	// Close closes the store
	Close() error
}

// JobFilter defines filtering options for listing jobs
type JobFilter struct {
	Status          job.Status
	HasStatusFilter bool // Set to true to filter by status
	Limit           int
	Offset          int
}

// WorkerInfo holds information about a registered worker.
//
// ARCHITECTURAL NOTE: This struct is duplicated in worker.WorkerInfo with slight
// differences. This is intentional for layer separation:
//
//   - storage.WorkerInfo: Persistence layer with int64 timestamp for efficient BoltDB
//     serialization and database operations.
//
//   - worker.WorkerInfo: In-memory representation with time.Time for fast time-based
//     operations in scheduling and health monitoring.
//
// Conversion is handled via worker.WorkerInfo.ToStorageWorkerInfo() method.
type WorkerInfo struct {
	ID                string
	Address           string
	MaxConcurrentJobs int
	ActiveJobs        int
	LastHeartbeat     int64 // Unix timestamp
	Status            string
}

// Clone creates a deep copy of WorkerInfo for snapshot purposes
func (w *WorkerInfo) Clone() *WorkerInfo {
	if w == nil {
		return nil
	}
	return &WorkerInfo{
		ID:                w.ID,
		Address:           w.Address,
		MaxConcurrentJobs: w.MaxConcurrentJobs,
		ActiveJobs:        w.ActiveJobs,
		LastHeartbeat:     w.LastHeartbeat,
		Status:            w.Status,
	}
}
