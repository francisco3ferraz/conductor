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

// WorkerInfo holds information about a registered worker
type WorkerInfo struct {
	ID                string
	Address           string
	MaxConcurrentJobs int
	ActiveJobs        int
	LastHeartbeat     int64 // Unix timestamp
	Status            string
}
