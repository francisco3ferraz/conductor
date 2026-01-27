package storage

import (
	"testing"

	"github.com/francisco3ferraz/conductor/internal/job"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMemoryStore_Jobs(t *testing.T) {
	store := NewMemory()
	defer store.Close()

	// Create a job
	j := job.New(job.TypeImageProcessing, []byte("payload"), 5, 3)

	// Save job
	err := store.SaveJob(j)
	require.NoError(t, err)

	// Get job
	retrieved, err := store.GetJob(j.ID)
	require.NoError(t, err)
	assert.Equal(t, j.ID, retrieved.ID)
	assert.Equal(t, j.Type, retrieved.Type)

	// Get non-existent job
	_, err = store.GetJob("non-existent")
	assert.Equal(t, ErrNotFound, err)

	// List jobs
	jobs, err := store.ListJobs(JobFilter{})
	require.NoError(t, err)
	assert.Len(t, jobs, 1)

	// Delete job
	err = store.DeleteJob(j.ID)
	require.NoError(t, err)

	// Verify deletion
	_, err = store.GetJob(j.ID)
	assert.Equal(t, ErrNotFound, err)
}

func TestMemoryStore_JobFiltering(t *testing.T) {
	store := NewMemory()
	defer store.Close()

	// Create jobs with different statuses
	j1 := job.New(job.TypeImageProcessing, []byte("1"), 5, 3)
	j1.Status = job.StatusPending

	j2 := job.New(job.TypeWebScraping, []byte("2"), 3, 3)
	j2.Assign("worker-1")
	j2.Status = job.StatusAssigned

	j3 := job.New(job.TypeDataAnalysis, []byte("3"), 1, 3)
	j3.Status = job.StatusPending

	store.SaveJob(j1)
	store.SaveJob(j2)
	store.SaveJob(j3)

	// Filter by status
	pending, err := store.ListJobs(JobFilter{Status: job.StatusPending, HasStatusFilter: true})
	require.NoError(t, err)
	assert.Len(t, pending, 2)

	assigned, err := store.ListJobs(JobFilter{Status: job.StatusAssigned, HasStatusFilter: true})
	require.NoError(t, err)
	assert.Len(t, assigned, 1)

	// Test pagination
	page1, err := store.ListJobs(JobFilter{Limit: 2})
	require.NoError(t, err)
	assert.Len(t, page1, 2)

	page2, err := store.ListJobs(JobFilter{Limit: 2, Offset: 2})
	require.NoError(t, err)
	assert.Len(t, page2, 1)
}

func TestMemoryStore_Workers(t *testing.T) {
	store := NewMemory()
	defer store.Close()

	// Create a worker
	w := &WorkerInfo{
		ID:                "worker-1",
		Address:           "localhost:9001",
		MaxConcurrentJobs: 10,
		ActiveJobs:        0,
		Status:            "active",
	}

	// Save worker
	err := store.SaveWorker(w)
	require.NoError(t, err)

	// Get worker
	retrieved, err := store.GetWorker(w.ID)
	require.NoError(t, err)
	assert.Equal(t, w.ID, retrieved.ID)
	assert.Equal(t, w.Address, retrieved.Address)

	// List workers
	workers, err := store.ListWorkers()
	require.NoError(t, err)
	assert.Len(t, workers, 1)

	// Delete worker
	err = store.DeleteWorker(w.ID)
	require.NoError(t, err)

	// Verify deletion
	_, err = store.GetWorker(w.ID)
	assert.Equal(t, ErrNotFound, err)
}

func TestMemoryStore_Concurrent(t *testing.T) {
	store := NewMemory()
	defer store.Close()

	// Test concurrent writes
	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func(id int) {
			j := job.New(job.TypeImageProcessing, []byte("payload"), 1, 3)
			store.SaveJob(j)
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}

	// Verify all jobs saved
	jobs, err := store.ListJobs(JobFilter{})
	require.NoError(t, err)
	assert.Len(t, jobs, 10)
}
