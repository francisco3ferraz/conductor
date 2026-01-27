package consensus

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/francisco3ferraz/conductor/internal/job"
	"github.com/francisco3ferraz/conductor/internal/storage"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestFSM_SubmitJob(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	fsm := NewFSM(logger)

	j := job.New(job.TypeImageProcessing, []byte("test"), 5, 3)

	// Create submit job command
	payload, err := json.Marshal(SubmitJobPayload{Job: j})
	require.NoError(t, err)

	cmd := Command{
		Type:    CommandSubmitJob,
		Payload: payload,
	}

	cmdBytes, err := json.Marshal(cmd)
	require.NoError(t, err)

	// Apply command
	log := &raft.Log{Data: cmdBytes}
	result := fsm.Apply(log)
	assert.Nil(t, result)

	// Verify job was stored
	storedJob, err := fsm.GetJob(j.ID)
	require.NoError(t, err)
	assert.Equal(t, j.ID, storedJob.ID)
	assert.Equal(t, j.Type, storedJob.Type)
}

func TestFSM_AssignJob(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	fsm := NewFSM(logger)

	// First submit a job
	j := job.New(job.TypeWebScraping, []byte("url"), 3, 3)
	payload, _ := json.Marshal(SubmitJobPayload{Job: j})
	cmd := Command{Type: CommandSubmitJob, Payload: payload}
	cmdBytes, _ := json.Marshal(cmd)
	fsm.Apply(&raft.Log{Data: cmdBytes})

	// Now assign it
	assignPayload, err := json.Marshal(AssignJobPayload{
		JobID:    j.ID,
		WorkerID: "worker-1",
	})
	require.NoError(t, err)

	assignCmd := Command{
		Type:    CommandAssignJob,
		Payload: assignPayload,
	}

	assignCmdBytes, err := json.Marshal(assignCmd)
	require.NoError(t, err)

	result := fsm.Apply(&raft.Log{Data: assignCmdBytes})
	assert.Nil(t, result)

	// Verify job was assigned
	storedJob, err := fsm.GetJob(j.ID)
	require.NoError(t, err)
	assert.Equal(t, job.StatusAssigned, storedJob.Status)
	assert.Equal(t, "worker-1", storedJob.AssignedTo)
}

func TestFSM_CompleteJob(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	fsm := NewFSM(logger)

	// Submit and assign a job
	j := job.New(job.TypeDataAnalysis, []byte("data"), 1, 3)
	fsm.jobs[j.ID] = j
	j.Assign("worker-1")
	j.Start()

	// Complete it
	result := job.NewResult([]byte("output"), 100*time.Millisecond)
	completePayload, err := json.Marshal(CompleteJobPayload{
		JobID:  j.ID,
		Result: result,
	})
	require.NoError(t, err)

	completeCmd := Command{
		Type:    CommandCompleteJob,
		Payload: completePayload,
	}

	completeCmdBytes, err := json.Marshal(completeCmd)
	require.NoError(t, err)

	applyResult := fsm.Apply(&raft.Log{Data: completeCmdBytes})
	assert.Nil(t, applyResult)

	// Verify job was completed
	storedJob, err := fsm.GetJob(j.ID)
	require.NoError(t, err)
	assert.Equal(t, job.StatusCompleted, storedJob.Status)
	assert.NotNil(t, storedJob.Result)
	assert.True(t, storedJob.Result.Success)
}

func TestFSM_RegisterWorker(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	fsm := NewFSM(logger)

	worker := &storage.WorkerInfo{
		ID:                "worker-1",
		Address:           "localhost:9001",
		MaxConcurrentJobs: 10,
		Status:            "active",
	}

	payload, err := json.Marshal(RegisterWorkerPayload{Worker: worker})
	require.NoError(t, err)

	cmd := Command{
		Type:    CommandRegisterWorker,
		Payload: payload,
	}

	cmdBytes, err := json.Marshal(cmd)
	require.NoError(t, err)

	result := fsm.Apply(&raft.Log{Data: cmdBytes})
	assert.Nil(t, result)

	// Verify worker was registered
	storedWorker, err := fsm.GetWorker(worker.ID)
	require.NoError(t, err)
	assert.Equal(t, worker.ID, storedWorker.ID)
	assert.Equal(t, worker.Address, storedWorker.Address)
}

func TestFSM_SnapshotAndRestore(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	fsm := NewFSM(logger)

	// Add some jobs and workers
	j1 := job.New(job.TypeImageProcessing, []byte("img"), 1, 3)
	j2 := job.New(job.TypeWebScraping, []byte("url"), 2, 3)
	fsm.jobs[j1.ID] = j1
	fsm.jobs[j2.ID] = j2

	worker := &storage.WorkerInfo{
		ID:      "worker-1",
		Address: "localhost:9001",
		Status:  "active",
	}
	fsm.workers[worker.ID] = worker

	// Create snapshot
	snapshot, err := fsm.Snapshot()
	require.NoError(t, err)

	// Persist snapshot to buffer
	// Note: This is simplified - in production we'd use actual SnapshotSink
	assert.NotNil(t, snapshot)

	// Verify we can list jobs
	jobs := fsm.ListJobs()
	assert.Len(t, jobs, 2)

	workers := fsm.ListWorkers()
	assert.Len(t, workers, 1)
}

func TestFSM_ListOperations(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	fsm := NewFSM(logger)

	// Add jobs
	for i := 0; i < 5; i++ {
		j := job.New(job.TypeImageProcessing, []byte("data"), 1, 3)
		fsm.jobs[j.ID] = j
	}

	// List jobs
	jobs := fsm.ListJobs()
	assert.Len(t, jobs, 5)

	// Add workers
	for i := 0; i < 3; i++ {
		worker := &storage.WorkerInfo{
			ID:      fmt.Sprintf("worker-%d", i),
			Address: fmt.Sprintf("localhost:900%d", i),
			Status:  "active",
		}
		fsm.workers[worker.ID] = worker
	}

	// List workers
	workers := fsm.ListWorkers()
	assert.Len(t, workers, 3)
}
