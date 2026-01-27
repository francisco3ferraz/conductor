package consensus

import (
	"encoding/json"
	"fmt"
	"io"
	"sync"

	"github.com/francisco3ferraz/conductor/internal/job"
	"github.com/francisco3ferraz/conductor/internal/storage"
	"github.com/hashicorp/raft"
	"go.uber.org/zap"
)

// FSM implements the Raft finite state machine
// It maintains the replicated state for jobs and workers
type FSM struct {
	mu      sync.RWMutex
	jobs    map[string]*job.Job
	workers map[string]*storage.WorkerInfo
	logger  *zap.Logger
}

// Command types for state machine operations
const (
	CommandSubmitJob      = "submit_job"
	CommandAssignJob      = "assign_job"
	CommandCompleteJob    = "complete_job"
	CommandFailJob        = "fail_job"
	CommandRegisterWorker = "register_worker"
	CommandRemoveWorker   = "remove_worker"
)

// Command represents a state machine command
type Command struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

// SubmitJobPayload is the payload for submitting a job
type SubmitJobPayload struct {
	Job *job.Job `json:"job"`
}

// AssignJobPayload is the payload for assigning a job
type AssignJobPayload struct {
	JobID    string `json:"job_id"`
	WorkerID string `json:"worker_id"`
}

// CompleteJobPayload is the payload for completing a job
type CompleteJobPayload struct {
	JobID  string      `json:"job_id"`
	Result *job.Result `json:"result"`
}

// FailJobPayload is the payload for failing a job
type FailJobPayload struct {
	JobID string `json:"job_id"`
	Error string `json:"error"`
}

// RegisterWorkerPayload is the payload for registering a worker
type RegisterWorkerPayload struct {
	Worker *storage.WorkerInfo `json:"worker"`
}

// RemoveWorkerPayload is the payload for removing a worker
type RemoveWorkerPayload struct {
	WorkerID string `json:"worker_id"`
}

// NewFSM creates a new finite state machine
func NewFSM(logger *zap.Logger) *FSM {
	return &FSM{
		jobs:    make(map[string]*job.Job),
		workers: make(map[string]*storage.WorkerInfo),
		logger:  logger,
	}
}

// Apply applies a Raft log entry to the FSM
func (f *FSM) Apply(log *raft.Log) interface{} {
	var cmd Command
	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		f.logger.Error("Failed to unmarshal command", zap.Error(err))
		return err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	switch cmd.Type {
	case CommandSubmitJob:
		return f.applySubmitJob(cmd.Payload)
	case CommandAssignJob:
		return f.applyAssignJob(cmd.Payload)
	case CommandCompleteJob:
		return f.applyCompleteJob(cmd.Payload)
	case CommandFailJob:
		return f.applyFailJob(cmd.Payload)
	case CommandRegisterWorker:
		return f.applyRegisterWorker(cmd.Payload)
	case CommandRemoveWorker:
		return f.applyRemoveWorker(cmd.Payload)
	default:
		err := fmt.Errorf("unknown command type: %s", cmd.Type)
		f.logger.Error("Unknown command", zap.Error(err))
		return err
	}
}

func (f *FSM) applySubmitJob(payload json.RawMessage) interface{} {
	var p SubmitJobPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return err
	}

	f.jobs[p.Job.ID] = p.Job
	f.logger.Debug("Job submitted", zap.String("job_id", p.Job.ID))
	return nil
}

func (f *FSM) applyAssignJob(payload json.RawMessage) interface{} {
	var p AssignJobPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return err
	}

	job, exists := f.jobs[p.JobID]
	if !exists {
		return fmt.Errorf("job not found: %s", p.JobID)
	}

	if err := job.Assign(p.WorkerID); err != nil {
		return err
	}

	f.logger.Debug("Job assigned",
		zap.String("job_id", p.JobID),
		zap.String("worker_id", p.WorkerID))
	return nil
}

func (f *FSM) applyCompleteJob(payload json.RawMessage) interface{} {
	var p CompleteJobPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return err
	}

	job, exists := f.jobs[p.JobID]
	if !exists {
		return fmt.Errorf("job not found: %s", p.JobID)
	}

	if err := job.Complete(p.Result); err != nil {
		return err
	}

	f.logger.Debug("Job completed", zap.String("job_id", p.JobID))
	return nil
}

func (f *FSM) applyFailJob(payload json.RawMessage) interface{} {
	var p FailJobPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return err
	}

	job, exists := f.jobs[p.JobID]
	if !exists {
		return fmt.Errorf("job not found: %s", p.JobID)
	}

	if err := job.Fail(p.Error); err != nil {
		return err
	}

	f.logger.Debug("Job failed",
		zap.String("job_id", p.JobID),
		zap.String("error", p.Error))
	return nil
}

func (f *FSM) applyRegisterWorker(payload json.RawMessage) interface{} {
	var p RegisterWorkerPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return err
	}

	f.workers[p.Worker.ID] = p.Worker
	f.logger.Debug("Worker registered", zap.String("worker_id", p.Worker.ID))
	return nil
}

func (f *FSM) applyRemoveWorker(payload json.RawMessage) interface{} {
	var p RemoveWorkerPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return err
	}

	delete(f.workers, p.WorkerID)
	f.logger.Debug("Worker removed", zap.String("worker_id", p.WorkerID))
	return nil
}

// Snapshot creates a snapshot of the current state
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	// Clone the state
	jobs := make(map[string]*job.Job, len(f.jobs))
	for k, v := range f.jobs {
		jobs[k] = v
	}

	workers := make(map[string]*storage.WorkerInfo, len(f.workers))
	for k, v := range f.workers {
		workers[k] = v
	}

	return &FSMSnapshot{
		jobs:    jobs,
		workers: workers,
	}, nil
}

// Restore restores the FSM from a snapshot
func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()

	var snapshot struct {
		Jobs    map[string]*job.Job            `json:"jobs"`
		Workers map[string]*storage.WorkerInfo `json:"workers"`
	}

	if err := json.NewDecoder(rc).Decode(&snapshot); err != nil {
		return err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	f.jobs = snapshot.Jobs
	f.workers = snapshot.Workers

	f.logger.Info("Restored from snapshot",
		zap.Int("jobs", len(f.jobs)),
		zap.Int("workers", len(f.workers)))

	return nil
}

// GetJob retrieves a job by ID (read-only)
func (f *FSM) GetJob(id string) (*job.Job, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	j, exists := f.jobs[id]
	if !exists {
		return nil, storage.ErrNotFound
	}
	return j, nil
}

// ListJobs lists all jobs (read-only)
func (f *FSM) ListJobs() []*job.Job {
	f.mu.RLock()
	defer f.mu.RUnlock()

	jobs := make([]*job.Job, 0, len(f.jobs))
	for _, j := range f.jobs {
		jobs = append(jobs, j)
	}
	return jobs
}

// GetWorker retrieves a worker by ID (read-only)
func (f *FSM) GetWorker(id string) (*storage.WorkerInfo, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	w, exists := f.workers[id]
	if !exists {
		return nil, storage.ErrNotFound
	}
	return w, nil
}

// ListWorkers lists all workers (read-only)
func (f *FSM) ListWorkers() []*storage.WorkerInfo {
	f.mu.RLock()
	defer f.mu.RUnlock()

	workers := make([]*storage.WorkerInfo, 0, len(f.workers))
	for _, w := range f.workers {
		workers = append(workers, w)
	}
	return workers
}

// FSMSnapshot represents a point-in-time snapshot of the FSM
type FSMSnapshot struct {
	jobs    map[string]*job.Job
	workers map[string]*storage.WorkerInfo
}

// Persist writes the snapshot to the given sink
func (s *FSMSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		snapshot := struct {
			Jobs    map[string]*job.Job            `json:"jobs"`
			Workers map[string]*storage.WorkerInfo `json:"workers"`
		}{
			Jobs:    s.jobs,
			Workers: s.workers,
		}

		b, err := json.Marshal(snapshot)
		if err != nil {
			return err
		}

		if _, err := sink.Write(b); err != nil {
			return err
		}

		return sink.Close()
	}()

	if err != nil {
		sink.Cancel()
	}

	return err
}

// Release is called when we're finished with the snapshot
func (s *FSMSnapshot) Release() {}
