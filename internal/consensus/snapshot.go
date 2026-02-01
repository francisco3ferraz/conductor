package consensus

import (
	"encoding/json"
	"io"

	"github.com/francisco3ferraz/conductor/internal/job"
	"github.com/francisco3ferraz/conductor/internal/storage"
	"github.com/hashicorp/raft"
	"go.uber.org/zap"
)

// FSMSnapshot represents a point-in-time snapshot of the FSM state
type FSMSnapshot struct {
	jobs    map[string]*job.Job
	workers map[string]*storage.WorkerInfo
}

// Persist writes the snapshot to the given sink
func (s *FSMSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		// Encode data as JSON
		b, err := json.Marshal(map[string]interface{}{
			"jobs":    s.jobs,
			"workers": s.workers,
		})
		if err != nil {
			return err
		}

		// Write to the sink
		if _, err := sink.Write(b); err != nil {
			return err
		}

		// Close the sink
		return sink.Close()
	}()

	if err != nil {
		sink.Cancel()
	}

	return err
}

// Release is called when the snapshot is no longer needed
func (s *FSMSnapshot) Release() {}

// CreateSnapshot creates a snapshot of the FSM state
func (f *FSM) CreateSnapshot() (*FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	// Deep copy jobs
	jobs := make(map[string]*job.Job)
	for k, v := range f.jobs {
		jobs[k] = v
	}

	// Deep copy workers
	workers := make(map[string]*storage.WorkerInfo)
	for k, v := range f.workers {
		workers[k] = v
	}

	return &FSMSnapshot{
		jobs:    jobs,
		workers: workers,
	}, nil
}

// RestoreFromSnapshot restores the FSM from a snapshot
func (f *FSM) RestoreFromSnapshot(rc io.ReadCloser) error {
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
