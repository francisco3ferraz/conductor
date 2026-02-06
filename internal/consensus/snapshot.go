package consensus

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"

	"github.com/francisco3ferraz/conductor/internal/job"
	"github.com/francisco3ferraz/conductor/internal/storage"
	"github.com/hashicorp/raft"
	"go.uber.org/zap"
)

// FSMSnapshot represents a point-in-time snapshot of the FSM state
type FSMSnapshot struct {
	jobs            map[string]*job.Job
	workers         map[string]*storage.WorkerInfo
	deadLetterQueue map[string]*job.Job
}

// Persist writes the snapshot to the given sink with gzip compression
func (s *FSMSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		// Create gzip writer for compression
		gzWriter := gzip.NewWriter(sink)
		defer gzWriter.Close()

		// Encode data as JSON
		data := map[string]interface{}{
			"jobs":              s.jobs,
			"workers":           s.workers,
			"dead_letter_queue": s.deadLetterQueue,
		}

		// Use JSON encoder for streaming (more memory efficient than Marshal)
		encoder := json.NewEncoder(gzWriter)
		if err := encoder.Encode(data); err != nil {
			return err
		}

		// Ensure all data is flushed to gzip writer
		if err := gzWriter.Close(); err != nil {
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

	// Deep copy jobs using Clone method
	jobs := make(map[string]*job.Job)
	for k, v := range f.jobs {
		jobs[k] = v.Clone()
	}

	// Deep copy workers using Clone method
	workers := make(map[string]*storage.WorkerInfo)
	for k, v := range f.workers {
		workers[k] = v.Clone()
	}

	// Deep copy DLQ using Clone method
	dlq := make(map[string]*job.Job)
	for k, v := range f.deadLetterQueue {
		dlq[k] = v.Clone()
	}

	return &FSMSnapshot{
		jobs:            jobs,
		workers:         workers,
		deadLetterQueue: dlq,
	}, nil
}

// RestoreFromSnapshot restores the FSM from a compressed snapshot
func (f *FSM) RestoreFromSnapshot(rc io.ReadCloser) error {
	defer rc.Close()

	// Read all data first to avoid consuming the reader
	data, err := io.ReadAll(rc)
	if err != nil {
		return err
	}

	var snapshot struct {
		Jobs            map[string]*job.Job            `json:"jobs"`
		Workers         map[string]*storage.WorkerInfo `json:"workers"`
		DeadLetterQueue map[string]*job.Job            `json:"dead_letter_queue"`
	}

	// Try gzip decompression first
	gzReader, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		// If gzip fails, try uncompressed (backward compatibility)
		f.logger.Warn("Failed to create gzip reader, trying uncompressed restore", zap.Error(err))

		if err := json.Unmarshal(data, &snapshot); err != nil {
			return err
		}

		f.mu.Lock()
		defer f.mu.Unlock()

		f.jobs = snapshot.Jobs
		f.workers = snapshot.Workers
		if snapshot.DeadLetterQueue != nil {
			f.deadLetterQueue = snapshot.DeadLetterQueue
		} else {
			f.deadLetterQueue = make(map[string]*job.Job)
		}

		f.logger.Info("Restored from uncompressed snapshot",
			zap.Int("jobs", len(f.jobs)),
			zap.Int("workers", len(f.workers)),
			zap.Int("dlq_jobs", len(f.deadLetterQueue)))

		return nil
	}
	defer gzReader.Close()

	// Decode from gzip stream
	if err := json.NewDecoder(gzReader).Decode(&snapshot); err != nil {
		return err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	f.jobs = snapshot.Jobs
	f.workers = snapshot.Workers
	if snapshot.DeadLetterQueue != nil {
		f.deadLetterQueue = snapshot.DeadLetterQueue
	} else {
		f.deadLetterQueue = make(map[string]*job.Job)
	}

	f.logger.Info("Restored from compressed snapshot",
		zap.Int("jobs", len(f.jobs)),
		zap.Int("workers", len(f.workers)),
		zap.Int("dlq_jobs", len(f.deadLetterQueue)))

	return nil
}
