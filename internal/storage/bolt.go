package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/francisco3ferraz/conductor/internal/job"
	bolt "go.etcd.io/bbolt"
)

var (
	jobsBucket    = []byte("jobs")
	workersBucket = []byte("workers")
)

// BoltStore implements Store using BoltDB for persistence
type BoltStore struct {
	db *bolt.DB
	mu sync.RWMutex
}

// NewBoltStore creates a new BoltDB-backed store
func NewBoltStore(path string) (*BoltStore, error) {
	// Create directory if it doesn't exist
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}

	db, err := bolt.Open(path, 0600, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to open boltdb: %w", err)
	}

	// Create buckets if they don't exist
	err = db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(jobsBucket); err != nil {
			return fmt.Errorf("create jobs bucket: %w", err)
		}
		if _, err := tx.CreateBucketIfNotExists(workersBucket); err != nil {
			return fmt.Errorf("create workers bucket: %w", err)
		}
		return nil
	})
	if err != nil {
		db.Close()
		return nil, err
	}

	return &BoltStore{db: db}, nil
}

// Close closes the BoltDB database
func (s *BoltStore) Close() error {
	return s.db.Close()
}

// SaveJob persists a job to BoltDB
func (s *BoltStore) SaveJob(j *job.Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	data, err := json.Marshal(j)
	if err != nil {
		return fmt.Errorf("marshal job: %w", err)
	}

	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(jobsBucket)
		return b.Put([]byte(j.ID), data)
	})
}

// GetJob retrieves a job from BoltDB
func (s *BoltStore) GetJob(id string) (*job.Job, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var j job.Job
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(jobsBucket)
		data := b.Get([]byte(id))
		if data == nil {
			return fmt.Errorf("job not found: %s", id)
		}
		return json.Unmarshal(data, &j)
	})

	if err != nil {
		return nil, err
	}
	return &j, nil
}

// ListJobs returns all jobs from BoltDB
func (s *BoltStore) ListJobs(filter JobFilter) ([]*job.Job, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var jobs []*job.Job
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(jobsBucket)
		return b.ForEach(func(k, v []byte) error {
			var j job.Job
			if err := json.Unmarshal(v, &j); err != nil {
				return err
			}
			jobs = append(jobs, &j)
			return nil
		})
	})

	if err != nil {
		return nil, err
	}
	return jobs, nil
}

// DeleteJob removes a job from BoltDB
func (s *BoltStore) DeleteJob(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(jobsBucket)
		return b.Delete([]byte(id))
	})
}

// SaveWorker persists a worker to BoltDB
func (s *BoltStore) SaveWorker(w *WorkerInfo) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	data, err := json.Marshal(w)
	if err != nil {
		return fmt.Errorf("marshal worker: %w", err)
	}

	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(workersBucket)
		return b.Put([]byte(w.ID), data)
	})
}

// GetWorker retrieves a worker from BoltDB
func (s *BoltStore) GetWorker(id string) (*WorkerInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var w WorkerInfo
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(workersBucket)
		data := b.Get([]byte(id))
		if data == nil {
			return fmt.Errorf("worker not found: %s", id)
		}
		return json.Unmarshal(data, &w)
	})

	if err != nil {
		return nil, err
	}
	return &w, nil
}

// ListWorkers returns all workers from BoltDB
func (s *BoltStore) ListWorkers() ([]*WorkerInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var workers []*WorkerInfo
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(workersBucket)
		return b.ForEach(func(k, v []byte) error {
			var w WorkerInfo
			if err := json.Unmarshal(v, &w); err != nil {
				return err
			}
			workers = append(workers, &w)
			return nil
		})
	})

	if err != nil {
		return nil, err
	}
	return workers, nil
}

// DeleteWorker removes a worker from BoltDB
func (s *BoltStore) DeleteWorker(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(workersBucket)
		return b.Delete([]byte(id))
	})
}
