package worker

import (
	"context"

	"go.uber.org/zap"
)

// Manager coordinates worker lifecycle and components
type Manager struct {
	id        string
	executor  *Executor
	heartbeat *HeartbeatSender
	logger    *zap.Logger
}

// NewManager creates a new worker manager
func NewManager(id string, executor *Executor, heartbeat *HeartbeatSender, logger *zap.Logger) *Manager {
	return &Manager{
		id:        id,
		executor:  executor,
		heartbeat: heartbeat,
		logger:    logger,
	}
}

// Start initializes worker components
func (m *Manager) Start(ctx context.Context) error {
	m.logger.Info("Worker manager starting", zap.String("worker_id", m.id))

	// Start heartbeat sender (if provided)
	if m.heartbeat != nil {
		go m.heartbeat.Start(ctx)
	}

	m.logger.Info("Worker manager started", zap.String("worker_id", m.id))
	return nil
}

// Stop gracefully shuts down worker components
func (m *Manager) Stop() error {
	m.logger.Info("Worker manager stopping", zap.String("worker_id", m.id))

	// Stop heartbeat (if provided)
	if m.heartbeat != nil {
		m.heartbeat.Stop()
	}

	m.logger.Info("Worker manager stopped", zap.String("worker_id", m.id))
	return nil
}

// GetID returns the worker ID
func (m *Manager) GetID() string {
	return m.id
}

// GetExecutor returns the executor
func (m *Manager) GetExecutor() *Executor {
	return m.executor
}
