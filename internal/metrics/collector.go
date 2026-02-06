package metrics

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"
)

// Collector periodically collects metrics from various components
type Collector struct {
	metrics  *Metrics
	logger   *zap.Logger
	interval time.Duration

	mu      sync.RWMutex
	stopCh  chan struct{}
	stopped bool
}

// NewCollector creates a new metrics collector
func NewCollector(metrics *Metrics, interval time.Duration, logger *zap.Logger) *Collector {
	return &Collector{
		metrics:  metrics,
		logger:   logger,
		interval: interval,
		stopCh:   make(chan struct{}),
	}
}

// Start begins periodic metric collection
func (c *Collector) Start(ctx context.Context) {
	c.mu.Lock()
	if c.stopped {
		c.mu.Unlock()
		return
	}
	c.mu.Unlock()

	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	c.logger.Info("Metrics collector started", zap.Duration("interval", c.interval))

	for {
		select {
		case <-ctx.Done():
			c.logger.Info("Metrics collector stopped due to context cancellation")
			return
		case <-c.stopCh:
			c.logger.Info("Metrics collector stopped")
			return
		case <-ticker.C:
			// Periodic collection happens here
			// Individual components update metrics directly
			c.logger.Debug("Metrics collection tick")
		}
	}
}

// Stop stops the metrics collector
func (c *Collector) Stop() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.stopped {
		close(c.stopCh)
		c.stopped = true
	}
}

// GetMetrics returns the metrics instance
func (c *Collector) GetMetrics() *Metrics {
	return c.metrics
}
