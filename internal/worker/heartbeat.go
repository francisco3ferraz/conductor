package worker

import (
	"context"
	"time"

	proto "github.com/francisco3ferraz/conductor/api/proto"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// HeartbeatSender manages periodic heartbeat transmission to master
type HeartbeatSender struct {
	workerID string
	conn     *grpc.ClientConn
	interval time.Duration
	getStats func() (active, completed, failed int32)
	logger   *zap.Logger
	stopCh   chan struct{}
}

// NewHeartbeatSender creates a new heartbeat sender
func NewHeartbeatSender(
	workerID string,
	conn *grpc.ClientConn,
	interval time.Duration,
	getStats func() (active, completed, failed int32),
	logger *zap.Logger,
) *HeartbeatSender {
	return &HeartbeatSender{
		workerID: workerID,
		conn:     conn,
		interval: interval,
		getStats: getStats,
		logger:   logger,
		stopCh:   make(chan struct{}),
	}
}

// Start begins sending periodic heartbeats
func (h *HeartbeatSender) Start(ctx context.Context) {
	ticker := time.NewTicker(h.interval)
	defer ticker.Stop()

	h.logger.Info("Heartbeat sender started",
		zap.String("worker_id", h.workerID),
		zap.Duration("interval", h.interval),
	)

	for {
		select {
		case <-ctx.Done():
			h.logger.Info("Heartbeat sender stopped (context cancelled)")
			return
		case <-h.stopCh:
			h.logger.Info("Heartbeat sender stopped")
			return
		case <-ticker.C:
			if err := h.send(ctx); err != nil {
				h.logger.Error("Failed to send heartbeat", zap.Error(err))
			}
		}
	}
}

// Stop stops the heartbeat sender
func (h *HeartbeatSender) Stop() {
	close(h.stopCh)
}

// send sends a single heartbeat to the master
func (h *HeartbeatSender) send(ctx context.Context) error {
	client := proto.NewMasterServiceClient(h.conn)

	sendCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	active, completed, failed := h.getStats()

	resp, err := client.Heartbeat(sendCtx, &proto.HeartbeatRequest{
		WorkerId: h.workerID,
		Stats: &proto.WorkerStats{
			ActiveJobs:    active,
			CompletedJobs: completed,
			FailedJobs:    failed,
		},
	})
	if err != nil {
		return err
	}

	if !resp.Ack {
		h.logger.Warn("Heartbeat not acknowledged")
	}

	h.logger.Debug("Heartbeat sent",
		zap.String("worker_id", h.workerID),
		zap.Int32("active_jobs", active),
	)

	return nil
}
