package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	proto "github.com/francisco3ferraz/conductor/api/proto"
	"github.com/francisco3ferraz/conductor/internal/config"
	"github.com/francisco3ferraz/conductor/internal/job"
	"github.com/francisco3ferraz/conductor/internal/rpc"
	"github.com/francisco3ferraz/conductor/internal/worker"
	"google.golang.org/grpc"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func main() {
	// Load configuration
	cfg, err := config.Load("")
	if err != nil {
		fmt.Printf("Failed to load config: %v\n", err)
		os.Exit(1)
	}

	// Initialize logger
	logger, err := initLogger(cfg.Log.Level)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync()

	logger.Info("Starting worker node",
		zap.String("worker_id", cfg.Worker.WorkerID),
		zap.String("master_addr", cfg.Worker.MasterAddr),
		zap.Int("max_jobs", cfg.Worker.MaxConcurrentJobs),
	)

	// Create job executor
	exec := worker.NewExecutor(cfg.Worker.WorkerID, logger)

	// Create worker client
	workerClient, err := rpc.NewWorkerClient(
		cfg.Worker.WorkerID,
		cfg.Worker.MasterAddr,
		exec,
		logger,
	)
	if err != nil {
		logger.Fatal("Failed to create worker client", zap.Error(err))
	}
	defer workerClient.Close()

	// Determine worker's gRPC address to advertise (use WORKER_ADDR env var or default)
	advertiseAddr := fmt.Sprintf("localhost:%d", cfg.GRPC.WorkerPort)

	// Register with master
	ctx := context.Background()
	if err := workerClient.Register(ctx, int32(cfg.Worker.MaxConcurrentJobs), advertiseAddr); err != nil {
		logger.Fatal("Failed to register with master", zap.Error(err))
	}

	// Start heartbeat goroutine
	heartbeatCtx, heartbeatCancel := context.WithCancel(ctx)
	defer heartbeatCancel()

	go workerClient.StartHeartbeat(heartbeatCtx, cfg.Worker.HeartbeatInterval)

	// Create result reporter function that sends results back to master
	resultReporter := func(ctx context.Context, jobID string, result *job.Result) error {
		logger.Info("Job execution completed",
			zap.String("job_id", jobID),
			zap.Bool("success", result.Success),
			zap.Int64("duration_ms", result.DurationMs),
		)

		// Report result to master via gRPC
		workerSvcClient := proto.NewWorkerServiceClient(workerClient.GetConn())
		reportCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()

		resp, err := workerSvcClient.ReportResult(reportCtx, &proto.ReportResultRequest{
			JobId:    jobID,
			WorkerId: cfg.Worker.WorkerID,
			Result: &proto.JobResult{
				Success:    result.Success,
				Output:     result.Output,
				Error:      result.Error,
				DurationMs: result.DurationMs,
			},
		})
		if err != nil {
			logger.Error("Failed to report result", zap.Error(err))
			return err
		}

		if !resp.Success {
			logger.Warn("Result report not acknowledged", zap.String("message", resp.Message))
		}

		return nil
	}

	// Create and start worker gRPC server
	grpcServer := grpc.NewServer()
	workerSvc := rpc.NewWorkerServer(cfg.Worker.WorkerID, exec, resultReporter, logger)
	proto.RegisterWorkerServiceServer(grpcServer, workerSvc)

	workerAddr := fmt.Sprintf(":%d", cfg.GRPC.WorkerPort)
	grpcListener, err := net.Listen("tcp", workerAddr)
	if err != nil {
		logger.Fatal("Failed to create gRPC listener", zap.Error(err))
	}

	go func() {
		logger.Info("Starting gRPC server", zap.String("addr", workerAddr))
		if err := grpcServer.Serve(grpcListener); err != nil {
			logger.Error("gRPC server error", zap.Error(err))
		}
	}()

	// Register worker with scheduler (including gRPC address)
	// We need to expose the scheduler registration endpoint
	// For now, we'll register via direct scheduler access (to be implemented)

	// Create HTTP server for health checks
	mux := http.NewServeMux()
	mux.HandleFunc("/health", healthHandler(logger))
	mux.HandleFunc("/ready", readyHandler(logger))

	httpServer := &http.Server{
		Addr:    ":8081",
		Handler: mux,
	}

	// Start HTTP server
	go func() {
		logger.Info("Starting HTTP server", zap.String("addr", ":8081"))
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("HTTP server error", zap.Error(err))
		}
	}()

	logger.Info("Worker node is running. Press Ctrl+C to stop.")

	// Wait for interrupt signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	// Graceful shutdown
	logger.Info("Shutting down worker node...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Stop heartbeat
	heartbeatCancel()

	// Stop HTTP server
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("HTTP server shutdown error", zap.Error(err))
	}

	// Close worker client
	workerClient.Close()

	// Close Raft
	logger.Info("Shutdown complete")
}

func healthHandler(logger *zap.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	}
}

func readyHandler(logger *zap.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("READY"))
	}
}

func initLogger(level string) (*zap.Logger, error) {
	cfg := zap.NewProductionConfig()
	cfg.Level.SetLevel(parseLogLevel(level))
	return cfg.Build()
}

func parseLogLevel(level string) zapcore.Level {
	switch level {
	case "debug":
		return zapcore.DebugLevel
	case "info":
		return zapcore.InfoLevel
	case "warn":
		return zapcore.WarnLevel
	case "error":
		return zapcore.ErrorLevel
	default:
		return zapcore.InfoLevel
	}
}
