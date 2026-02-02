package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/francisco3ferraz/conductor/api/proto"
	"github.com/francisco3ferraz/conductor/internal/config"
	"github.com/francisco3ferraz/conductor/internal/consensus"
	"github.com/francisco3ferraz/conductor/internal/rpc"
	"github.com/francisco3ferraz/conductor/internal/scheduler"
	"github.com/francisco3ferraz/conductor/internal/storage"
	"google.golang.org/grpc"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func main() {
	// Load configuration
	cfg, err := config.Load("")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load config: %v\n", err)
		os.Exit(1)
	}

	// Initialize logger
	logger, err := initLogger(cfg.Log.Level)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync()

	logger.Info("Starting master node",
		zap.String("node_id", cfg.Cluster.NodeID),
		zap.String("bind_addr", cfg.Cluster.BindAddr),
		zap.Int("grpc_port", cfg.GRPC.MasterPort),
	)

	// Initialize storage with BoltDB
	boltPath := filepath.Join(cfg.Cluster.DataDir, "jobs.db")
	store, err := storage.NewBoltStore(boltPath)
	if err != nil {
		logger.Fatal("Failed to create bolt store", zap.Error(err))
	}
	defer store.Close()

	logger.Info("Storage initialized", zap.String("type", "boltdb"), zap.String("path", boltPath))

	// Initialize Raft FSM
	fsm := consensus.NewFSM(logger)

	// Initialize Raft node
	raftConfig := &consensus.Config{
		NodeID:            cfg.Cluster.NodeID,
		BindAddr:          cfg.Cluster.BindAddr,
		DataDir:           cfg.Cluster.RaftDir,
		Bootstrap:         true, // TODO: make this configurable for multi-node setups
		HeartbeatTimeout:  cfg.Raft.HeartbeatTimeout,
		ElectionTimeout:   cfg.Raft.ElectionTimeout,
		SnapshotInterval:  cfg.Raft.SnapshotInterval,
		SnapshotThreshold: cfg.Raft.SnapshotThreshold,
	}

	raftNode, err := consensus.NewRaftNode(raftConfig, fsm, logger)
	if err != nil {
		logger.Fatal("Failed to create Raft node", zap.Error(err))
	}
	defer raftNode.Shutdown()

	logger.Info("Raft node initialized",
		zap.String("node_id", raftConfig.NodeID),
		zap.String("bind_addr", raftConfig.BindAddr),
	)

	// Wait for leader election
	if err := raftNode.WaitForLeader(30 * time.Second); err != nil {
		logger.Warn("No leader elected yet", zap.Error(err))
	} else {
		logger.Info("Leader elected", zap.String("leader", raftNode.Leader()))
	}

	// Create gRPC server with interceptors
	interceptors := []grpc.UnaryServerInterceptor{
		rpc.LoggingInterceptor(logger),
		rpc.AuthInterceptorWithConfig(logger, &rpc.JWTConfig{
			SecretKey:  cfg.JWT.SecretKey,
			Issuer:     cfg.JWT.Issuer,
			Audience:   cfg.JWT.Audience,
			SkipExpiry: cfg.JWT.SkipExpiry,
		}),
		rpc.RecoveryInterceptor(logger),
	}
	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(rpc.ChainInterceptors(interceptors...)),
	)
	masterSvc := rpc.NewMasterServer(raftNode, fsm, logger)
	proto.RegisterMasterServiceServer(grpcServer, masterSvc)
	proto.RegisterWorkerServiceServer(grpcServer, masterSvc)

	// Create and start scheduler
	sched := scheduler.NewScheduler(raftNode, fsm, logger)

	// Set job store for recovery manager
	sched.SetJobStore(store)

	// Wire scheduler to master server
	masterSvc.SetScheduler(sched)

	// Start gRPC server
	grpcAddr := fmt.Sprintf(":%d", cfg.GRPC.MasterPort)
	grpcListener, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		logger.Fatal("Failed to create gRPC listener", zap.Error(err))
	}

	go func() {
		logger.Info("Starting gRPC server", zap.String("addr", grpcAddr))
		if err := grpcServer.Serve(grpcListener); err != nil {
			logger.Error("gRPC server error", zap.Error(err))
		}
	}()

	// Start scheduler in background
	schedCtx, schedCancel := context.WithCancel(context.Background())
	defer schedCancel()

	go sched.Start(schedCtx)

	logger.Info("Scheduler started")

	// Create HTTP server for health checks
	mux := http.NewServeMux()
	mux.HandleFunc("/health", healthHandler(logger))
	mux.HandleFunc("/ready", readyHandler(logger, store))
	mux.HandleFunc("/failover/status", failoverStatusHandler(logger, sched))
	mux.HandleFunc("/cluster/status", clusterStatusHandler(logger, raftNode))

	httpServer := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	// Start HTTP server
	go func() {
		logger.Info("Starting HTTP server", zap.String("addr", httpServer.Addr))
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("HTTP server error", zap.Error(err))
		}
	}()

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	logger.Info("Master node is running. Press Ctrl+C to stop.")
	<-sigChan

	// Graceful shutdown
	logger.Info("Shutting down master node...")

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Stop scheduler first (stops failover components)
	sched.Stop()

	// Stop gRPC server
	grpcServer.GracefulStop()

	// Stop HTTP server
	if err := httpServer.Shutdown(ctx); err != nil {
		logger.Error("HTTP server shutdown error", zap.Error(err))
	}

	logger.Info("Master node stopped")
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

func healthHandler(logger *zap.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	}
}

func readyHandler(logger *zap.Logger, store storage.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Check if storage is accessible
		_, err := store.ListJobs(storage.JobFilter{Limit: 1})
		if err != nil {
			logger.Error("Storage health check failed", zap.Error(err))
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte("NOT READY"))
			return
		}

		w.WriteHeader(http.StatusOK)
		w.Write([]byte("READY"))
	}
}

func failoverStatusHandler(logger *zap.Logger, sched *scheduler.Scheduler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		status := map[string]interface{}{
			"failure_detector": "unknown",
			"recovery_manager": "unknown",
			"active_workers":   0,
			"failed_workers":   0,
		}

		// Check failure detector status
		if failureDetector := sched.GetFailureDetector(); failureDetector != nil {
			status["failure_detector"] = "active"

			activeWorkers := failureDetector.ListActiveWorkers()
			status["active_workers"] = len(activeWorkers)

			// Get failed worker count (would need to add this method to registry)
			// For now, just indicate detector is running
		} else {
			status["failure_detector"] = "not_initialized"
		}

		// Check recovery manager status
		if recoveryManager := sched.GetRecoveryManager(); recoveryManager != nil {
			if err := recoveryManager.HealthCheck(); err != nil {
				status["recovery_manager"] = "unhealthy: " + err.Error()
			} else {
				status["recovery_manager"] = "healthy"
			}
		} else {
			status["recovery_manager"] = "not_initialized"
		}

		// Write JSON response
		if err := json.NewEncoder(w).Encode(status); err != nil {
			logger.Error("Failed to encode failover status", zap.Error(err))
			w.WriteHeader(http.StatusInternalServerError)
		}
	}
}

func clusterStatusHandler(logger *zap.Logger, raftNode *consensus.RaftNode) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		status := map[string]interface{}{
			"state":  raftNode.State().String(),
			"leader": raftNode.Leader(),
		}

		// Get cluster configuration
		config, err := raftNode.GetConfiguration()
		if err != nil {
			logger.Error("Failed to get Raft configuration", zap.Error(err))
			status["error"] = err.Error()
		} else {
			servers := make([]map[string]string, 0, len(config.Servers))
			for _, server := range config.Servers {
				servers = append(servers, map[string]string{
					"id":       string(server.ID),
					"address":  string(server.Address),
					"suffrage": server.Suffrage.String(),
				})
			}
			status["servers"] = servers
		}

		// Add Raft stats
		stats := raftNode.Stats()
		status["stats"] = stats

		// Write JSON response
		if err := json.NewEncoder(w).Encode(status); err != nil {
			logger.Error("Failed to encode cluster status", zap.Error(err))
			w.WriteHeader(http.StatusInternalServerError)
		}
	}
}
