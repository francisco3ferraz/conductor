package main

import (
	"context"
	"crypto/tls"
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
	"github.com/francisco3ferraz/conductor/internal/security"
	"github.com/francisco3ferraz/conductor/internal/storage"
	"github.com/francisco3ferraz/conductor/internal/tracing"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

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

	// Initialize distributed tracing
	tracingConfig := tracing.DefaultConfig()
	tracingConfig.ServiceName = "conductor-master"
	tracingConfig.ServiceVersion = "1.0.0"

	ctx := context.Background()
	tracingShutdown, err := tracing.Initialize(ctx, tracingConfig)
	if err != nil {
		logger.Error("Failed to initialize tracing", zap.Error(err))
		// Don't exit - tracing is optional in development
	} else {
		defer tracingShutdown()
		logger.Info("Distributed tracing initialized",
			zap.String("service", tracingConfig.ServiceName),
			zap.String("endpoint", tracingConfig.OTLPEndpoint),
		)
	}

	logger.Info("Starting master node",
		zap.String("node_id", cfg.Cluster.NodeID),
		zap.String("bind_addr", cfg.Cluster.BindAddr),
		zap.Int("grpc_port", cfg.GRPC.MasterPort),
		zap.String("profile", string(cfg.Profile)),
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

	// Initialize security components
	var raftTLSConfig *security.TLSConfig
	if cfg.Security.RaftTLS.Enabled {
		raftTLSConfig = &security.TLSConfig{
			Enabled:      cfg.Security.RaftTLS.Enabled,
			CertFile:     cfg.Security.RaftTLS.CertFile,
			KeyFile:      cfg.Security.RaftTLS.KeyFile,
			CAFile:       cfg.Security.RaftTLS.CAFile,
			SkipVerify:   cfg.Security.RaftTLS.SkipVerify,
			AutoGenerate: cfg.Security.RaftTLS.AutoGenerate,
		}
	}

	var raftTLS *security.CertManager
	var tlsConfigForRaft *tls.Config
	if raftTLSConfig != nil {
		raftTLS = security.NewCertManager(raftTLSConfig, logger)
		tlsConfigForRaft, err = raftTLS.LoadRaftTLSConfig()
		if err != nil {
			logger.Fatal("Failed to load Raft TLS config", zap.Error(err))
		}
		if tlsConfigForRaft != nil {
			logger.Info("Raft TLS encryption enabled")
		}
	}

	// Initialize Raft node with TLS
	raftConfig := &consensus.Config{
		NodeID:            cfg.Cluster.NodeID,
		BindAddr:          cfg.Cluster.BindAddr,
		DataDir:           cfg.Cluster.RaftDir,
		Bootstrap:         cfg.Cluster.Bootstrap,
		JoinAddr:          cfg.Cluster.JoinAddr,
		HeartbeatTimeout:  cfg.Raft.HeartbeatTimeout,
		ElectionTimeout:   cfg.Raft.ElectionTimeout,
		SnapshotInterval:  cfg.Raft.SnapshotInterval,
		SnapshotThreshold: cfg.Raft.SnapshotThreshold,
		TLSConfig:         tlsConfigForRaft,
	}

	raftNode, err := consensus.NewRaftNode(raftConfig, fsm, logger)
	if err != nil {
		logger.Fatal("Failed to create Raft node", zap.Error(err))
	}
	defer raftNode.Shutdown()

	logger.Info("Raft node initialized",
		zap.String("node_id", raftConfig.NodeID),
		zap.String("bind_addr", raftConfig.BindAddr),
		zap.Bool("bootstrap", raftConfig.Bootstrap),
	)

	// If joining existing cluster, send join request
	if !cfg.Cluster.Bootstrap && cfg.Cluster.JoinAddr != "" {
		logger.Info("Joining existing cluster",
			zap.String("join_addr", cfg.Cluster.JoinAddr),
		)

		// Create gRPC client to join
		conn, err := grpc.DialContext(context.Background(), cfg.Cluster.JoinAddr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithBlock(),
			grpc.WithTimeout(10*time.Second),
		)
		if err != nil {
			logger.Fatal("Failed to connect to cluster", zap.Error(err))
		}
		defer conn.Close()

		client := proto.NewMasterServiceClient(conn)
		resp, err := client.JoinCluster(context.Background(), &proto.JoinClusterRequest{
			NodeId:  cfg.Cluster.NodeID,
			Address: cfg.Cluster.BindAddr,
		})
		if err != nil {
			logger.Fatal("Failed to join cluster", zap.Error(err))
		}

		if !resp.Success {
			logger.Fatal("Join rejected", zap.String("message", resp.Message))
		}

		logger.Info("Successfully joined cluster",
			zap.String("leader", resp.Leader),
		)
	}

	// Wait for leader election
	if err := raftNode.WaitForLeader(30 * time.Second); err != nil {
		logger.Warn("No leader elected yet", zap.Error(err))
	} else {
		logger.Info("Leader elected", zap.String("leader", raftNode.Leader()))
	}

	// Initialize auth manager
	authManager := security.NewAuthManager(&security.JWTConfig{
		SecretKey:       cfg.Security.JWT.SecretKey,
		Issuer:          cfg.Security.JWT.Issuer,
		Audience:        cfg.Security.JWT.Audience,
		SkipExpiry:      cfg.Security.JWT.SkipExpiry,
		DevelopmentMode: cfg.Profile == config.ProfileDevelopment,
	}, logger)

	// Initialize RBAC if enabled
	var rbac *security.RBAC
	logger.Info("RBAC configuration",
		zap.Bool("enabled", cfg.Security.RBAC.Enabled),
		zap.String("policy_file", cfg.Security.RBAC.PolicyFile))
	if cfg.Security.RBAC.Enabled {
		rbacConfig := &security.RBACConfig{
			Enabled:         cfg.Security.RBAC.Enabled,
			DevelopmentMode: cfg.Profile == config.ProfileDevelopment,
			PolicyFile:      cfg.Security.RBAC.PolicyFile,
		}
		rbac = security.NewRBAC(rbacConfig, logger)
		// Add default admin user for testing
		if err := rbac.AddUser("admin", "admin", "admin"); err != nil {
			logger.Fatal("Failed to create admin user", zap.Error(err))
		}
		logger.Info("RBAC enabled")
	}

	// Get tracing-enabled gRPC server options
	grpcOpts := rpc.GetServerInterceptors(logger)

	// Add additional interceptors
	additionalInterceptors := []grpc.UnaryServerInterceptor{
		authManager.AuthInterceptor(),
	}

	// Add RBAC interceptor if enabled
	if rbac != nil {
		additionalInterceptors = append(additionalInterceptors, rbac.RBACInterceptor())
	}

	// Combine with existing server options
	if len(additionalInterceptors) > 0 {
		grpcOpts = append(grpcOpts, grpc.ChainUnaryInterceptor(additionalInterceptors...))
	}

	// Load TLS credentials for gRPC if enabled
	if cfg.Security.TLS.Enabled {
		tlsConfig := &security.TLSConfig{
			Enabled:      cfg.Security.TLS.Enabled,
			CertFile:     cfg.Security.TLS.CertFile,
			KeyFile:      cfg.Security.TLS.KeyFile,
			CAFile:       cfg.Security.TLS.CAFile,
			SkipVerify:   cfg.Security.TLS.SkipVerify,
			AutoGenerate: cfg.Security.TLS.AutoGenerate,
		}
		certManager := security.NewCertManager(tlsConfig, logger)
		creds, err := certManager.LoadOrGenerateServerTLS()
		if err != nil {
			logger.Fatal("Failed to load TLS credentials", zap.Error(err))
		}
		if creds != nil {
			grpcOpts = append(grpcOpts, grpc.Creds(creds))
			logger.Info("gRPC mTLS enabled")
		}
	} else {
		logger.Warn("gRPC TLS DISABLED - NOT FOR PRODUCTION")
	}

	// Create gRPC server with security
	grpcServer := grpc.NewServer(grpcOpts...)
	masterSvc := rpc.NewMasterServer(raftNode, fsm, logger)
	proto.RegisterMasterServiceServer(grpcServer, masterSvc)
	proto.RegisterWorkerServiceServer(grpcServer, masterSvc)

	// Create and start scheduler
	sched := scheduler.NewScheduler(raftNode, fsm, cfg, logger)

	// Configure scheduling policy based on config
	policy := getSchedulingPolicy(cfg.Scheduler.SchedulingPolicy, logger)
	sched.SetSchedulingPolicy(policy)

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

	// Shutdown auth manager to stop token cleanup goroutine
	if err := authManager.Shutdown(); err != nil {
		logger.Error("Auth manager shutdown error", zap.Error(err))
	}

	// Shutdown master service to close forwarder connections
	if err := masterSvc.Shutdown(); err != nil {
		logger.Error("Master service shutdown error", zap.Error(err))
	}

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

// getSchedulingPolicy creates a scheduling policy based on the configured name
func getSchedulingPolicy(policyName string, logger *zap.Logger) scheduler.SchedulingPolicy {
	switch policyName {
	case "round-robin":
		logger.Info("Using round-robin scheduling policy")
		return scheduler.NewRoundRobinPolicy()
	case "least-loaded":
		logger.Info("Using least-loaded scheduling policy")
		return scheduler.NewLeastLoadedPolicy()
	case "priority":
		logger.Info("Using priority scheduling policy")
		return scheduler.NewPriorityPolicy()
	case "random":
		logger.Info("Using random scheduling policy")
		return scheduler.NewRandomPolicy()
	case "capacity-aware":
		logger.Info("Using capacity-aware scheduling policy")
		return scheduler.NewCapacityAwarePolicy()
	default:
		logger.Warn("Unknown scheduling policy, using least-loaded", zap.String("policy", policyName))
		return scheduler.NewLeastLoadedPolicy()
	}
}
