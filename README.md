# Conductor

A distributed job scheduling system built in Go with Raft consensus, gRPC communication, and production-grade security.

## Features

- **Distributed Consensus**: Raft-based leader election with automatic failover
- **Job Scheduling**: Multiple policies (round-robin, least-loaded, capacity-aware)
- **Security**: mTLS, JWT authentication, RBAC authorization
- **Observability**: Prometheus metrics, OpenTelemetry tracing, structured audit logs
- **Persistence**: BoltDB storage with crash recovery
- **Job Types**: Image processing, web scraping, data analysis, custom types

## Quick Start

### Prerequisites
- Go 1.25+
- Make
- protoc (for proto generation)

### Build
```bash
make build        # Build all binaries
make proto        # Regenerate protobuf files
make help         # Show all available targets
```

### Run Single Node (Development)
```bash
# Terminal 1: Start master
make run-master

# Terminal 2: Start worker
make run-worker

# Terminal 3: Submit a job
./bin/client -addr localhost:9000 -type image_processing -payload '{"url": "example.com/image.jpg"}'
```

### Run Development Cluster (1 Master + 1 Worker)
```bash
./scripts/start-cluster.sh
```
This script starts a single-node cluster with automatic worker registration. Useful for local development and testing.

### Run HA Cluster (3 Masters + 1 Worker)
```bash
./scripts/start-ha-cluster.sh
```
This script bootstraps a 3-node Raft cluster for high availability testing. Nodes use ports:
- Master 1: gRPC 9000, HTTP 8080 (bootstrap/leader)
- Master 2: gRPC 9002, HTTP 8081
- Master 3: gRPC 9004, HTTP 8082

## Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Master 1  │◄───►│   Master 2  │◄───►│   Master 3  │
│   (Leader)  │     │  (Follower) │     │  (Follower) │
└──────┬──────┘     └─────────────┘     └─────────────┘
       │ Raft Consensus
       ▼
┌─────────────┐     ┌─────────────┐
│   Worker 1  │     │   Worker 2  │
└─────────────┘     └─────────────┘
```

### Components
| Component | Description |
|-----------|-------------|
| **Master** | Raft leader handles job submission, scheduling, and state replication |
| **Worker** | Executes jobs and reports results via heartbeat |
| **Client** | CLI for job submission and status queries |

### Job Types
| Type | Description |
|------|-------------|
| `image_processing` | Image manipulation tasks |
| `web_scraping` | Web page scraping |
| `data_analysis` | Data processing jobs |
| `test_sleep` | Test job that sleeps (for testing) |

## Configuration

Configuration uses profile-based YAML files + environment variable overrides:

| Profile | File | Use Case |
|---------|------|----------|
| development | `config/dev.yaml` | Local testing, relaxed security |
| staging | `config/staging.yaml` | Pre-production with TLS |
| production | `config/prod.yaml` | Full security, HA required |

### Key Environment Variables
```bash
# Core
CONDUCTOR_PROFILE=staging          # Profile: development, staging, production
NODE_ID=master-1                   # Unique node identifier
GRPC_MASTER_PORT=9000              # gRPC listen port
HTTP_PORT=8080                     # HTTP API port

# Security
JWT_SECRET_KEY=<secret>            # JWT signing key (required in staging/prod)
SECURITY_TLS_ENABLED=true          # Enable mTLS
SECURITY_TLS_SKIP_VERIFY=true      # Skip TLS verification (dev only)

# Cluster
BOOTSTRAP=true                     # Bootstrap new Raft cluster
JOIN_ADDR=localhost:9000           # Address of existing cluster to join
```

## API Endpoints

### HTTP
| Endpoint | Description |
|----------|-------------|
| `GET /health` | Health check (returns `OK`) |
| `GET /ready` | Readiness check |
| `GET /cluster/status` | Cluster info (leader, peers, state) |
| `GET /failover/status` | Failover detector status |

### gRPC (MasterService)
| Method | Description |
|--------|-------------|
| `SubmitJob` | Submit a new job |
| `GetJobStatus` | Query job status by ID |
| `ListJobs` | List all jobs with filtering |
| `CancelJob` | Cancel a pending/running job |
| `RegisterWorker` | Register a new worker |
| `Heartbeat` | Worker heartbeat with stats |

## Observability

### Prometheus Metrics
```bash
curl http://localhost:9090/metrics | grep conductor
```
Key metrics:
- `conductor_jobs_total{type, status}` - Job counts
- `conductor_scheduling_latency_seconds` - Scheduling latency
- `conductor_raft_is_leader` - Leadership status

### OpenTelemetry Tracing
Trace IDs are logged for each RPC call. Configure OTLP exporter:
```bash
OTEL_EXPORTER_OTLP_ENDPOINT=localhost:4318
OTEL_TRACE_ENABLED=true
```

### Audit Logs
Structured JSON logs for security events:
```json
{"logger":"audit","msg":"Authentication successful","user_id":"admin","method":"/proto.MasterService/SubmitJob"}
{"logger":"rbac-audit","msg":"Authorization successful","user_id":"admin","method":"/proto.MasterService/SubmitJob"}
```

## Scripts

| Script | Description |
|--------|-------------|
| `scripts/start-cluster.sh` | Start dev cluster (1 master + 1 worker) |
| `scripts/start-ha-cluster.sh` | Start HA cluster (3 masters + 1 worker) |

## Development

### Make Targets
```bash
make build          # Build all binaries
make test           # Run tests with coverage
make test-short     # Run tests (skip slow)
make check          # Run fmt + vet + staticcheck
make clean          # Clean build artifacts
make stop           # Stop all conductor processes
make ha-cluster     # Start HA cluster
```

### Project Structure
```
conductor/
├── cmd/                    # Entry points
│   ├── master/            # Master node
│   ├── worker/            # Worker node
│   └── client/            # CLI client
├── internal/              # Private packages
│   ├── config/            # Configuration
│   ├── consensus/         # Raft consensus
│   ├── scheduler/         # Job scheduling
│   ├── storage/           # Persistence (BoltDB)
│   ├── security/          # TLS, JWT, RBAC
│   └── metrics/           # Prometheus metrics
├── api/proto/             # gRPC definitions
├── config/                # YAML configs
└── scripts/               # Cluster scripts
```

## License

MIT
