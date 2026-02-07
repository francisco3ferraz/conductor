#!/bin/bash
set -e

# Configuration
PROFILE="${CONDUCTOR_PROFILE:-staging}"
JWT_SECRET="${CONDUCTOR_JWT_SECRET:-production-jwt-secret-key-safe}"
BIN_DIR="./bin"
LOG_DIR="./logs"
DATA_DIR="/tmp/conductor/data"
RAFT_DIR="/tmp/conductor/raft"

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

log() {
    echo -e "${GREEN}[$(date +'%H:%M:%S')] $1${NC}"
}

warn() {
    echo -e "${YELLOW}[$(date +'%H:%M:%S')] WARN: $1${NC}"
}

error() {
    echo -e "${RED}[$(date +'%H:%M:%S')] ERROR: $1${NC}"
}

cleanup() {
    log "Shutting down cluster..."
    pkill -f "$BIN_DIR/master" || true
    pkill -f "$BIN_DIR/worker" || true
    # Force kill anything on ports
    fuser -k 9000/tcp >/dev/null 2>&1 || true
    fuser -k 9001/tcp >/dev/null 2>&1 || true
    log "Cleanup complete."
}

# Trap interrupts for cleanup
trap cleanup EXIT

# Check for binaries
if [ ! -f "$BIN_DIR/master" ] || [ ! -f "$BIN_DIR/worker" ]; then
    warn "Binaries not found. Building..."
    make build
fi

# Prepare directories
mkdir -p "$LOG_DIR"
rm -rf "$DATA_DIR" "$RAFT_DIR"
mkdir -p "$DATA_DIR" "$RAFT_DIR"

log "Starting Conductor Cluster (Profile: $PROFILE)"

# Export Common Environment Variables
export CONDUCTOR_PROFILE="$PROFILE"
export CONDUCTOR_JWT_SECRET="$JWT_SECRET"
export LOG_OUTPUT="stdout" 

# Security skips for local testing (Staging only)
if [ "$PROFILE" == "staging" ]; then
    export SECURITY_TLS_SKIP_VERIFY=true
    export SECURITY_RAFT_TLS_SKIP_VERIFY=true
    export SECURITY_TLS_SERVER_NAME=localhost
    export SECURITY_RAFT_TLS_SERVER_NAME=localhost
fi

# 1. Start Master
log "Starting Master Node (staging-master-1)..."
export NODE_ID="staging-master-1"
export BOOTSTRAP=true
export GRPC_MASTER_PORT=9000

nohup $BIN_DIR/master > "$LOG_DIR/master.log" 2>&1 &
MASTER_PID=$!
log "Master PID: $MASTER_PID. Logs: $LOG_DIR/master.log"

# Wait for Master to be ready
log "Waiting for Master to initialize..."
MAX_RETRIES=30
for i in $(seq 1 $MAX_RETRIES); do
    if curl -s -k http://localhost:8080/health > /dev/null; then
        log "Master is healthy!"
        break
    fi
    if [ "$i" -eq "$MAX_RETRIES" ]; then
        error "Master failed to start. Check logs:"
        tail -n 20 "$LOG_DIR/master.log"
        exit 1
    fi
    sleep 1
done

# 2. Start Worker
log "Starting Worker Node (staging-worker-1)..."
export WORKER_ID="staging-worker-1"
export MASTER_ADDR="localhost:9000"

nohup $BIN_DIR/worker > "$LOG_DIR/worker.log" 2>&1 &
WORKER_PID=$!
log "Worker PID: $WORKER_PID. Logs: $LOG_DIR/worker.log"

# Wait for Worker registration
sleep 2
if ps -p $WORKER_PID > /dev/null; then
    log "Worker started successfully."
else
    error "Worker failed to start. Check logs:"
    tail -n 20 "$LOG_DIR/worker.log"
    exit 1
fi

log "Cluster is running via TLS + JWT + RBAC."
log "Dashboard: http://localhost:8080/cluster/status"
log "Press Ctrl+C to stop."

# Keep script running to maintain trap
wait $MASTER_PID
