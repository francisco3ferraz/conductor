#!/bin/bash
set -e

# Configuration
# Using staging profile for near-production behavior (TLS enabled, etc.)
export CONDUCTOR_PROFILE="staging"
BIN_DIR="./bin"
LOG_DIR="./logs"
DATA_BASE="/tmp/conductor/data"
# Unset potentially conflicting global variables from parent shell
unset CONDUCTOR_CLUSTER_NODE_ID
unset CONDUCTOR_CLUSTER_BIND_ADDR
unset CONDUCTOR_GRPC_MASTER_PORT
unset CONDUCTOR_CLUSTER_HTTP_PORT
unset CONDUCTOR_CLUSTER_BOOTSTRAP
unset CONDUCTOR_CLUSTER_JOIN_ADDR

DATA_BASE="/tmp/conductor/data"
RAFT_BASE="/tmp/conductor/raft"

# Correct Env Vars mapping from config.go
export JWT_SECRET_KEY="production-jwt-secret-key-safe"
export LOG_OUTPUT="stdout"

# TLS Config - using skip verify for localhost self-signed certs
export SECURITY_TLS_ENABLED="true"
export SECURITY_TLS_SKIP_VERIFY="true"
export SECURITY_RAFT_TLS_ENABLED="true"
export SECURITY_RAFT_TLS_SKIP_VERIFY="true"
export SECURITY_TLS_CERT_FILE="/etc/conductor/certs/server.crt"
export SECURITY_TLS_KEY_FILE="/etc/conductor/certs/server.key"
export SECURITY_TLS_CA_FILE="/etc/conductor/certs/ca.crt"
export SECURITY_RAFT_TLS_CERT_FILE="/etc/conductor/certs/raft.crt"
export SECURITY_RAFT_TLS_KEY_FILE="/etc/conductor/certs/raft.key"
export SECURITY_RAFT_TLS_CA_FILE="/etc/conductor/certs/ca.crt"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

log() { echo -e "${GREEN}[$(date +'%H:%M:%S')] $1${NC}"; }
warn() { echo -e "${YELLOW}[$(date +'%H:%M:%S')] WARN: $1${NC}"; }
error() { echo -e "${RED}[$(date +'%H:%M:%S')] ERROR: $1${NC}"; }

wait_for_port() {
    local port=$1
    local retries=30
    log "Waiting for port $port..."
    while ! timeout 1 bash -c "echo > /dev/tcp/localhost/$port" 2>/dev/null; do
        sleep 0.5
        retries=$((retries-1))
        if [ $retries -le 0 ]; then
            error "Timed out waiting for port $port"
            return 1
        fi
    done
    log "Port $port is ready."
}

cleanup() {
    log "Shutting down cluster..."
    pkill -f "$BIN_DIR/master" || true
    pkill -f "$BIN_DIR/worker" || true
    sleep 2
    log "Cleanup complete."
}
# trap cleanup EXIT # Don't cleanup on exit, we want the cluster to stay running for manual testing if needed.

# Build binaries if missing
if [ ! -f "$BIN_DIR/master" ] || [ ! -f "$BIN_DIR/worker" ]; then
    warn "Binaries not found. Building..."
    make build
fi

# Prepare directories
mkdir -p "$LOG_DIR"
rm -rf "$DATA_BASE" "$RAFT_BASE" "$LOG_DIR"/*

log "Starting High Availability Cluster (3 Nodes) - Profile: $CONDUCTOR_PROFILE"

# --- Node 1 (Bootstrap) ---
log "Starting Node 1 (Bootstrap)..."
(
    export NODE_ID="ha-master-1"
    export BIND_ADDR="localhost:7000"
    export NODE_ID="ha-master-1"
    export CONDUCTOR_CLUSTER_NODE_ID="$NODE_ID"
    export BIND_ADDR="localhost:7000"
    export CONDUCTOR_CLUSTER_BIND_ADDR="$BIND_ADDR"
    export RAFT_DIR="$RAFT_BASE/node1"
    export CONDUCTOR_CLUSTER_RAFT_DIR="$RAFT_DIR"
    export DATA_DIR="$DATA_BASE/node1"
    export CONDUCTOR_CLUSTER_DATA_DIR="$DATA_DIR"
    export BOOTSTRAP="true"
    export CONDUCTOR_CLUSTER_BOOTSTRAP="$BOOTSTRAP"
    export JOIN_ADDR=""
    export CONDUCTOR_CLUSTER_JOIN_ADDR="$JOIN_ADDR"
    export GRPC_MASTER_PORT=9000
    export CONDUCTOR_GRPC_MASTER_PORT="$GRPC_MASTER_PORT"
    export HTTP_PORT=8080
    export CONDUCTOR_CLUSTER_HTTP_PORT="$HTTP_PORT"
    
    mkdir -p "$RAFT_DIR" "$DATA_DIR"
    echo "DEBUG: Node 1 Env NODE_ID=$NODE_ID" > "$LOG_DIR/master-1.log"
    exec $BIN_DIR/master >> "$LOG_DIR/master-1.log" 2>&1
) &
PID1=$!
echo $PID1 > "$LOG_DIR/master-1.pid"
log "Node 1 PID: $PID1"

wait_for_port 9000
wait_for_port 8080
log "Node 1 started. Waiting 5s before Node 2..."
sleep 5

# --- Node 2 ---
log "Starting Node 2..."
(
    export NODE_ID="ha-master-2"
    export BIND_ADDR="localhost:7001"
    export NODE_ID="ha-master-2"
    export CONDUCTOR_CLUSTER_NODE_ID="$NODE_ID"
    export BIND_ADDR="localhost:7001"
    export CONDUCTOR_CLUSTER_BIND_ADDR="$BIND_ADDR"
    export RAFT_DIR="$RAFT_BASE/node2"
    export CONDUCTOR_CLUSTER_RAFT_DIR="$RAFT_DIR"
    export DATA_DIR="$DATA_BASE/node2"
    export CONDUCTOR_CLUSTER_DATA_DIR="$DATA_DIR"
    export BOOTSTRAP="false"
    export CONDUCTOR_CLUSTER_BOOTSTRAP="$BOOTSTRAP"
    export JOIN_ADDR="localhost:9000"
    export CONDUCTOR_CLUSTER_JOIN_ADDR="$JOIN_ADDR"
    export GRPC_MASTER_PORT=9002
    export CONDUCTOR_GRPC_MASTER_PORT="$GRPC_MASTER_PORT"
    export HTTP_PORT=8081
    export CONDUCTOR_CLUSTER_HTTP_PORT="$HTTP_PORT"
    
    mkdir -p "$RAFT_DIR" "$DATA_DIR"
    exec $BIN_DIR/master > "$LOG_DIR/master-2.log" 2>&1
) &
PID2=$!
echo $PID2 > "$LOG_DIR/master-2.pid"
log "Node 2 PID: $PID2"

wait_for_port 9002
wait_for_port 8081
log "Node 2 started. Waiting 5s before Node 3..."
sleep 5

# --- Node 3 ---
log "Starting Node 3..."
(
    export NODE_ID="ha-master-3"
    export BIND_ADDR="localhost:7002"
    export NODE_ID="ha-master-3"
    export CONDUCTOR_CLUSTER_NODE_ID="$NODE_ID"
    export BIND_ADDR="localhost:7002"
    export CONDUCTOR_CLUSTER_BIND_ADDR="$BIND_ADDR"
    export RAFT_DIR="$RAFT_BASE/node3"
    export CONDUCTOR_CLUSTER_RAFT_DIR="$RAFT_DIR"
    export DATA_DIR="$DATA_BASE/node3"
    export CONDUCTOR_CLUSTER_DATA_DIR="$DATA_DIR"
    export BOOTSTRAP="false"
    export CONDUCTOR_CLUSTER_BOOTSTRAP="$BOOTSTRAP"
    export JOIN_ADDR="localhost:9000"
    export CONDUCTOR_CLUSTER_JOIN_ADDR="$JOIN_ADDR"
    export GRPC_MASTER_PORT=9004
    export CONDUCTOR_GRPC_MASTER_PORT="$GRPC_MASTER_PORT"
    export HTTP_PORT=8082
    export CONDUCTOR_CLUSTER_HTTP_PORT="$HTTP_PORT"
    
    mkdir -p "$RAFT_DIR" "$DATA_DIR"
    exec $BIN_DIR/master > "$LOG_DIR/master-3.log" 2>&1
) &
PID3=$!
echo $PID3 > "$LOG_DIR/master-3.pid"
log "Node 3 PID: $PID3"

wait_for_port 9004
wait_for_port 8082
log "Node 3 started."

# --- Worker ---
log "Starting Worker..."
(
    export WORKER_ID="worker-1"
    export MASTER_ADDR="localhost:9000"
    # Worker needs to trust the master's cert or skip verify
    export SECURITY_TLS_SKIP_VERIFY="true"
    
    exec $BIN_DIR/worker > "$LOG_DIR/worker.log" 2>&1
) &
PIDW=$!
echo $PIDW > "$LOG_DIR/worker.pid"
log "Worker PID: $PIDW"

log "Waiting for cluster convergence..."
sleep 5
log "Cluster running."
log "Access logs at $LOG_DIR"
