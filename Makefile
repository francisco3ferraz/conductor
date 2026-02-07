# Conductor - Distributed Job Scheduler
# Makefile for building, testing, and running the system

.PHONY: all build test clean proto deps lint fmt vet check \
        run-master run-worker cluster ha-cluster stop help \
        docker-build docker-up docker-up-dev docker-down docker-logs docker-clean

# ==============================================================================
# Configuration
# ==============================================================================

BINARY_DIR     := bin
PROTO_DIR      := api/proto
COVERAGE_FILE  := coverage.out
DATA_DIR       := /tmp/conductor

# Binaries
MASTER  := $(BINARY_DIR)/master
WORKER  := $(BINARY_DIR)/worker
CLIENT  := $(BINARY_DIR)/client

# Proto files
PROTO_FILES := $(wildcard $(PROTO_DIR)/*.proto)

# Go commands
GO       := go
GOBUILD  := $(GO) build
GOTEST   := $(GO) test
GOMOD    := $(GO) mod
GOVET    := $(GO) vet
GOFMT    := $(GO) fmt

# Build flags
BUILD_FLAGS := -trimpath
LDFLAGS     := -s -w

# ==============================================================================
# Default Target
# ==============================================================================

all: deps build test

# ==============================================================================
# Help
# ==============================================================================

help: ## Display this help message
	@echo "Conductor Makefile"
	@echo ""
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

# ==============================================================================
# Dependencies
# ==============================================================================

deps: ## Download and tidy Go dependencies
	@echo "==> Downloading dependencies..."
	@$(GOMOD) download
	@$(GOMOD) tidy

proto-deps: ## Install protobuf code generators
	@command -v protoc >/dev/null 2>&1 || { echo "Error: protoc not found"; exit 1; }
	@$(GO) install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	@$(GO) install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

# ==============================================================================
# Code Generation
# ==============================================================================

proto: proto-deps ## Generate protobuf Go code
	@echo "==> Generating protobuf files..."
	@protoc \
		--go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		$(PROTO_FILES)
	@echo "    Generated: $(PROTO_DIR)/*.pb.go"

# ==============================================================================
# Build
# ==============================================================================

build: $(MASTER) $(WORKER) $(CLIENT) ## Build all binaries

$(BINARY_DIR):
	@mkdir -p $@

$(MASTER): $(BINARY_DIR)
	@echo "==> Building master..."
	@$(GOBUILD) $(BUILD_FLAGS) -ldflags "$(LDFLAGS)" -o $@ ./cmd/master

$(WORKER): $(BINARY_DIR)
	@echo "==> Building worker..."
	@$(GOBUILD) $(BUILD_FLAGS) -ldflags "$(LDFLAGS)" -o $@ ./cmd/worker

$(CLIENT): $(BINARY_DIR)
	@echo "==> Building client..."
	@$(GOBUILD) $(BUILD_FLAGS) -ldflags "$(LDFLAGS)" -o $@ ./cmd/client

build-race: ## Build with race detector
	@echo "==> Building with race detector..."
	@mkdir -p $(BINARY_DIR)
	@$(GOBUILD) -race -o $(MASTER) ./cmd/master
	@$(GOBUILD) -race -o $(WORKER) ./cmd/worker
	@$(GOBUILD) -race -o $(CLIENT) ./cmd/client

# ==============================================================================
# Testing
# ==============================================================================

test: ## Run all tests
	@echo "==> Running tests..."
	@$(GOTEST) -race -coverprofile=$(COVERAGE_FILE) ./...
	@echo ""
	@$(GO) tool cover -func=$(COVERAGE_FILE) | grep total

test-short: ## Run tests (skip slow tests)
	@echo "==> Running short tests..."
	@$(GOTEST) -short ./...

test-verbose: ## Run tests with verbose output
	@$(GOTEST) -v -race -coverprofile=$(COVERAGE_FILE) ./...

cover: test ## Generate and open coverage report
	@$(GO) tool cover -html=$(COVERAGE_FILE) -o coverage.html
	@echo "    Coverage report: coverage.html"

# ==============================================================================
# Code Quality
# ==============================================================================

fmt: ## Format Go source files
	@echo "==> Formatting code..."
	@$(GOFMT) ./...

vet: ## Run go vet
	@echo "==> Running go vet..."
	@$(GOVET) ./...

lint: ## Run golangci-lint (requires installation)
	@echo "==> Running linter..."
	@command -v golangci-lint >/dev/null 2>&1 || { echo "Install: go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest"; exit 1; }
	@golangci-lint run ./...

staticcheck: ## Run staticcheck
	@echo "==> Running staticcheck..."
	@command -v staticcheck >/dev/null 2>&1 || $(GO) install honnef.co/go/tools/cmd/staticcheck@latest
	@staticcheck ./...

check: fmt vet staticcheck ## Run all code quality checks

# ==============================================================================
# Run Targets
# ==============================================================================

run-master: build ## Run master node (development mode)
	@echo "==> Starting master node..."
	@CONDUCTOR_PROFILE=development $(MASTER)

run-worker: build ## Run worker node (development mode)
	@echo "==> Starting worker node..."
	@CONDUCTOR_PROFILE=development $(WORKER)

cluster: build ## Start single node cluster (1 master + 1 worker)
	@echo "==> Starting single node cluster..."
	@./scripts/start-cluster.sh

ha-cluster: build ## Start HA cluster (3 masters + 1 worker)
	@echo "==> Starting HA cluster..."
	@./scripts/start-ha-cluster.sh

# ==============================================================================
# Cleanup
# ==============================================================================

stop: ## Stop all running conductor processes
	@echo "==> Stopping conductor processes..."
	@pkill -f "$(BINARY_DIR)/master" 2>/dev/null || true
	@pkill -f "$(BINARY_DIR)/worker" 2>/dev/null || true
	@echo "    Done."

clean: stop ## Remove build artifacts and temp files
	@echo "==> Cleaning..."
	@rm -rf $(BINARY_DIR)
	@rm -f $(COVERAGE_FILE) coverage.html
	@rm -rf $(DATA_DIR)
	@rm -rf logs/
	@echo "    Clean complete."

clean-data: ## Remove only data files (keep binaries)
	@echo "==> Cleaning data..."
	@rm -rf $(DATA_DIR)
	@rm -rf logs/

# ==============================================================================
# Docker
# ==============================================================================

docker-build: ## Build Docker images
	@echo "==> Building Docker images..."
	@docker build --target master -t conductor-master:latest .
	@docker build --target worker -t conductor-worker:latest .
	@docker build --target client -t conductor-client:latest .

docker-up: ## Start HA cluster with Docker Compose
	@echo "==> Starting Docker HA cluster..."
	@docker compose up -d

docker-up-dev: ## Start dev cluster with Docker Compose
	@echo "==> Starting Docker dev cluster..."
	@docker compose -f docker-compose.dev.yml up -d

docker-down: ## Stop Docker Compose services
	@echo "==> Stopping Docker services..."
	@docker compose down
	@docker compose -f docker-compose.dev.yml down 2>/dev/null || true

docker-logs: ## View Docker Compose logs
	@docker compose logs -f

docker-clean: docker-down ## Remove Docker images and volumes
	@echo "==> Cleaning Docker resources..."
	@docker compose down -v --rmi local 2>/dev/null || true
	@docker compose -f docker-compose.dev.yml down -v --rmi local 2>/dev/null || true

