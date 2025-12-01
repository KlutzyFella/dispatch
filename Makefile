PROTO_DIR := proto
GEN_DIR   := gen
BIN_DIR   := bin

.PHONY: install-tools generate build build-worker test lint run run-worker clean setup docker-build

# ── Setup ─────────────────────────────────────────────────────────────────────

## install-tools: install protoc plugins (requires Go in PATH)
install-tools:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

## setup: full first-time setup (install tools → generate → tidy)
setup: install-tools generate tidy

# ── Code Generation ───────────────────────────────────────────────────────────

## generate: compile task.proto → Go bindings under gen/
generate:
	@mkdir -p $(GEN_DIR)
	protoc \
		--go_out=$(GEN_DIR) \
		--go_opt=paths=source_relative \
		--go-grpc_out=$(GEN_DIR) \
		--go-grpc_opt=paths=source_relative \
		-I $(PROTO_DIR) \
		$(PROTO_DIR)/task/v1/task.proto

## tidy: resolve and tidy module dependencies
tidy:
	go mod tidy

# ── Build & Test ──────────────────────────────────────────────────────────────

## build: compile the dispatch binary
build:
	go build -o $(BIN_DIR)/dispatch ./cmd/dispatch

## build-worker: compile the worker binary
build-worker:
	go build -o $(BIN_DIR)/worker ./cmd/worker

## test: run all tests with race detector and coverage
test:
	go test ./... -v -race -coverprofile=coverage.out
	go tool cover -func=coverage.out | tail -1

## lint: run static analysis
lint:
	go vet ./...

# ── Run ───────────────────────────────────────────────────────────────────────

## run: start the server on the default port (override with PORT=)
run:
	go run ./cmd/dispatch

## run-worker: start the worker with default settings
run-worker:
	go run ./cmd/worker

# ── Cleanup ───────────────────────────────────────────────────────────────────

## clean: remove generated artifacts and compiled binaries
clean:
	rm -rf $(BIN_DIR) $(GEN_DIR) coverage.out

# ── Docker ────────────────────────────────────────────────────────────────────

## docker-build: build Docker images for dispatch server and worker
docker-build:
	docker build -f build/Dockerfile.dispatch -t dispatch-server:latest .
	docker build -f build/Dockerfile.worker -t dispatch-worker:latest .