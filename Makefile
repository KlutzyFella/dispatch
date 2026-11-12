PROTO_DIR := proto
GEN_DIR   := gen
BIN_DIR   := bin

.PHONY: install-tools generate build build-worker test test-integration test-stack lint lint-scripts run run-worker up logs clean setup docker-build

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

## test: unit tier only (no Kafka/Redis needed), with the race detector
test:
	go test -race -count=1 ./...

## test-integration: full tier against real Kafka and Redis; any skipped test
##     fails. Start them first: docker compose up -d --wait kafka redis
test-integration:
	go test -tags=integration -race -count=1 -timeout=900s -json ./... > test-results.json; \
		status=$$?; python3 scripts/ci/gotest_summary.py --no-skips test-results.json && exit $$status

## test-stack: drive `make up` end to end (needs Docker, curl and grpcurl)
test-stack:
	./scripts/test-stack.sh

## lint: formatting, vet under both build tags, module checksums and drift;
##     fails on any diff
lint:
	@unformatted=$$(gofmt -l $$(git ls-files '*.go')); \
		if [ -n "$$unformatted" ]; then echo "gofmt needed:"; gofmt -d $$unformatted; exit 1; fi
	go vet ./...
	go vet -tags=integration ./...
	go mod verify
	go mod tidy -diff

## lint-scripts: ShellCheck (pinned Docker image) over the shell scripts, and
##     actionlint over the workflow, including ShellCheck on its run: blocks.
##     Needs Docker and actionlint v1.7.7.
lint-scripts:
	./scripts/ci/shellcheck scripts/*.sh scripts/ci/shellcheck
	actionlint -shellcheck scripts/ci/shellcheck .github/workflows/ci.yml

# ── Run ───────────────────────────────────────────────────────────────────────

## run: start the server on the default port (override with PORT=)
run:
	go run ./cmd/dispatch

## run-worker: start the worker with default settings
run-worker:
	go run ./cmd/worker

## up: start Redis, Kafka, dispatch and worker in dependency order and supervise
##     them in the foreground (Ctrl-C stops everything); see scripts/stack.sh
up:
	./scripts/stack.sh

## logs: follow the stack's logs (run in a second terminal while `make up` runs)
logs:
	tail -n 50 -F .stack/logs/stack.log .stack/logs/dispatch.log .stack/logs/worker-*.log

# ── Cleanup ───────────────────────────────────────────────────────────────────

## clean: remove generated artifacts and compiled binaries
clean:
	rm -rf $(BIN_DIR) $(GEN_DIR)

# ── Docker ────────────────────────────────────────────────────────────────────

## docker-build: build Docker images for dispatch server and worker
docker-build:
	docker build -f build/Dockerfile.dispatch -t dispatch-server:latest .
	docker build -f build/Dockerfile.worker -t dispatch-worker:latest .