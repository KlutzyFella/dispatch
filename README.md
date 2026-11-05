# Dispatch

A task-dispatch simulation built with Go, gRPC, Kafka, and Redis.

![Go](https://img.shields.io/badge/Go-1.23-00ADD8?style=flat&logo=go&logoColor=white)
![Kafka](https://img.shields.io/badge/Apache_Kafka-4.1-231F20?style=flat&logo=apache-kafka&logoColor=white)
![Redis](https://img.shields.io/badge/Redis-7.2-DC382D?style=flat&logo=redis&logoColor=white)
![Kubernetes](https://img.shields.io/badge/Kubernetes-HPA-326CE5?style=flat&logo=kubernetes&logoColor=white)
![Prometheus](https://img.shields.io/badge/Prometheus-metrics-E6522C?style=flat&logo=prometheus&logoColor=white)

---

## Architecture Highlights

This section explains the system design decisions — not just what it does, but *why* it works the way it does.

### Decoupled Microservice Design

Dispatch separates concerns across two independent binaries:

- **`cmd/dispatch`** — the Ingestion API. A gRPC server that accepts task submissions, performs idempotency checks, persists state to Redis, and publishes serialized tasks (Protobuf) to a Kafka topic. It never executes tasks; its only job is to accept work reliably.
- **`cmd/worker`** — the Worker Pool. Independently scalable consumers that read from Kafka, execute tasks, and stream progress updates back to the dispatch server via a long-lived bidirectional gRPC stream (`ReportProgress`). One stream handles multiple task updates, avoiding connection-per-task overhead.

The boundary between these two services is the Kafka topic `tasks.pending`. Neither service is aware of the other's internal state — they communicate exclusively through the message bus and the gRPC progress stream.

### Delivery and Replay

Kafka consumer groups resume from committed offsets. The worker stops on processing errors so a later commit cannot pass unresolved work. A new consumer session can fetch that work again.

Execution can repeat if a worker exits after doing work but before recording completion or committing its offset. There is no exactly-once execution guarantee. Old Redis lock keys do not suppress replay or acknowledge unfinished work.

Sequential submissions sharing an `idempotency_key` return the original `task_id`. Concurrent admission and publication failure recovery remain open issues.

### Dead-Letter Queue (DLQ) for Poison-Pill Tasks

Some tasks will fail regardless of how many times they are retried — malformed payloads, unavailable external dependencies, logic errors. Retrying these indefinitely blocks worker capacity and pollutes metrics.

Dispatch implements a **DLQ via a second Kafka topic** (`tasks.dead_letter`). Each task carries a `max_retries` counter. On failure:

1. `max_retries` is decremented and a `TASK_STATUS_FAILED` progress update is sent upstream.
2. If `max_retries >= 0`, the worker exits with an error without committing this message. Kafka does not rewind the running reader when a commit is skipped; a new consumer session starts from the group's committed offset.
3. If `max_retries < 0`, the worker attempts to publish the task to `tasks.dead_letter` and treats the message as handled.

Retry accounting across restarts and propagation of DLQ publication failures remain incomplete. The current implementation does not guarantee loss-free quarantine or automatic recovery.

### Offset Management as a Reliability Guarantee

Kafka consumer groups track one committed offset per partition. Committing a later message also acknowledges earlier messages in that partition. When `processMessage` returns an error, the consumer stops instead of fetching later work:

```go
return fmt.Errorf("processing message from %s[%d] at offset %d: %w",
    msg.Topic, msg.Partition, msg.Offset, err)
```

The worker closes its reader and exits nonzero on that error. This prevents a later commit from passing the failed message. Real-Kafka integration tests verify the committed offset and that a new reader in the same group can fetch the unresolved message. Cancellation shuts down without committing interrupted work.

---

## Tech Stack

- **Go 1.23** — statically compiled, single-binary services with structured logging via `log/slog`
- **Apache Kafka 4.1** — durable, ordered message log for task queuing and DLQ
- **Redis 7.2** — task state store
- **gRPC + Protobuf** — typed, bidirectional streaming API between dispatch server and workers
- **Prometheus + Grafana** — metrics collection and dashboarding
- **Docker + Kubernetes** — multi-stage container builds, Deployments, Services, and HPA

---

## Local Quickstart

### Prerequisites

- Docker & Docker Compose
- Go 1.23+
- [`grpcurl`](https://github.com/fullstorydev/grpcurl)

### 1. Start Infrastructure

```bash
docker compose up -d
```

This starts Kafka (KRaft, no ZooKeeper), Redis, Prometheus, and Grafana.

### 2. Start the Dispatch Server

```bash
go run ./cmd/dispatch
```

The gRPC server listens on `:50051`. Metrics are exposed on `:9090`.

### 3. Start a Worker

Open a second terminal:

```bash
go run ./cmd/worker
```

The worker connects to Kafka and opens a gRPC progress stream to the dispatch server. Metrics are exposed on `:9091`.

### 4. Submit a Task

```bash
grpcurl -plaintext \
  -d '{
    "task": {
      "type": "video_transcode",
      "max_retries": 3,
      "priority": "PRIORITY_HIGH",
      "metadata": {"source": "grpcurl"}
    }
  }' \
  localhost:50051 \
  dispatch.v1.DispatchService/SubmitTask
```

Expected response:

```json
{
  "status": "SUBMIT_STATUS_ACCEPTED",
  "taskId": "a3f1c2d4-..."
}
```

To verify idempotency, supply the same `idempotency_key` in a second call — the server returns the original `task_id` without re-enqueuing.

---

## Observability

### Prometheus Metrics

Both services expose a `/metrics` endpoint scraped by Prometheus every 15 seconds. Custom application metrics:

| Metric | Type | Description |
|---|---|---|
| `tasks_submitted_total` | Counter | Task submissions, labeled `status=accepted\|rejected` |
| `tasks_completed_total` | Counter | Successfully executed tasks |
| `tasks_failed_total` | Counter | Failed executions (includes DLQ sends) |
| `task_execution_duration_seconds` | Histogram | End-to-end task execution latency |

### Grafana

Navigate to `http://localhost:3000` (credentials: `admin` / `admin`).

Add a Prometheus data source pointing to `http://prometheus:9090`, then build dashboards using the metrics above. Key panels to track:

- **Throughput**: `rate(tasks_completed_total[1m])`
- **Error rate**: `rate(tasks_failed_total[1m]) / rate(tasks_submitted_total[1m])`
- **P99 execution latency**: `histogram_quantile(0.99, rate(task_execution_duration_seconds_bucket[5m]))`

---

## Kubernetes Deployment

The repository includes production-ready Kubernetes manifests in `k8s/` and multi-stage Dockerfiles in `build/`.

### Build Images

```bash
make docker-build
```

This builds both `dispatch-server:latest` and `dispatch-worker:latest` using `golang:1.23-alpine` builder stages and minimal `alpine:latest` runtime images. Binaries run as a non-root user.

### Apply Manifests

```bash
kubectl apply -f k8s/
```

| Manifest | Resource | Details |
|---|---|---|
| `dispatch-server.yaml` | Deployment + Service | 2 replicas, 100m CPU / 128Mi, exposes gRPC :50051 |
| `dispatch-worker.yaml` | Deployment | 2 replicas, 100m CPU / 128Mi, connects to dispatch-server via cluster DNS |
| `worker-hpa.yaml` | HorizontalPodAutoscaler | Scales workers 2–10 replicas at 70% CPU utilization |

The HPA requests CPU-based worker scaling. Cluster deployment and scaling behavior have not been verified.

---

## Project Layout

```
.
├── build/
│   ├── Dockerfile.dispatch     # Multi-stage build for the dispatch server
│   └── Dockerfile.worker       # Multi-stage build for the worker
├── cmd/
│   ├── dispatch/main.go        # Dispatch server entrypoint
│   └── worker/main.go          # Worker entrypoint
├── gen/                        # Generated Protobuf Go bindings
├── internal/
│   ├── broker/                 # Kafka producer abstraction
│   ├── metrics/                # Prometheus metric definitions
│   ├── server/                 # gRPC DispatchService implementation
│   ├── state/                  # Redis task store
│   └── worker/                 # Kafka consumer, progress reporter
├── k8s/                        # Kubernetes manifests
├── proto/task/v1/task.proto    # Protobuf service definition
├── docker-compose.yml
└── Makefile
```

## Makefile Reference

```bash
make setup          # Install protoc plugins, generate bindings, tidy
make generate       # Compile task.proto → Go bindings
make build          # Build dispatch server binary
make build-worker   # Build worker binary
make test           # Run tests with race detector + coverage report
make lint           # go vet
make docker-build   # Build both Docker images
make clean          # Remove generated artifacts and binaries
```

## Tests

Start Kafka and Redis, verify that both answer protocol requests, then include
the real-Kafka regression tests with the `integration` build tag:

```bash
docker compose up -d kafka redis
docker compose exec -T redis redis-cli ping
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
GOTOOLCHAIN=go1.23.0 go test -tags=integration -count=1 -race ./...
```

Kafka tests use `localhost:9092` by default, accept `KAFKA_BROKER` as an override,
and fail when the broker is unavailable. They create uniquely named topics and
delete those topics at cleanup. Redis tests currently require `localhost:6379`
and skip when Redis is absent. The commands above provide both services.
