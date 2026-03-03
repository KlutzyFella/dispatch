# Dispatch

**A high-throughput, fault-tolerant distributed task execution engine built in Go.**

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

### At-Least-Once Delivery → Exactly-Once Execution

Kafka provides **at-least-once delivery** semantics by design: in failure scenarios (worker crash, network partition), a message may be redelivered. This is generally desirable for durability, but it means the same task could be dispatched to multiple workers simultaneously.

Dispatch resolves this with a **Redis distributed lock** (`SETNX` with TTL) acquired before any task begins processing:

```
AcquireLock(ctx, taskID, 5 * time.Minute)
```

If a worker cannot acquire the lock, it skips the message and commits the offset — the task is already owned by another worker. This converts at-least-once delivery into **effectively-once execution** without requiring Kafka's idempotent producer or exactly-once transaction protocol.

The dispatch server also enforces **idempotent task submission**: clients may supply an `idempotency_key`, and the server returns the original `task_id` for any duplicate submission rather than enqueuing a second copy.

### Dead-Letter Queue (DLQ) for Poison-Pill Tasks

Some tasks will fail regardless of how many times they are retried — malformed payloads, unavailable external dependencies, logic errors. Retrying these indefinitely blocks worker capacity and pollutes metrics.

Dispatch implements a **DLQ via a second Kafka topic** (`tasks.dead_letter`). Each task carries a `max_retries` counter. On failure:

1. `max_retries` is decremented and a `TASK_STATUS_FAILED` progress update is sent upstream.
2. If `max_retries >= 0`, the worker returns an error — the Kafka **offset is not committed**, so the broker redelivers the message on the next poll.
3. If `max_retries < 0`, all retries are exhausted. The task is published to `tasks.dead_letter` and the offset **is** committed — the task is removed from the active queue permanently and available for inspection or replay.

This ensures poison-pill tasks are quarantined without data loss and without blocking healthy work.

### Offset Management as a Reliability Guarantee

The consumer's commit strategy is the linchpin of the retry mechanism. Offsets are committed **only after a message is fully and successfully handled** — either completed, or dead-lettered. If processing fails and retries remain, the consumer deliberately skips the commit:

```go
// Do not commit: the message will be redelivered.
continue
```

This is intentional. It means the system's retry behavior is enforced at the transport layer (Kafka), not in application memory. Worker crashes, OOM kills, and ungraceful shutdowns are handled for free — the message simply reappears on the next poll.

---

## Tech Stack

- **Go 1.23** — statically compiled, single-binary services with structured logging via `log/slog`
- **Apache Kafka 4.1** — durable, ordered message log for task queuing and DLQ
- **Redis 7.2** — task state store and distributed lock backend (`SETNX` / `SET NX PX`)
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

Workers autoscale horizontally under load. Because execution is gated by Redis distributed locks, adding replicas increases throughput linearly without the risk of duplicate task execution.

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
│   ├── state/                  # Redis task store + distributed lock
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
