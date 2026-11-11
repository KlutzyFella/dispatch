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

Tasks travel through the Kafka topic `tasks.pending`. Both services use Redis task records; workers report progress over the gRPC stream and reserve execution attempts in Redis.

Both services honor `KAFKA_TOPIC` for the pending-work topic; its default is
`tasks.pending`. Set the same value on dispatch and worker. The DLQ remains
`tasks.dead_letter`; the worker exits at startup if `KAFKA_TOPIC` names it.

Both services require Redis PING and a Kafka metadata response during startup.
The Kafka probe has a ten-second deadline. Dispatch opens its gRPC listener only
after that check, and the worker checks before consuming. The gRPC health status
is a startup snapshot, not continuous dependency monitoring. This probe does not
create topics or verify publishing permissions; topic provisioning remains a
startup-orchestration requirement.

### Delivery and Replay

Kafka consumer groups resume from committed offsets. The worker stops on processing errors so a later commit cannot pass unresolved work. A new consumer session can fetch that work again.

Execution can repeat if a worker exits after doing work but before recording completion. Replayed tasks already recorded COMPLETED are acknowledged without another execution. There is no exactly-once execution guarantee. Old Redis lock keys do not suppress replay or acknowledge unfinished work.

Task creation and a retained publication intent are atomic. `ACCEPTED` requires
both a Kafka acknowledgment and Redis confirmation of publication. A duplicate
whose original publication is pending returns gRPC `Unavailable`; it does not
publish a replacement payload. Retry with the same task ID/idempotency key.
Publication errors and cancelled requests retain the original intent instead of
rolling back the canonical task. A legacy record without publication evidence
returns `FailedPrecondition` and needs inspection; it is not assumed published.

Dispatch runs a supervised publication-recovery loop. Pending intents retain the
original protobuf payload and topic; a topic-scoped Redis index schedules retries.
Redis time and owner tokens fence claims and confirmations. A live owner has a
15-second lease; publication attempts have a ten-second timeout. Failed writes
are deferred by one second, and idle recovery polls every 200 ms. These are
internal policies, not performance measurements.

After a dispatch crash, expired intents are retried and confirmed only after a
Kafka ACK. A crash or ambiguous timeout after Kafka receives a record can cause
duplicate delivery. `Unavailable` therefore does not guarantee that no work was
queued or executed; always retry with the same idempotency key. Once published,
a duplicate returns the original ID and does not replace the canonical payload.
Changing `KAFKA_TOPIC` does not reroute old pending intents. Redis should be
isolated per Kafka environment; the intent binds a topic, not a Kafka cluster ID.

Recovery requires retained Redis data. The current Compose file does not establish
crash-safe Redis persistence, and no guarantee against Redis data loss is claimed.
Corrupt intent/index data or recovery storage failures stop dispatch with an
error rather than leave a healthy-looking service with a dead recovery loop.

### Dead-Letter Queue (DLQ) for Poison-Pill Tasks

The worker simulates task failures and quarantines exhausted valid task messages. Undecodable protobuf stops the worker without committing; it is not automatically dead-lettered and needs inspection.

Dispatch uses a second Kafka topic, `tasks.dead_letter`, for exhausted tasks. `max_retries=N` permits at most **N+1 started attempts per task ID**:

1. Before RUNNING, a Redis Lua script atomically reserves a start in the task hash's `attempts` field. Missing fields begin at zero; missing task records fail execution rather than being recreated.
2. A simulated task failure reports FAILED and retries the same message before the reader advances. The original `max_retries` value is not decremented or republished.
3. An exhausted budget publishes the original task to the DLQ. Only an acknowledged publication counts as handled. A missing publisher or rejected write stops the worker without committing the source message. A restart with an exhausted budget retries quarantine rather than executing another attempt.

An interrupted start counts toward the budget, including an interruption before work begins. Attempt accounting survives worker restarts while the Redis record is retained. Redis persistence and retention bound this guarantee; this is not a guarantee against Redis data loss. DLQ records may be duplicated if a worker exits after publication but before committing the source offset.

### Offset Management as a Reliability Guarantee

Kafka consumer groups track one committed offset per partition. Committing a later message also acknowledges earlier messages in that partition. When `processMessage` returns an error, the consumer stops instead of fetching later work:

```go
return fmt.Errorf("processing message from %s[%d] at offset %d: %w",
    msg.Topic, msg.Partition, msg.Offset, err)
```

The worker closes its reader and exits nonzero on that error. This prevents a later commit from passing the failed message. Real-Kafka integration tests verify the committed offset and that a new reader in the same group can fetch the unresolved message. Cancellation shuts down without committing interrupted work.

Progress updates require `recorded=true`, including FAILED updates for exhausted replays. A successful gRPC exchange with `recorded=false` means the state update was rejected: the worker stops with an error and leaves the source message uncommitted. Transport errors also stop consumption. Each update must be acknowledged within ten seconds; a silent dispatch peer aborts the stream and stops the worker rather than leaving it blocked. The stream is not reconnected. A started attempt remains counted; rejecting RUNNING prevents work, while rejecting COMPLETED can occur after work has already happened.

Worker progress accepts RUNNING, FAILED, and COMPLETED only, with percentages
in 0–100; COMPLETED requires exactly 100. PENDING is assigned by submission,
and cancellation is not implemented. Unsupported statuses are rejected without
changing the task record.

Redis checks task existence and writes progress atomically. If deletion wins before the update, the update is rejected without recreating a partial task record. Progress updates cannot reverse COMPLETED; repeating COMPLETED/100 leaves the record and completion time unchanged. FAILED may return to RUNNING for a retry. Intermediate updates are not attempt-fenced.

---

## Tech Stack

- **Go 1.23** — statically compiled, single-binary services with structured logging via `log/slog`
- **Apache Kafka 4.1** — durable, ordered message log for task queuing and DLQ
- **Redis 7.2** — task state and atomic started-attempt accounting
- **gRPC + Protobuf** — typed, bidirectional streaming API between dispatch server and workers
- **Prometheus + Grafana** — metrics collection and dashboarding
- **Docker + Kubernetes** — multi-stage container builds, Deployments, Services, and HPA

---

## Local Quickstart

### Prerequisites

- Docker with the Compose v2 plugin
- Go 1.23.0 or newer
- [`grpcurl`](https://github.com/fullstorydev/grpcurl), only for submitting tasks by hand

### 1. Start the stack

```bash
make up
```

This runs `scripts/stack.sh` in the foreground. It checks tools and ports
first, starts Redis and Kafka and waits for their healthchecks, creates
`tasks.pending` and `tasks.dead_letter`, builds the binaries into
`.stack/bin`, starts dispatch and waits for gRPC health, then starts a worker
and waits until it has joined its consumer group. It prints `stack ready` when
all of that holds. If any component exits or a container stops, the whole stack
is stopped and the command exits 1, naming the cause.

Measured from a clean state on one machine (WSL2, Docker 29.8.0, Go 1.26.1;
images and Go modules already downloaded): 42-45 s with an empty Go build
cache, 31-32 s with a warm one. Most of the time goes to the Go build, Kafka's
startup, and the worker's first consumer-group join on a new broker.
`scripts/measure-startup.sh --delete-volumes` reproduces the measurement; it
deletes this project's Kafka and Redis data on every run.

`WORKERS=N` starts N workers (default 1; 0 runs dispatch alone), and
`KAFKA_PARTITIONS` sets the topic partition count used when the topics are
first created. Workers beyond the partition count receive no work. Worker *i*
serves metrics on `WORKER_METRICS_PORT + 10*(i-1)`, i.e. 9091, 9101, ...

Logs are in `.stack/logs`; follow them from a second terminal with
`make logs`. Prometheus and Grafana are not started by `make up`.

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

### Shutdown

Press Ctrl-C (or send SIGTERM) to the `make up` terminal. The worker stops
first, then dispatch, then the Redis and Kafka containers; their data stays on
the `kafka-data` and `redis-data` volumes. A second Ctrl-C does not interrupt
the shutdown. `scripts/stack.sh` exits 0 after a requested stop and 1 after a
failure; through `make`, those appear as make's own `Interrupt` (130) and
`Error 1` (exit 2). `scripts/test-stack.sh` checks these paths, and component death,
against real containers.

Within dispatch, on SIGINT or SIGTERM, dispatch gives existing gRPC calls five seconds to drain.
It then logs `forcing gRPC shutdown` and closes remaining connections, including
workers' open progress streams. Closed streams drain without waiting for the
deadline. The integration regression executes the actual `main` in a compiled
subprocess, checks both paths, and verifies that its gRPC and metrics ports are
released. This deadline bounds gRPC draining, not every dependency's cleanup.

---

## Observability

### Prometheus Metrics

Both services expose a `/metrics` endpoint scraped by Prometheus every 15 seconds. Custom application metrics:

| Metric | Type | Description |
|---|---|---|
| `tasks_submitted_total` | Counter | SubmitTask responses (retries count again), labeled `status=accepted\|pending\|rejected\|error`; `pending` is Unavailable with the intent retained for recovery |
| `tasks_completed_total` | Counter | Successfully executed tasks |
| `tasks_failed_total` | Counter | Failed simulated execution attempts |
| `task_execution_duration_seconds` | Histogram | Per-attempt simulated execution duration; excludes queue wait |

Both services require their metrics listener to bind during startup. An invalid
or occupied metrics address exits with status 1 and identifies the address in
the error; choose an unused `METRICS_PORT` or stop the process holding that port.
Errors after the metrics HTTP server has started are currently logged rather
than propagated to the main service.

### Grafana

Navigate to `http://localhost:3000` (credentials: `admin` / `admin`).

Add a Prometheus data source pointing to `http://prometheus:9090`, then build dashboards using the metrics above. Key panels to track:

- **Throughput**: `rate(tasks_completed_total[1m])`
- **Error rate**: `rate(tasks_failed_total[1m]) / rate(tasks_submitted_total[1m])`
- **P99 execution latency**: `histogram_quantile(0.99, rate(task_execution_duration_seconds_bucket[5m]))`

---

## Kubernetes Deployment

The repository includes Kubernetes manifests in `k8s/` and multi-stage Dockerfiles in `build/`. The manifests have not been applied to a cluster. They do not deploy Redis or Kafka; they expect `redis` and `kafka` Services in the `default` namespace.

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
delete those topics at cleanup. Integration-tagged Redis regressions default to
`localhost:6379`, accept `REDIS_ADDR`, and fail if Redis is absent. Older Redis
tests in the default suite still use `localhost:6379` and skip if it is absent.
The commands above provide both services.
