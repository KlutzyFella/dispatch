# Dispatch

A task-dispatch system in Go. Clients submit tasks over gRPC; dispatch records
each one in Redis and publishes it to Kafka; workers consume it, report
progress back over a gRPC stream, retry failures within a budget, and send
exhausted tasks to a dead-letter topic.

**Execution is simulated.** A worker's "task" is a two-second timer that fails
10% of the time. Everything around it is real: gRPC, Protobuf, Kafka, Redis,
offset handling, retries, the dead-letter queue, and crash recovery.

What it guarantees, and what it does not:

- **At-least-once delivery**, not exactly-once. A task can run more than once,
  including concurrently on two workers around a consumer-group rebalance.
- `ACCEPTED` means Kafka acknowledged the task and Redis recorded that. A
  dispatch crash before then leaves a retained intent that dispatch republishes
  after restart.
- Durability is bounded by one Kafka broker (replication factor 1) and one Redis
  (AOF, fsync every second). Redis data loss is not handled.

Non-goals: real task execution, scheduling by priority or deadline,
cancellation, a task-status query API, authentication/TLS, multi-broker Kafka,
Redis Cluster.

## Architecture

```text
client ──SubmitTask──▶ dispatch :50051 ──publish (acks=all)──▶ Kafka tasks.pending
                          │  ▲                                       │
          admit / confirm │  │ ReportProgress (one stream per worker)│ consumer group
                          ▼  │                                       ▼
                       Redis :6379  ◀── reserve attempts ──────── worker(s)
                                                                     │ budget exhausted
                                                                     ▼
                                                            Kafka tasks.dead_letter
dispatch metrics :9090      worker-1 metrics :9091      (optional) Prometheus :9093, Grafana :3000
```

**Submitting a task.** Dispatch validates the request and atomically writes
three things in Redis: the task record, a publication intent holding the exact
task bytes, and an entry in a recovery index. It then publishes to Kafka and
waits for the acknowledgement. Only after it has confirmed that in Redis does
it return `ACCEPTED`. If Kafka fails, the task and its intent stay, and a
recovery loop inside dispatch retries them, including after dispatch
restarts.

**Running a task.** A worker reads the next message and skips a task that is
already COMPLETED. Otherwise it atomically reserves an attempt in Redis
(`max_retries + 1` at most), reports RUNNING, waits two seconds, then reports
COMPLETED or FAILED. Each report must come back `recorded=true`. A failure
retries the same message. An exhausted budget publishes the task to
`tasks.dead_letter`. The offset is committed only after one of those outcomes,
so a worker that stops mid-task leaves the message to be delivered again.

## Quickstart

| Requirement | Needed for | Without it |
|---|---|---|
| Docker with Compose v2 | Kafka, Redis, monitoring | nothing starts |
| Go ≥ 1.23.0 | building the binaries | `make up` stops at preflight (no `go`) or at the build (older Go) |
| bash 4+, Linux | `scripts/stack.sh` | only tested on Linux (WSL2 and GitHub's Ubuntu runners) |
| `grpcurl` (optional) | submitting tasks by hand | use any gRPC client; reflection is enabled |
| `curl` (optional) | `make test-stack` | that test cannot run |

Start everything, in the foreground:

```bash
make up
```

`make up` (`scripts/stack.sh`) works through these steps in order:

1. Checks tools, ports and settings, and stops at once if anything is wrong.
2. Starts Redis and Kafka and waits for their healthchecks.
3. Creates `tasks.pending` and `tasks.dead_letter`.
4. Builds the binaries into `.stack/bin`.
5. Starts dispatch and waits for gRPC health `SERVING`.
6. Starts the worker(s) and waits until the consumer group is `Stable` with all
   of them.

It then prints `stack ready in Ns`. From a clean state (volumes deleted, empty
Go build cache) that took 39.7–40.9 s over 3 runs, and 26.6–28.4 s over 2 runs
with a warm build cache. That is one machine: WSL2, Docker 29.8.0, Go 1.23.0,
with the images and Go modules already downloaded. See
[Benchmarks](#benchmarks) for how this was measured.

In a second terminal, submit a task:

```bash
grpcurl -plaintext -d '{"task": {"idempotency_key": "demo-1", "type": "demo", "max_retries": 2}}' localhost:50051 dispatch.v1.DispatchService/SubmitTask
```

```json
{
  "status": "SUBMIT_STATUS_ACCEPTED",
  "taskId": "demo-1"
}
```

There is no status API. Read the task record in Redis instead (`status` 1 =
PENDING, 2 = RUNNING, 3 = COMPLETED, 4 = FAILED):

```bash
docker compose exec redis redis-cli HGETALL task:demo-1
```

After about two seconds it shows `status 3`, `progress 100` and
`publication_state published`. `attempts` is 2 if the first attempt hit the
simulated failure. Submitting the same key again returns the same `taskId`,
and the task is not queued again.

Stop with Ctrl-C in the `make up` terminal. The workers stop first, then
dispatch, then the containers; data stays on the `kafka-data` and `redis-data`
volumes. A second Ctrl-C does not interrupt the shutdown. If any component dies
while running, the whole stack stops and the command exits nonzero, naming the
component. Logs are in `.stack/logs`; `make logs` follows them.

## API

Defined in [`proto/task/v1/task.proto`](proto/task/v1/task.proto).

### SubmitTask

`type` must be non-empty and `max_retries` must not be negative. `payload` and
`metadata` are carried unchanged to Kafka and to the dead-letter topic.
`priority` is stored and logged but does not affect order. The task ID is the
`idempotency_key`, or a generated UUID if it is empty.

| Result | When | What to do |
|---|---|---|
| `SUBMIT_STATUS_ACCEPTED` + `taskId` | Kafka acknowledged the task and Redis confirmed it; also for a repeated key whose task was already accepted | nothing. A repeat never replaces the original task |
| `InvalidArgument` | no task, empty `type`, negative `max_retries` | fix the request |
| `Unavailable` | Kafka did not acknowledge in time, or the same key is still being published | **not a failure.** The task is kept and dispatch keeps retrying. Retry the call with the same `idempotency_key` (the message includes the ID if one was generated) |
| `FailedPrecondition` | the key belongs to a record with no publication evidence, or to another topic | inspect that record; it is never assumed published |
| `Internal` | Redis read or write failed | retry once Redis is healthy |

`Unavailable` does not mean nothing was queued: Kafka may have stored the task
before the failure. Retrying with the same key is always safe.

### ReportProgress (worker → dispatch)

A bidirectional stream; one per worker, carrying updates for many tasks. Each
update gets one `ProgressAck`. Accepted statuses are RUNNING, FAILED and
COMPLETED, with progress 0–100; COMPLETED requires 100. `recorded=false` means
the update changed nothing: unknown task, invalid status or progress, the task
is already COMPLETED, or Redis failed. A repeated COMPLETED/100 is recorded and
changes nothing. The worker treats `recorded=false` as fatal, except when it
re-reads the task and finds it already COMPLETED by another delivery, which it
counts as handled. Each update must be acknowledged within 10 s.

## Failure behavior

| Situation | Behavior |
|---|---|
| Kafka refuses or loses a publish | task stays pending in Redis; dispatch republishes it (1 s back-off) |
| dispatch killed mid-publish | on restart, an unexpired claim waits out its 15 s lease, then the intent is republished. A crash after Kafka's acknowledgement can publish the task twice |
| worker killed mid-task | the uncommitted message is redelivered; the interrupted attempt still counts toward the budget |
| task fails `max_retries + 1` times | published to `tasks.dead_letter` with its original payload and committed. A DLQ write failure stops the worker without committing |
| same message delivered twice | a COMPLETED task is skipped without a new attempt. Two concurrent copies share one attempt budget, so a task can end COMPLETED **and** in the DLQ (measured at 4.95% when every task was duplicated) |
| undecodable Kafka message | the worker stops without committing; nothing quarantines it |
| consumer-group rebalance | fetch errors are retried for up to 60 s before the worker exits |
| Redis unreachable | dispatch exits within seconds (its recovery loop needs Redis); a worker exits at its next message |
| dispatch unreachable from a worker | the worker exits; progress streams are not reconnected |

Each row is covered by an integration test against real Kafka and Redis
(`make test-integration`), with two exceptions:

- The rebalance retry has a unit test, and 12 real occurrences were seen in
  the concurrent-duplicate soak.
- A worker losing Redis is untested; that row is read from the code.

## Configuration

Dispatch (`cmd/dispatch`):

| Variable | Default | Meaning |
|---|---|---|
| `PORT` | `50051` | gRPC listen port |
| `METRICS_PORT` | `9090` | Prometheus `/metrics` port; must bind at startup |
| `KAFKA_BROKER` | `localhost:9092` | must answer a metadata request within 10 s at startup |
| `KAFKA_TOPIC` | `tasks.pending` | topic tasks are published to |
| `REDIS_ADDR` | `localhost:6379` | must answer PING at startup |

Worker (`cmd/worker`):

| Variable | Default | Meaning |
|---|---|---|
| `GRPC_SERVER_ADDR` | `localhost:50051` | dispatch address; must be reachable at startup |
| `METRICS_PORT` | `9091` | Prometheus `/metrics` port |
| `KAFKA_BROKER` | `localhost:9092` | |
| `KAFKA_TOPIC` | `tasks.pending` | must match dispatch, and must not be `tasks.dead_letter` |
| `KAFKA_GROUP_ID` | `dispatch-workers` | consumer group |
| `REDIS_ADDR` | `localhost:6379` | |

The dead-letter topic is fixed at `tasks.dead_letter`. `make up` additionally
reads:

| Variable | Default | Meaning |
|---|---|---|
| `WORKERS` | `1` | number of workers; `0` runs dispatch alone |
| `KAFKA_PARTITIONS` | `1` | partitions when topics are first created; workers beyond this get no work |
| `DISPATCH_METRICS_PORT` | `9090` | dispatch metrics port |
| `WORKER_METRICS_PORT` | `9091` | worker *i* uses this + 10·(i−1): 9091, 9101, … |
| `READY_TIMEOUT` / `STOP_TIMEOUT` | `120` / `15` | seconds to wait for readiness / for each component to stop |
| `KEEP_INFRA` | `0` | `1` leaves Kafka and Redis running after shutdown |

Redis holds `task:<id>` (the task record), `publication:<id>` (the retained
intent until confirmed) and `outbox:<topic>` (the recovery index). Records are
never expired.

## Monitoring (optional)

```bash
docker compose up -d prometheus grafana
```

Prometheus (http://localhost:9093) scrapes dispatch on :9090 and the first
worker on :9091. Additional workers are not scraped. Grafana
(http://localhost:3000, `admin`/`admin`) comes up with Prometheus as its data
source. Both processes export all four metrics, so filter by `job`:

| Query | Shows |
|---|---|
| `tasks_submitted_total{job="dispatch-server"}` | SubmitTask responses by `status`: `accepted`, `pending` (Unavailable), `rejected`, `error`. Retries count again |
| `rate(tasks_completed_total{job="dispatch-worker"}[5m])` | completed tasks per second |
| `rate(tasks_failed_total{job="dispatch-worker"}[5m]) / (rate(tasks_failed_total{job="dispatch-worker"}[5m]) + rate(tasks_completed_total{job="dispatch-worker"}[5m]))` | share of finished attempts that failed |
| `histogram_quantile(0.99, sum by (le) (rate(task_execution_duration_seconds_bucket{job="dispatch-worker"}[5m])))` | p99 attempt duration: about 2 s, the simulated work |

## Containers and Kubernetes

```bash
make docker-build
```

This builds `dispatch-server:latest` and `dispatch-worker:latest` (Go 1.23
Alpine builder, Alpine runtime, non-root `appuser`). Both images were run
against the Compose Kafka and Redis: dispatch reached gRPC health `SERVING`
and the worker joined its group. The manifests in `k8s/` (two Deployments, a
Service and a CPU-based HPA) have never been applied to a cluster, and they
expect existing `redis` and `kafka` Services.

## Benchmarks

Measured on one laptop with the load generator on the same machine. Method,
environment and caveats are in [docs/BENCHMARKS.md](docs/BENCHMARKS.md).

- `SubmitTask` held p95 ≤ 50 ms at 6,000 req/s in 7 of 7 runs (p95
  14.0–16.4 ms) and at 7,000 req/s in 3 of 3. Above that the limit is noisy.
  Across 20.2 million requests every response was `ACCEPTED`, and every
  accepted task was verified in Kafka.
- 20,000 tasks through 32 workers: none lost (19,769 completed, 231 in the
  DLQ). Delivering every message a second time started no new attempts. With
  every task executed by two workers at once, none was lost and 924 duplicates
  were absorbed.
- Worker throughput is not measured: it would only reflect the two-second
  timer.

Startup times come from `scripts/measure-startup.sh --delete-volumes`, which
deletes this project's Kafka and Redis data on every run.

## Tests and CI

```bash
make lint                                  # gofmt, go vet (both build tags), go mod verify, go mod tidy -diff
make test                                  # unit tier, race detector, needs nothing running
docker compose up -d --wait kafka redis
make test-integration                      # every test against real Kafka and Redis
make test-stack                            # drives `make up` end to end (needs curl and grpcurl)
make lint-scripts                          # ShellCheck + actionlint (needs Docker and actionlint v1.7.7)
```

Integration tests fail, not skip, when Kafka or Redis is missing.
`make test-integration` also fails if any test is skipped, and prints the full
output of anything that failed. Tests default to `localhost:9092` and
`localhost:6379` (override with `KAFKA_BROKER` / `REDIS_ADDR`) and create and
delete their own uniquely named Kafka topics.

GitHub Actions ([`.github/workflows/ci.yml`](.github/workflows/ci.yml)) runs
three jobs on Go 1.23.0, the `go.mod` minimum:

1. lint, unit tests and script lint;
2. the integration tier against Kafka and Redis from this repository's
   `docker-compose.yml`;
3. `make test-stack`.

## Layout

```text
cmd/dispatch/        gRPC server: admission, publication recovery, progress
cmd/worker/          Kafka consumer, progress reporter, DLQ publisher
cmd/healthcheck/     gRPC health probe used by scripts/stack.sh
internal/server/     SubmitTask, ReportProgress, publication recovery loop
internal/state/      Redis store and its Lua scripts
internal/worker/     consumer loop, retry budget, progress client
internal/broker/     Kafka producer and startup readiness probe
internal/metrics/    Prometheus metrics and HTTP server
internal/testutil/   integration fixtures (real Kafka/Redis, subprocesses)
proto/, gen/         task.proto and the generated Go code
scripts/             stack.sh, test-stack.sh, measure-startup.sh, bench/, ci/
docs/BENCHMARKS.md   load tests and soaks
build/, k8s/         Dockerfiles; unverified Kubernetes manifests
docker-compose.yml   Kafka, Redis, optional Prometheus and Grafana
```

Regenerating `gen/` needs protoc 3.21.12 and the plugin versions installed by
`make install-tools`, then `make generate`. The generated code is committed,
so building and testing do not need protoc.
