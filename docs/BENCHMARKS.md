# Benchmarks

Two measurements, both run against the real local stack (Redis, Kafka, and the
compiled dispatch and worker binaries started by `scripts/stack.sh`):

1. **Ingestion**: how fast `SubmitTask` can admit tasks while holding a
   latency target, with every accepted task checked against Kafka.
2. **Zero-loss soak**: thousands of tasks through dispatch and several
   workers, each one accounted for at the end.

Worker throughput is **not** benchmarked. A worker's "task" is a fixed
two-second timer with a 10% simulated failure rate, so any tasks-per-second
figure would only restate that constant (workers × 0.5/s, capped by
partitions).

## Environment

Every number below comes from this one machine. None of it was measured on
anything else.

| | |
|---|---|
| CPU | 12th Gen Intel Core i7-12650H, 16 threads |
| Memory | 7 GB available to the WSL2 VM |
| OS | Ubuntu on WSL2 (kernel 6.6.87.2-microsoft-standard-WSL2) |
| Docker | Engine 29.8.0, Compose 2.40.3 |
| Kafka | apache/kafka:4.1.2, one KRaft broker, replication factor 1, one partition for ingestion |
| Redis | redis:7.2 (7.2.13), AOF `everysec`, default RDB save points still enabled |
| Go | binaries built with the machine's Go 1.26.1, no race detector |
| Load generator | ghz built from v0.121.0, **on the same machine** |
| Commit | ingestion runs at `19ec8f3`, soak at `a49de56` (recorded by the harness); harness in `scripts/bench` |

The load generator shares the 16 threads with Kafka, Redis and dispatch. At
the highest rates ghz itself used about 2.5 cores and the host was about 60%
busy. A co-located client competes with the system, so these results
understate what the system could do with the client on another machine; they
are lower bounds for this hardware, not limits.

## 1. Ingestion (`SubmitTask`)

### What one request does

Each request is a Redis read, an atomic Lua admission (task record, retained
publication intent, recovery-index entry), a Kafka write that waits for the
broker's acknowledgement (`RequireAll`), then an atomic Lua confirmation in
Redis. `ACCEPTED` is returned only after all of that. Requests carry no
idempotency key, so dispatch assigns each a new ID and none takes the cheap
duplicate path.

### Method

`scripts/bench/ingest_sweep.py`:

- Each repetition starts from deleted Kafka/Redis volumes and runs the stack
  with `WORKERS=0`, so only ingestion runs. A 15 s warm-up at 100 req/s
  precedes the steps.
- Each step is a fixed request rate for 60 s (`ghz --rps`, `--count-errors` so
  non-OK responses count toward latency and rate, `--duration-stop wait`,
  5 s client timeout, 4 connections).
- **A step is "sustained" only if** the achieved rate is within 3% of the
  target, **p95 ≤ 50 ms**, and **fewer than 0.1% of responses are not OK**.
  `Unavailable` counts as not OK. The target was fixed before any formal run.
- **Every step is verified.** After the step the harness waits until no
  publication intent is pending, then requires the Kafka log-end-offset
  delta (summed over partitions) to equal the number of new Redis task
  records and to be at least the number of OK responses.
- After verification Redis memory is recorded and Redis is flushed, so each
  step starts from an empty store. Task records never expire, and one rep
  would otherwise accumulate millions of them.
- CPU comes from kernel counters over the step: container cgroups for
  Kafka/Redis, `/proc/<pid>/task/<pid>` for Redis's main thread,
  `/proc/<pid>` for dispatch, reaped-child rusage for ghz, `/proc/stat` for the
  host.
- p99 is reported only when a step has at least 10,000 samples (every step
  here does).
- Runs: three ascending repetitions over 2,000-12,000 req/s, one descending
  repetition as a thermal control, then three more ascending repetitions over
  6,000-9,000 req/s. That is 44 steps in all.

### Results

Medians across repetitions, with the range in parentheses. CPU is in cores.

| target req/s | sustained | achieved | p50 ms | p95 ms | p99 ms | non-OK | verified | Redis main thread | dispatch | ghz | Kafka | host busy |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 2,000 | 4/4 | 2,000 | 8.6 (8.3–8.6) | 13.5 (13.2–13.5) | 14.5 (14.2–14.7) | 0 / 479,990 | yes | 0.30 | 1.15 | 0.78 | 0.23 | 21% |
| 4,000 | 4/4 | 3,999 | 8.5 (8.3–9.6) | 13.3 (13.2–17.8) | 14.4 (14.2–31.1) | 0 / 959,975 | yes | 0.47 | 1.80 | 1.17 | 0.15 | 34% |
| **6,000** | **7/7** | 5,999 | 9.2 (9.1–10.4) | **14.3 (14.0–16.4)** | 18.8 (15.5–24.9) | 0 / 2,519,784 | yes | 0.63 | 2.54 | 1.63 | 0.18 | 45% |
| 7,000 | 3/3 | 6,999 | 10.8 (10.7–12.2) | 18.5 (16.6–29.7) | 41.1 (24.0–73.9) | 0 / 1,258,217 | yes | 0.74 | 3.11 | 2.02 | 0.21 | 55% |
| 8,000 | 5/7 | 7,998 | 10.7 (10.1–14.1) | 17.8 (15.7–98.2) | 41.9 (20.1–145.9) | 0 / 3,359,876 | yes | 0.75 | 3.18 | 2.10 | 0.20 | 56% |
| 9,000 | 5/7 | 8,998 | 11.4 (10.9–12.3) | 33.9 (17.1–86.7) | 97.9 (22.7–112.3) | 0 / 3,778,654 | yes | 0.79 | 3.32 | 2.28 | 0.20 | 59% |
| 10,000 | 1/4 | 9,952 | 47.2 (12.4–87.5) | 120.3 (24.5–178.2) | 145.6 (77.2–295.7) | 0 / 2,352,035 | yes | 0.83 | 3.31 | 2.40 | 0.22 | 60% |
| 11,000 | 0/4 | 10,997 | 13.2 (12.6–14.6) | 69.4 (64.0–95.4) | 110.6 (101.7–113.5) | 0 / 2,638,501 | yes | 0.85 | 3.44 | 2.56 | 0.20 | 62% |
| 12,000 | 0/4 | 11,928 | 23.1 (11.4–80.4) | 128.9 (76.8–139.4) | 158.7 (110.9–170.2) | 0 / 2,823,589 | yes | 0.86 | 3.44 | 2.60 | 0.21 | 62% |

All 44 steps passed verification. **20,170,621 requests were sent, all returned
`ACCEPTED`, and every accepted task was in Kafka exactly once.** No
`Unavailable`, timeout or other error occurred at any rate.

### Reading it

- **Sustained: 7,000 req/s held the target in every run (3/3), and 6,000
  req/s in every run (7/7) with p95 14.0-16.4 ms.** The conservative claim is
  "≥ 6,000 req/s at p95 ≤ 50 ms"; 7,000 has fewer runs behind it.
- **Above about 7,000 req/s the boundary is noisy, not a clean knee.** 8,000
  and 9,000 held in 5 of 7 runs; a failing run had p95 around 60-100 ms while
  the next rate up sometimes passed. The descending control fails at 10,000 and
  passes at 9,000, like the ascending runs. Failures do not cluster late in a
  sweep, so the control shows no thermal drift.
- **The latency floor is configuration, not scalability.** The p50 of about
  9 ms is mostly the producer's 10 ms batch window (`publishBatchTimeout` in
  `internal/broker/producer.go`). The regression test
  `TestKafkaProducer_PublishDoesNotWaitForBatchTimer` is the ablation: with
  kafka-go's default 1 s window the same publish took about 1.003 s, and with
  10 ms about 12 ms. In kafka-go a zero `BatchTimeout` means the 1 s default,
  not "send immediately".
- **Where the time goes at the boundary.** At 9,000-12,000 req/s Redis's
  single command thread was at 0.79-0.86 of a core, dispatch at about 3.3-3.4
  cores, ghz at about 2.3-2.6, Kafka at about 0.2, and the host 59-62% busy.
  Redis's main thread is the only single-threaded component near saturation,
  so it is the most likely next bottleneck. That is an inference from
  utilization; no experiment isolated it.
- **The intermittent p95/p99 spikes above about 7,000 req/s are unexplained.**
  Ruled out: Redis fork pauses (7-9 forks per step in passing and failing
  steps alike, 1.3-3.5 ms each) and AOF fsync stalls over 2 s (none logged).
  Not ruled out: Go GC in dispatch, WSL2 disk I/O from 6-8 AOF rewrites per
  minute, and contention with the co-located load generator.

### Caveats

- Single broker, replication factor 1, one partition. With replicas,
  `RequireAll` would wait for more acknowledgements.
- Redis is the shipped configuration (AOF everysec). Dispatch writes two INFO
  log lines per request to a file; that cost is included.
- Fixed-rate ghz is not a fully open-loop generator. The 3% achieved-rate gate
  is what keeps latency at saturated steps from being under-reported.
- Records accumulate without expiry in real use. Here each step started from an
  empty Redis, so these numbers do not show the effect of a large keyspace.

### Reproduce

```bash
go install github.com/bojand/ghz/cmd/ghz@v0.121.0
python3 scripts/bench/ingest_sweep.py --delete-volumes --rates 2000,4000,6000,8000,9000,10000,11000,12000 --reps 3 --out sweep.jsonl
python3 scripts/bench/summarize.py sweep.jsonl
```

`--delete-volumes` is required because every repetition deletes this
project's Kafka and Redis data.

## 2. Zero-loss soak

`scripts/bench/soak.py` submits tasks through the whole stack and accounts
for every one of them.

### Method

- Deleted volumes, then `stack.sh` with **8 workers on 8 partitions**. The
  stack reports ready only once the consumer group is Stable with exactly 8
  members.
- **2,400 tasks** submitted through ghz at 50 req/s, `max_retries=1` (two
  attempts), no idempotency key. Workers keep the production 10% simulated
  failure rate, so some tasks retry and about 1% exhaust both attempts.
- Wait until every partition's consumer lag is 0 and every task is terminal:
  COMPLETED, or FAILED with both attempts used.
- **Lost** means admitted (has a Redis record) but neither COMPLETED nor
  present in `tasks.dead_letter`. The run also requires: every task
  published, the pending topic holding exactly one record per task, no task
  over its attempt budget, and no task both completed and quarantined.

### Result

| | |
|---|---|
| submitted / `ACCEPTED` | 2,400 / 2,400 |
| admitted (Redis records) / pending-topic records | 2,400 / 2,400 |
| COMPLETED | 2,380 |
| in the DLQ (records / distinct tasks) | 20 / 20 |
| **lost** | **0** |
| completed and also quarantined | 0 |
| over the attempt budget | 0 |
| attempts used: 1 / 2 | 2,134 / 266 |
| final consumer lag | 0 |
| time from first submission to all terminal | 798 s |

The attempt counts fit the simulated failure rate: 266 tasks needed a second
attempt (about 240 expected at 10%) and 20 exhausted both (about 24 expected
at 1%). The 798 s is the two-second simulated work spread over 8 workers, not
a throughput result.

**The first soak attempt failed, and it found a bug.** The stack reported
ready while the group was still rebalancing. A message already being
executed was handed to a newly joined worker. The first copy completed; the
second copy's simulated attempt failed, and its FAILED update was correctly
refused by the completed-task guard. The worker treated that refusal as fatal,
so the stack stopped five seconds in. Nothing was lost. Two fixes followed, each
with a regression shown red before and green after:

- the worker now re-reads a task whose update was refused, and treats a
  duplicate that another delivery already COMPLETED as handled
  (`TestConsumer_DuplicateFinishedElsewhereIsHandled`);
- `stack.sh` waits for a Stable group with exactly N members.

The passing run above had no duplicate deliveries, because the readiness fix
removed the startup rebalance. The duplicate-handling fix is covered by its
regression test, not by this soak.

### Not covered

No process or broker was killed during the soak; the supervisor stops the whole
stack when a component dies, so this soak cannot exercise that. Crash and restart
recovery are covered by the integration tests instead (SIGKILL of dispatch
before and after the Kafka acknowledgement, consumer replay after restart,
live rebalance handoff). Single broker, no replication; Redis data loss is not
tested.

### Reproduce

```bash
python3 scripts/bench/soak.py --delete-volumes --tasks 2400 --workers 8 --rate 50 --out soak.json
```
