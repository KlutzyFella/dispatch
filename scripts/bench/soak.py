#!/usr/bin/env python3
"""Zero-loss soak: submit N tasks through the full stack and account for each.

From deleted volumes, starts the stack with WORKERS workers on as many
partitions, submits N tasks (no idempotency key, max_retries=1) through ghz at
a fixed rate, then waits until every partition's consumer lag is zero and every
task is terminal. Workers keep their production 10% simulated failure rate,
so some tasks retry and about 1% exhaust both attempts and go to the DLQ.

A task is LOST if it was admitted (has a Redis record) but is neither
COMPLETED nor present in tasks.dead_letter. The soak also reports DLQ
duplicates, retry-budget violations, and tasks that both completed and were
quarantined. Observed failure rates come with 95% Wilson intervals.

Optional modes:
  --replay           After the soak, stop the stack, rewind the worker group to
                     the earliest offsets, restart, and let every message be
                     delivered a second time. Sequential redelivery: requires
                     zero new executions and unchanged task outcomes; the DLQ
                     is expected to hold one extra record per quarantined task.
  --duplicate-group N  Run N more workers in a second consumer group on the
                     same topic for the whole soak, so every task is executed
                     twice concurrently. This is what exercises the refusal
                     path for a duplicate that another delivery completed.

Exits 1 if any task is lost or any invariant fails.

Not covered: process or broker crashes during the soak (the supervisor stops
the whole stack if a component dies), multi-broker Kafka, Redis data loss.

DESTRUCTIVE: deletes this project's Kafka/Redis volumes first.
Usage: scripts/bench/soak.py --delete-volumes [--tasks 2400] [--workers 8] [--rate 50]
       [--replay] [--duplicate-group N]
"""
import argparse
import json
import math
import os
import signal
import subprocess
import sys
import time
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from ingest_sweep import ROOT, Stack, compose, redis, wait_quiescent  # noqa: E402

PENDING, DLQ, GROUP, DUP_GROUP = "tasks.pending", "tasks.dead_letter", "dispatch-workers", "dispatch-workers-dup"
COMPLETED, FAILED = 3, 4
MAX_RETRIES = 1
DUP_METRICS_BASE = 9500
LOGS = ROOT / ".stack/logs"

ALL_TASKS = """
local out = {}
for _, key in ipairs(redis.call('KEYS', 'task:*')) do
  local v = redis.call('HMGET', key, 'id', 'status', 'attempts', 'publication_state')
  table.insert(out, (v[1] or '') .. ' ' .. (v[2] or '') .. ' ' .. (v[3] or '0') .. ' ' .. (v[4] or ''))
end
return out
"""


def kafka(tool: str, *args: str) -> str:
    return compose("exec", "-T", "kafka", f"/opt/kafka/bin/{tool}", "--bootstrap-server", "localhost:9092", *args)


def total_lag(group: str) -> int | None:
    out = kafka("kafka-consumer-groups.sh", "--describe", "--group", group)
    lags = [line.split()[5] for line in out.splitlines() if line.startswith(group + " ")]
    if not lags or any(lag == "-" for lag in lags):
        return None
    return sum(int(lag) for lag in lags)


def group_stable(group: str, members: int) -> bool:
    out = kafka("kafka-consumer-groups.sh", "--describe", "--group", group, "--state")
    return any(line.split()[:1] == [group] and line.split()[-2:] == ["Stable", str(members)]
               for line in out.splitlines() if line.strip())


def tasks() -> dict[str, tuple[int, int, str]]:
    rows = {}
    for line in redis("--no-raw", "EVAL", ALL_TASKS, "0").splitlines():
        line = line.split(") ", 1)[-1].strip().strip('"')
        if line:
            tid, status, attempts, publication = (line.split(" ") + [""] * 4)[:4]
            rows[tid] = (int(status), int(attempts or 0), publication)
    return rows


def dlq_keys(expected_records: int) -> list[str]:
    if expected_records == 0:
        return []
    out = kafka("kafka-console-consumer.sh", "--topic", DLQ, "--from-beginning",
                "--property", "print.key=true", "--property", "key.separator=|",
                "--property", "print.value=false", "--max-messages", str(expected_records),
                "--timeout-ms", "30000")
    return [line.split("|", 1)[0] for line in out.splitlines() if line.strip()]


def topic_records(topic: str) -> int:
    return sum(int(line.rsplit(":", 1)[1]) for line in kafka("kafka-get-offsets.sh", "--topic", topic).split())


def log_count(needle: str) -> int:
    return sum(path.read_text().count(needle) for path in LOGS.glob("*worker-*.log"))


def wilson(k: int, n: int, z: float = 1.959964) -> dict:
    if n == 0:
        return {"k": k, "n": n}
    p = k / n
    denom = 1 + z * z / n
    centre = (p + z * z / (2 * n)) / denom
    half = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / denom
    return {"k": k, "n": n, "rate": round(p, 5), "ci95": [round(centre - half, 5), round(centre + half, 5)]}


def require_alive(stack: Stack, extra: list[subprocess.Popen], out: str) -> None:
    """Stop with a diagnosis if the stack or any extra worker died."""
    if stack.proc.poll() is not None:
        sys.exit(f"SOAK FAILED: the stack exited {stack.proc.returncode}; "
                 f"its log ({out}.stack.log) names the component that died")
    for i, proc in enumerate(extra, 1):
        if proc.poll() is not None:
            sys.exit(f"SOAK FAILED: duplicate-group worker {i} exited {proc.returncode}; see {LOGS}/dup-worker-{i}.log")


def start_duplicate_group(count: int) -> list[subprocess.Popen]:
    procs = []
    for i in range(1, count + 1):
        env = dict(os.environ, KAFKA_TOPIC=PENDING, KAFKA_GROUP_ID=DUP_GROUP,
                   METRICS_PORT=str(DUP_METRICS_BASE + i), GRPC_SERVER_ADDR="localhost:50051")
        log = open(LOGS / f"dup-worker-{i}.log", "a")
        procs.append(subprocess.Popen([str(ROOT / ".stack/bin/worker")], env=env, stdout=log,
                                      stderr=subprocess.STDOUT, start_new_session=True))
    deadline = time.time() + 300
    while not group_stable(DUP_GROUP, count):
        if any(p.poll() is not None for p in procs) or time.time() > deadline:
            stop_processes(procs)
            sys.exit(f"SOAK FAILED: duplicate group never became Stable with {count} members")
        time.sleep(1)
    print(f"duplicate group {DUP_GROUP} stable with {count} workers", flush=True)
    return procs


def stop_processes(procs: list[subprocess.Popen]) -> None:
    for proc in procs:
        if proc.poll() is None:
            os.killpg(proc.pid, signal.SIGTERM)
    for proc in procs:
        try:
            proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            os.killpg(proc.pid, signal.SIGKILL)
            proc.wait()


def wait_terminal(stack: Stack, extra: list[subprocess.Popen], groups: list[str], started: float,
                  timeout: int, out: str) -> None:
    deadline = time.time() + timeout
    while True:
        require_alive(stack, extra, out)
        rows = tasks()
        terminal = sum(1 for s, a, _ in rows.values() if s == COMPLETED or (s == FAILED and a > MAX_RETRIES))
        lags = {g: total_lag(g) for g in groups}
        print(f"t+{time.time() - started:.0f}s terminal {terminal}/{len(rows)} lag {lags}", flush=True)
        if all(lag == 0 for lag in lags.values()) and terminal == len(rows):
            return
        if time.time() > deadline:
            print("timed out waiting for terminal state", flush=True)
            return
        time.sleep(10)


def ledger(groups: list[str]) -> dict:
    rows = tasks()
    dlq_records = topic_records(DLQ)
    dlq = dlq_keys(dlq_records)
    dlq_set = set(dlq)
    completed = {t for t, (s, _, _) in rows.items() if s == COMPLETED}
    lost = sorted(t for t in rows if t not in completed and t not in dlq_set)
    attempts = Counter(a for _, a, _ in rows.values())
    retried = attempts.get(2, 0)
    exhausted = sum(1 for s, a, _ in rows.values() if s == FAILED and a > MAX_RETRIES)
    return {
        "admitted": len(rows), "pending_topic_records": topic_records(PENDING),
        "unpublished": sum(1 for *_, p in rows.values() if p != "published"),
        "completed": len(completed), "failed_status": sum(1 for s, _, _ in rows.values() if s == FAILED),
        "dlq_records": dlq_records, "dlq_distinct": len(dlq_set), "dlq_read": len(dlq),
        "lost": len(lost), "lost_ids": lost[:20],
        "completed_and_dlq": len(completed & dlq_set), "completed_and_dlq_ids": sorted(completed & dlq_set)[:20],
        "budget_violations": sum(1 for _, a, _ in rows.values() if a > MAX_RETRIES + 1),
        "attempt_histogram": dict(sorted(attempts.items())),
        "final_lag": {g: total_lag(g) for g in groups},
        "attempts_started_logged": log_count('"msg":"task attempt started"'),
        "duplicates_completed_elsewhere_logged": log_count('"msg":"duplicate delivery already completed elsewhere"'),
        "stats": {
            "first_attempt_failed": wilson(retried, len(rows)),
            "both_attempts_failed": wilson(exhausted, len(rows)),
            "retry_recovered": wilson(retried - exhausted, retried),
        },
        "_rows": rows,
    }


def submit(count: int, rate: int) -> dict:
    ghz = subprocess.run(
        ["ghz", "--insecure", "--proto", str(ROOT / "proto/task/v1/task.proto"),
         "--call", "dispatch.v1.DispatchService/SubmitTask",
         "-d", json.dumps({"task": {"type": "soak", "max_retries": MAX_RETRIES}}),
         "--total", str(count), "--rps", str(rate), "--concurrency", "10",
         "--timeout", "5s", "--count-errors", "--format", "json", "localhost:50051"],
        cwd=ROOT, check=True, text=True, capture_output=True)
    return json.loads(ghz.stdout).get("statusCodeDistribution", {})


def check(result: dict, codes: dict, concurrent: bool) -> list[str]:
    failures = []
    if result["lost"]:
        failures.append(f"{result['lost']} task(s) lost")
    if result["unpublished"]:
        failures.append(f"{result['unpublished']} task(s) never published")
    if result["budget_violations"]:
        failures.append(f"{result['budget_violations']} task(s) over the attempt budget")
    if any(lag != 0 for lag in result["final_lag"].values()):
        failures.append(f"consumer lag not zero: {result['final_lag']}")
    if result["admitted"] < codes.get("OK", 0):
        failures.append("fewer Redis records than ACCEPTED responses")
    if result["pending_topic_records"] != result["admitted"]:
        failures.append("pending-topic records != admitted tasks")
    if result["dlq_read"] != result["dlq_records"]:
        failures.append("could not read back every DLQ record")
    # Under concurrent duplicate delivery a task can be quarantined while the
    # other copy's attempt is still running and then complete (documented in
    # the README DLQ section); report it there, fail on it otherwise.
    if result["completed_and_dlq"] and not concurrent:
        failures.append(f"{result['completed_and_dlq']} task(s) both COMPLETED and quarantined")
    if not concurrent and result["dlq_records"] != result["dlq_distinct"]:
        failures.append("duplicate DLQ records without concurrent delivery")
    return failures


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--delete-volumes", action="store_true", required=True)
    parser.add_argument("--tasks", type=int, default=2400)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--rate", type=int, default=50)
    parser.add_argument("--replay", action="store_true")
    parser.add_argument("--duplicate-group", type=int, default=0, metavar="N")
    parser.add_argument("--timeout", type=int, default=5400, help="seconds to wait for all tasks to finish")
    parser.add_argument("--out", default="soak.json")
    args = parser.parse_args()
    if args.replay and args.duplicate_group:
        sys.exit("--replay and --duplicate-group are separate runs")

    groups = [GROUP] + ([DUP_GROUP] if args.duplicate_group else [])
    env = {"WORKERS": str(args.workers), "KAFKA_PARTITIONS": str(args.workers)}
    report: dict = {"tasks_requested": args.tasks, "workers": args.workers, "partitions": args.workers,
                    "duplicate_group_workers": args.duplicate_group, "max_retries": MAX_RETRIES}
    extra: list[subprocess.Popen] = []
    with Stack(f"{args.out}.stack.log", env) as stack:
        try:
            if args.duplicate_group:
                extra = start_duplicate_group(args.duplicate_group)
            started = time.time()
            codes = submit(args.tasks, args.rate)
            require_alive(stack, extra, args.out)
            wait_quiescent()
            report["ghz_status_codes"] = codes
            report["submit_seconds"] = round(time.time() - started)
            print(f"submitted {args.tasks} in {report['submit_seconds']}s: {codes}", flush=True)
            wait_terminal(stack, extra, groups, started, args.timeout, args.out)
            report["drain_seconds"] = round(time.time() - started)
            first = ledger(groups)
        finally:
            stop_processes(extra)
    first_rows = first.pop("_rows")
    report["soak"] = first
    failures = check(first, codes, concurrent=bool(args.duplicate_group))

    if args.replay and not failures:
        compose("up", "-d", "--wait", "kafka", "redis")
        reset = kafka("kafka-consumer-groups.sh", "--reset-offsets", "--to-earliest", "--group", GROUP,
                      "--topic", PENDING, "--execute")
        print("reset-offsets:\n" + reset.strip(), flush=True)
        # Measure before any worker starts: replaying completed tasks takes
        # milliseconds, so the lag is back to 0 by the time the stack is ready.
        before_lag = total_lag(GROUP)
        print(f"replaying: lag {before_lag} after rewind", flush=True)
        with Stack(f"{args.out}.stack.log", env, fresh=False) as stack:
            started = time.time()
            wait_terminal(stack, [], [GROUP], started, args.timeout, args.out)
            report["replay_seconds"] = round(time.time() - started)
            second = ledger([GROUP])
        second_rows = second.pop("_rows")
        exhausted = first["dlq_distinct"]
        replay = {
            "redelivered": first["pending_topic_records"], "lag_after_rewind": before_lag,
            "new_attempts_started": second["attempts_started_logged"] - first["attempts_started_logged"],
            "tasks_changed": sum(1 for t, row in first_rows.items() if second_rows.get(t, (None,))[:2] != row[:2]),
            "dlq_records_before": first["dlq_records"], "dlq_records_after": second["dlq_records"],
            "dlq_distinct_after": second["dlq_distinct"],
            "duplicates_completed_elsewhere_logged":
                second["duplicates_completed_elsewhere_logged"] - first["duplicates_completed_elsewhere_logged"],
            "ledger_after": second,
        }
        report["replay"] = replay
        if replay["lag_after_rewind"] != first["pending_topic_records"]:
            failures.append(f"rewind did not expose every message: lag {replay['lag_after_rewind']}")
        if replay["new_attempts_started"]:
            failures.append(f"replay started {replay['new_attempts_started']} new attempt(s)")
        if replay["tasks_changed"]:
            failures.append(f"replay changed {replay['tasks_changed']} task outcome(s)")
        if second["dlq_distinct"] != first["dlq_distinct"] or second["dlq_records"] != first["dlq_records"] + exhausted:
            failures.append("DLQ after replay is not one extra record per quarantined task")
        failures += [f"after replay: {f}" for f in check(second, codes, concurrent=True)
                     if "duplicate DLQ" not in f]

    report["failures"] = failures
    report["passed"] = not failures
    Path(args.out).write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))
    sys.exit(0 if not failures else 1)


if __name__ == "__main__":
    main()
