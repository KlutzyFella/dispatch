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
quarantined. Exits 1 if any task is lost or any invariant fails.

Not covered: process or broker crashes during the soak (the supervisor stops
the whole stack if a component dies), multi-broker Kafka, Redis data loss.

DESTRUCTIVE: deletes this project's Kafka/Redis volumes first.
Usage: scripts/bench/soak.py --delete-volumes [--tasks 2400] [--workers 8] [--rate 50]
"""
import argparse
import json
import subprocess
import sys
import time
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from ingest_sweep import ROOT, Stack, compose, redis, wait_quiescent  # noqa: E402

PENDING, DLQ, GROUP = "tasks.pending", "tasks.dead_letter", "dispatch-workers"
COMPLETED, FAILED = 3, 4
MAX_RETRIES = 1

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


def total_lag() -> int | None:
    out = kafka("kafka-consumer-groups.sh", "--describe", "--group", GROUP)
    lags = [line.split()[5] for line in out.splitlines() if line.startswith(GROUP)]
    if not lags or any(lag == "-" for lag in lags):
        return None
    return sum(int(lag) for lag in lags)


def tasks() -> list[tuple[str, int, int, str]]:
    rows = []
    for line in redis("--no-raw", "EVAL", ALL_TASKS, "0").splitlines():
        line = line.split(") ", 1)[-1].strip().strip('"')
        if line:
            tid, status, attempts, publication = (line.split(" ") + [""] * 4)[:4]
            rows.append((tid, int(status), int(attempts or 0), publication))
    return rows


def dlq_keys(expected_records: int) -> list[str]:
    if expected_records == 0:
        return []
    out = kafka("kafka-console-consumer.sh", "--topic", DLQ, "--from-beginning",
                "--property", "print.key=true", "--property", "key.separator=|",
                "--property", "print.value=false", "--max-messages", str(expected_records),
                "--timeout-ms", "20000")
    return [line.split("|", 1)[0] for line in out.splitlines() if line.strip()]


def topic_records(topic: str) -> int:
    return sum(int(line.rsplit(":", 1)[1]) for line in kafka("kafka-get-offsets.sh", "--topic", topic).split())


def require_stack(stack: Stack, out: str) -> None:
    """Stop with the stack's own diagnosis if any component died."""
    if stack.proc.poll() is not None:
        sys.exit(f"SOAK FAILED: the stack exited {stack.proc.returncode} during the soak; "
                 f"its log ({out}.stack.log) names the component that died")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--delete-volumes", action="store_true", required=True)
    parser.add_argument("--tasks", type=int, default=2400)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--rate", type=int, default=50)
    parser.add_argument("--timeout", type=int, default=3600, help="seconds to wait for all tasks to finish")
    parser.add_argument("--out", default="soak.json")
    args = parser.parse_args()

    env = {"WORKERS": str(args.workers), "KAFKA_PARTITIONS": str(args.workers)}
    with Stack(f"{args.out}.stack.log", env) as stack:
        started = time.time()
        ghz = subprocess.run(
            ["ghz", "--insecure", "--proto", str(ROOT / "proto/task/v1/task.proto"),
             "--call", "dispatch.v1.DispatchService/SubmitTask",
             "-d", json.dumps({"task": {"type": "soak", "max_retries": MAX_RETRIES}}),
             "--total", str(args.tasks), "--rps", str(args.rate), "--concurrency", "10",
             "--timeout", "5s", "--count-errors", "--format", "json", "localhost:50051"],
            cwd=ROOT, check=True, text=True, capture_output=True)
        report = json.loads(ghz.stdout)
        codes = report.get("statusCodeDistribution", {})
        require_stack(stack, args.out)
        wait_quiescent()
        submitted_s = time.time() - started
        print(f"submitted {report['count']} requests in {submitted_s:.0f}s: {codes}", flush=True)

        deadline = time.time() + args.timeout
        while True:
            require_stack(stack, args.out)
            rows = tasks()
            terminal = sum(1 for _, s, a, _ in rows if s == COMPLETED or (s == FAILED and a > MAX_RETRIES))
            lag = total_lag()
            print(f"t+{time.time() - started:.0f}s terminal {terminal}/{len(rows)} lag {lag}", flush=True)
            if lag == 0 and terminal == len(rows):
                break
            if time.time() > deadline:
                break
            time.sleep(10)
        drained_s = time.time() - started

        rows = tasks()
        dlq_records = topic_records(DLQ)
        dlq = dlq_keys(dlq_records)
        dlq_set = set(dlq)
        status = Counter(s for _, s, _, _ in rows)
        completed = {t for t, s, _, _ in rows if s == COMPLETED}
        lost = sorted(t for t, _, _, _ in rows if t not in completed and t not in dlq_set)
        result = {
            "tasks_requested": args.tasks, "ghz_status_codes": codes, "workers": args.workers,
            "partitions": args.workers, "admitted": len(rows),
            "pending_topic_records": topic_records(PENDING),
            "unpublished": sum(1 for *_, p in rows if p != "published"),
            "completed": len(completed), "failed_status": status.get(FAILED, 0),
            "dlq_records": dlq_records, "dlq_distinct": len(dlq_set), "dlq_read": len(dlq),
            "lost": len(lost), "lost_ids": lost[:20],
            "completed_and_dlq": len(completed & dlq_set),
            "budget_violations": sum(1 for _, _, a, _ in rows if a > MAX_RETRIES + 1),
            "attempt_histogram": dict(sorted(Counter(a for _, _, a, _ in rows).items())),
            "final_lag": total_lag(), "submit_seconds": round(submitted_s), "drain_seconds": round(drained_s),
            "max_retries": MAX_RETRIES,
        }
        ok = (result["lost"] == 0 and result["unpublished"] == 0 and result["budget_violations"] == 0
              and result["final_lag"] == 0 and result["admitted"] >= codes.get("OK", 0)
              and result["pending_topic_records"] == result["admitted"] and result["dlq_read"] == dlq_records)
        result["passed"] = ok
        Path(args.out).write_text(json.dumps(result, indent=2) + "\n")
        print(json.dumps(result, indent=2))
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
