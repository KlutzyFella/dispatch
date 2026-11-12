#!/usr/bin/env python3
"""Fixed-rate load sweep of dispatch's SubmitTask, with every result verified.

Each repetition starts from deleted Kafka/Redis volumes, runs the stack with
WORKERS=0 (ingestion only, one partition), and drives SubmitTask with ghz at
fixed request rates. A step counts as sustained only if all of these hold:

  * achieved rate within 3% of the target rate;
  * p95 latency <= 50 ms, over all responses (ghz --count-errors);
  * fewer than 0.1% of responses are anything other than OK (Unavailable is
    counted against this and also reported in its own column).

After each step the harness waits until no publication intent is pending, then
requires the Kafka offset delta (summed over partitions) to equal the number of
new Redis task records, and to be at least the number of OK responses. The
excess is intents recovered after a non-OK response. Redis memory is then
recorded and Redis is flushed, so every step starts from an empty store rather
than one holding every earlier step's tasks (records are never expired).

Requests carry no idempotency key, so dispatch assigns a fresh ID to each and
none takes the duplicate path. CPU time for dispatch, ghz, the Kafka and Redis
containers and the whole host comes from kernel counters for the step.

DESTRUCTIVE: deletes this project's Kafka/Redis volumes before each rep.
Usage: scripts/bench/ingest_sweep.py --delete-volumes --rates 200,400 [--reps 3]
       [--duration 60] [--descending] [--out results.jsonl]
"""
import argparse
import json
import os
import platform
import re
import resource
import signal
import statistics
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
TOPIC = "tasks.pending"
SLO_P95_MS, SLO_NON_OK, RATE_TOLERANCE = 50.0, 0.001, 0.03
P99_MIN_SAMPLES = 10_000
TICKS = os.sysconf("SC_CLK_TCK")


def sh(*args: str, check: bool = True, **kw) -> str:
    return subprocess.run(args, cwd=ROOT, check=check, text=True, capture_output=True, **kw).stdout


def compose(*args: str, **kw) -> str:
    return sh("docker", "compose", "--project-directory", str(ROOT), *args, **kw)


def redis(*args: str) -> str:
    return compose("exec", "-T", "redis", "redis-cli", *args).strip()


def kafka_offsets() -> dict[int, int]:
    out = compose("exec", "-T", "kafka", "/opt/kafka/bin/kafka-get-offsets.sh",
                  "--bootstrap-server", "localhost:9092", "--topic", TOPIC)
    offsets = {}
    for line in out.split():
        topic, partition, offset = line.rsplit(":", 2)
        offsets[int(partition)] = int(offset)
    return offsets


def container_cgroup(name: str) -> Path:
    cid = sh("docker", "inspect", "-f", "{{.Id}}", name).strip()
    return Path(f"/sys/fs/cgroup/system.slice/docker-{cid}.scope/cpu.stat")


def cgroup_cpu_s(path: Path) -> float:
    for line in path.read_text().splitlines():
        key, value = line.split()
        if key == "usage_usec":
            return int(value) / 1e6
    raise RuntimeError(f"no usage_usec in {path}")


def proc_cpu_s(pid: int) -> float:
    fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
    return (int(fields[11]) + int(fields[12])) / TICKS  # utime + stime


def thread_cpu_s(pid: int) -> float:
    """CPU seconds of a process's main thread only."""
    fields = Path(f"/proc/{pid}/task/{pid}/stat").read_text().rsplit(")", 1)[1].split()
    return (int(fields[11]) + int(fields[12])) / TICKS


def redis_info(*sections: str) -> dict[str, str]:
    info = {}
    for section in sections:
        for line in redis("INFO", section).splitlines():
            if ":" in line:
                key, value = line.split(":", 1)
                info[key] = value.strip()
    return info


FORK_COUNTERS = ("total_forks", "rdb_saves", "aof_rewrites")


def host_cpu() -> tuple[int, int]:
    fields = [int(x) for x in Path("/proc/stat").read_text().splitlines()[0].split()[1:]]
    idle = fields[3] + fields[4]
    return sum(fields), idle


class Stack:
    """Runs scripts/stack.sh from deleted volumes; extra env selects workers etc."""

    def __init__(self, log_path: str, env: dict[str, str] | None = None) -> None:
        self.log_path = log_path
        self.env = env or {"WORKERS": "0"}

    def __enter__(self) -> "Stack":
        compose("down", "-v")
        sh("rm", "-rf", str(ROOT / ".stack"))
        self.log = open(self.log_path, "w")
        env = dict(os.environ, **self.env)
        self.proc = subprocess.Popen([str(ROOT / "scripts/stack.sh")], cwd=ROOT, env=env,
                                     stdout=self.log, stderr=subprocess.STDOUT, start_new_session=True)
        deadline = time.time() + 240
        while "stack ready" not in Path(self.log_path).read_text():
            if self.proc.poll() is not None or time.time() > deadline:
                raise RuntimeError(f"stack did not become ready; see {self.log_path}")
            time.sleep(0.5)
        match = re.search(r"dispatch ready \(pid (\d+)", Path(self.log_path).read_text())
        self.dispatch_pid = int(match.group(1))
        return self

    def __exit__(self, *exc: object) -> None:
        os.killpg(self.proc.pid, signal.SIGINT)
        self.proc.wait(timeout=120)
        self.log.close()
        if self.proc.returncode != 0:
            print(f"warning: stack exited {self.proc.returncode}; see {self.log_path}", file=sys.stderr)


def ghz(rate: int, duration: int) -> tuple[subprocess.Popen, int]:
    concurrency = max(50, rate // 10)
    args = ["ghz", "--insecure", "--proto", str(ROOT / "proto/task/v1/task.proto"),
            "--call", "dispatch.v1.DispatchService/SubmitTask",
            "-d", '{"task":{"type":"bench","max_retries":1}}',
            "--rps", str(rate), "--concurrency", str(concurrency), "--connections", "4",
            "--duration", f"{duration}s", "--duration-stop", "wait", "--timeout", "5s",
            "--count-errors", "--format", "json", "localhost:50051"]
    return subprocess.Popen(args, cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True), concurrency


def wait_quiescent(timeout: int = 90) -> None:
    deadline = time.time() + timeout
    while True:
        pending = int(redis("ZCARD", f"outbox:{TOPIC}"))
        intents = len([k for k in redis("--scan", "--pattern", "publication:*").split() if k])
        if pending == 0 and intents == 0:
            return
        if time.time() > deadline:
            raise RuntimeError(f"not quiescent after {timeout}s: outbox={pending} intents={intents}")
        time.sleep(0.5)


def run_step(stack: Stack, rate: int, duration: int, cgroups: dict[str, Path], redis_pid: int) -> dict:
    before_offsets, before_keys = kafka_offsets(), int(redis("DBSIZE"))
    cpu0 = {name: cgroup_cpu_s(path) for name, path in cgroups.items()}
    redis_main0 = thread_cpu_s(redis_pid)
    forks0 = redis_info("stats", "persistence")
    started = time.strftime("%Y-%m-%dT%H:%M:%S%z")
    dispatch0, host0, wall0 = proc_cpu_s(stack.dispatch_pid), host_cpu(), time.time()
    children0 = resource.getrusage(resource.RUSAGE_CHILDREN)
    proc, concurrency = ghz(rate, duration)
    out, err = proc.communicate()  # reaps ghz, so its CPU time lands in RUSAGE_CHILDREN
    wall = time.time() - wall0
    children1 = resource.getrusage(resource.RUSAGE_CHILDREN)
    ended = time.strftime("%Y-%m-%dT%H:%M:%S%z")
    forks1 = redis_info("stats", "persistence")
    ghz_cpu = (children1.ru_utime + children1.ru_stime) - (children0.ru_utime + children0.ru_stime)
    if proc.returncode != 0:
        raise RuntimeError(f"ghz failed ({proc.returncode}): {err.strip()}")
    report = json.loads(out)
    host1 = host_cpu()
    cpu = {name: (cgroup_cpu_s(path) - cpu0[name]) / wall for name, path in cgroups.items()}
    cpu["redis_main_thread"] = (thread_cpu_s(redis_pid) - redis_main0) / wall
    cpu["dispatch"] = (proc_cpu_s(stack.dispatch_pid) - dispatch0) / wall
    cpu["ghz"] = ghz_cpu / wall
    busy = 1 - (host1[1] - host0[1]) / (host1[0] - host0[0])
    cpu["host_busy_fraction"] = busy

    wait_quiescent()
    after_offsets = kafka_offsets()
    kafka_delta = sum(after_offsets[p] - before_offsets.get(p, 0) for p in after_offsets)
    redis_delta = int(redis("DBSIZE")) - before_keys
    used_memory = int(re.search(r"used_memory:(\d+)", redis("INFO", "memory")).group(1))
    redis("FLUSHALL")

    codes = report.get("statusCodeDistribution", {})
    count = report["count"]
    ok = codes.get("OK", 0)
    pct = {int(d["percentage"]): d["latency"] / 1e6 for d in report.get("latencyDistribution") or []}
    achieved = report["rps"]
    step = {
        "target_rps": rate, "achieved_rps": round(achieved, 1), "count": count, "ok": ok,
        "unavailable": codes.get("Unavailable", 0),
        "other_non_ok": count - ok - codes.get("Unavailable", 0), "status_codes": codes,
        "p50_ms": round(pct.get(50, float("nan")), 2), "p95_ms": round(pct.get(95, float("nan")), 2),
        "p99_ms": round(pct[99], 2) if count >= P99_MIN_SAMPLES and 99 in pct else None,
        "kafka_delta": kafka_delta, "redis_new_tasks": redis_delta,
        "recovered_after_non_ok": kafka_delta - ok,
        "cpu_cores": {k: round(v, 2) for k, v in cpu.items()},
        "redis_used_memory_mb": round(used_memory / 2**20, 1),
        "redis_forks": {k: int(forks1.get(k, 0)) - int(forks0.get(k, 0)) for k in FORK_COUNTERS},
        "redis_latest_fork_ms": round(int(forks1.get("latest_fork_usec", 0)) / 1000, 1),
        "slowest_ms": round(report.get("slowest", 0) / 1e6, 1),
        "started": started, "ended": ended, "wall_s": round(wall, 1),
        "ghz_concurrency": concurrency, "duration_s": duration,
    }
    step["verified"] = kafka_delta == redis_delta and kafka_delta >= ok
    step["rate_ok"] = abs(achieved - rate) / rate <= RATE_TOLERANCE
    step["slo_ok"] = step["p95_ms"] <= SLO_P95_MS and (count - ok) / max(count, 1) < SLO_NON_OK
    step["sustained"] = step["verified"] and step["rate_ok"] and step["slo_ok"]
    return step


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--delete-volumes", action="store_true", required=True)
    parser.add_argument("--rates", required=True, help="comma-separated target req/s")
    parser.add_argument("--reps", type=int, default=3)
    parser.add_argument("--duration", type=int, default=60)
    parser.add_argument("--warmup", type=int, default=15)
    parser.add_argument("--descending", action="store_true", help="run the rates high to low")
    parser.add_argument("--out", default="ingest_sweep.jsonl")
    args = parser.parse_args()
    rates = [int(r) for r in args.rates.split(",")]
    if args.descending:
        rates.sort(reverse=True)
    meta = {"kind": "meta", "started": time.strftime("%Y-%m-%dT%H:%M:%S%z"), "host": platform.node(),
            "cpu": next((l.split(":", 1)[1].strip() for l in Path("/proc/cpuinfo").read_text().splitlines()
                         if l.startswith("model name")), "?"), "threads": os.cpu_count(),
            "kernel": platform.release(), "rates": rates, "reps": args.reps, "duration_s": args.duration,
            "slo": {"p95_ms": SLO_P95_MS, "max_non_ok_fraction": SLO_NON_OK, "rate_tolerance": RATE_TOLERANCE},
            "git": sh("git", "rev-parse", "--short", "HEAD").strip(),
            "ghz": "built from github.com/bojand/ghz v0.121.0 via go install (reports version 'dev')"}
    with open(args.out, "a") as out:
        out.write(json.dumps(meta) + "\n")
        for rep in range(1, args.reps + 1):
            with Stack(f"{args.out}.rep{rep}.stack.log") as stack:
                cgroups = {"kafka": container_cgroup("dispatch-kafka"), "redis": container_cgroup("dispatch-redis")}
                redis_pid = int(sh("docker", "inspect", "-f", "{{.State.Pid}}", "dispatch-redis").strip())
                warm, _ = ghz(100, args.warmup)
                warm.communicate()
                wait_quiescent()
                redis("FLUSHALL")
                for rate in rates:
                    step = run_step(stack, rate, args.duration, cgroups, redis_pid)
                    step.update(kind="step", rep=rep)
                    out.write(json.dumps(step) + "\n")
                    out.flush()
                    print(f"rep {rep} target {rate:>5}/s achieved {step['achieved_rps']:>7} p50 {step['p50_ms']:>6}ms "
                          f"p95 {step['p95_ms']:>6}ms ok {step['ok']}/{step['count']} unavail {step['unavailable']} "
                          f"kafka+{step['kafka_delta']} verified={step['verified']} sustained={step['sustained']} "
                          f"redis_mem={step['redis_used_memory_mb']}MB forks={step['redis_forks']} "
                          f"fork_ms={step['redis_latest_fork_ms']} wall={step['wall_s']}s cpu={step['cpu_cores']}", flush=True)


if __name__ == "__main__":
    main()
