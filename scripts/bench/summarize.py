#!/usr/bin/env python3
"""Summarize ingest_sweep.py JSONL results as a Markdown table.

For each target rate: how many reps sustained it, median achieved rate, median
and range of p50/p95, median p99 where every rep had enough samples, non-OK
totals, whether every rep passed the Kafka/Redis count check, and median CPU
(in cores) per component.
Usage: scripts/bench/summarize.py results.jsonl [more.jsonl ...]
"""
import json
import statistics
import sys
from collections import defaultdict


def spread(values: list[float]) -> str:
    return f"{statistics.median(values):.1f} ({min(values):.1f}–{max(values):.1f})"


def main() -> None:
    steps = defaultdict(list)
    for path in sys.argv[1:]:
        for line in open(path):
            record = json.loads(line)
            if record.get("kind") == "step":
                steps[record["target_rps"]].append(record)
    print("| target req/s | sustained reps | achieved req/s | p50 ms | p95 ms | p99 ms | non-OK | counts verified "
          "| Redis main thread | Redis total | dispatch | ghz | Kafka | host busy |")
    print("|" + "---|" * 14)
    for rate in sorted(steps):
        rs = steps[rate]
        p99 = [r["p99_ms"] for r in rs]
        cpu = lambda key: f"{statistics.median(r['cpu_cores'][key] for r in rs):.2f}"
        print(f"| {rate} | {sum(r['sustained'] for r in rs)}/{len(rs)} "
              f"| {statistics.median(r['achieved_rps'] for r in rs):.0f} "
              f"| {spread([r['p50_ms'] for r in rs])} | {spread([r['p95_ms'] for r in rs])} "
              f"| {spread(p99) if all(v is not None for v in p99) else 'n/a'} "
              f"| {sum(r['count'] - r['ok'] for r in rs)} / {sum(r['count'] for r in rs)} "
              f"| {'yes' if all(r['verified'] for r in rs) else 'NO'} "
              f"| {cpu('redis_main_thread')} | {cpu('redis')} | {cpu('dispatch')} | {cpu('ghz')} | {cpu('kafka')} "
              f"| {statistics.median(r['cpu_cores']['host_busy_fraction'] for r in rs):.0%} |")


if __name__ == "__main__":
    main()
