#!/usr/bin/env python3
"""Summarize `go test -json` output so a failure can be diagnosed from the log.

Prints top-level and leaf test counts (a parent test is not counted as a leaf),
the complete output of every failed test and of every package that failed to
build, and, with --no-skips, treats any skipped test as a failure: in the
integration tier a skip means missing infrastructure was hidden as a pass.
If GITHUB_STEP_SUMMARY is set, a Markdown summary is appended to it.
Exit status: 1 if anything failed (or was skipped with --no-skips), else 0.

Usage: scripts/ci/gotest_summary.py [--no-skips] test-results.json
"""
import argparse
import json
import os
import sys
from collections import Counter, defaultdict


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--no-skips", action="store_true", help="fail if any test was skipped")
    parser.add_argument("results")
    args = parser.parse_args()

    outcome: dict[tuple[str, str], str] = {}
    output: dict[tuple[str, str], list[str]] = defaultdict(list)
    package_failed: set[str] = set()
    unparsed: list[str] = []
    with open(args.results) as f:
        for line in f:
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                unparsed.append(line.rstrip())  # e.g. build errors printed outside the JSON stream
                continue
            key = (event.get("Package", ""), event.get("Test", ""))
            action = event.get("Action")
            if action == "output":
                output[key].append(event.get("Output", ""))
            elif action in ("pass", "fail", "skip"):
                if key[1]:
                    outcome[key] = action
                elif action == "fail":
                    package_failed.add(key[0])

    tests = outcome
    top = {k: v for k, v in tests.items() if "/" not in k[1]}
    leaf = {k: v for k, v in tests.items()
            if not any(p == k[0] and name.startswith(k[1] + "/") for p, name in tests)}
    failed = sorted(k for k, v in leaf.items() if v == "fail")
    skipped = sorted(k for k, v in leaf.items() if v == "skip")
    # A package can fail without any failing test: build errors, panics in
    # init, or a timeout. Report those with the package's own output.
    failed_packages = sorted(p for p in package_failed if not any(k[0] == p for k in failed))

    top_counts, leaf_counts = Counter(top.values()), Counter(leaf.values())
    print(f"top-level tests: {len(top)} {dict(top_counts)}")
    print(f"leaf cases:      {len(leaf)} {dict(leaf_counts)}")
    for pkg, name in failed:
        print(f"\n===== FAIL {pkg} {name}\n" + "".join(output[(pkg, name)]).rstrip())
    for pkg in failed_packages:
        print(f"\n===== PACKAGE FAIL {pkg}\n" + "".join(output[(pkg, "")]).rstrip())
    if unparsed:
        print("\n===== non-JSON output\n" + "\n".join(unparsed))
    if skipped:
        print("\n===== SKIPPED" + (" (not allowed in this tier)" if args.no_skips else ""))
        for pkg, name in skipped:
            reason = [line.strip() for line in "".join(output[(pkg, name)]).splitlines()
                      if line.strip() and not line.startswith(("=== ", "--- "))]
            print(f"{pkg} {name}: {' | '.join(reason) or '(no reason given)'}")

    bad = bool(failed or failed_packages or unparsed or (args.no_skips and skipped))
    if not tests and not package_failed:
        print("no test results found; did go test run at all?")
        bad = True

    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
        with open(summary_path, "a") as s:
            s.write(f"### {'❌' if bad else '✅'} Go tests\n\n| | total | pass | fail | skip |\n|---|---|---|---|---|\n")
            for label, counts, n in (("top-level", top_counts, len(top)), ("leaf", leaf_counts, len(leaf))):
                s.write(f"| {label} | {n} | {counts['pass']} | {counts['fail']} | {counts['skip']} |\n")
            for pkg, name in failed:
                s.write(f"\n**FAIL** `{name}` ({pkg})\n")
            for pkg in failed_packages:
                s.write(f"\n**PACKAGE FAIL** `{pkg}`\n")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
