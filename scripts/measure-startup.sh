#!/usr/bin/env bash
# Measures `make up` from a clean project state: Compose containers and named
# volumes removed (docker compose down -v), .stack removed, and, with
# COLD_BUILD=1, an empty temporary GOCACHE. Docker images and the Go module
# cache are kept (re-downloading them would measure the network).
#
# DESTRUCTIVE: deletes this project's Kafka and Redis volumes on every run.
# Usage: scripts/measure-startup.sh --delete-volumes [RUNS] [COLD_BUILD=1|0]
set -euo pipefail
set -m
if [[ ${1:-} != --delete-volumes ]]; then
	echo "refusing to run: each run deletes this project's Kafka/Redis data; pass --delete-volumes" >&2
	exit 2
fi
shift
runs=${1:-3} cold=${2:-1}
cd "$(dirname "${BASH_SOURCE[0]}")/.."
for run in $(seq 1 "$runs"); do
	docker compose down -v >/dev/null 2>&1
	rm -rf .stack
	cache=""
	if [[ $cold == 1 ]]; then cache=$(mktemp -d); export GOCACHE=$cache; else unset GOCACHE; fi
	out=$(mktemp)
	start=$(date +%s.%N)
	make up >"$out" 2>&1 &
	pid=$!
	until grep -q 'stack ready\|ERROR' "$out"; do
		kill -0 "$pid" 2>/dev/null || break
		sleep 0.1
	done
	ready=$(date +%s.%N)
	reported=$(grep -o 'stack ready in [0-9]*s' "$out" || echo "NOT READY")
	steps=$(awk '{print $1, substr($0, index($0,$3))}' "$out" | grep -E 'building|starting Redis|topics ready|dispatch ready|worker ready' | awk '{print $1}' | paste -sd' ')
	kill -INT -- "-$pid"
	wait "$pid" || true
	printf 'run=%d cold_build=%d wall_to_ready=%.1fs %s step_times=[%s]\n' "$run" "$cold" "$(echo "$ready - $start" | bc)" "$reported" "$steps"
	grep -q ERROR "$out" && cat "$out"
	rm -f "$out"
	[[ -n $cache ]] && rm -rf "$cache"
done
