#!/usr/bin/env bash
# Runs the local Dispatch stack in the foreground:
#   Redis + Kafka (Compose, healthchecked) -> topics -> dispatch -> workers.
# Ctrl-C (SIGINT) or SIGTERM stops the workers, then dispatch, then the
# containers (their data stays on named volumes). If any component dies, the
# rest is stopped and the script exits 1. Logs go to $STACK_DIR/logs.
set -euo pipefail
set -m # job control: background jobs get their own process group, so a
       # terminal Ctrl-C reaches only this script, which stops them in order.

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
STACK_DIR=${STACK_DIR:-$ROOT/.stack}
LOG_DIR=$STACK_DIR/logs
BIN_DIR=$STACK_DIR/bin

PORT=${PORT:-50051}
DISPATCH_METRICS_PORT=${DISPATCH_METRICS_PORT:-9090}
WORKER_METRICS_PORT=${WORKER_METRICS_PORT:-9091}
KAFKA_TOPIC=${KAFKA_TOPIC:-tasks.pending}
KAFKA_GROUP_ID=${KAFKA_GROUP_ID:-dispatch-workers}
KAFKA_PARTITIONS=${KAFKA_PARTITIONS:-1}
WORKERS=${WORKERS:-1} # worker i (1-based) serves metrics on WORKER_METRICS_PORT+10*(i-1)
READY_TIMEOUT=${READY_TIMEOUT:-120}
STOP_TIMEOUT=${STOP_TIMEOUT:-15}
KEEP_INFRA=${KEEP_INFRA:-0}

readonly DLQ_TOPIC=tasks.dead_letter     # fixed in cmd/worker
readonly KAFKA_PORT=9092 REDIS_PORT=6379 # published in docker-compose.yml
readonly INFRA_SERVICES=(kafka redis)
readonly CONTAINERS=(dispatch-kafka dispatch-redis)

declare -A PIDS=() # component name -> pid
STOPPING=0 INFRA_STARTED=0
PROBE="" # scratch file for probe output

log() { printf '%s [stack] %s\n' "$(date +%H:%M:%S)" "$*" | tee -a "$LOG_DIR/stack.log" >&2; }
die() { log "ERROR: $*"; exit 1; }

# pause SECONDS: an interruptible sleep, so SIGTERM sent to this script alone
# runs the trap immediately instead of after the sleep finishes.
pause() {
	sleep "$1" &
	wait $! || true
}

compose() { docker compose --project-directory "$ROOT" "$@"; }
kafka_cli() { compose exec -T kafka "/opt/kafka/bin/$1" --bootstrap-server "localhost:$KAFKA_PORT" "${@:2}"; }

port_in_use() { (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null; }

# container_state NAME: prints "<status> <health>", e.g. "running healthy".
container_state() {
	docker inspect -f '{{.State.Status}} {{if .State.Health}}{{.State.Health.Status}}{{end}}' "$1" >"$PROBE" 2>&1 || true
	local state=""
	read -r state <"$PROBE" || true
	printf '%s' "$state"
}

container_running() { [[ $(container_state "$1") == running* ]]; }

# ── Preflight ──────────────────────────────────────────────────────────────────

preflight() {
	local tool missing=()
	for tool in docker go; do
		command -v "$tool" >/dev/null || missing+=("$tool")
	done
	((${#missing[@]} == 0)) || die "missing required tools: ${missing[*]} (install Docker with the Compose plugin and Go >= 1.23.0)"
	docker info >/dev/null 2>&1 || die "cannot reach the Docker daemon; start Docker and retry"
	docker compose version >/dev/null 2>&1 || die "'docker compose' (Compose v2) is required"
	go version >/dev/null || die "go is installed but not runnable"

	[[ $KAFKA_TOPIC != "$DLQ_TOPIC" ]] || die "KAFKA_TOPIC must differ from the dead-letter topic $DLQ_TOPIC"
	[[ $KAFKA_PARTITIONS =~ ^[1-9][0-9]*$ ]] || die "KAFKA_PARTITIONS must be a positive integer, got '$KAFKA_PARTITIONS'"
	[[ $WORKERS =~ ^[0-9]+$ ]] || die "WORKERS must be a non-negative integer, got '$WORKERS'"
	((WORKERS <= KAFKA_PARTITIONS)) ||
		log "WARNING: WORKERS=$WORKERS exceeds KAFKA_PARTITIONS=$KAFKA_PARTITIONS; $((WORKERS - KAFKA_PARTITIONS)) worker(s) will receive no partition"
	local name value
	for name in PORT DISPATCH_METRICS_PORT WORKER_METRICS_PORT; do
		value=${!name}
		[[ $value =~ ^[0-9]+$ ]] && ((value > 0 && value + 10 * WORKERS < 65536)) || die "$name must be a TCP port, got '$value'"
	done
	local ports=("$PORT" "$DISPATCH_METRICS_PORT") i
	for ((i = 1; i <= WORKERS; i++)); do ports+=("$(worker_metrics_port "$i")"); done
	[[ $(printf '%s\n' "${ports[@]}" $KAFKA_PORT $REDIS_PORT | sort | uniq -d) == "" ]] ||
		die "application ports overlap each other or Kafka/Redis ($KAFKA_PORT/$REDIS_PORT): ${ports[*]}"

	# Application ports must be free. Kafka/Redis ports may be held only by this
	# project's own running containers, which are reused.
	local port
	for port in "${ports[@]}"; do
		if port_in_use "$port"; then
			die "port $port is already in use$(port_owner "$port"); stop that process or override the port variable"
		fi
	done
	for i in 0 1; do
		port=$([[ $i == 0 ]] && echo $KAFKA_PORT || echo $REDIS_PORT)
		if port_in_use "$port" && ! container_running "${CONTAINERS[$i]}"; then
			die "port $port is in use by something other than ${CONTAINERS[$i]}$(port_owner "$port"); stop it first"
		fi
	done
}

worker_metrics_port() { echo $((WORKER_METRICS_PORT + 10 * ($1 - 1))); }

port_owner() {
	local owner
	owner=$(docker ps --filter "publish=$1" --format '{{.Names}}' 2>/dev/null | head -1)
	[[ -n $owner ]] && echo " (container $owner)"
	return 0
}

# ── Startup ────────────────────────────────────────────────────────────────────

start_infra() {
	log "starting Redis and Kafka (waiting for healthchecks)"
	INFRA_STARTED=1
	compose up -d --wait --wait-timeout "$READY_TIMEOUT" "${INFRA_SERVICES[@]}" >>"$LOG_DIR/infra.log" 2>&1 ||
		die "Redis/Kafka did not become healthy within ${READY_TIMEOUT}s; see $LOG_DIR/infra.log and 'docker compose logs kafka redis'"
}

create_topics() {
	local topic deadline
	for topic in "$KAFKA_TOPIC" "$DLQ_TOPIC"; do
		kafka_cli kafka-topics.sh --create --if-not-exists --topic "$topic" \
			--partitions "$KAFKA_PARTITIONS" --replication-factor 1 >>"$LOG_DIR/infra.log" 2>&1 ||
			die "creating Kafka topic $topic failed; see $LOG_DIR/infra.log"
		deadline=$((SECONDS + READY_TIMEOUT))
		until kafka_cli kafka-topics.sh --describe --topic "$topic" >"$PROBE" 2>/dev/null &&
			grep -Eq 'Leader: [0-9]+' "$PROBE"; do
			((SECONDS < deadline)) || die "topic $topic has no partition leader after ${READY_TIMEOUT}s"
			pause 0.5
		done
	done
	log "topics ready: $KAFKA_TOPIC, $DLQ_TOPIC"
}

build() {
	log "building binaries into $BIN_DIR"
	go -C "$ROOT" build -o "$BIN_DIR/" ./cmd/dispatch ./cmd/worker ./cmd/healthcheck >>"$LOG_DIR/build.log" 2>&1 ||
		die "go build failed; see $LOG_DIR/build.log"
}

# wait_until NAME PID CHECK... : poll CHECK until it succeeds, failing fast if
# the process exits first.
wait_until() {
	local name=$1 pid=$2 deadline=$((SECONDS + READY_TIMEOUT))
	shift 2
	until "$@" >/dev/null 2>&1; do
		kill -0 "$pid" 2>/dev/null || {
			report_exit "$name" "$pid"
			die "$name exited during startup"
		}
		((SECONDS < deadline)) || die "$name not ready after ${READY_TIMEOUT}s; see $LOG_DIR/$name.log"
		pause 0.5
	done
}

dispatch_healthy() { "$BIN_DIR/healthcheck" -addr "localhost:$PORT"; }

# workers_joined N: the consumer group has at least N members.
workers_joined() {
	kafka_cli kafka-consumer-groups.sh --describe --group "$KAFKA_GROUP_ID" --members >"$PROBE" 2>/dev/null &&
		awk -v g="$KAFKA_GROUP_ID" -v n="$1" '$1 == g { count++ } END { exit !(count >= n) }' "$PROBE"
}

# group_stable N: the consumer group has finished rebalancing with exactly N
# members, so every partition is assigned and no handoff is in progress.
group_stable() {
	kafka_cli kafka-consumer-groups.sh --describe --group "$KAFKA_GROUP_ID" --state >"$PROBE" 2>/dev/null &&
		awk -v g="$KAFKA_GROUP_ID" -v n="$1" '$1 == g && $(NF - 1) == "Stable" && $NF == n { ok = 1 } END { exit !ok }' "$PROBE"
}

start_dispatch() {
	log "starting dispatch (gRPC :$PORT, metrics :$DISPATCH_METRICS_PORT)"
	PORT=$PORT METRICS_PORT=$DISPATCH_METRICS_PORT KAFKA_TOPIC=$KAFKA_TOPIC \
		"$BIN_DIR/dispatch" >>"$LOG_DIR/dispatch.log" 2>&1 &
	PIDS[dispatch]=$!
	wait_until dispatch "${PIDS[dispatch]}" dispatch_healthy
	log "dispatch ready (pid ${PIDS[dispatch]}, gRPC health SERVING)"
}

start_workers() {
	local i name
	for ((i = 1; i <= WORKERS; i++)); do
		name=worker-$i
		log "starting $name (metrics :$(worker_metrics_port "$i"), group $KAFKA_GROUP_ID)"
		METRICS_PORT=$(worker_metrics_port "$i") KAFKA_TOPIC=$KAFKA_TOPIC KAFKA_GROUP_ID=$KAFKA_GROUP_ID \
			GRPC_SERVER_ADDR="localhost:$PORT" "$BIN_DIR/worker" >>"$LOG_DIR/$name.log" 2>&1 &
		PIDS[$name]=$!
		wait_until "$name" "${PIDS[$name]}" workers_joined "$i"
		log "$name ready (pid ${PIDS[$name]}, joined consumer group $KAFKA_GROUP_ID)"
	done
	((WORKERS > 0)) || return 0
	# Each join triggers a rebalance. Reporting ready mid-rebalance lets the
	# first tasks be handed between workers while in flight.
	local deadline=$((SECONDS + READY_TIMEOUT))
	until group_stable "$WORKERS"; do
		for name in "${!PIDS[@]}"; do
			if [[ $name == worker-* ]] && ! kill -0 "${PIDS[$name]}" 2>/dev/null; then
				report_exit "$name" "${PIDS[$name]}"
				die "$name exited while the consumer group was rebalancing"
			fi
		done
		((SECONDS < deadline)) || die "consumer group $KAFKA_GROUP_ID not Stable with $WORKERS members after ${READY_TIMEOUT}s"
		pause 0.5
	done
	log "consumer group $KAFKA_GROUP_ID stable with $WORKERS member(s)"
}

# ── Supervision and shutdown ───────────────────────────────────────────────────

report_exit() {
	local name=$1 pid=$2 code=0
	wait "$pid" 2>/dev/null || code=$?
	log "$name (pid $pid) exited with status $code; last log lines:"
	tail -n 20 "$LOG_DIR/$name.log" >&2 || true
}

supervise() {
	local name i
	while true; do
		for name in "${!PIDS[@]}"; do
			if ! kill -0 "${PIDS[$name]}" 2>/dev/null; then
				report_exit "$name" "${PIDS[$name]}"
				unset "PIDS[$name]"
				die "$name died; stopping the stack"
			fi
		done
		for i in "${!CONTAINERS[@]}"; do
			local state
			state=$(container_state "${CONTAINERS[$i]}")
			if [[ $state != running* || $state == *unhealthy* ]]; then
				die "${CONTAINERS[$i]} is down or unhealthy (state: $state); stopping the stack (see 'docker compose logs ${INFRA_SERVICES[$i]}')"
			fi
		done
		pause 2
	done
}

# stop_processes NAME...: SIGTERM every named live component at once, wait up
# to STOP_TIMEOUT for all of them, then SIGKILL any that remain.
stop_processes() {
	local name live=() deadline
	for name in "$@"; do
		[[ -n ${PIDS[$name]:-} ]] && kill -0 "${PIDS[$name]}" 2>/dev/null && live+=("$name")
	done
	((${#live[@]})) || return 0
	for name in "${live[@]}"; do
		log "stopping $name (pid ${PIDS[$name]})"
		kill -TERM "${PIDS[$name]}" 2>/dev/null || true
	done
	deadline=$((SECONDS + STOP_TIMEOUT))
	for name in "${live[@]}"; do
		while kill -0 "${PIDS[$name]}" 2>/dev/null; do
			if ((SECONDS >= deadline)); then
				log "WARNING: $name did not exit within ${STOP_TIMEOUT}s; sending SIGKILL"
				kill -KILL "${PIDS[$name]}" 2>/dev/null || true
				break
			fi
			pause 0.2
		done
		wait "${PIDS[$name]}" 2>/dev/null || true
	done
	return 0
}

shutdown() {
	local status=$?
	((STOPPING)) && return
	STOPPING=1
	# A second Ctrl-C must not abandon a half-finished shutdown.
	trap 'log "already stopping; waiting for components to exit"' INT TERM
	local name workers=()
	for name in "${!PIDS[@]}"; do
		if [[ $name == worker-* ]]; then workers+=("$name"); fi
	done
	stop_processes "${workers[@]}"
	stop_processes dispatch
	if ((!INFRA_STARTED)); then
		: # failed before touching Redis/Kafka; leave any running containers alone
	elif [[ $KEEP_INFRA == 1 ]]; then
		log "leaving Redis and Kafka running (KEEP_INFRA=1)"
	else
		log "stopping Redis and Kafka (data kept on named volumes)"
		compose stop "${INFRA_SERVICES[@]}" >>"$LOG_DIR/infra.log" 2>&1 || log "WARNING: docker compose stop failed; see $LOG_DIR/infra.log"
	fi
	rm -f "$STACK_DIR/stack.pid"
	log "stack stopped (exit $status)"
	exit "$status"
}

main() {
	mkdir -p "$LOG_DIR" "$BIN_DIR"
	PROBE=$STACK_DIR/probe.out
	if [[ -f $STACK_DIR/stack.pid ]] && kill -0 "$(cat "$STACK_DIR/stack.pid")" 2>/dev/null; then
		echo "a stack is already running (pid $(cat "$STACK_DIR/stack.pid"))" >&2
		exit 1
	fi
	echo $$ >"$STACK_DIR/stack.pid"
	trap shutdown EXIT
	trap 'exit 0' INT TERM
	log "logs: $LOG_DIR (tail with: make logs)"
	preflight
	build
	start_infra
	create_topics
	start_dispatch
	start_workers
	log "stack ready in ${SECONDS}s — gRPC localhost:$PORT; press Ctrl-C to stop"
	supervise
}

main "$@"
