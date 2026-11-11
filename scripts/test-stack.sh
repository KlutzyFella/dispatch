#!/usr/bin/env bash
# Exercises scripts/stack.sh end to end against real Docker, Kafka and Redis:
# readiness, a submitted task completing, Ctrl-C shutdown, component death,
# infrastructure death, and preflight failures. Each scenario checks for
# leftover processes and held ports afterwards. Requires the same tools as
# stack.sh plus grpcurl; stops this project's Kafka/Redis containers when done.
set -euo pipefail
# Job control, as in an interactive terminal: each background job gets its own
# process group and keeps SIGINT deliverable. Without it, bash starts background
# jobs with SIGINT ignored, so stack.sh could not trap the emulated Ctrl-C.
set -m

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
WORK=$(mktemp -d)
export STACK_DIR=$WORK/stack
PORTS=(50051 9090 9091 9092 6379)
FAILED=0 PASSED=0 STACK_PID=""

trap 'stop_stack; rm -rf "$WORK"' EXIT

say() { printf '\n=== %s\n' "$*"; }
pass() { echo "PASS: $*"; PASSED=$((PASSED + 1)); }
fail() {
	echo "FAIL: $*"
	FAILED=$((FAILED + 1))
	[[ -f $WORK/out ]] && { echo "--- stack output"; tail -n 30 "$WORK/out"; }
}

# Start stack.sh as its own job; its process group then receives the emulated
# Ctrl-C exactly as a terminal's foreground job would.
start_stack() {
	: >"$WORK/out"
	env "$@" "$ROOT/scripts/stack.sh" >"$WORK/out" 2>&1 &
	STACK_PID=$!
}

wait_for_output() {
	local pattern=$1 timeout=$2 deadline=$((SECONDS + $2))
	until grep -q "$pattern" "$WORK/out"; do
		kill -0 "$STACK_PID" 2>/dev/null || return 1
		((SECONDS < deadline)) || return 1
		sleep 0.5
	done
}

# wait_exit TIMEOUT: wait for stack.sh to exit; sets EXIT_CODE.
wait_exit() {
	local deadline=$((SECONDS + $1))
	while kill -0 "$STACK_PID" 2>/dev/null; do
		((SECONDS < deadline)) || { EXIT_CODE=timeout; return; }
		sleep 0.2
	done
	EXIT_CODE=0
	wait "$STACK_PID" || EXIT_CODE=$?
	STACK_PID=""
}

stop_stack() {
	[[ -n $STACK_PID ]] && kill -0 "$STACK_PID" 2>/dev/null && kill -INT -- "-$STACK_PID" 2>/dev/null && wait_exit 60
	return 0
}

port_in_use() { (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null; }

# assert_clean: no binaries from this stack still running and no ports held.
assert_clean() {
	local what=$1 leftovers port held=()
	leftovers=$(pgrep -f "$STACK_DIR/bin/" || true)
	for port in "${PORTS[@]}"; do port_in_use "$port" && held+=("$port"); done
	if [[ -n $leftovers ]]; then
		fail "$what: orphaned processes $(echo $leftovers)"
		kill -KILL $leftovers 2>/dev/null || true
	elif ((${#held[@]})); then
		fail "$what: ports still held: ${held[*]}"
	else
		pass "$what: no orphaned processes, ports released"
	fi
}

pid_from_log() { sed -n "s/.*$1 ready (pid \([0-9]*\).*/\1/p" "$WORK/out" | tail -1; }

scenario_ready_and_interrupt() {
	say "start, complete one task, then Ctrl-C"
	start_stack
	if ! wait_for_output 'stack ready' 180; then
		fail "stack never became ready"
		stop_stack
		return
	fi
	pass "stack ready: $(grep -o 'stack ready in [0-9]*s' "$WORK/out")"
	local id=stack-test-$RANDOM$RANDOM
	if grpcurl -plaintext -d "{\"task\":{\"type\":\"simulation\",\"idempotency_key\":\"$id\",\"max_retries\":2}}" \
		localhost:50051 dispatch.v1.DispatchService/SubmitTask >"$WORK/submit.out" 2>"$WORK/submit.err" &&
		grep -q SUBMIT_STATUS_ACCEPTED "$WORK/submit.out"; then
		pass "task $id accepted"
	else
		fail "submission failed: $(cat "$WORK/submit.out" "$WORK/submit.err")"
	fi
	local status="" deadline=$((SECONDS + 30))
	until [[ $status == 3 ]] || ((SECONDS >= deadline)); do
		status=$(docker compose --project-directory "$ROOT" exec -T redis redis-cli hget "task:$id" status | tr -d '\r')
		sleep 0.5
	done
	[[ $status == 3 ]] && pass "worker completed $id" || fail "task $id status=$status, never COMPLETED"
	docker compose --project-directory "$ROOT" exec -T redis redis-cli del "task:$id" >/dev/null

	kill -INT -- "-$STACK_PID"
	wait_exit 60
	[[ $EXIT_CODE == 0 ]] && pass "Ctrl-C exited 0" || fail "Ctrl-C exit=$EXIT_CODE"
	grep -q 'stopping worker' "$WORK/out" && grep -q 'stopping dispatch' "$WORK/out" &&
		[[ $(grep -n 'stopping worker' "$WORK/out" | cut -d: -f1) -lt $(grep -n 'stopping dispatch' "$WORK/out" | cut -d: -f1) ]] &&
		pass "worker stopped before dispatch" || fail "shutdown order wrong or missing"
	assert_clean "after Ctrl-C"
}

scenario_component_dies() {
	local victim=$1
	say "$victim killed with SIGKILL while running"
	start_stack
	wait_for_output 'stack ready' 180 || { fail "stack never became ready"; stop_stack; return; }
	kill -KILL "$(pid_from_log "$victim")"
	wait_exit 60
	[[ $EXIT_CODE == 1 ]] && pass "stack exited 1 after $victim died" || fail "exit=$EXIT_CODE after $victim died"
	grep -q "$victim died" "$WORK/out" && pass "failure names $victim" || fail "no '$victim died' message"
	assert_clean "after $victim died"
}

scenario_kafka_dies() {
	say "Kafka container stopped while running"
	start_stack
	wait_for_output 'stack ready' 180 || { fail "stack never became ready"; stop_stack; return; }
	docker kill dispatch-kafka >/dev/null
	wait_exit 60
	[[ $EXIT_CODE == 1 ]] && pass "stack exited 1 after Kafka died" || fail "exit=$EXIT_CODE after Kafka died"
	grep -q 'dispatch-kafka is down' "$WORK/out" && pass "failure names dispatch-kafka" || fail "no Kafka failure message"
	assert_clean "after Kafka died"
}

scenario_preflight() {
	say "preflight rejects a busy port before starting anything"
	python3 -c 'import socket,time; s=socket.socket(); s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1); s.bind(("0.0.0.0", 50051)); s.listen(); time.sleep(60)' &
	local holder=$!
	sleep 0.5
	start_stack
	wait_exit 30
	kill "$holder" 2>/dev/null || true
	wait "$holder" 2>/dev/null || true
	[[ $EXIT_CODE == 1 ]] && grep -q 'port 50051 is already in use' "$WORK/out" &&
		pass "busy port rejected with its number" || fail "busy port: exit=$EXIT_CODE"
	grep -q 'starting Redis' "$WORK/out" && fail "infrastructure started despite preflight failure" || pass "nothing started"

	say "preflight rejects KAFKA_TOPIC equal to the DLQ"
	start_stack KAFKA_TOPIC=tasks.dead_letter
	wait_exit 30
	[[ $EXIT_CODE == 1 ]] && grep -q 'must differ from the dead-letter topic' "$WORK/out" &&
		pass "DLQ topic rejected" || fail "DLQ topic: exit=$EXIT_CODE"

	say "preflight reports a missing tool"
	local fakebin=$WORK/fakebin
	mkdir -p "$fakebin"
	ln -sf "$(command -v docker)" "$fakebin/docker"
	start_stack PATH="$fakebin:/usr/bin:/bin"
	wait_exit 30
	if command -v go | grep -qE '^/(usr/)?bin/go$'; then
		echo "SKIP-NOTE: go is in /usr/bin; missing-tool check not meaningful here"
	else
		[[ $EXIT_CODE == 1 ]] && grep -q 'missing required tools: go' "$WORK/out" &&
			pass "missing go reported" || fail "missing go: exit=$EXIT_CODE"
	fi
}

# A second Ctrl-C while shutdown is in progress must not abandon it: the
# script would exit and leave dispatch (and possibly the worker) running.
scenario_double_interrupt() {
	say "second Ctrl-C during shutdown"
	start_stack
	wait_for_output 'stack ready' 180 || { fail "stack never became ready"; stop_stack; return; }
	kill -INT -- "-$STACK_PID"
	wait_for_output 'stopping worker' 30 || { fail "shutdown never started"; stop_stack; return; }
	kill -INT -- "-$STACK_PID"
	wait_exit 60
	[[ $EXIT_CODE == 0 ]] && pass "double Ctrl-C exited 0" || fail "double Ctrl-C exit=$EXIT_CODE"
	grep -q 'stopping dispatch' "$WORK/out" && pass "shutdown continued to dispatch" || fail "shutdown abandoned before dispatch"
	assert_clean "after double Ctrl-C"
}

# SIGTERM sent only to the script (a service manager or `kill PID`) must stop
# the children it supervises, which do not receive the signal themselves.
scenario_sigterm() {
	say "SIGTERM to the script only"
	start_stack
	wait_for_output 'stack ready' 180 || { fail "stack never became ready"; stop_stack; return; }
	kill -TERM "$STACK_PID"
	wait_exit 60
	[[ $EXIT_CODE == 0 ]] && pass "SIGTERM exited 0" || fail "SIGTERM exit=$EXIT_CODE"
	assert_clean "after SIGTERM"
}

# A preflight failure must not stop Kafka/Redis containers it never started.
scenario_preflight_leaves_infra() {
	say "preflight failure leaves already-running infrastructure alone"
	docker compose --project-directory "$ROOT" up -d --wait kafka redis >/dev/null 2>&1 || { fail "could not start infra for setup"; return; }
	start_stack KAFKA_PARTITIONS=0
	wait_exit 30
	[[ $EXIT_CODE == 1 ]] && grep -q 'KAFKA_PARTITIONS must be a positive integer' "$WORK/out" &&
		pass "invalid KAFKA_PARTITIONS rejected" || fail "invalid partitions: exit=$EXIT_CODE"
	local k r
	k=$(docker inspect -f '{{.State.Running}}' dispatch-kafka)
	r=$(docker inspect -f '{{.State.Running}}' dispatch-redis)
	[[ $k == true && $r == true ]] && pass "running containers untouched" || fail "preflight failure stopped containers (kafka=$k redis=$r)"
	docker compose --project-directory "$ROOT" stop kafka redis >/dev/null 2>&1
}

main() {
	command -v grpcurl >/dev/null || { echo "grpcurl is required: go install github.com/fullstorydev/grpcurl/cmd/grpcurl@v1.9.3" >&2; exit 1; }
	# Start from stopped project containers; anything else on these ports is foreign.
	docker compose --project-directory "$ROOT" stop kafka redis >/dev/null 2>&1 || true
	local port
	for port in "${PORTS[@]}"; do
		if port_in_use "$port"; then
			echo "port $port is busy; stop whatever holds it before running this test" >&2
			exit 1
		fi
	done
	if (($#)); then # run named scenarios only, e.g. double_interrupt
		local name
		for name in "$@"; do "scenario_$name"; done
	else
		scenario_preflight
		scenario_preflight_leaves_infra
		scenario_ready_and_interrupt
		scenario_double_interrupt
		scenario_sigterm
		scenario_component_dies worker
		scenario_component_dies dispatch
		scenario_kafka_dies
	fi
	echo
	echo "stack scenarios: $PASSED passed, $FAILED failed"
	((FAILED == 0))
}

main "$@"
