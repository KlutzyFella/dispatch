//go:build integration

package server_test

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	taskv1 "github.com/ronnitchopra/dispatch/gen/task/v1"
	"github.com/ronnitchopra/dispatch/internal/broker"
	"github.com/ronnitchopra/dispatch/internal/state"
	"github.com/ronnitchopra/dispatch/internal/testutil"
	"github.com/segmentio/kafka-go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// countingPublication records publish attempts per task while delegating to a
// real producer, so tests can see retries without replacing broker behaviour.
type countingPublication struct {
	next  broker.Producer
	mu    sync.Mutex
	calls map[string]int
}

func (p *countingPublication) Publish(ctx context.Context, id string, task *taskv1.Task) error {
	p.mu.Lock()
	p.calls[id]++
	p.mu.Unlock()
	return p.next.Publish(ctx, id, task)
}

func (p *countingPublication) Close() error { return p.next.Close() }

func (p *countingPublication) attempts(id string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls[id]
}

// recoveryRun is a recovery loop started by a test. done closes when the loop
// returns; err is valid after that.
type recoveryRun struct {
	stop context.CancelFunc
	done chan struct{}
	err  error
}

// startRecovery runs the recovery loop until stopped; cleanup stops and joins it.
func startRecovery(t *testing.T, ctx context.Context, srv interface {
	RecoverPublications(context.Context) error
}) *recoveryRun {
	t.Helper()
	ctx, cancel := context.WithCancel(ctx)
	run := &recoveryRun{stop: cancel, done: make(chan struct{})}
	go func() {
		defer close(run.done)
		run.err = srv.RecoverPublications(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-run.done:
		case <-time.After(5 * time.Second):
			t.Error("recovery loop did not stop after cancellation")
		}
	})
	return run
}

// TestPublicationRecovery_PoisonIntentDoesNotStarveOthers exercises the core
// admission invariant under a permanent broker rejection: every pending intent
// the broker accepts is published exactly once with its original body, while
// the rejected one stays pending and retried rather than being dropped or
// blocking the topic's recovery queue.
func TestPublicationRecovery_PoisonIntentDoesNotStarveOthers(t *testing.T) {
	addr, topic := testutil.KafkaTopic(t, kafka.ConfigEntry{ConfigName: "max.message.bytes", ConfigValue: "1024"})
	client := testutil.RedisClient(t)
	store := state.NewRedisTaskStore(client)
	poison := "poison-" + uuid.NewString()
	good := []string{"good-a-" + uuid.NewString(), "good-b-" + uuid.NewString(), "good-c-" + uuid.NewString()}
	t.Cleanup(func() {
		keys := []string{"outbox:" + topic}
		for _, id := range append([]string{poison}, good...) {
			keys = append(keys, "task:"+id, "publication:"+id)
		}
		client.Del(context.Background(), keys...)
	})

	// Admit all four while the broker is unreachable from the submitter, so each
	// is a retained pending intent. The poison intent is admitted first and is
	// therefore the first one due for recovery.
	failing := admissionServer(t, &controlledPublication{}, store, topic)
	tasks := map[string]*taskv1.Task{
		poison: {IdempotencyKey: poison, Type: "oversized", Payload: bytes.Repeat([]byte("x"), 4096)},
	}
	for _, id := range good {
		tasks[id] = &taskv1.Task{IdempotencyKey: id, Type: "original", Payload: []byte("body of " + id)}
	}
	for _, id := range append([]string{poison}, good...) {
		if _, err := failing.SubmitTask(context.Background(), &taskv1.SubmitTaskRequest{Task: tasks[id]}); status.Code(err) != codes.Unavailable {
			t.Fatalf("setup: unpublished %s was not left pending: %v", id, err)
		}
	}

	producer := &countingPublication{next: broker.NewKafkaProducer(addr, topic, slog.New(slog.NewTextHandler(io.Discard, nil))), calls: map[string]int{}}
	defer producer.Close()
	recovering := admissionServer(t, producer, store, topic)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	run := startRecovery(t, ctx, recovering)

	for _, id := range good {
		waitForPublicationState(t, ctx, store, id, "published")
	}
	// Give the loop time to retry the poison intent after the others finished.
	deadline := time.Now().Add(10 * time.Second)
	for producer.attempts(poison) < 2 && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	select {
	case <-run.done:
		t.Fatalf("a permanent broker rejection stopped recovery: %v", run.err)
	default:
	}
	if n := producer.attempts(poison); n < 2 {
		t.Fatalf("rejected intent was not retried: attempts=%d", n)
	}

	reader := kafka.NewReader(kafka.ReaderConfig{Brokers: []string{addr}, Topic: topic, Partition: 0, MinBytes: 1, MaxBytes: 10e6})
	defer reader.Close()
	seen := map[string]int{}
	for range good {
		message, err := reader.ReadMessage(ctx)
		if err != nil {
			t.Fatalf("reading recovered publications: %v", err)
		}
		var got taskv1.Task
		if err := proto.Unmarshal(message.Value, &got); err != nil || !proto.Equal(tasks[string(message.Key)], &got) {
			t.Fatalf("recovered %q with a changed body: %v (error %v)", message.Key, &got, err)
		}
		seen[string(message.Key)]++
	}
	end := lastOffset(t, ctx, addr, topic)
	for _, id := range good {
		if seen[id] != 1 || producer.attempts(id) != 1 {
			t.Errorf("%s: Kafka records=%d publish attempts=%d; want exactly one", id, seen[id], producer.attempts(id))
		}
	}
	if end != int64(len(good)) {
		t.Errorf("Kafka log holds %d records; want %d (rejected intent must not appear)", end, len(good))
	}

	record, err := store.GetTask(ctx, poison)
	if err != nil || record.PublicationState != "pending" {
		t.Fatalf("rejected intent lost its pending state: %+v error=%v", record, err)
	}
	payload, err := client.HGet(ctx, "publication:"+poison, "payload").Bytes()
	var retained taskv1.Task
	if err != nil || proto.Unmarshal(payload, &retained) != nil || !proto.Equal(tasks[poison], &retained) {
		t.Fatalf("rejected intent's original body was not retained: error=%v", err)
	}
	if indexed, err := client.ZScore(ctx, "outbox:"+topic, poison).Result(); err != nil || indexed == 0 {
		t.Fatalf("rejected intent left the recovery index: score=%v error=%v", indexed, err)
	}
	t.Logf("poison attempts=%d; good published once each; Kafka records=%d", producer.attempts(poison), end)
}

// blockingPublication holds a publish until its context ends, standing in for
// a broker write that is still in flight when dispatch shuts down.
type blockingPublication struct{ entered chan struct{} }

func (p *blockingPublication) Publish(ctx context.Context, _ string, _ *taskv1.Task) error {
	close(p.entered)
	<-ctx.Done()
	return ctx.Err()
}

func (*blockingPublication) Close() error { return nil }

// TestPublicationRecovery_ShutdownReleasesClaim checks that cancelling the loop
// during an in-flight recovery publish releases the claim. Otherwise the intent
// would stay owned by a stopped process for the full lease before a restart
// could retry it.
func TestPublicationRecovery_ShutdownReleasesClaim(t *testing.T) {
	client := testutil.RedisClient(t)
	store := state.NewRedisTaskStore(client)
	id := "shutdown-recovery-" + uuid.NewString()
	topic := id
	t.Cleanup(func() { client.Del(context.Background(), "task:"+id, "publication:"+id, "outbox:"+topic) })
	task := &taskv1.Task{IdempotencyKey: id, Type: "original", Payload: []byte("in flight at shutdown")}
	if _, err := admissionServer(t, &controlledPublication{}, store, topic).SubmitTask(context.Background(),
		&taskv1.SubmitTaskRequest{Task: task}); status.Code(err) != codes.Unavailable {
		t.Fatalf("setup: unpublished task was not left pending: %v", err)
	}

	producer := &blockingPublication{entered: make(chan struct{})}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	run := startRecovery(t, ctx, admissionServer(t, producer, store, topic))
	select {
	case <-producer.entered:
	case <-ctx.Done():
		t.Fatal("recovery never claimed the pending intent")
	}
	if owner, err := client.HGet(ctx, "publication:"+id, "owner").Result(); err != nil || owner == "" {
		t.Fatalf("setup: in-flight recovery publish holds no claim: owner=%q error=%v", owner, err)
	}
	run.stop()
	select {
	case <-run.done:
		if run.err != nil {
			t.Fatalf("cancellation was reported as a recovery failure: %v", run.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("recovery loop did not return after cancellation")
	}

	query := context.Background()
	if owner, err := client.HGet(query, "publication:"+id, "owner").Result(); err != nil || owner != "" {
		t.Errorf("stopped recovery kept its claim: owner=%q error=%v", owner, err)
	}
	now, err := client.Time(query).Result()
	if err != nil {
		t.Fatal(err)
	}
	score, err := client.ZScore(query, "outbox:"+topic, id).Result()
	if err != nil {
		t.Fatalf("intent left the recovery index on shutdown: %v", err)
	}
	// Released intents become due after the 1s retry delay, not the 15s lease.
	if wait := time.Duration(score-float64(now.UnixMilli())) * time.Millisecond; wait > 5*time.Second {
		t.Errorf("released intent is not due for %v; a restart would wait out the lease", wait)
	}
	if record, err := store.GetTask(query, id); err != nil || record.PublicationState != "pending" {
		t.Errorf("shutdown changed publication state: %+v error=%v", record, err)
	}
}

// TestPublicationRecovery_StoreFailureStopsLoop checks the fail-loud policy: a
// recovery loop that can no longer reach Redis returns an error instead of
// spinning silently behind a live API.
func TestPublicationRecovery_StoreFailureStopsLoop(t *testing.T) {
	client := testutil.RedisClient(t)
	topic := "store-failure-" + uuid.NewString()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	run := startRecovery(t, ctx, admissionServer(t, &controlledPublication{}, state.NewRedisTaskStore(client), topic))
	time.Sleep(300 * time.Millisecond) // let at least one idle poll succeed
	select {
	case <-run.done:
		t.Fatalf("setup: recovery stopped before the store failed: %v", run.err)
	default:
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-run.done:
		if run.err == nil || !strings.Contains(run.err.Error(), "publication recovery") {
			t.Fatalf("store failure was not reported as a recovery error: %v", run.err)
		}
	case <-ctx.Done():
		t.Fatal("recovery kept running after its store became unusable")
	}
}

func waitForPublicationState(t *testing.T, ctx context.Context, store state.TaskStore, id, want string) {
	t.Helper()
	for {
		if record, err := store.GetTask(ctx, id); err == nil && record.PublicationState == want {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("%s never reached publication state %q", id, want)
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func lastOffset(t *testing.T, ctx context.Context, addr, topic string) int64 {
	t.Helper()
	leader, err := kafka.DialLeader(ctx, "tcp", addr, topic, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer leader.Close()
	deadline, _ := ctx.Deadline()
	leader.SetDeadline(deadline)
	end, err := leader.ReadLastOffset()
	if err != nil {
		t.Fatal(err)
	}
	return end
}
