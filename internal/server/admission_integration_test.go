//go:build integration

package server_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	taskv1 "github.com/ronnitchopra/dispatch/gen/task/v1"
	"github.com/ronnitchopra/dispatch/internal/broker"
	"github.com/ronnitchopra/dispatch/internal/server"
	"github.com/ronnitchopra/dispatch/internal/state"
	"github.com/ronnitchopra/dispatch/internal/testutil"
	"github.com/segmentio/kafka-go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// Snapshot both reads before either caller can write. The underlying reads,
// writes, and resulting Kafka publications are real.
type simultaneousAdmissionStore struct {
	state.PublicationStore
	reads atomic.Int32
	ready chan struct{}
}

func admissionServer(t *testing.T, producer broker.Producer, store state.PublicationStore, topic string) *server.DispatchServer {
	t.Helper()
	return server.New(slog.New(slog.NewTextHandler(io.Discard, nil)), producer, store, topic)
}

type controlledPublication struct {
	entered chan struct{}
	release chan struct{}
	cancel  context.CancelFunc
	calls   atomic.Int32
}

func (p *controlledPublication) Publish(ctx context.Context, _ string, _ *taskv1.Task) error {
	p.calls.Add(1)
	if p.entered != nil {
		close(p.entered)
		select {
		case <-p.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if p.cancel != nil {
		p.cancel()
		return context.Canceled
	}
	return errors.New("publication unavailable")
}

func (*controlledPublication) Close() error { return nil }

func TestSubmitTask_PendingDuplicateIsNotAccepted(t *testing.T) {
	client := testutil.RedisClient(t)
	id := "pending-admission-" + uuid.NewString()
	t.Cleanup(func() { client.Del(context.Background(), "task:"+id, "publication:"+id, "outbox:"+id) })
	producer := &controlledPublication{entered: make(chan struct{}), release: make(chan struct{})}
	srv := admissionServer(t, producer, state.NewRedisTaskStore(client), id)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req := &taskv1.SubmitTaskRequest{Task: &taskv1.Task{IdempotencyKey: id, Type: "simulation"}}
	first := make(chan error, 1)
	go func() { _, err := srv.SubmitTask(ctx, req); first <- err }()
	select {
	case <-producer.entered:
	case <-ctx.Done():
		t.Fatal("first submission never reached publication")
	}
	response, err := srv.SubmitTask(ctx, req)
	close(producer.release)
	if firstErr := <-first; firstErr == nil {
		t.Fatal("publication failure setup did not fail")
	}
	if status.Code(err) != codes.Unavailable || response != nil {
		t.Fatalf("an unconfirmed record was accepted: response=%v error=%v", response, err)
	}
}

func TestSubmitTask_PublicationFailureRetainsIntent(t *testing.T) {
	for _, interrupted := range []bool{false, true} {
		name := "broker_error"
		if interrupted {
			name = "cancelled_request"
		}
		t.Run(name, func(t *testing.T) {
			client := testutil.RedisClient(t)
			id := "retained-admission-" + uuid.NewString()
			t.Cleanup(func() { client.Del(context.Background(), "task:"+id, "publication:"+id, "outbox:"+id) })
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			producer := &controlledPublication{}
			if interrupted {
				producer.cancel = cancel
			}
			srv := admissionServer(t, producer, state.NewRedisTaskStore(client), id)
			task := &taskv1.Task{IdempotencyKey: id, Type: "original", Payload: []byte("retained payload"), MaxRetries: 2}
			if _, err := srv.SubmitTask(ctx, &taskv1.SubmitTaskRequest{Task: task}); err == nil {
				t.Fatal("unconfirmed publication must not be accepted")
			}
			query := context.Background()
			if state, err := client.HGet(query, "task:"+id, "publication_state").Result(); err != nil || state != "pending" {
				t.Errorf("canonical pending admission was lost: state=%q error=%v", state, err)
			}
			payload, err := client.HGet(query, "publication:"+id, "payload").Bytes()
			var retained taskv1.Task
			if err != nil || proto.Unmarshal(payload, &retained) != nil || !proto.Equal(task, &retained) {
				t.Errorf("original publication intent was not retained: task=%v error=%v", &retained, err)
			}
			if owner, err := client.HGet(query, "publication:"+id, "owner").Result(); err != nil || owner != "" {
				t.Errorf("failed attempt was not safely released: owner=%q error=%v", owner, err)
			}
			response, err := srv.SubmitTask(query, &taskv1.SubmitTaskRequest{Task: task})
			if status.Code(err) != codes.Unavailable || response != nil || producer.calls.Load() != 1 {
				t.Errorf("pending retry was falsely accepted or republished: response=%v error=%v calls=%d", response, err, producer.calls.Load())
			}
		})
	}
}

type acknowledgedPublication struct{ afterAck func() }

func (p *acknowledgedPublication) Publish(context.Context, string, *taskv1.Task) error {
	if p.afterAck != nil {
		p.afterAck()
	}
	return nil
}
func (*acknowledgedPublication) Close() error { return nil }

type retryingPublication struct {
	next  broker.Producer
	calls atomic.Int32
}

func (p *retryingPublication) Publish(ctx context.Context, id string, task *taskv1.Task) error {
	if p.calls.Add(1) <= 2 {
		return errors.New("temporary broker failure")
	}
	return p.next.Publish(ctx, id, task)
}
func (p *retryingPublication) Close() error { return p.next.Close() }

func TestPublicationRecovery_RetriesBrokerFailure(t *testing.T) {
	addr, topic := testutil.KafkaTopic(t)
	client := testutil.RedisClient(t)
	store := state.NewRedisTaskStore(client)
	t.Cleanup(func() { client.Del(context.Background(), "task:"+topic, "publication:"+topic, "outbox:"+topic) })
	producer := &retryingPublication{next: broker.NewKafkaProducer(addr, topic, slog.New(slog.NewTextHandler(io.Discard, nil)))}
	defer producer.Close()
	srv := admissionServer(t, producer, store, topic)
	task := &taskv1.Task{IdempotencyKey: topic, Type: "original", Payload: []byte("recovered unchanged")}
	if _, err := srv.SubmitTask(context.Background(), &taskv1.SubmitTaskRequest{Task: task}); status.Code(err) != codes.Unavailable {
		t.Fatalf("failed first publication was accepted: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- srv.RecoverPublications(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(4 * time.Second):
			t.Error("recovery did not stop")
		}
	})
	reader := kafka.NewReader(kafka.ReaderConfig{Brokers: []string{addr}, Topic: topic, Partition: 0, MinBytes: 1, MaxBytes: 10e6})
	defer reader.Close()
	message, err := reader.ReadMessage(ctx)
	if err != nil {
		t.Fatalf("temporary recovery error lost the retained task: %v", err)
	}
	var got taskv1.Task
	if err := proto.Unmarshal(message.Value, &got); err != nil || !proto.Equal(task, &got) {
		t.Fatalf("recovery changed payload: %v error=%v", &got, err)
	}
	for {
		if record, err := store.GetTask(ctx, topic); err == nil && record.PublicationState == "published" {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("recovery never confirmed publication")
		case <-time.After(10 * time.Millisecond):
		}
	}
	if producer.calls.Load() != 3 {
		t.Fatalf("unexpected publish attempts: %d", producer.calls.Load())
	}
	response, err := srv.SubmitTask(ctx, &taskv1.SubmitTaskRequest{Task: task})
	if err != nil || response.GetStatus() != taskv1.SubmitStatus_SUBMIT_STATUS_ACCEPTED {
		t.Fatalf("confirmed recovery was not accepted: %v %v", response, err)
	}
}

func TestSubmitTask_ConfirmationSurvivesRequestCancellation(t *testing.T) {
	client := testutil.RedisClient(t)
	id := "confirmed-admission-" + uuid.NewString()
	t.Cleanup(func() { client.Del(context.Background(), "task:"+id, "publication:"+id, "outbox:"+id) })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	srv := admissionServer(t, &acknowledgedPublication{afterAck: cancel}, state.NewRedisTaskStore(client), id)
	response, err := srv.SubmitTask(ctx, &taskv1.SubmitTaskRequest{Task: &taskv1.Task{IdempotencyKey: id, Type: "simulation"}})
	confirmed, getErr := client.HGet(context.Background(), "task:"+id, "publication_state").Result()
	if err != nil || getErr != nil || response.GetStatus() != taskv1.SubmitStatus_SUBMIT_STATUS_ACCEPTED || confirmed != "published" {
		t.Fatalf("acknowledged publication was not finalized independently: response=%v error=%v state=%q get=%v", response, err, confirmed, getErr)
	}
}

func (s *simultaneousAdmissionStore) GetTask(ctx context.Context, id string) (state.TaskRecord, error) {
	record, err := s.PublicationStore.GetTask(ctx, id)
	if n := s.reads.Add(1); n <= 2 {
		if n == 2 {
			close(s.ready)
		}
		select {
		case <-s.ready:
		case <-ctx.Done():
			return state.TaskRecord{}, ctx.Err()
		}
	}
	return record, err
}

func TestSubmitTask_ConcurrentKeyPublishesOnce(t *testing.T) {
	addr, topic := testutil.KafkaTopic(t)
	client := testutil.RedisClient(t)
	store := state.NewRedisTaskStore(client)
	t.Cleanup(func() { client.Del(context.Background(), "task:"+topic, "publication:"+topic, "outbox:"+topic) })
	producer := broker.NewKafkaProducer(addr, topic, slog.New(slog.NewTextHandler(io.Discard, nil)))
	defer producer.Close()
	srv := admissionServer(t, producer, &simultaneousAdmissionStore{PublicationStore: store, ready: make(chan struct{})}, topic)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	type result struct {
		response *taskv1.SubmitTaskResponse
		err      error
	}
	results := make(chan result, 2)
	for i := 0; i < 2; i++ {
		go func() {
			response, err := srv.SubmitTask(ctx, &taskv1.SubmitTaskRequest{
				Task: &taskv1.Task{IdempotencyKey: topic, Type: "simulation"},
			})
			results <- result{response, err}
		}()
	}
	accepted := 0
	for i := 0; i < 2; i++ {
		result := <-results
		if result.err == nil && result.response.GetStatus() == taskv1.SubmitStatus_SUBMIT_STATUS_ACCEPTED {
			accepted++
		}
	}
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
	if end != 1 || accepted < 1 {
		t.Fatalf("concurrent same-key admission: Kafka records=%d accepted=%d; want one publication", end, accepted)
	}
}
