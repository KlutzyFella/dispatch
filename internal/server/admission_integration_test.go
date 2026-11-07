//go:build integration

package server_test

import (
	"context"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	taskv1 "github.com/ronnitchopra/dispatch/gen/task/v1"
	"github.com/ronnitchopra/dispatch/internal/broker"
	"github.com/ronnitchopra/dispatch/internal/state"
	"github.com/ronnitchopra/dispatch/internal/testutil"
	"github.com/segmentio/kafka-go"
)

// Snapshot both reads before either caller can write. The underlying reads,
// writes, and resulting Kafka publications are real.
type simultaneousAdmissionStore struct {
	state.TaskStore
	reads atomic.Int32
	ready chan struct{}
}

func (s *simultaneousAdmissionStore) GetTask(ctx context.Context, id string) (state.TaskRecord, error) {
	record, err := s.TaskStore.GetTask(ctx, id)
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
	t.Cleanup(func() { client.Del(context.Background(), "task:"+topic) })
	producer := broker.NewKafkaProducer(addr, topic, slog.New(slog.NewTextHandler(io.Discard, nil)))
	defer producer.Close()
	srv := newTestServer(t, producer, &simultaneousAdmissionStore{TaskStore: store, ready: make(chan struct{})})
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
