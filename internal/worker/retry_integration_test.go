//go:build integration

package worker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	taskv1 "github.com/ronnitchopra/dispatch/gen/task/v1"
	"github.com/ronnitchopra/dispatch/internal/broker"
	"github.com/ronnitchopra/dispatch/internal/state"
	"github.com/ronnitchopra/dispatch/internal/testutil"
	"github.com/segmentio/kafka-go"
)

func TestConsumer_RetryBudgetSurvivesConsumerRestart(t *testing.T) {
	addr, topic := testutil.KafkaTopic(t)
	client := testutil.RedisClient(t)
	store := state.NewRedisTaskStore(client)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	t.Cleanup(func() { client.Del(context.Background(), "task:"+topic) })
	if err := store.SaveTask(ctx, state.TaskRecord{ID: topic, TaskType: "simulation", MaxRetries: 1,
		Status: 1, CreatedAt: time.Now(), UpdatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	producer := broker.NewKafkaProducer(addr, topic, logger)
	defer producer.Close()
	if err := testutil.RetryKafkaMetadata(ctx, func() error {
		return producer.Publish(ctx, topic, &taskv1.Task{Type: "simulation", MaxRetries: 1})
	}); err != nil {
		t.Fatal(err)
	}
	interrupted := errors.New("progress connection interrupted")
	first := NewConsumer(addr, topic, topic, "first", &mockSender{err: interrupted}, store, nil, logger)
	if err := first.Run(ctx); !errors.Is(err, interrupted) {
		first.Close()
		t.Fatalf("first attempt must be interrupted before a commit: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if count, err := client.HGet(ctx, "task:"+topic, "attempts").Int64(); err != nil || count != 1 {
		t.Errorf("interrupted started attempt was not persisted: attempts=%d error=%v", count, err)
	}

	dlq := &mockDLQPublisher{}
	// A separate client/store rules out retaining the budget in consumer memory.
	replacementStore := state.NewRedisTaskStore(testutil.RedisClient(t))
	second := NewConsumer(addr, topic, topic, "replacement", &mockSender{}, replacementStore, dlq, logger)
	defer second.Close()
	attempts := 0
	second.failureDecider = func() bool { attempts++; return true }
	reader := &observedKafkaReader{Reader: second.reader.(*kafka.Reader), cancel: cancel}
	second.reader = reader
	if err := second.Run(ctx); err != nil {
		t.Errorf("last permitted attempt should exhaust the budget and quarantine: %v", err)
	}
	queryCtx, stopQuery := context.WithTimeout(context.Background(), 3*time.Second)
	defer stopQuery()
	count, countErr := client.HGet(queryCtx, "task:"+topic, "attempts").Int64()
	t.Logf("replacement executions=%d stored attempts=%d DLQ=%v commits=%v", attempts, count, dlq.published, reader.committed)
	if countErr != nil || count != 2 || attempts != 1 || len(dlq.published) != 1 || len(reader.committed) != 1 {
		t.Fatalf("retry budget reset or lost: stored=%d error=%v executions=%d DLQ=%v commits=%v", count, countErr, attempts, dlq.published, reader.committed)
	}
}

func TestConsumer_CompletedReplayDoesNotSpendAttempt(t *testing.T) {
	client := testutil.RedisClient(t)
	store := state.NewRedisTaskStore(client)
	id := "completed-replay-" + time.Now().Format("20060102150405.000000000")
	t.Cleanup(func() { client.Del(context.Background(), "task:"+id) })
	if err := store.SaveTask(context.Background(), state.TaskRecord{ID: id, TaskType: "simulation",
		Status: int32(taskv1.TaskStatus_TASK_STATUS_COMPLETED), Progress: 100,
		CreatedAt: time.Now(), UpdatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	sender, dlq := &mockSender{}, &mockDLQPublisher{}
	c := newTestConsumer(sender)
	c.attempts, c.dlqPublisher = store, dlq
	if err := c.processMessage(context.Background(), []byte(id), marshalTask(t, &taskv1.Task{Type: "simulation"})); err != nil {
		t.Fatal(err)
	}
	exists, err := client.HExists(context.Background(), "task:"+id, "attempts").Result()
	if err != nil || exists || sender.callCount != 0 || len(dlq.published) != 0 {
		t.Fatalf("completed replay executed or was quarantined: attempts=%v error=%v updates=%v DLQ=%v", exists, err, sender.updates, dlq.published)
	}
}

func TestConsumer_ExhaustedReplayReportsFailureWithoutExecution(t *testing.T) {
	client := testutil.RedisClient(t)
	store := state.NewRedisTaskStore(client)
	id := "exhausted-replay-" + time.Now().Format("20060102150405.000000000")
	ctx := context.Background()
	t.Cleanup(func() { client.Del(ctx, "task:"+id) })
	if err := store.SaveTask(ctx, state.TaskRecord{ID: id, TaskType: "simulation",
		Status: int32(taskv1.TaskStatus_TASK_STATUS_RUNNING), CreatedAt: time.Now(), UpdatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if err := client.HSet(ctx, "task:"+id, "attempts", 1).Err(); err != nil {
		t.Fatal(err)
	}
	sender, dlq := &mockSender{}, &mockDLQPublisher{}
	c := newTestConsumer(sender)
	c.attempts, c.dlqPublisher = store, dlq
	executions := 0
	c.failureDecider = func() bool { executions++; return true }
	if err := c.processMessage(ctx, []byte(id), marshalTask(t, &taskv1.Task{Type: "simulation"})); err != nil {
		t.Fatal(err)
	}
	if executions != 0 || len(dlq.published) != 1 || sender.callCount != 1 || sender.updates[0].GetStatus() != taskv1.TaskStatus_TASK_STATUS_FAILED {
		t.Fatalf("exhausted replay must report terminal failure without re-execution: executions=%d updates=%v DLQ=%v", executions, sender.updates, dlq.published)
	}
}
