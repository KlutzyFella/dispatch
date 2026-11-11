//go:build integration

package worker

import (
	"context"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	taskv1 "github.com/ronnitchopra/dispatch/gen/task/v1"
	"github.com/ronnitchopra/dispatch/internal/broker"
	"github.com/ronnitchopra/dispatch/internal/server"
	"github.com/ronnitchopra/dispatch/internal/state"
	"github.com/ronnitchopra/dispatch/internal/testutil"
	"github.com/segmentio/kafka-go"
	"google.golang.org/grpc"
)

// concurrentCompletion plays the other worker of a rebalance: once this
// worker's attempt is RUNNING, a different Redis client records the task as
// COMPLETED, exactly what a worker that received the same message first does.
type concurrentCompletion struct {
	ProgressSender
	other *state.RedisTaskStore
	id    string
	once  sync.Once
	err   error
}

func (c *concurrentCompletion) SendUpdate(update *taskv1.ProgressUpdate) (*taskv1.ProgressAck, error) {
	ack, err := c.ProgressSender.SendUpdate(update)
	if update.GetTaskId() == c.id && update.GetStatus() == taskv1.TaskStatus_TASK_STATUS_RUNNING {
		c.once.Do(func() {
			c.err = c.other.UpdateProgress(context.Background(), c.id, int32(taskv1.TaskStatus_TASK_STATUS_COMPLETED), 100)
		})
	}
	return ack, err
}

// commitCountingReader cancels the run after a given number of commits.
type commitCountingReader struct {
	*kafka.Reader
	cancel    context.CancelFunc
	after     int
	committed []int64
}

func (r *commitCountingReader) CommitMessages(ctx context.Context, messages ...kafka.Message) error {
	if err := r.Reader.CommitMessages(ctx, messages...); err != nil {
		return err
	}
	for _, message := range messages {
		r.committed = append(r.committed, message.Offset)
	}
	if len(r.committed) >= r.after {
		r.cancel()
	}
	return nil
}

// A rebalance can deliver an in-flight, uncommitted message to a second
// worker. If the first copy completes while the second is still running, the
// second's later FAILED update is correctly refused by the terminal guard. That
// refusal means the task is done, not that infrastructure failed: the duplicate
// must be treated as handled. Previously the worker exited, and in the soak
// (8 workers) that stopped the whole stack seconds after start.
func TestConsumer_DuplicateFinishedElsewhereIsHandled(t *testing.T) {
	addr, topic := testutil.KafkaTopic(t)
	client := testutil.RedisClient(t)
	store := state.NewRedisTaskStore(client)
	duplicate, next := "duplicate-"+uuid.NewString(), "next-"+uuid.NewString()
	t.Cleanup(func() { client.Del(context.Background(), "task:"+duplicate, "task:"+next) })
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	for _, id := range []string{duplicate, next} {
		if err := store.SaveTask(ctx, state.TaskRecord{ID: id, TaskType: "simulation", MaxRetries: 1,
			Status: int32(taskv1.TaskStatus_TASK_STATUS_PENDING), CreatedAt: time.Now(), UpdatedAt: time.Now()}); err != nil {
			t.Fatal(err)
		}
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	producer := broker.NewKafkaProducer(addr, topic, logger)
	defer producer.Close()
	for _, id := range []string{duplicate, next} {
		if err := testutil.RetryKafkaMetadata(ctx, func() error {
			return producer.Publish(ctx, id, &taskv1.Task{Type: "simulation", MaxRetries: 1})
		}); err != nil {
			t.Fatal(err)
		}
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	grpcServer := grpc.NewServer()
	taskv1.RegisterDispatchServiceServer(grpcServer, server.New(logger, producer, state.NewRedisTaskStore(testutil.RedisClient(t)), topic))
	go grpcServer.Serve(listener)
	t.Cleanup(grpcServer.Stop)
	reporter, err := NewProgressReporter(ctx, listener.Addr().String(), logger)
	if err != nil {
		t.Fatal(err)
	}
	defer reporter.Close()
	sender := &concurrentCompletion{ProgressSender: reporter, other: state.NewRedisTaskStore(testutil.RedisClient(t)), id: duplicate}
	dlq := &mockDLQPublisher{}
	consumer := NewConsumer(addr, topic, topic, "duplicate-worker", sender, store, dlq, logger)
	defer consumer.Close()
	decisions := 0
	consumer.failureDecider = func() bool { decisions++; return decisions == 1 } // the duplicate's attempt fails
	reader := &commitCountingReader{Reader: consumer.reader.(*kafka.Reader), cancel: cancel, after: 2}
	consumer.reader = reader

	runErr := consumer.Run(ctx)
	if sender.err != nil {
		t.Fatalf("setup: the concurrent completion was not recorded: %v", sender.err)
	}
	if runErr != nil || len(reader.committed) != 2 {
		t.Fatalf("a duplicate that finished elsewhere stopped the worker: Run=%v commits=%v", runErr, reader.committed)
	}
	for _, id := range []string{duplicate, next} {
		record, err := store.GetTask(context.Background(), id)
		if err != nil || record.Status != int32(taskv1.TaskStatus_TASK_STATUS_COMPLETED) {
			t.Errorf("%s: status=%d error=%v; want COMPLETED", id, record.Status, err)
		}
	}
	if len(dlq.published) != 0 {
		t.Errorf("a completed task was quarantined: %v", dlq.published)
	}
}
