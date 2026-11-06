//go:build integration

package worker

import (
	"context"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	taskv1 "github.com/ronnitchopra/dispatch/gen/task/v1"
	"github.com/ronnitchopra/dispatch/internal/broker"
	"github.com/ronnitchopra/dispatch/internal/server"
	"github.com/ronnitchopra/dispatch/internal/state"
	"github.com/ronnitchopra/dispatch/internal/testutil"
	"github.com/segmentio/kafka-go"
	"google.golang.org/grpc"
)

// closeProgressStore makes the real server's Redis client fail exactly when it
// persists the selected update. The worker uses its own healthy Redis client.
type closeProgressStore struct {
	state.TaskStore
	client *redis.Client
	status int32
	closed atomic.Bool
}

func (s *closeProgressStore) UpdateProgress(ctx context.Context, id string, status, progress int32) error {
	if status == s.status && !s.closed.Swap(true) {
		s.client.Close()
	}
	return s.TaskStore.UpdateProgress(ctx, id, status, progress)
}

func TestConsumer_ProgressRejectionPreservesKafkaOffset(t *testing.T) {
	for _, rejected := range []taskv1.TaskStatus{
		taskv1.TaskStatus_TASK_STATUS_RUNNING,
		taskv1.TaskStatus_TASK_STATUS_COMPLETED,
	} {
		t.Run(rejected.String(), func(t *testing.T) {
			t.Parallel()
			addr, topic := testutil.KafkaTopic(t)
			client := testutil.RedisClient(t)
			store := state.NewRedisTaskStore(client)
			t.Cleanup(func() { client.Del(context.Background(), "task:"+topic) })
			ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
			defer cancel()
			if err := store.SaveTask(ctx, state.TaskRecord{ID: topic, TaskType: "simulation", MaxRetries: 1,
				Status: int32(taskv1.TaskStatus_TASK_STATUS_PENDING), CreatedAt: time.Now(), UpdatedAt: time.Now()}); err != nil {
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

			serverClient := testutil.RedisClient(t)
			failing := &closeProgressStore{TaskStore: state.NewRedisTaskStore(serverClient), client: serverClient, status: int32(rejected)}
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			grpcServer := grpc.NewServer()
			taskv1.RegisterDispatchServiceServer(grpcServer, server.New(logger, producer, failing))
			go grpcServer.Serve(listener)
			t.Cleanup(grpcServer.Stop)
			reporter, err := NewProgressReporter(ctx, listener.Addr().String(), logger)
			if err != nil {
				t.Fatal(err)
			}
			defer reporter.Close()
			dlq := &mockDLQPublisher{}
			consumer := NewConsumer(addr, topic, topic, "progress-worker", reporter, store, dlq, logger)
			defer consumer.Close()
			executions := 0
			consumer.failureDecider = func() bool { executions++; return false }
			reader := &observedKafkaReader{Reader: consumer.reader.(*kafka.Reader), cancel: cancel}
			consumer.reader = reader
			runErr := consumer.Run(ctx)
			offset := committedGroupOffset(t, addr, topic, topic)
			if !failing.closed.Load() {
				t.Fatal("the actual server never reached the injected Redis write failure")
			}
			t.Logf("rejected=%s Run=%v executions=%d commits=%v broker next offset=%d", rejected, runErr, executions, reader.committed, offset)
			if runErr == nil || !strings.Contains(runErr.Error(), "not recorded") || !strings.Contains(runErr.Error(), rejected.String()) {
				t.Errorf("negative gRPC acknowledgment was swallowed: %v", runErr)
			}
			wantExecutions := 0
			if rejected == taskv1.TaskStatus_TASK_STATUS_COMPLETED {
				wantExecutions = 1
			}
			if executions != wantExecutions || len(dlq.published) != 0 {
				t.Errorf("rejected update advanced execution: executions=%d DLQ=%v", executions, dlq.published)
			}
			if len(reader.committed) != 0 || offset > 0 {
				t.Fatalf("unrecorded progress acknowledged source work: commits=%v next offset=%d", reader.committed, offset)
			}
			if err := consumer.Close(); err != nil {
				t.Fatal(err)
			}
			replay := kafka.NewReader(kafka.ReaderConfig{
				Brokers: []string{addr}, Topic: topic, GroupID: topic, MinBytes: 1, MaxBytes: 10e6,
			})
			defer replay.Close()
			replayCtx, stopReplay := context.WithTimeout(context.Background(), 20*time.Second)
			defer stopReplay()
			message, err := replay.FetchMessage(replayCtx)
			if err != nil || message.Offset != 0 || string(message.Key) != topic {
				t.Fatalf("unrecorded work was not replayable: offset=%d key=%q error=%v", message.Offset, message.Key, err)
			}
		})
	}
}
