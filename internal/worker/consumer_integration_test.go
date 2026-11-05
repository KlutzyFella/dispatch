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

// observedKafkaReader delegates to a real reader. Cancelling after a commit
// bounds the old, unsafe consumption loop without changing broker semantics.
type observedKafkaReader struct {
	*kafka.Reader
	cancel    context.CancelFunc
	fetched   []kafka.Message
	committed []int64
}

func TestConsumer_Run_ReplaysInterruptedTaskWithLegacyLock(t *testing.T) {
	addr, topic := testutil.KafkaTopic(t)
	client := testutil.RedisClient(t)
	store := state.NewRedisTaskStore(client)
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	id := topic
	t.Cleanup(func() { client.Del(context.Background(), "task:"+id, "lock:task:"+id) })
	if err := store.SaveTask(ctx, state.TaskRecord{ID: id, TaskType: "simulation", MaxRetries: 1, Status: 1,
		CreatedAt: time.Now(), UpdatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	publisher := broker.NewKafkaProducer(addr, topic, logger)
	defer publisher.Close()
	if err := testutil.RetryKafkaMetadata(ctx, func() error {
		return publisher.Publish(ctx, id, &taskv1.Task{Type: "simulation", MaxRetries: 1})
	}); err != nil {
		t.Fatal(err)
	}

	reportErr := errors.New("interrupted before work")
	first := NewConsumer(addr, topic, topic, "first-worker", &mockSender{err: reportErr}, store, nil, logger)
	if err := first.Run(ctx); !errors.Is(err, reportErr) {
		first.Close()
		t.Fatalf("first run must leave interrupted work uncommitted, got %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	// Preserve the exact stale key produced by older workers, including when the
	// new implementation no longer creates it on the first interrupted attempt.
	if err := client.Set(ctx, "lock:task:"+id, 1, 5*time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	sender := &mockSender{}
	second := NewConsumer(addr, topic, topic, "second-worker", sender, store, nil, logger)
	defer second.Close()
	second.failureDecider = func() bool { return false }
	reader := &observedKafkaReader{Reader: second.reader.(*kafka.Reader), cancel: cancel}
	second.reader = reader
	if err := second.Run(ctx); err != nil {
		t.Fatal(err)
	}
	t.Logf("replayed task: updates=%d commits=%v", sender.callCount, reader.committed)
	if sender.callCount != 2 || sender.updates[1].GetStatus() != taskv1.TaskStatus_TASK_STATUS_COMPLETED {
		t.Fatalf("unfinished task was acknowledged without execution: updates=%v commits=%v", sender.updates, reader.committed)
	}
	if len(reader.committed) != 1 || reader.committed[0] != 0 {
		t.Fatalf("expected completed replay to commit offset 0, got %v", reader.committed)
	}
}

func (r *observedKafkaReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	message, err := r.Reader.FetchMessage(ctx)
	if err == nil {
		r.fetched = append(r.fetched, message)
	}
	return message, err
}

func (r *observedKafkaReader) CommitMessages(ctx context.Context, messages ...kafka.Message) error {
	if err := r.Reader.CommitMessages(ctx, messages...); err != nil {
		return err
	}
	for _, message := range messages {
		r.committed = append(r.committed, message.Offset)
	}
	r.cancel()
	return nil
}

func TestConsumer_Run_UnhandledMessageRemainsReplayable(t *testing.T) {
	valid := marshalTask(t, &taskv1.Task{Type: "simulation", MaxRetries: 1})
	reportErr := errors.New("progress stream broken")
	tests := []struct {
		name      string
		body      []byte
		reportErr error
	}{
		{name: "corrupt_protobuf", body: []byte("not-protobuf")},
		{name: "progress_failure", body: valid, reportErr: reportErr},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			addr, topic := testutil.KafkaTopic(t)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			writer := &kafka.Writer{
				Addr: kafka.TCP(addr), Topic: topic,
				RequiredAcks: kafka.RequireAll, BatchTimeout: time.Millisecond,
			}
			defer writer.Close()
			if err := testutil.RetryKafkaMetadata(ctx, func() error {
				return writer.WriteMessages(ctx,
					kafka.Message{Key: []byte("failed"), Value: tc.body},
					kafka.Message{Key: []byte("healthy"), Value: valid},
				)
			}); err != nil {
				t.Fatalf("seeding partition: %v", err)
			}

			sender := &mockSender{err: tc.reportErr, errOnCall: 1}
			// Real Redis budget/restart behavior has its own integration regression.
			// These cases isolate the Kafka response to transport/decoding errors.
			consumer := NewConsumer(addr, topic, topic, "test-worker", sender, &mockAttemptStore{},
				&mockDLQPublisher{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
			consumer.failureDecider = func() bool { return false }
			reader := &observedKafkaReader{Reader: consumer.reader.(*kafka.Reader), cancel: cancel}
			consumer.reader = reader
			defer consumer.Close()
			runErr := consumer.Run(ctx)

			queryCtx, stopQuery := context.WithTimeout(context.Background(), 5*time.Second)
			defer stopQuery()
			client := &kafka.Client{Addr: kafka.TCP(addr)}
			offsets, err := client.OffsetFetch(queryCtx, &kafka.OffsetFetchRequest{
				GroupID: topic, Topics: map[string][]int{topic: {0}},
			})
			if err != nil {
				t.Fatalf("reading committed broker offset: %v", err)
			}
			if offsets.Error != nil {
				t.Fatal(offsets.Error)
			}
			partitions := offsets.Topics[topic]
			if len(partitions) != 1 || partitions[0].Error != nil {
				t.Fatalf("unexpected offset response: %+v", offsets)
			}
			t.Logf("Run error=%v; fetched=%d; explicit commits=%v; broker next offset=%d",
				runErr, len(reader.fetched), reader.committed, partitions[0].CommittedOffset)
			if runErr == nil {
				t.Error("Run must fail when a message cannot be handled")
			}
			if tc.reportErr != nil && !errors.Is(runErr, tc.reportErr) {
				t.Errorf("Run error = %v, want wrapped %v", runErr, tc.reportErr)
			}
			if len(reader.fetched) != 1 || string(reader.fetched[0].Key) != "failed" {
				t.Errorf("consumer advanced past unresolved work: fetched %+v", reader.fetched)
			}
			if len(reader.committed) != 0 {
				t.Errorf("committed unresolved work via offsets %v", reader.committed)
			}
			if partitions[0].CommittedOffset > 0 {
				t.Fatalf("broker committed past unresolved offset 0: next offset=%d", partitions[0].CommittedOffset)
			}
			if err := consumer.Close(); err != nil {
				t.Fatalf("closing failed consumer: %v", err)
			}

			replay := kafka.NewReader(kafka.ReaderConfig{
				Brokers: []string{addr}, Topic: topic, GroupID: topic,
				MinBytes: 1, MaxBytes: 10e6,
			})
			defer replay.Close()
			replayCtx, stopReplay := context.WithTimeout(context.Background(), 20*time.Second)
			defer stopReplay()
			message, err := replay.FetchMessage(replayCtx)
			if err != nil {
				t.Fatalf("fetching uncommitted work in a new reader: %v", err)
			}
			if message.Offset != 0 || string(message.Key) != "failed" {
				t.Errorf("restart fetched offset %d key %q, want unresolved offset 0 key failed", message.Offset, message.Key)
			}
		})
	}
}
