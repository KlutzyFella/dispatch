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
		failTask  bool
		reportErr error
	}{
		{name: "corrupt_protobuf", body: []byte("not-protobuf")},
		{name: "task_failure", body: valid, failTask: true},
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
			// Isolate offset handling from the separately identified Redis lock and
			// retry-budget defects. Processing and all Kafka operations remain real.
			consumer := NewConsumer(addr, topic, topic, "test-worker", sender, nil,
				&mockDLQPublisher{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
			attempts := 0
			consumer.failureDecider = func() bool {
				attempts++
				return tc.failTask && attempts == 1
			}
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
