//go:build integration

package worker

import (
	"bytes"
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
	"google.golang.org/protobuf/proto"
)

func committedGroupOffset(t *testing.T, addr, topic, group string) int64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client := &kafka.Client{Addr: kafka.TCP(addr)}
	offsets, err := client.OffsetFetch(ctx, &kafka.OffsetFetchRequest{
		GroupID: group, Topics: map[string][]int{topic: {0}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if offsets.Error != nil {
		t.Fatal(offsets.Error)
	}
	partitions := offsets.Topics[topic]
	if len(partitions) != 1 || partitions[0].Error != nil {
		t.Fatalf("unexpected offset response: %+v", offsets)
	}
	return partitions[0].CommittedOffset
}

func TestConsumer_DLQRejectionPreservesWorkUntilRecovery(t *testing.T) {
	addr, topic := testutil.KafkaTopic(t)
	_, rejectedTopic := testutil.KafkaTopic(t, kafka.ConfigEntry{ConfigName: "max.message.bytes", ConfigValue: "1024"})
	_, recoveredTopic := testutil.KafkaTopic(t)
	client := testutil.RedisClient(t)
	store := state.NewRedisTaskStore(client)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	t.Cleanup(func() { client.Del(context.Background(), "task:"+topic) })
	if err := store.SaveTask(ctx, state.TaskRecord{ID: topic, TaskType: "simulation", Status: 1,
		CreatedAt: time.Now(), UpdatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	source := broker.NewKafkaProducer(addr, topic, logger)
	rejected := broker.NewKafkaProducer(addr, rejectedTopic, logger)
	recovered := broker.NewKafkaProducer(addr, recoveredTopic, logger)
	defer source.Close()
	defer rejected.Close()
	defer recovered.Close()
	task := &taskv1.Task{Type: "simulation", Payload: bytes.Repeat([]byte("x"), 4096)}
	if err := testutil.RetryKafkaMetadata(ctx, func() error { return source.Publish(ctx, topic, task) }); err != nil {
		t.Fatal(err)
	}
	// Confirm the intended server rejection and warm metadata before the worker.
	controlErr := testutil.RetryKafkaMetadata(ctx, func() error { return rejected.Publish(ctx, "control", task) })
	var writeErrors kafka.WriteErrors
	if !errors.As(controlErr, &writeErrors) || len(writeErrors) != 1 || !errors.Is(writeErrors[0], kafka.MessageSizeTooLarge) {
		t.Fatalf("DLQ rejection setup failed: %v", controlErr)
	}
	if err := testutil.RetryKafkaMetadata(ctx, func() error {
		return recovered.Publish(ctx, "readiness", &taskv1.Task{Type: "control"})
	}); err != nil {
		t.Fatal(err)
	}
	deadLetters := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{addr}, Topic: recoveredTopic, Partition: 0, MinBytes: 1, MaxBytes: 10e6,
	})
	defer deadLetters.Close()
	if ready, err := deadLetters.ReadMessage(ctx); err != nil || string(ready.Key) != "readiness" {
		t.Fatalf("DLQ recovery setup failed: key=%q error=%v", ready.Key, err)
	}

	firstCtx, stopFirst := context.WithCancel(ctx)
	defer stopFirst()
	first := NewConsumer(addr, topic, topic, "first", &mockSender{}, store, rejected, logger)
	defer first.Close()
	executions := 0
	first.failureDecider = func() bool { executions++; return true }
	firstReader := &observedKafkaReader{Reader: first.reader.(*kafka.Reader), cancel: stopFirst}
	first.reader = firstReader
	runErr := first.Run(firstCtx)
	offset := committedGroupOffset(t, addr, topic, topic)
	t.Logf("rejected DLQ: Run=%v executions=%d commits=%v broker next offset=%d", runErr, executions, firstReader.committed, offset)
	if !errors.As(runErr, &writeErrors) || len(writeErrors) != 1 || !errors.Is(writeErrors[0], kafka.MessageSizeTooLarge) {
		t.Errorf("worker swallowed the broker's DLQ rejection: %v", runErr)
	}
	if len(firstReader.committed) != 0 || offset > 0 {
		t.Fatalf("failed quarantine acknowledged source work: commits=%v next offset=%d", firstReader.committed, offset)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	secondCtx, stopSecond := context.WithCancel(ctx)
	defer stopSecond()
	second := NewConsumer(addr, topic, topic, "replacement", &mockSender{},
		state.NewRedisTaskStore(testutil.RedisClient(t)), recovered, logger)
	defer second.Close()
	second.failureDecider = func() bool { executions++; return true }
	secondReader := &observedKafkaReader{Reader: second.reader.(*kafka.Reader), cancel: stopSecond}
	second.reader = secondReader
	if err := second.Run(secondCtx); err != nil {
		t.Fatalf("recovering quarantine: %v", err)
	}
	message, err := deadLetters.ReadMessage(ctx)
	if err != nil {
		t.Fatalf("reading recovered dead letter: %v", err)
	}
	var got taskv1.Task
	if err := proto.Unmarshal(message.Value, &got); err != nil {
		t.Fatal(err)
	}
	if string(message.Key) != topic || !proto.Equal(&got, task) {
		t.Fatalf("recovered dead letter changed key/payload: key=%q task=%v", message.Key, &got)
	}
	count, err := client.HGet(ctx, "task:"+topic, "attempts").Int64()
	offset = committedGroupOffset(t, addr, topic, topic)
	if err != nil || count != 1 || executions != 1 || offset != 1 {
		t.Fatalf("recovery spent another attempt or failed to commit: attempts=%d error=%v executions=%d offset=%d", count, err, executions, offset)
	}
	t.Logf("recovered DLQ key=%s attempts=%d executions=%d broker next offset=%d", message.Key, count, executions, offset)
}
