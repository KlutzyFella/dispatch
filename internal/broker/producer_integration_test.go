//go:build integration

package broker_test

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
	"github.com/ronnitchopra/dispatch/internal/testutil"
	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestKafkaProducer_Publish_Integration(t *testing.T) {
	addr, topic := testutil.KafkaTopic(t, kafka.ConfigEntry{
		ConfigName: "max.message.bytes", ConfigValue: "1024",
	})
	p := broker.NewKafkaProducer(addr, topic, testLogger())
	t.Cleanup(func() {
		if err := p.Close(); err != nil {
			t.Errorf("closing producer: %v", err)
		}
	})

	t.Run("accepted_message_round_trips", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		task := &taskv1.Task{Type: "simulation", Payload: []byte("small payload"), MaxRetries: 2}
		if err := testutil.RetryKafkaMetadata(ctx, func() error {
			return p.Publish(ctx, "accepted", task)
		}); err != nil {
			t.Fatalf("publishing valid task: %v", err)
		}
		reader := kafka.NewReader(kafka.ReaderConfig{
			Brokers: []string{addr}, Topic: topic, Partition: 0,
			MinBytes: 1, MaxBytes: 10e6, MaxWait: 100 * time.Millisecond,
		})
		defer reader.Close()
		message, err := reader.ReadMessage(ctx)
		if err != nil {
			t.Fatalf("reading published task: %v", err)
		}
		if string(message.Key) != "accepted" {
			t.Fatalf("message key = %q, want accepted", message.Key)
		}
		var got taskv1.Task
		if err := proto.Unmarshal(message.Value, &got); err != nil {
			t.Fatalf("decoding published task: %v", err)
		}
		if !proto.Equal(&got, task) {
			t.Errorf("published task = %v, want %v", &got, task)
		}
	})

	t.Run("rejects_oversized_message", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		task := &taskv1.Task{Type: "simulation", Payload: bytes.Repeat([]byte("x"), 4096)}
		payload, err := proto.Marshal(task)
		if err != nil {
			t.Fatal(err)
		}

		// Establish the server's rejection independently of the production writer.
		control := &kafka.Writer{
			Addr: kafka.TCP(addr), Topic: topic,
			RequiredAcks: kafka.RequireAll, BatchTimeout: time.Millisecond,
		}
		defer control.Close()
		controlErr := testutil.RetryKafkaMetadata(ctx, func() error {
			return control.WriteMessages(ctx, kafka.Message{Key: []byte("control"), Value: payload})
		})
		assertMessageTooLarge(t, controlErr)

		err = testutil.RetryKafkaMetadata(ctx, func() error {
			return p.Publish(ctx, "oversized", task)
		})
		assertMessageTooLarge(t, err)
	})
}

func assertMessageTooLarge(t *testing.T, err error) {
	t.Helper()
	var writeErrors kafka.WriteErrors
	if !errors.As(err, &writeErrors) || len(writeErrors) != 1 || !errors.Is(writeErrors[0], kafka.MessageSizeTooLarge) {
		t.Fatalf("Publish must report Kafka's MessageSizeTooLarge rejection, got %v", err)
	}
}

// Publish is synchronous and SubmitTask waits for it, so the writer must not
// hold a lone message waiting for a batch to fill. kafka-go's default
// BatchTimeout is one second; with it every single-message Publish to a
// healthy local broker took about 1.0s. The bound is deliberately loose: it
// separates "waits for a batch timer" from "waits for the broker ACK", and is
// not a latency claim.
func TestKafkaProducer_PublishDoesNotWaitForBatchTimer(t *testing.T) {
	addr, topic := testutil.KafkaTopic(t)
	p := broker.NewKafkaProducer(addr, topic, testLogger())
	t.Cleanup(func() {
		if err := p.Close(); err != nil {
			t.Errorf("closing producer: %v", err)
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	task := &taskv1.Task{Type: "simulation"}
	// Warm up connection and topic metadata outside the measurement.
	if err := testutil.RetryKafkaMetadata(ctx, func() error { return p.Publish(ctx, "warmup", task) }); err != nil {
		t.Fatalf("warm-up publish: %v", err)
	}
	const bound = 500 * time.Millisecond
	durations := make([]time.Duration, 0, 5)
	for i := 0; i < 5; i++ {
		start := time.Now()
		if err := p.Publish(ctx, "timed", task); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
		durations = append(durations, time.Since(start))
	}
	t.Logf("single-message publish durations (race detector on, local broker): %v", durations)
	for i, d := range durations {
		if d >= bound {
			t.Errorf("publish %d took %v; a lone message is waiting on the batch timer (bound %v)", i, d, bound)
		}
	}
}
