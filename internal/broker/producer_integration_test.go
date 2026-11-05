//go:build integration

package broker_test

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	taskv1 "github.com/ronnitchopra/dispatch/gen/task/v1"
	"github.com/ronnitchopra/dispatch/internal/broker"
	"github.com/ronnitchopra/dispatch/internal/testutil"
	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"
)

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
