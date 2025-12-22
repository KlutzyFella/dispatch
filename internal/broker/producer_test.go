package broker_test

import (
	"io"
	"log/slog"
	"testing"

	"github.com/ronnitchopra/dispatch/internal/broker"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestNewKafkaProducer_ReturnsNonNil(t *testing.T) {
	p := broker.NewKafkaProducer("localhost:9092", "tasks.pending", testLogger())
	if p == nil {
		t.Fatal("NewKafkaProducer returned nil")
	}
}

func TestNewKafkaProducer_Close(t *testing.T) {
	p := broker.NewKafkaProducer("localhost:9092", "tasks.pending", testLogger())
	if err := p.Close(); err != nil {
		t.Errorf("Close returned unexpected error: %v", err)
	}
}

// TestKafkaProducer_Publish_Integration verifies a full round-trip against a
// real Kafka broker. This test is excluded from the standard suite and only
// runs when explicitly targeted with: go test -tags integration ./internal/broker/...
