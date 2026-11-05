//go:build integration

// Package testutil provides real-service fixtures for integration tests.
package testutil

import (
	"context"
	"errors"
	"net"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

// KafkaTopic creates a ready, single-partition topic and deletes only that topic
// at cleanup. Kafka must be running; missing infrastructure fails the test.
func KafkaTopic(t *testing.T, config ...kafka.ConfigEntry) (addr, topic string) {
	t.Helper()
	addr = os.Getenv("KAFKA_BROKER")
	if addr == "" {
		addr = "localhost:9092"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	conn, err := kafka.DialContext(ctx, "tcp", addr)
	if err != nil {
		t.Fatalf("Kafka unavailable at %s: %v; start it with docker compose up -d kafka (or set KAFKA_BROKER)", addr, err)
	}
	deadline, _ := ctx.Deadline()
	conn.SetDeadline(deadline)
	controller, err := conn.Controller()
	conn.Close()
	if err != nil {
		t.Fatalf("finding Kafka controller: %v", err)
	}
	controllerAddr := net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port))
	conn, err = kafka.DialContext(ctx, "tcp", controllerAddr)
	if err != nil {
		t.Fatalf("connecting to Kafka controller: %v", err)
	}
	defer conn.Close()
	conn.SetDeadline(deadline)
	topic = "dispatch-test-" + uuid.NewString()
	if err := conn.CreateTopics(kafka.TopicConfig{
		Topic: topic, NumPartitions: 1, ReplicationFactor: 1, ConfigEntries: config,
	}); err != nil {
		t.Fatalf("creating Kafka topic %s: %v", topic, err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		conn, err := kafka.DialContext(ctx, "tcp", controllerAddr)
		if err != nil {
			t.Errorf("connecting to clean up Kafka topic %s: %v", topic, err)
			return
		}
		defer conn.Close()
		deadline, _ := ctx.Deadline()
		conn.SetDeadline(deadline)
		if err := conn.DeleteTopics(topic); err != nil {
			t.Errorf("deleting test Kafka topic %s: %v", topic, err)
		}
	})
	for {
		leader, err := kafka.DialLeader(ctx, "tcp", addr, topic, 0)
		if err == nil {
			leader.SetDeadline(deadline)
			_, err = leader.ReadLastOffset()
			leader.Close()
			if err == nil {
				return addr, topic
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("waiting for Kafka topic %s readiness: %v (last error: %v)", topic, ctx.Err(), err)
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// RetryKafkaMetadata tolerates only the metadata propagation errors of a newly
// created topic. It never retries message rejection or transport errors.
func RetryKafkaMetadata(ctx context.Context, operation func() error) error {
	for {
		err := operation()
		if !errors.Is(err, kafka.UnknownTopicOrPartition) && !errors.Is(err, kafka.LeaderNotAvailable) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}
