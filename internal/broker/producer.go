// Package broker provides message-broker abstractions and implementations
// for publishing tasks to downstream consumers.
package broker

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"

	taskv1 "github.com/ronnitchopra/dispatch/gen/task/v1"
)

// Producer publishes validated tasks to a message broker.
// Implementations must be safe for concurrent use.
type Producer interface {
	// Publish serialises task and writes it to the broker under the given taskID key.
	// The taskID is used as the partition key to ensure all updates for the same
	// task land on the same partition.
	Publish(ctx context.Context, taskID string, task *taskv1.Task) error

	// Close flushes any buffered messages and releases underlying resources.
	// It must be called once when the producer is no longer needed.
	Close() error
}

// KafkaProducer implements Producer using the segmentio/kafka-go library.
type KafkaProducer struct {
	writer *kafka.Writer
	logger *slog.Logger
}

// NewKafkaProducer creates a KafkaProducer that writes to the given topic on
// the specified broker address. The writer uses lazy dialling — it does not
// connect to Kafka until the first message is written.
func NewKafkaProducer(brokerAddr, topic string, logger *slog.Logger) *KafkaProducer {
	w := &kafka.Writer{
		Addr:                   kafka.TCP(brokerAddr),
		Topic:                  topic,
		Balancer:               &kafka.Hash{}, // route by key hash for partition consistency
		AllowAutoTopicCreation: true,
	}
	return &KafkaProducer{writer: w, logger: logger}
}

// Publish serialises task via protobuf and writes a single message to Kafka.
// The taskID is used as the message key for deterministic partition routing.
func (p *KafkaProducer) Publish(ctx context.Context, taskID string, task *taskv1.Task) error {
	payload, err := proto.Marshal(task)
	if err != nil {
		return fmt.Errorf("marshalling task %q: %w", taskID, err)
	}

	msg := kafka.Message{
		Key:   []byte(taskID),
		Value: payload,
	}

	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("writing message for task %q: %w", taskID, err)
	}

	p.logger.InfoContext(ctx, "task published to kafka",
		slog.String("task_id", taskID),
		slog.String("topic", p.writer.Topic),
	)
	return nil
}

// Close flushes pending writes and closes the underlying Kafka writer.
func (p *KafkaProducer) Close() error {
	if err := p.writer.Close(); err != nil {
		return fmt.Errorf("closing kafka writer: %w", err)
	}
	return nil
}
