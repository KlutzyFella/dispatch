package worker

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"time"

	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"

	taskv1 "github.com/ronnitchopra/dispatch/gen/task/v1"
	"github.com/ronnitchopra/dispatch/internal/metrics"
)

const lockTTL = 5 * time.Minute

// ProgressSender is the interface the Consumer uses to report task progress.
// It is satisfied by *ProgressReporter and can be mocked in tests.
type ProgressSender interface {
	SendUpdate(update *taskv1.ProgressUpdate) (*taskv1.ProgressAck, error)
}

// TaskLocker is the subset of the state.TaskStore interface needed by the
// worker. It is defined locally so the worker package does not import state.
// *state.RedisTaskStore satisfies this interface implicitly.
type TaskLocker interface {
	AcquireLock(ctx context.Context, taskID string, ttl time.Duration) (bool, error)
}

// DLQPublisher publishes exhausted tasks to the dead-letter queue.
// *broker.KafkaProducer satisfies this interface implicitly.
type DLQPublisher interface {
	Publish(ctx context.Context, taskID string, task *taskv1.Task) error
}

// messageReader is satisfied by *kafka.Reader and can be swapped in tests.
type messageReader interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

// Consumer reads tasks from a Kafka topic and reports progress to the server
// via a ProgressSender.
type Consumer struct {
	reader         messageReader
	reporter       ProgressSender
	locker         TaskLocker
	dlqPublisher   DLQPublisher
	failureDecider func() bool // returns true to simulate task failure
	workerID       string
	logger         *slog.Logger
}

// NewConsumer creates a Consumer backed by a segmentio/kafka-go Reader.
// locker and dlqPublisher may be nil (locking and DLQ are skipped when nil).
func NewConsumer(
	brokerAddr, topic, groupID, workerID string,
	reporter ProgressSender,
	locker TaskLocker,
	dlqPublisher DLQPublisher,
	logger *slog.Logger,
) *Consumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{brokerAddr},
		GroupID:  groupID,
		Topic:    topic,
		MinBytes: 1,
		MaxBytes: 10e6,
	})

	return &Consumer{
		reader:         reader,
		reporter:       reporter,
		locker:         locker,
		dlqPublisher:   dlqPublisher,
		failureDecider: func() bool { return rand.Float64() < 0.10 },
		workerID:       workerID,
		logger:         logger,
	}
}

// Run starts the consumption loop, blocking until ctx is cancelled or processing fails.
// For each message it deserialises the task, acquires a distributed lock,
// processes it (with simulated 10% failure), and commits the offset when
// processMessage reports that the message was handled.
// On processing failure the caller must close this consumer. A new reader can
// replay uncommitted work from the consumer group's committed offset.
func (c *Consumer) Run(ctx context.Context) error {
	c.logger.Info("consumer started",
		slog.String("worker_id", c.workerID),
	)

	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				c.logger.Info("consumer shutting down")
				return nil
			}
			return fmt.Errorf("fetching message: %w", err)
		}

		if err := c.processMessage(ctx, msg.Key, msg.Value); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			// Later commits in this partition would also acknowledge this failure.
			return fmt.Errorf("processing message from %s[%d] at offset %d: %w",
				msg.Topic, msg.Partition, msg.Offset, err)
		}

		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			c.logger.Error("failed to commit message offset",
				slog.String("error", err.Error()),
			)
		}
	}
}

// processMessage is the per-message execution logic, extracted for testability.
func (c *Consumer) processMessage(ctx context.Context, key, value []byte) error {
	taskID := string(key)

	// 1. Acquire distributed lock before processing.
	if c.locker != nil {
		acquired, err := c.locker.AcquireLock(ctx, taskID, lockTTL)
		if err != nil {
			return fmt.Errorf("acquiring lock for task %q: %w", taskID, err)
		}
		if !acquired {
			c.logger.Info("lock not acquired — skipping task (another worker has it)",
				slog.String("task_id", taskID),
				slog.String("worker_id", c.workerID),
			)
			// Return nil so the offset is committed; another worker owns this task.
			return nil
		}
	}

	start := time.Now()

	// 2. Deserialise the task payload.
	var task taskv1.Task
	if err := proto.Unmarshal(value, &task); err != nil {
		return fmt.Errorf("unmarshalling task %q: %w", taskID, err)
	}

	c.logger.Info("task received",
		slog.String("task_id", taskID),
		slog.String("type", task.GetType()),
		slog.String("worker_id", c.workerID),
	)

	// 3. Report RUNNING.
	ack, err := c.reporter.SendUpdate(&taskv1.ProgressUpdate{
		TaskId:   taskID,
		WorkerId: c.workerID,
		Status:   taskv1.TaskStatus_TASK_STATUS_RUNNING,
		Progress: 0,
	})
	if err != nil {
		return fmt.Errorf("sending RUNNING update for task %q: %w", taskID, err)
	}
	if !ack.GetRecorded() {
		c.logger.Warn("RUNNING update not recorded by server",
			slog.String("task_id", taskID),
			slog.String("worker_id", c.workerID),
		)
	}

	// 4. Simulate work — interruptible by context cancellation.
	select {
	case <-time.After(2 * time.Second):
	case <-ctx.Done():
		return ctx.Err()
	}

	// 5. Simulate 10% failure rate.
	decider := c.failureDecider
	if decider == nil {
		decider = func() bool { return false }
	}
	if decider() {
		return c.handleFailure(ctx, taskID, &task, start)
	}

	// 6. Report COMPLETED.
	ack, err = c.reporter.SendUpdate(&taskv1.ProgressUpdate{
		TaskId:   taskID,
		WorkerId: c.workerID,
		Status:   taskv1.TaskStatus_TASK_STATUS_COMPLETED,
		Progress: 100,
	})
	if err != nil {
		return fmt.Errorf("sending COMPLETED update for task %q: %w", taskID, err)
	}
	if !ack.GetRecorded() {
		c.logger.Warn("COMPLETED update not recorded by server",
			slog.String("task_id", taskID),
			slog.String("worker_id", c.workerID),
		)
	}

	metrics.TasksCompletedTotal.Inc()
	metrics.TaskExecutionDuration.Observe(time.Since(start).Seconds())

	c.logger.Info("task completed",
		slog.String("task_id", taskID),
		slog.String("worker_id", c.workerID),
	)

	return nil
}

// handleFailure processes a simulated task failure. It decrements max_retries
// and either sends the task to the DLQ (retries exhausted) or returns an error
// so Run stops without committing the Kafka offset.
func (c *Consumer) handleFailure(ctx context.Context, taskID string, task *taskv1.Task, start time.Time) error {
	task.MaxRetries--

	metrics.TasksFailedTotal.Inc()
	metrics.TaskExecutionDuration.Observe(time.Since(start).Seconds())

	c.logger.Warn("task failed",
		slog.String("task_id", taskID),
		slog.String("worker_id", c.workerID),
		slog.Int("retries_remaining", int(task.GetMaxRetries())),
	)

	// Always report FAILED status.
	if _, err := c.reporter.SendUpdate(&taskv1.ProgressUpdate{
		TaskId:   taskID,
		WorkerId: c.workerID,
		Status:   taskv1.TaskStatus_TASK_STATUS_FAILED,
		Progress: 0,
	}); err != nil {
		c.logger.Error("failed to send FAILED update",
			slog.String("task_id", taskID),
			slog.String("error", err.Error()),
		)
	}

	if task.GetMaxRetries() < 0 {
		// Retries exhausted — publish to dead-letter queue and commit offset.
		c.logger.Error("task exhausted all retries — sending to DLQ",
			slog.String("task_id", taskID),
			slog.String("worker_id", c.workerID),
		)
		if c.dlqPublisher != nil {
			if err := c.dlqPublisher.Publish(ctx, taskID, task); err != nil {
				c.logger.Error("failed to publish task to DLQ",
					slog.String("task_id", taskID),
					slog.String("error", err.Error()),
				)
			}
		}
		// Return nil so the offset IS committed (task is dead-lettered).
		return nil
	}

	// Retries remain — stop consumption before a later commit can pass this task.
	return fmt.Errorf("task %q failed, %d retries remaining", taskID, task.GetMaxRetries())
}

// Close releases the underlying Kafka reader.
func (c *Consumer) Close() error {
	if err := c.reader.Close(); err != nil {
		return fmt.Errorf("closing kafka reader: %w", err)
	}
	return nil
}
