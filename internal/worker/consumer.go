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
	"github.com/ronnitchopra/dispatch/internal/state"
)

// ProgressSender is the interface the Consumer uses to report task progress.
// It is satisfied by *ProgressReporter and can be mocked in tests.
type ProgressSender interface {
	SendUpdate(update *taskv1.ProgressUpdate) (*taskv1.ProgressAck, error)
}

// AttemptStore persists started-attempt counts across consumer restarts.
type AttemptStore interface {
	GetTask(ctx context.Context, taskID string) (state.TaskRecord, error)
	StartAttempt(ctx context.Context, taskID string, maxAttempts int64) (int64, error)
}

// DLQPublisher publishes exhausted tasks to the dead-letter queue. Publish must
// return broker rejections rather than acknowledge fire-and-forget writes.
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
	attempts       AttemptStore
	dlqPublisher   DLQPublisher
	failureDecider func() bool // returns true to simulate task failure
	workerID       string
	logger         *slog.Logger
}

// NewConsumer creates a Consumer backed by a segmentio/kafka-go Reader.
// attempts must persist reservations across reader/worker restarts.
// Without a dlqPublisher, exhaustion returns an error and leaves work uncommitted.
func NewConsumer(
	brokerAddr, topic, groupID, workerID string,
	reporter ProgressSender,
	attempts AttemptStore,
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
		attempts:       attempts,
		dlqPublisher:   dlqPublisher,
		failureDecider: func() bool { return rand.Float64() < 0.10 },
		workerID:       workerID,
		logger:         logger,
	}
}

// Run starts the consumption loop, blocking until ctx is cancelled or processing fails.
// It retries simulated task failures within the durable started-attempt budget
// and commits when processMessage reports that the message was handled.
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
	var task taskv1.Task
	if err := proto.Unmarshal(value, &task); err != nil {
		return fmt.Errorf("unmarshalling task %q: %w", taskID, err)
	}
	if c.attempts == nil {
		return fmt.Errorf("attempt store is not configured")
	}
	record, err := c.attempts.GetTask(ctx, taskID)
	if err != nil {
		return fmt.Errorf("reading task %q before execution: %w", taskID, err)
	}
	if record.Status == int32(taskv1.TaskStatus_TASK_STATUS_COMPLETED) {
		return nil
	}

	c.logger.Info("task received",
		slog.String("task_id", taskID),
		slog.String("type", task.GetType()),
		slog.String("worker_id", c.workerID),
	)
	maxAttempts := int64(task.GetMaxRetries()) + 1
	for {
		attempt, err := c.attempts.StartAttempt(ctx, taskID, maxAttempts)
		if err != nil {
			return fmt.Errorf("recording attempt for task %q: %w", taskID, err)
		}
		if attempt == 0 {
			// Completion may have been recorded by another worker during a replay.
			record, err := c.attempts.GetTask(ctx, taskID)
			if err != nil {
				return fmt.Errorf("checking exhausted task %q: %w", taskID, err)
			}
			if record.Status == int32(taskv1.TaskStatus_TASK_STATUS_COMPLETED) {
				return nil
			}
			if record.Status != int32(taskv1.TaskStatus_TASK_STATUS_FAILED) {
				if _, err := c.reporter.SendUpdate(&taskv1.ProgressUpdate{
					TaskId: taskID, WorkerId: c.workerID,
					Status: taskv1.TaskStatus_TASK_STATUS_FAILED,
				}); err != nil {
					return fmt.Errorf("reporting exhausted task %q: %w", taskID, err)
				}
			}
			return c.publishDeadLetter(ctx, taskID, &task)
		}
		c.logger.Info("task attempt started", slog.String("task_id", taskID),
			slog.Int64("attempt", attempt), slog.Int64("max_attempts", maxAttempts))
		failed, err := c.processAttempt(ctx, taskID)
		if err != nil {
			return err
		}
		if !failed {
			return nil
		}
	}
}

// processAttempt reports one simulated execution. Infrastructure errors stop
// processing; a simulated task failure is retried within the persisted budget.
func (c *Consumer) processAttempt(ctx context.Context, taskID string) (bool, error) {
	start := time.Now()

	// Report RUNNING.
	ack, err := c.reporter.SendUpdate(&taskv1.ProgressUpdate{
		TaskId:   taskID,
		WorkerId: c.workerID,
		Status:   taskv1.TaskStatus_TASK_STATUS_RUNNING,
		Progress: 0,
	})
	if err != nil {
		return false, fmt.Errorf("sending RUNNING update for task %q: %w", taskID, err)
	}
	if !ack.GetRecorded() {
		c.logger.Warn("RUNNING update not recorded by server",
			slog.String("task_id", taskID),
			slog.String("worker_id", c.workerID),
		)
	}

	// Simulate work — interruptible by context cancellation.
	select {
	case <-time.After(2 * time.Second):
	case <-ctx.Done():
		return false, ctx.Err()
	}

	// Simulate 10% failure rate.
	decider := c.failureDecider
	if decider == nil {
		decider = func() bool { return false }
	}
	if decider() {
		return true, c.handleFailure(ctx, taskID, start)
	}

	// Report COMPLETED.
	ack, err = c.reporter.SendUpdate(&taskv1.ProgressUpdate{
		TaskId:   taskID,
		WorkerId: c.workerID,
		Status:   taskv1.TaskStatus_TASK_STATUS_COMPLETED,
		Progress: 100,
	})
	if err != nil {
		return false, fmt.Errorf("sending COMPLETED update for task %q: %w", taskID, err)
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

	return false, nil
}

// handleFailure records a simulated failed attempt without mutating the task's
// configured retry budget or acknowledging its Kafka message.
func (c *Consumer) handleFailure(ctx context.Context, taskID string, start time.Time) error {
	metrics.TasksFailedTotal.Inc()
	metrics.TaskExecutionDuration.Observe(time.Since(start).Seconds())

	c.logger.Warn("task failed",
		slog.String("task_id", taskID),
		slog.String("worker_id", c.workerID),
	)

	// Always report FAILED status.
	if _, err := c.reporter.SendUpdate(&taskv1.ProgressUpdate{
		TaskId:   taskID,
		WorkerId: c.workerID,
		Status:   taskv1.TaskStatus_TASK_STATUS_FAILED,
		Progress: 0,
	}); err != nil {
		return fmt.Errorf("sending FAILED update for task %q: %w", taskID, err)
	}
	return nil
}

func (c *Consumer) publishDeadLetter(ctx context.Context, taskID string, task *taskv1.Task) error {
	c.logger.Error("task exhausted all retries — sending to DLQ",
		slog.String("task_id", taskID), slog.String("worker_id", c.workerID))
	if c.dlqPublisher == nil {
		return fmt.Errorf("DLQ publisher is not configured for exhausted task %q", taskID)
	}
	if err := c.dlqPublisher.Publish(ctx, taskID, task); err != nil {
		return fmt.Errorf("publishing task %q to DLQ: %w", taskID, err)
	}
	return nil
}

// Close releases the underlying Kafka reader.
func (c *Consumer) Close() error {
	if err := c.reader.Close(); err != nil {
		return fmt.Errorf("closing kafka reader: %w", err)
	}
	return nil
}
