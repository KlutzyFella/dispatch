// Package server implements the gRPC DispatchService.
package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	taskv1 "github.com/ronnitchopra/dispatch/gen/task/v1"
	"github.com/ronnitchopra/dispatch/internal/broker"
	"github.com/ronnitchopra/dispatch/internal/metrics"
	"github.com/ronnitchopra/dispatch/internal/state"
)

// DispatchServer implements taskv1.DispatchServiceServer.
type DispatchServer struct {
	taskv1.UnimplementedDispatchServiceServer

	logger   *slog.Logger
	producer broker.Producer
	store    state.TaskStore
}

// New returns a new DispatchServer ready to serve.
// producer and store must not be nil; inject no-op or mock implementations in tests.
func New(logger *slog.Logger, producer broker.Producer, store state.TaskStore) *DispatchServer {
	return &DispatchServer{
		logger:   logger,
		producer: producer,
		store:    store,
	}
}

// SubmitTask accepts a task from a client, assigns a canonical ID, and stores it.
func (s *DispatchServer) SubmitTask(ctx context.Context, req *taskv1.SubmitTaskRequest) (*taskv1.SubmitTaskResponse, error) {
	if req.GetTask() == nil {
		metrics.TasksSubmittedTotal.WithLabelValues("rejected").Inc()
		return nil, status.Error(codes.InvalidArgument, "task must not be nil")
	}

	task := req.GetTask()

	if task.GetType() == "" {
		metrics.TasksSubmittedTotal.WithLabelValues("rejected").Inc()
		return nil, status.Error(codes.InvalidArgument, "task.type must not be empty")
	}

	if task.GetMaxRetries() < 0 {
		metrics.TasksSubmittedTotal.WithLabelValues("rejected").Inc()
		return nil, status.Error(codes.InvalidArgument, "task.max_retries must not be negative")
	}

	taskID := task.GetIdempotencyKey()
	if taskID == "" {
		taskID = uuid.NewString()
	}

	// Idempotency check: if this key was already accepted, return the same task_id.
	existing, err := s.store.GetTask(ctx, taskID)
	if err != nil && !errors.Is(err, state.ErrTaskNotFound) {
		s.logger.ErrorContext(ctx, "store.GetTask failed during idempotency check",
			slog.String("task_id", taskID),
			slog.String("error", err.Error()),
		)
		return nil, status.Errorf(codes.Internal, "failed to check task state")
	}
	if err == nil {
		s.logger.InfoContext(ctx, "idempotent resubmission",
			slog.String("task_id", existing.ID),
			slog.String("type", existing.TaskType),
		)
		metrics.TasksSubmittedTotal.WithLabelValues("accepted").Inc()
		return &taskv1.SubmitTaskResponse{
			Status: taskv1.SubmitStatus_SUBMIT_STATUS_ACCEPTED,
			TaskId: existing.ID,
		}, nil
	}

	now := time.Now()
	record := state.TaskRecord{
		ID:         taskID,
		TaskType:   task.GetType(),
		Priority:   int32(task.GetPriority()),
		MaxRetries: task.GetMaxRetries(),
		Status:     int32(taskv1.TaskStatus_TASK_STATUS_PENDING),
		Progress:   0,
		CreatedAt:  now,
		UpdatedAt:  now,
	}

	if err := s.store.SaveTask(ctx, record); err != nil {
		s.logger.ErrorContext(ctx, "failed to save task to store",
			slog.String("task_id", taskID),
			slog.String("error", err.Error()),
		)
		metrics.TasksSubmittedTotal.WithLabelValues("rejected").Inc()
		return nil, status.Errorf(codes.Internal, "failed to save task")
	}

	if err := s.producer.Publish(ctx, taskID, task); err != nil {
		// Best-effort rollback: remove the stored record so the key is not
		// treated as idempotent on a subsequent retry.
		if delErr := s.store.DeleteTask(ctx, taskID); delErr != nil {
			s.logger.ErrorContext(ctx, "rollback DeleteTask failed",
				slog.String("task_id", taskID),
				slog.String("error", delErr.Error()),
			)
		}
		s.logger.ErrorContext(ctx, "failed to publish task — rolling back store",
			slog.String("task_id", taskID),
			slog.String("error", err.Error()),
		)
		metrics.TasksSubmittedTotal.WithLabelValues("rejected").Inc()
		return nil, status.Errorf(codes.Internal, "failed to publish task")
	}

	s.logger.InfoContext(ctx, "task accepted",
		slog.String("task_id", taskID),
		slog.String("type", task.GetType()),
		slog.String("priority", task.GetPriority().String()),
		slog.Int("max_retries", int(task.GetMaxRetries())),
	)

	metrics.TasksSubmittedTotal.WithLabelValues("accepted").Inc()
	return &taskv1.SubmitTaskResponse{
		Status: taskv1.SubmitStatus_SUBMIT_STATUS_ACCEPTED,
		TaskId: taskID,
	}, nil
}

// ReportProgress handles a long-lived bidirectional stream from a worker.
// A single stream can carry progress updates for multiple tasks via task_id.
// Returns nil on clean EOF (client closed stream).
func (s *DispatchServer) ReportProgress(stream taskv1.DispatchService_ReportProgressServer) error {
	for {
		update, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("receiving progress update: %w", err)
		}

		ack := s.applyProgressUpdate(stream.Context(), update)

		if err := stream.Send(ack); err != nil {
			return fmt.Errorf("sending progress ack: %w", err)
		}
	}
}

// applyProgressUpdate validates a single progress update and, if valid, writes
// it to the task store.
func (s *DispatchServer) applyProgressUpdate(ctx context.Context, update *taskv1.ProgressUpdate) *taskv1.ProgressAck {
	if update.GetProgress() < 0 || update.GetProgress() > 100 {
		s.logger.WarnContext(ctx, "invalid progress value — ignoring",
			slog.String("worker_id", update.GetWorkerId()),
			slog.String("task_id", update.GetTaskId()),
			slog.Int("progress", int(update.GetProgress())),
		)
		return &taskv1.ProgressAck{Recorded: false}
	}

	_, err := s.store.GetTask(ctx, update.GetTaskId())
	if errors.Is(err, state.ErrTaskNotFound) {
		s.logger.WarnContext(ctx, "progress update for unknown task — ignoring",
			slog.String("worker_id", update.GetWorkerId()),
			slog.String("task_id", update.GetTaskId()),
		)
		return &taskv1.ProgressAck{Recorded: false}
	}
	if err != nil {
		s.logger.ErrorContext(ctx, "store.GetTask failed during progress update",
			slog.String("task_id", update.GetTaskId()),
			slog.String("error", err.Error()),
		)
		return &taskv1.ProgressAck{Recorded: false}
	}

	if err := s.store.UpdateProgress(ctx, update.GetTaskId(), int32(update.GetStatus()), update.GetProgress()); err != nil {
		s.logger.ErrorContext(ctx, "store.UpdateProgress failed",
			slog.String("task_id", update.GetTaskId()),
			slog.String("error", err.Error()),
		)
		return &taskv1.ProgressAck{Recorded: false}
	}

	s.logger.InfoContext(ctx, "progress recorded",
		slog.String("worker_id", update.GetWorkerId()),
		slog.String("task_id", update.GetTaskId()),
		slog.String("status", update.GetStatus().String()),
		slog.Int("progress", int(update.GetProgress())),
	)

	return &taskv1.ProgressAck{Recorded: true}
}
