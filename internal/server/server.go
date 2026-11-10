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
	"google.golang.org/protobuf/proto"

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
	store    state.PublicationStore
	topic    string
}

// New returns a new DispatchServer ready to serve.
// producer and store must not be nil; inject no-op or mock implementations in tests.
func New(logger *slog.Logger, producer broker.Producer, store state.PublicationStore, topic string) *DispatchServer {
	return &DispatchServer{
		logger:   logger,
		producer: producer,
		store:    store,
		topic:    topic,
	}
}

// SubmitTask accepts a task from a client, assigns a canonical ID, and stores it.
// Each response is counted once by the outcome the client receives.
func (s *DispatchServer) SubmitTask(ctx context.Context, req *taskv1.SubmitTaskRequest) (*taskv1.SubmitTaskResponse, error) {
	response, err := s.submitTask(ctx, req)
	metrics.TasksSubmittedTotal.WithLabelValues(submissionOutcome(err)).Inc()
	return response, err
}

// submissionOutcome labels a SubmitTask result. Unavailable is "pending", not a
// rejection: the intent is retained and recovery may still publish it.
func submissionOutcome(err error) string {
	switch status.Code(err) {
	case codes.OK:
		return "accepted"
	case codes.Unavailable:
		return "pending"
	case codes.InvalidArgument, codes.FailedPrecondition:
		return "rejected"
	default:
		return "error"
	}
}

func (s *DispatchServer) submitTask(ctx context.Context, req *taskv1.SubmitTaskRequest) (*taskv1.SubmitTaskResponse, error) {
	if req.GetTask() == nil {
		return nil, status.Error(codes.InvalidArgument, "task must not be nil")
	}

	task := req.GetTask()

	if task.GetType() == "" {
		return nil, status.Error(codes.InvalidArgument, "task.type must not be empty")
	}

	if task.GetMaxRetries() < 0 {
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
		if existing.PublicationState != "pending" && existing.PublicationState != "published" {
			return nil, status.Errorf(codes.FailedPrecondition, "task %q has no publication evidence; inspect the legacy record", taskID)
		}
		if existing.PublicationTopic != s.topic {
			return nil, status.Errorf(codes.FailedPrecondition, "task %q belongs to a different publication topic", taskID)
		}
		if existing.PublicationState != "published" {
			return nil, pendingPublication(taskID)
		}
		s.logger.InfoContext(ctx, "idempotent resubmission",
			slog.String("task_id", existing.ID),
			slog.String("type", existing.TaskType),
		)
		return &taskv1.SubmitTaskResponse{
			Status: taskv1.SubmitStatus_SUBMIT_STATUS_ACCEPTED,
			TaskId: existing.ID,
		}, nil
	}

	now := time.Now()
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(task)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "task cannot be encoded")
	}
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

	publication := state.Publication{ID: taskID, Topic: s.topic, Payload: payload, Owner: uuid.NewString()}
	if err := s.store.AdmitTask(ctx, record, publication, publicationLease); err != nil {
		if errors.Is(err, state.ErrTaskExists) {
			return nil, pendingPublication(taskID)
		}
		s.logger.ErrorContext(ctx, "failed to save task to store",
			slog.String("task_id", taskID),
			slog.String("error", err.Error()),
		)
		return nil, status.Errorf(codes.Internal, "failed to save task")
	}

	if _, err := s.publishIntent(ctx, publication); err != nil {
		s.logger.ErrorContext(ctx, "task publication remains pending",
			slog.String("task_id", taskID),
			slog.String("error", err.Error()),
		)
		return nil, pendingPublication(taskID)
	}

	s.logger.InfoContext(ctx, "task accepted",
		slog.String("task_id", taskID),
		slog.String("type", task.GetType()),
		slog.String("priority", task.GetPriority().String()),
		slog.Int("max_retries", int(task.GetMaxRetries())),
	)

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
	switch update.GetStatus() {
	case taskv1.TaskStatus_TASK_STATUS_RUNNING, taskv1.TaskStatus_TASK_STATUS_COMPLETED, taskv1.TaskStatus_TASK_STATUS_FAILED:
	default:
		s.logger.WarnContext(ctx, "unsupported worker progress status", slog.String("task_id", update.GetTaskId()),
			slog.String("status", update.GetStatus().String()))
		return &taskv1.ProgressAck{Recorded: false}
	}
	if update.GetProgress() < 0 || update.GetProgress() > 100 ||
		(update.GetStatus() == taskv1.TaskStatus_TASK_STATUS_COMPLETED && update.GetProgress() != 100) {
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
