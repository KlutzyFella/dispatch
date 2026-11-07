// Package state defines the TaskStore interface and shared data types for
// persisting and querying task state. It is a leaf package with no dependencies
// on other internal packages.
package state

import (
	"context"
	"errors"
	"time"
)

// ErrTaskNotFound is returned by GetTask when the requested task does not exist.
var ErrTaskNotFound = errors.New("task not found")

// ErrTaskCompleted rejects progress changes to an already completed task.
var ErrTaskCompleted = errors.New("task already completed")

// TaskRecord is the canonical storage model for a task.
// It uses primitive types so the state package has no dependency on proto-generated code.
type TaskRecord struct {
	ID         string
	TaskType   string
	Priority   int32
	MaxRetries int32
	Status     int32
	Progress   int32
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

// TaskStore defines the persistence operations required by the dispatch server.
// All implementations must be safe for concurrent use.
type TaskStore interface {
	// SaveTask persists a new task record. It must be idempotent — calling
	// SaveTask twice with the same ID overwrites the previous record.
	SaveTask(ctx context.Context, record TaskRecord) error

	// GetTask returns the task with the given ID, or ErrTaskNotFound if absent.
	GetTask(ctx context.Context, taskID string) (TaskRecord, error)

	// UpdateProgress updates the status and progress fields of an existing task.
	// Returns ErrTaskNotFound if the task does not exist.
	UpdateProgress(ctx context.Context, taskID string, status, progress int32) error

	// DeleteTask removes the task record. It is a no-op if the task does not exist.
	DeleteTask(ctx context.Context, taskID string) error
}
