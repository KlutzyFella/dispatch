package state_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/ronnitchopra/dispatch/internal/state"
)

// memoryStore is an in-memory implementation of TaskStore used only in tests.
type memoryStore struct {
	mu    sync.RWMutex
	tasks map[string]state.TaskRecord
	locks map[string]struct{}
}

func newMemoryStore() *memoryStore {
	return &memoryStore{
		tasks: make(map[string]state.TaskRecord),
		locks: make(map[string]struct{}),
	}
}

func (m *memoryStore) SaveTask(_ context.Context, record state.TaskRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.tasks[record.ID] = record
	return nil
}

func (m *memoryStore) GetTask(_ context.Context, taskID string) (state.TaskRecord, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	r, ok := m.tasks[taskID]
	if !ok {
		return state.TaskRecord{}, state.ErrTaskNotFound
	}
	return r, nil
}

func (m *memoryStore) UpdateProgress(_ context.Context, taskID string, status, progress int32) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.tasks[taskID]
	if !ok {
		return state.ErrTaskNotFound
	}
	r.Status = status
	r.Progress = progress
	r.UpdatedAt = time.Now()
	m.tasks[taskID] = r
	return nil
}

func (m *memoryStore) DeleteTask(_ context.Context, taskID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.tasks, taskID)
	return nil
}

func (m *memoryStore) AcquireLock(_ context.Context, taskID string, _ time.Duration) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, held := m.locks[taskID]; held {
		return false, nil
	}
	m.locks[taskID] = struct{}{}
	return true, nil
}

// compile-time check that memoryStore satisfies the interface.
var _ state.TaskStore = (*memoryStore)(nil)

// ── Tests ──────────────────────────────────────────────────────────────────────

func TestMemoryStore_SaveAndGet(t *testing.T) {
	t.Parallel()

	store := newMemoryStore()
	ctx := context.Background()

	record := state.TaskRecord{
		ID:        "task-1",
		TaskType:  "email",
		Priority:  2,
		Status:    1,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	if err := store.SaveTask(ctx, record); err != nil {
		t.Fatalf("SaveTask: %v", err)
	}

	got, err := store.GetTask(ctx, "task-1")
	if err != nil {
		t.Fatalf("GetTask: %v", err)
	}
	if got.TaskType != "email" {
		t.Errorf("TaskType = %q, want 'email'", got.TaskType)
	}
	if got.Priority != 2 {
		t.Errorf("Priority = %d, want 2", got.Priority)
	}
}

func TestMemoryStore_GetTask_NotFound(t *testing.T) {
	t.Parallel()

	store := newMemoryStore()
	_, err := store.GetTask(context.Background(), "nonexistent")
	if !errors.Is(err, state.ErrTaskNotFound) {
		t.Errorf("expected ErrTaskNotFound, got %v", err)
	}
}

func TestMemoryStore_UpdateProgress(t *testing.T) {
	t.Parallel()

	store := newMemoryStore()
	ctx := context.Background()

	record := state.TaskRecord{ID: "task-2", TaskType: "batch", Status: 1}
	if err := store.SaveTask(ctx, record); err != nil {
		t.Fatalf("SaveTask: %v", err)
	}

	if err := store.UpdateProgress(ctx, "task-2", 3, 100); err != nil {
		t.Fatalf("UpdateProgress: %v", err)
	}

	got, err := store.GetTask(ctx, "task-2")
	if err != nil {
		t.Fatalf("GetTask: %v", err)
	}
	if got.Status != 3 {
		t.Errorf("Status = %d, want 3", got.Status)
	}
	if got.Progress != 100 {
		t.Errorf("Progress = %d, want 100", got.Progress)
	}
}

func TestMemoryStore_UpdateProgress_NotFound(t *testing.T) {
	t.Parallel()

	store := newMemoryStore()
	err := store.UpdateProgress(context.Background(), "ghost", 2, 50)
	if !errors.Is(err, state.ErrTaskNotFound) {
		t.Errorf("expected ErrTaskNotFound, got %v", err)
	}
}

func TestMemoryStore_DeleteTask(t *testing.T) {
	t.Parallel()

	store := newMemoryStore()
	ctx := context.Background()

	record := state.TaskRecord{ID: "task-3", TaskType: "video"}
	if err := store.SaveTask(ctx, record); err != nil {
		t.Fatalf("SaveTask: %v", err)
	}
	if err := store.DeleteTask(ctx, "task-3"); err != nil {
		t.Fatalf("DeleteTask: %v", err)
	}

	_, err := store.GetTask(ctx, "task-3")
	if !errors.Is(err, state.ErrTaskNotFound) {
		t.Errorf("expected ErrTaskNotFound after delete, got %v", err)
	}
}

func TestMemoryStore_AcquireLock(t *testing.T) {
	t.Parallel()

	store := newMemoryStore()
	ctx := context.Background()

	got, err := store.AcquireLock(ctx, "task-4", time.Minute)
	if err != nil {
		t.Fatalf("AcquireLock (first): %v", err)
	}
	if !got {
		t.Error("expected lock acquired on first call")
	}

	got, err = store.AcquireLock(ctx, "task-4", time.Minute)
	if err != nil {
		t.Fatalf("AcquireLock (second): %v", err)
	}
	if got {
		t.Error("expected lock denied on second call")
	}
}
