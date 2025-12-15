package state_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/ronnitchopra/dispatch/internal/state"
)

// newTestRedisClient returns a Redis client pointed at localhost:6379.
// Tests using this helper skip themselves if Redis is not reachable.
func newTestRedisClient(t *testing.T) *redis.Client {
	t.Helper()
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("Redis not available at localhost:6379 — skipping integration test: %v", err)
	}
	t.Cleanup(func() { client.Close() })
	return client
}

func TestRedisStore_SaveAndGet(t *testing.T) {
	t.Parallel()

	client := newTestRedisClient(t)
	store := state.NewRedisTaskStore(client)
	ctx := context.Background()

	id := "redis-test-" + t.Name()
	t.Cleanup(func() { client.Del(ctx, "task:"+id) })

	record := state.TaskRecord{
		ID:         id,
		TaskType:   "email",
		Priority:   2,
		MaxRetries: 3,
		Status:     1,
		Progress:   0,
		CreatedAt:  time.Now().Truncate(time.Millisecond),
		UpdatedAt:  time.Now().Truncate(time.Millisecond),
	}

	if err := store.SaveTask(ctx, record); err != nil {
		t.Fatalf("SaveTask: %v", err)
	}

	got, err := store.GetTask(ctx, id)
	if err != nil {
		t.Fatalf("GetTask: %v", err)
	}
	if got.TaskType != "email" {
		t.Errorf("TaskType = %q, want 'email'", got.TaskType)
	}
	if got.Priority != 2 {
		t.Errorf("Priority = %d, want 2", got.Priority)
	}
	if got.MaxRetries != 3 {
		t.Errorf("MaxRetries = %d, want 3", got.MaxRetries)
	}
}

func TestRedisStore_GetTask_NotFound(t *testing.T) {
	t.Parallel()

	client := newTestRedisClient(t)
	store := state.NewRedisTaskStore(client)

	_, err := store.GetTask(context.Background(), "does-not-exist-ever")
	if !errors.Is(err, state.ErrTaskNotFound) {
		t.Errorf("expected ErrTaskNotFound, got %v", err)
	}
}

func TestRedisStore_UpdateProgress(t *testing.T) {
	t.Parallel()

	client := newTestRedisClient(t)
	store := state.NewRedisTaskStore(client)
	ctx := context.Background()

	id := "redis-upd-" + t.Name()
	t.Cleanup(func() { client.Del(ctx, "task:"+id) })

	record := state.TaskRecord{
		ID: id, TaskType: "batch", Status: 1,
		CreatedAt: time.Now(), UpdatedAt: time.Now(),
	}
	if err := store.SaveTask(ctx, record); err != nil {
		t.Fatalf("SaveTask: %v", err)
	}

	if err := store.UpdateProgress(ctx, id, 3, 100); err != nil {
		t.Fatalf("UpdateProgress: %v", err)
	}

	got, err := store.GetTask(ctx, id)
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

func TestRedisStore_UpdateProgress_NotFound(t *testing.T) {
	t.Parallel()

	client := newTestRedisClient(t)
	store := state.NewRedisTaskStore(client)

	err := store.UpdateProgress(context.Background(), "ghost-task", 2, 50)
	if !errors.Is(err, state.ErrTaskNotFound) {
		t.Errorf("expected ErrTaskNotFound, got %v", err)
	}
}

func TestRedisStore_DeleteTask(t *testing.T) {
	t.Parallel()

	client := newTestRedisClient(t)
	store := state.NewRedisTaskStore(client)
	ctx := context.Background()

	id := "redis-del-" + t.Name()
	t.Cleanup(func() { client.Del(ctx, "task:"+id) })

	record := state.TaskRecord{
		ID: id, TaskType: "video",
		CreatedAt: time.Now(), UpdatedAt: time.Now(),
	}
	if err := store.SaveTask(ctx, record); err != nil {
		t.Fatalf("SaveTask: %v", err)
	}
	if err := store.DeleteTask(ctx, id); err != nil {
		t.Fatalf("DeleteTask: %v", err)
	}

	_, err := store.GetTask(ctx, id)
	if !errors.Is(err, state.ErrTaskNotFound) {
		t.Errorf("expected ErrTaskNotFound after delete, got %v", err)
	}
}

func TestRedisStore_GetTask_CorruptField(t *testing.T) {
	t.Parallel()

	client := newTestRedisClient(t)
	store := state.NewRedisTaskStore(client)
	ctx := context.Background()

	id := "corrupt-" + t.Name()
	t.Cleanup(func() { client.Del(ctx, "task:"+id) })

	// Write a hash that has a non-numeric "priority" field so parseRecord fails.
	client.HSet(ctx, "task:"+id, map[string]any{
		"id":          id,
		"task_type":   "batch",
		"priority":    "not-a-number",
		"max_retries": "3",
		"status":      "1",
		"progress":    "0",
		"created_at":  "0",
		"updated_at":  "0",
	})

	_, err := store.GetTask(ctx, id)
	if err == nil {
		t.Fatal("expected error for corrupt field, got nil")
	}
}

func TestRedisStore_AcquireLock(t *testing.T) {
	t.Parallel()

	client := newTestRedisClient(t)
	store := state.NewRedisTaskStore(client)
	ctx := context.Background()

	taskID := "lock-test-" + t.Name()
	t.Cleanup(func() { client.Del(ctx, "lock:task:"+taskID) })

	got, err := store.AcquireLock(ctx, taskID, 5*time.Second)
	if err != nil {
		t.Fatalf("AcquireLock (first): %v", err)
	}
	if !got {
		t.Error("expected lock acquired on first call")
	}

	got, err = store.AcquireLock(ctx, taskID, 5*time.Second)
	if err != nil {
		t.Fatalf("AcquireLock (second): %v", err)
	}
	if got {
		t.Error("expected lock denied on second call (already held)")
	}
}
