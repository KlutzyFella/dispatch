package state

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

const taskKeyPrefix = "task:"

var updateProgress = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 0 then
    return 0
end
redis.call('HSET', KEYS[1], 'status', ARGV[1], 'progress', ARGV[2], 'updated_at', ARGV[3])
return 1
`)

// RedisTaskStore implements TaskStore using Redis hashes for task records.
type RedisTaskStore struct {
	client *redis.Client
}

// NewRedisTaskStore returns a RedisTaskStore backed by the provided client.
func NewRedisTaskStore(client *redis.Client) *RedisTaskStore {
	return &RedisTaskStore{client: client}
}

// SaveTask serialises a TaskRecord into a Redis hash at key "task:{id}".
func (s *RedisTaskStore) SaveTask(ctx context.Context, record TaskRecord) error {
	key := taskKeyPrefix + record.ID
	fields := map[string]any{
		"id":          record.ID,
		"task_type":   record.TaskType,
		"priority":    record.Priority,
		"max_retries": record.MaxRetries,
		"status":      record.Status,
		"progress":    record.Progress,
		"created_at":  record.CreatedAt.UnixNano(),
		"updated_at":  record.UpdatedAt.UnixNano(),
	}
	if err := s.client.HSet(ctx, key, fields).Err(); err != nil {
		return fmt.Errorf("saving task %q: %w", record.ID, err)
	}
	return nil
}

// GetTask fetches the Redis hash at "task:{id}" and deserialises it.
// Returns ErrTaskNotFound if the key does not exist.
func (s *RedisTaskStore) GetTask(ctx context.Context, taskID string) (TaskRecord, error) {
	key := taskKeyPrefix + taskID
	vals, err := s.client.HGetAll(ctx, key).Result()
	if err != nil {
		return TaskRecord{}, fmt.Errorf("getting task %q: %w", taskID, err)
	}
	if len(vals) == 0 {
		return TaskRecord{}, ErrTaskNotFound
	}
	return parseRecord(taskID, vals)
}

// UpdateProgress updates the status, progress, and updated_at fields of an
// existing task atomically. Returns ErrTaskNotFound if the task hash does not
// exist, without recreating a task deleted concurrently.
func (s *RedisTaskStore) UpdateProgress(ctx context.Context, taskID string, status, progress int32) error {
	key := taskKeyPrefix + taskID

	updated, err := updateProgress.Run(ctx, s.client, []string{key}, status, progress, time.Now().UnixNano()).Int64()
	if err != nil {
		return fmt.Errorf("updating progress for task %q: %w", taskID, err)
	}
	if updated == 0 {
		return ErrTaskNotFound
	}
	return nil
}

// DeleteTask removes the Redis hash for the given task. It is a no-op if the
// key does not exist.
func (s *RedisTaskStore) DeleteTask(ctx context.Context, taskID string) error {
	key := taskKeyPrefix + taskID
	if err := s.client.Del(ctx, key).Err(); err != nil {
		return fmt.Errorf("deleting task %q: %w", taskID, err)
	}
	return nil
}

// parseRecord converts a Redis hash map into a TaskRecord.
func parseRecord(taskID string, vals map[string]string) (TaskRecord, error) {
	parseInt32 := func(key string) (int32, error) {
		v, err := strconv.ParseInt(vals[key], 10, 32)
		if err != nil {
			return 0, fmt.Errorf("parsing field %q for task %q: %w", key, taskID, err)
		}
		return int32(v), nil
	}
	parseInt64 := func(key string) (int64, error) {
		v, err := strconv.ParseInt(vals[key], 10, 64)
		if err != nil {
			return 0, fmt.Errorf("parsing field %q for task %q: %w", key, taskID, err)
		}
		return v, nil
	}

	priority, err := parseInt32("priority")
	if err != nil {
		return TaskRecord{}, err
	}
	maxRetries, err := parseInt32("max_retries")
	if err != nil {
		return TaskRecord{}, err
	}
	statusVal, err := parseInt32("status")
	if err != nil {
		return TaskRecord{}, err
	}
	progress, err := parseInt32("progress")
	if err != nil {
		return TaskRecord{}, err
	}
	createdNano, err := parseInt64("created_at")
	if err != nil {
		return TaskRecord{}, err
	}
	updatedNano, err := parseInt64("updated_at")
	if err != nil {
		return TaskRecord{}, err
	}

	return TaskRecord{
		ID:         taskID,
		TaskType:   vals["task_type"],
		Priority:   priority,
		MaxRetries: maxRetries,
		Status:     statusVal,
		Progress:   progress,
		CreatedAt:  time.Unix(0, createdNano),
		UpdatedAt:  time.Unix(0, updatedNano),
	}, nil
}
