package state

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

const taskKeyPrefix = "task:"

// The persisted dispatch.v1.TaskStatus wire value. Integration tests seed the
// generated COMPLETED enum to keep this primitive storage contract aligned.
const completedTaskStatus int32 = 3

var createTask = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) ~= 0 then
    return 0
end
redis.call('HSET', KEYS[1], 'id', ARGV[1], 'task_type', ARGV[2],
    'priority', ARGV[3], 'max_retries', ARGV[4], 'status', ARGV[5],
    'progress', ARGV[6], 'created_at', ARGV[7], 'updated_at', ARGV[8])
return 1
`)

var updateProgress = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 0 then
    return 0
end
if tonumber(redis.call('HGET', KEYS[1], 'status')) == tonumber(ARGV[4]) then
    if ARGV[1] == ARGV[4] and ARGV[2] == '100' then
        return 1
    end
    return -1
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

// SaveTask atomically creates the hash at "task:{id}" without replacing it.
func (s *RedisTaskStore) SaveTask(ctx context.Context, record TaskRecord) error {
	key := taskKeyPrefix + record.ID
	created, err := createTask.Run(ctx, s.client, []string{key}, record.ID, record.TaskType,
		record.Priority, record.MaxRetries, record.Status, record.Progress,
		record.CreatedAt.UnixNano(), record.UpdatedAt.UnixNano()).Int64()
	if err != nil {
		return fmt.Errorf("saving task %q: %w", record.ID, err)
	}
	if created == 0 {
		return ErrTaskExists
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
// Completed tasks accept only an unchanged completion acknowledgment; other
// updates return ErrTaskCompleted and leave all fields untouched.
func (s *RedisTaskStore) UpdateProgress(ctx context.Context, taskID string, status, progress int32) error {
	key := taskKeyPrefix + taskID

	updated, err := updateProgress.Run(ctx, s.client, []string{key}, status, progress, time.Now().UnixNano(), completedTaskStatus).Int64()
	if err != nil {
		return fmt.Errorf("updating progress for task %q: %w", taskID, err)
	}
	if updated == 0 {
		return ErrTaskNotFound
	}
	if updated == -1 {
		return ErrTaskCompleted
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
		ID:               taskID,
		TaskType:         vals["task_type"],
		Priority:         priority,
		MaxRetries:       maxRetries,
		Status:           statusVal,
		Progress:         progress,
		CreatedAt:        time.Unix(0, createdNano),
		UpdatedAt:        time.Unix(0, updatedNano),
		PublicationState: vals["publication_state"],
		PublicationTopic: vals["publication_topic"],
	}, nil
}
