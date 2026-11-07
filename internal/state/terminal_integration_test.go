//go:build integration

package state_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	taskv1 "github.com/ronnitchopra/dispatch/gen/task/v1"
	"github.com/ronnitchopra/dispatch/internal/state"
	"github.com/ronnitchopra/dispatch/internal/testutil"
)

type completeBeforeProgressWrite struct {
	other     *state.RedisTaskStore
	id        string
	completed atomic.Bool
}

func (*completeBeforeProgressWrite) DialHook(next redis.DialHook) redis.DialHook { return next }
func (*completeBeforeProgressWrite) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}
func (h *completeBeforeProgressWrite) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		switch cmd.Name() {
		case "hset", "evalsha", "eval":
			if !h.completed.Swap(true) {
				if err := h.other.UpdateProgress(ctx, h.id, int32(taskv1.TaskStatus_TASK_STATUS_COMPLETED), 100); err != nil {
					return err
				}
			}
		}
		return next(ctx, cmd)
	}
}

func TestRedisStore_CompletedStateIsTerminal(t *testing.T) {
	for _, incoming := range []taskv1.TaskStatus{
		taskv1.TaskStatus_TASK_STATUS_RUNNING,
		taskv1.TaskStatus_TASK_STATUS_FAILED,
		taskv1.TaskStatus_TASK_STATUS_COMPLETED,
	} {
		t.Run(incoming.String(), func(t *testing.T) {
			store, _, id := attemptTask(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := store.UpdateProgress(ctx, id, int32(taskv1.TaskStatus_TASK_STATUS_COMPLETED), 100); err != nil {
				t.Fatal(err)
			}
			before, err := store.GetTask(ctx, id)
			if err != nil {
				t.Fatal(err)
			}
			progress := int32(0)
			if incoming == taskv1.TaskStatus_TASK_STATUS_COMPLETED {
				progress = 100
			}
			err = store.UpdateProgress(ctx, id, int32(incoming), progress)
			if incoming == taskv1.TaskStatus_TASK_STATUS_COMPLETED && err != nil {
				t.Errorf("idempotent completion was rejected: %v", err)
			} else if incoming != taskv1.TaskStatus_TASK_STATUS_COMPLETED && err == nil {
				t.Error("late update was allowed to reverse completion")
			}
			after, getErr := store.GetTask(ctx, id)
			if getErr != nil || after != before {
				t.Errorf("completed record changed: before=%+v after=%+v error=%v", before, after, getErr)
			}
		})
	}
	t.Run("completion_wins_before_stale_write", func(t *testing.T) {
		store, client, id := attemptTask(t)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := store.UpdateProgress(ctx, id, int32(taskv1.TaskStatus_TASK_STATUS_RUNNING), 50); err != nil {
			t.Fatal(err)
		}
		hook := &completeBeforeProgressWrite{other: state.NewRedisTaskStore(testutil.RedisClient(t)), id: id}
		client.AddHook(hook)
		err := store.UpdateProgress(ctx, id, int32(taskv1.TaskStatus_TASK_STATUS_FAILED), 0)
		after, getErr := store.GetTask(ctx, id)
		if !hook.completed.Load() {
			t.Fatal("the concurrent completion was not exercised")
		}
		if err == nil || getErr != nil || after.Status != int32(taskv1.TaskStatus_TASK_STATUS_COMPLETED) || after.Progress != 100 {
			t.Fatalf("stale write undid concurrent completion: update=%v record=%+v get=%v", err, after, getErr)
		}
	})
	t.Run("failed_attempt_can_retry", func(t *testing.T) {
		store, _, id := attemptTask(t)
		ctx := context.Background()
		if err := store.UpdateProgress(ctx, id, int32(taskv1.TaskStatus_TASK_STATUS_FAILED), 0); err != nil {
			t.Fatal(err)
		}
		if err := store.UpdateProgress(ctx, id, int32(taskv1.TaskStatus_TASK_STATUS_RUNNING), 0); err != nil {
			t.Fatal(err)
		}
		if record, err := store.GetTask(ctx, id); err != nil || record.Status != int32(taskv1.TaskStatus_TASK_STATUS_RUNNING) {
			t.Fatalf("retry did not enter RUNNING: record=%+v error=%v", record, err)
		}
	})
}
