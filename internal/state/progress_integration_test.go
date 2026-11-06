//go:build integration

package state_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/ronnitchopra/dispatch/internal/state"
	"github.com/ronnitchopra/dispatch/internal/testutil"
)

// deleteBeforeProgressWrite schedules a real second client deleting the task
// immediately before the progress mutation reaches Redis. It covers both the
// original HSET path and a script's EVALSHA/EVAL path, without relying on sleeps.
type deleteBeforeProgressWrite struct {
	other   *redis.Client
	key     string
	deleted atomic.Bool
	removed atomic.Int64
}

func (*deleteBeforeProgressWrite) DialHook(next redis.DialHook) redis.DialHook { return next }

func (*deleteBeforeProgressWrite) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func (h *deleteBeforeProgressWrite) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		switch cmd.Name() {
		case "hset", "evalsha", "eval":
			if !h.deleted.Swap(true) {
				removed, err := h.other.Del(ctx, h.key).Result()
				if err != nil {
					return err
				}
				h.removed.Store(removed)
			}
		}
		return next(ctx, cmd)
	}
}

func TestRedisStore_UpdateProgress_Integration(t *testing.T) {
	t.Run("deletion_before_mutation_does_not_recreate_task", func(t *testing.T) {
		store, client, id := attemptTask(t)
		other := testutil.RedisClient(t)
		hook := &deleteBeforeProgressWrite{other: other, key: "task:" + id}
		client.AddHook(hook)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		err := store.UpdateProgress(ctx, id, 2, 42)
		if !hook.deleted.Load() || hook.removed.Load() != 1 {
			t.Fatalf("deletion interleaving was not exercised: deleted=%v removed=%d", hook.deleted.Load(), hook.removed.Load())
		}
		_, getErr := store.GetTask(ctx, id)
		fields, fieldsErr := other.HGetAll(ctx, "task:"+id).Result()
		if fieldsErr != nil {
			t.Fatal(fieldsErr)
		}
		t.Logf("UpdateProgress=%v GetTask=%v remaining fields=%v", err, getErr, fields)
		if !errors.Is(err, state.ErrTaskNotFound) {
			t.Errorf("deleted task update must return ErrTaskNotFound, got %v", err)
		}
		if len(fields) != 0 || !errors.Is(getErr, state.ErrTaskNotFound) {
			t.Fatalf("progress resurrected a deleted task: fields=%v GetTask=%v", fields, getErr)
		}
	})

	t.Run("existing_task_preserves_metadata_and_attempts", func(t *testing.T) {
		store, _, id := attemptTask(t)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		before, err := store.GetTask(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if n, err := store.StartAttempt(ctx, id, 2); err != nil || n != 1 {
			t.Fatalf("setting attempt history: attempt=%d error=%v", n, err)
		}
		if err := store.UpdateProgress(ctx, id, 3, 100); err != nil {
			t.Fatal(err)
		}
		after, err := store.GetTask(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if after.Status != 3 || after.Progress != 100 || after.ID != before.ID || after.TaskType != before.TaskType ||
			!after.CreatedAt.Equal(before.CreatedAt) || after.UpdatedAt.Before(before.UpdatedAt) {
			t.Errorf("progress update corrupted its record: before=%+v after=%+v", before, after)
		}
		if n, err := store.StartAttempt(ctx, id, 2); err != nil || n != 2 {
			t.Errorf("progress reset attempt history: next attempt=%d error=%v", n, err)
		}
	})

	t.Run("missing_task_is_not_created", func(t *testing.T) {
		store, client, id := attemptTask(t)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := store.DeleteTask(ctx, id); err != nil {
			t.Fatal(err)
		}
		if err := store.UpdateProgress(ctx, id, 2, 1); !errors.Is(err, state.ErrTaskNotFound) {
			t.Errorf("missing task returned %v", err)
		}
		if exists, err := client.Exists(ctx, "task:"+id).Result(); err != nil || exists != 0 {
			t.Fatalf("missing task was created: exists=%d error=%v", exists, err)
		}
	})
}
