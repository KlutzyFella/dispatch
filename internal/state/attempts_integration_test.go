//go:build integration

package state_test

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/ronnitchopra/dispatch/internal/state"
	"github.com/ronnitchopra/dispatch/internal/testutil"
)

func attemptTask(t *testing.T) (*state.RedisTaskStore, *redis.Client, string) {
	t.Helper()
	client := testutil.RedisClient(t)
	store := state.NewRedisTaskStore(client)
	id := "attempt-test-" + uuid.NewString()
	t.Cleanup(func() { client.Del(context.Background(), "task:"+id) })
	if err := store.SaveTask(context.Background(), state.TaskRecord{ID: id, TaskType: "simulation", Status: 1,
		CreatedAt: time.Now(), UpdatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	return store, client, id
}

func TestRedisStore_StartAttempt_Integration(t *testing.T) {
	t.Run("atomic_budget", func(t *testing.T) {
		store, client, id := attemptTask(t)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		type result struct {
			attempt int64
			err     error
		}
		results := make(chan result, 16)
		for i := 0; i < cap(results); i++ {
			go func() {
				attempt, err := store.StartAttempt(ctx, id, 3)
				results <- result{attempt, err}
			}()
		}
		seen := make(map[int64]bool)
		for i := 0; i < cap(results); i++ {
			result := <-results
			if result.err != nil {
				t.Errorf("reserving concurrent attempt: %v", result.err)
			} else if result.attempt != 0 {
				if seen[result.attempt] || result.attempt < 1 || result.attempt > 3 {
					t.Errorf("duplicate or over-budget reservation %d", result.attempt)
				}
				seen[result.attempt] = true
			}
		}
		count, err := client.HGet(ctx, "task:"+id, "attempts").Int64()
		if err != nil || count != 3 || len(seen) != 3 {
			t.Fatalf("stored=%d reservations=%v error=%v; want exactly three starts", count, seen, err)
		}
	})
	t.Run("missing_task_not_recreated", func(t *testing.T) {
		store, client, id := attemptTask(t)
		ctx := context.Background()
		if err := store.DeleteTask(ctx, id); err != nil {
			t.Fatal(err)
		}
		if _, err := store.StartAttempt(ctx, id, 1); !errors.Is(err, state.ErrTaskNotFound) {
			t.Fatalf("missing task returned %v", err)
		}
		if n, err := client.Exists(ctx, "task:"+id).Result(); err != nil || n != 0 {
			t.Fatalf("missing task was recreated: exists=%d error=%v", n, err)
		}
	})
	t.Run("int32_retry_limit_does_not_overflow", func(t *testing.T) {
		store, client, id := attemptTask(t)
		ctx := context.Background()
		limit := int64(math.MaxInt32) + 1
		if err := client.HSet(ctx, "task:"+id, "attempts", limit-1).Err(); err != nil {
			t.Fatal(err)
		}
		if n, err := store.StartAttempt(ctx, id, limit); err != nil || n != limit {
			t.Fatalf("final large-budget attempt=%d error=%v, want %d", n, err, limit)
		}
		if n, err := store.StartAttempt(ctx, id, limit); err != nil || n != 0 {
			t.Fatalf("exhausted large budget returned %d, %v", n, err)
		}
	})
	t.Run("cancelled_start_not_counted", func(t *testing.T) {
		store, client, id := attemptTask(t)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := store.StartAttempt(ctx, id, 1); !errors.Is(err, context.Canceled) {
			t.Fatalf("got %v", err)
		}
		if exists, err := client.HExists(context.Background(), "task:"+id, "attempts").Result(); err != nil || exists {
			t.Fatalf("cancelled reservation changed attempts: exists=%v error=%v", exists, err)
		}
	})
	for _, corrupt := range []string{"not-a-number", "-1", "1.5"} {
		t.Run("corrupt_count/"+corrupt, func(t *testing.T) {
			store, client, id := attemptTask(t)
			ctx := context.Background()
			if err := client.HSet(ctx, "task:"+id, "attempts", corrupt).Err(); err != nil {
				t.Fatal(err)
			}
			if _, err := store.StartAttempt(ctx, id, 3); err == nil {
				t.Fatal("accepted corrupt attempt count")
			}
			if got, err := client.HGet(ctx, "task:"+id, "attempts").Result(); err != nil || got != corrupt {
				t.Fatalf("corrupt state was overwritten: got=%q error=%v", got, err)
			}
		})
	}
}
