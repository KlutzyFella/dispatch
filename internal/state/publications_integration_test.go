//go:build integration

package state_test

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/ronnitchopra/dispatch/internal/state"
	"github.com/ronnitchopra/dispatch/internal/testutil"
)

func pendingPublication(t *testing.T) (*state.RedisTaskStore, *redis.Client, state.TaskRecord, state.Publication) {
	t.Helper()
	client := testutil.RedisClient(t)
	store := state.NewRedisTaskStore(client)
	id := "publication-test-" + uuid.NewString()
	record := state.TaskRecord{ID: id, TaskType: "simulation", Status: 1, CreatedAt: time.Now(), UpdatedAt: time.Now()}
	p := state.Publication{ID: id, Topic: id, Payload: []byte("original\x00payload"), Owner: uuid.NewString()}
	t.Cleanup(func() { client.Del(context.Background(), "task:"+id, "publication:"+id, "outbox:"+id) })
	return store, client, record, p
}

func TestRedisStore_PublicationAdmission_Integration(t *testing.T) {
	t.Run("atomic_intent_and_immutable_original", func(t *testing.T) {
		store, client, record, p := pendingPublication(t)
		ctx := context.Background()
		if err := store.AdmitTask(ctx, record, p, time.Second); err != nil {
			t.Fatal(err)
		}
		before, err := store.GetTask(ctx, p.ID)
		if err != nil || before.PublicationState != "pending" || before.PublicationTopic != p.Topic {
			t.Fatalf("canonical admission missing: record=%+v error=%v", before, err)
		}
		if err := store.UpdateProgress(ctx, p.ID, 2, 42); err != nil {
			t.Fatal(err)
		}
		if _, err := store.StartAttempt(ctx, p.ID, 2); err != nil {
			t.Fatal(err)
		}
		replacement := p
		replacement.Payload, replacement.Owner = []byte("replacement"), "different-owner"
		if err := store.AdmitTask(ctx, record, replacement, time.Second); !errors.Is(err, state.ErrTaskExists) {
			t.Fatalf("duplicate admission returned %v", err)
		}
		payload, err := client.HGet(ctx, "publication:"+p.ID, "payload").Bytes()
		after, getErr := store.GetTask(ctx, p.ID)
		if err != nil || getErr != nil || !bytes.Equal(payload, p.Payload) || after.Status != 2 || after.Progress != 42 {
			t.Fatalf("duplicate overwrote canonical work: record=%+v payload=%q error=%v/%v", after, payload, err, getErr)
		}
		if n, err := store.StartAttempt(ctx, p.ID, 2); err != nil || n != 2 {
			t.Fatalf("attempt history reset: %d %v", n, err)
		}
		if _, err := client.ZScore(ctx, "outbox:"+p.Topic, p.ID).Result(); err != nil {
			t.Fatalf("missing recovery index: %v", err)
		}
	})
	t.Run("confirmation_is_owner_fenced", func(t *testing.T) {
		store, client, record, p := pendingPublication(t)
		ctx := context.Background()
		if err := store.AdmitTask(ctx, record, p, time.Second); err != nil {
			t.Fatal(err)
		}
		wrong := p
		wrong.Owner = "other-owner"
		if err := store.ConfirmPublication(ctx, wrong); !errors.Is(err, state.ErrPublicationLeaseLost) {
			t.Fatalf("wrong owner confirmed: %v", err)
		}
		if err := store.ConfirmPublication(ctx, p); err != nil {
			t.Fatal(err)
		}
		confirmed, err := store.GetTask(ctx, p.ID)
		if err != nil || confirmed.PublicationState != "published" {
			t.Fatalf("confirmation missing: %+v %v", confirmed, err)
		}
		if n, err := client.Exists(ctx, "publication:"+p.ID).Result(); err != nil || n != 0 {
			t.Fatalf("confirmed payload retained: %d %v", n, err)
		}
		if _, err := client.ZScore(ctx, "outbox:"+p.Topic, p.ID).Result(); !errors.Is(err, redis.Nil) {
			t.Fatalf("confirmed intent still indexed: %v", err)
		}
	})
	t.Run("bad_index_cannot_half_admit", func(t *testing.T) {
		store, client, record, p := pendingPublication(t)
		ctx := context.Background()
		if err := client.Set(ctx, "outbox:"+p.Topic, "wrong type", 0).Err(); err != nil {
			t.Fatal(err)
		}
		if err := store.AdmitTask(ctx, record, p, time.Second); err == nil {
			t.Fatal("invalid index accepted")
		}
		if n, err := client.Exists(ctx, "task:"+p.ID, "publication:"+p.ID).Result(); err != nil || n != 0 {
			t.Fatalf("half admission survived: %d %v", n, err)
		}
	})
}
