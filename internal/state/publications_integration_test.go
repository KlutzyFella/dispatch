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

func TestRedisStore_PublicationClaim_Integration(t *testing.T) {
	t.Run("one_recovery_owner", func(t *testing.T) {
		store, _, record, p := pendingPublication(t)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := store.AdmitTask(ctx, record, p, time.Second); err != nil {
			t.Fatal(err)
		}
		if claimed, err := store.ClaimPublication(ctx, p.Topic, uuid.NewString(), time.Second); err != nil || claimed != nil {
			t.Fatalf("active owner was stolen: %+v %v", claimed, err)
		}
		if err := store.ReleasePublication(ctx, p, 0); err != nil {
			t.Fatal(err)
		}
		type result struct {
			p   *state.Publication
			err error
		}
		results := make(chan result, 16)
		for i := 0; i < cap(results); i++ {
			go func() {
				claimed, err := store.ClaimPublication(ctx, p.Topic, uuid.NewString(), 5*time.Second)
				results <- result{claimed, err}
			}()
		}
		var winner *state.Publication
		owners := 0
		for i := 0; i < cap(results); i++ {
			result := <-results
			if result.err != nil {
				t.Errorf("claim failed: %v", result.err)
			}
			if result.p != nil {
				winner = result.p
				owners++
			}
		}
		if owners != 1 || winner.ID != p.ID || !bytes.Equal(winner.Payload, p.Payload) {
			t.Fatalf("recovery ownership was not exclusive: owners=%d winner=%+v", owners, winner)
		}
		if err := store.ConfirmPublication(ctx, p); !errors.Is(err, state.ErrPublicationLeaseLost) {
			t.Fatalf("stale owner confirmed a takeover: %v", err)
		}
		if err := store.ConfirmPublication(ctx, *winner); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("expired_owner_can_be_recovered", func(t *testing.T) {
		store, _, record, p := pendingPublication(t)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := store.AdmitTask(ctx, record, p, 20*time.Millisecond); err != nil {
			t.Fatal(err)
		}
		for {
			claimed, err := store.ClaimPublication(ctx, p.Topic, "replacement", time.Second)
			if err != nil {
				t.Fatal(err)
			}
			if claimed != nil {
				if claimed.ID != p.ID || claimed.Owner != "replacement" {
					t.Fatalf("wrong recovered intent: %+v", claimed)
				}
				break
			}
			select {
			case <-ctx.Done():
				t.Fatal("expired publication never became eligible")
			case <-time.After(5 * time.Millisecond):
			}
		}
	})
	t.Run("corruption_fails_without_discarding_intent", func(t *testing.T) {
		store, client, record, p := pendingPublication(t)
		ctx := context.Background()
		if err := store.AdmitTask(ctx, record, p, time.Second); err != nil {
			t.Fatal(err)
		}
		if err := store.ReleasePublication(ctx, p, 0); err != nil {
			t.Fatal(err)
		}
		if err := client.HDel(ctx, "publication:"+p.ID, "payload").Err(); err != nil {
			t.Fatal(err)
		}
		if _, err := store.ClaimPublication(ctx, p.Topic, "replacement", time.Second); err == nil {
			t.Fatal("corrupt intent was silently ignored")
		}
		if _, err := client.ZScore(ctx, "outbox:"+p.Topic, p.ID).Result(); err != nil {
			t.Fatalf("corrupt intent was discarded: %v", err)
		}
	})
}
