package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	taskv1 "github.com/ronnitchopra/dispatch/gen/task/v1"
	"github.com/ronnitchopra/dispatch/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	publicationLease           = 15 * time.Second
	publicationTimeout         = 10 * time.Second
	publicationFinalizeTimeout = 3 * time.Second
	publicationRetryDelay      = time.Second
)

func pendingPublication(id string) error {
	return status.Errorf(codes.Unavailable, "task %q publication is pending; retry using this task ID as the idempotency key", id)
}

// RecoverPublications retries due intents until cancellation. Broker errors
// retain and defer an intent; corrupt state or persistence failures return an
// error so the service cannot keep running with a broken recovery component.
func (s *DispatchServer) RecoverPublications(ctx context.Context) error {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			return nil
		}
		claimCtx, stopClaim := context.WithTimeout(ctx, publicationFinalizeTimeout)
		p, err := s.store.ClaimPublication(claimCtx, s.topic, uuid.NewString(), publicationLease)
		stopClaim()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("publication recovery: %w", err)
		}
		if p != nil {
			retryable, err := s.publishIntent(ctx, *p)
			if ctx.Err() != nil {
				return nil
			}
			if err != nil {
				if !retryable {
					return fmt.Errorf("publication recovery: %w", err)
				}
				s.logger.Error("publication recovery will retry", slog.String("task_id", p.ID), slog.String("error", err.Error()))
			} else {
				s.logger.Info("publication recovered", slog.String("task_id", p.ID), slog.String("topic", p.Topic))
			}
			continue
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

// publishIntent returns retryable=true only for a broker error whose intent was
// successfully retained and released. Redis and decoding failures need attention.
func (s *DispatchServer) publishIntent(ctx context.Context, p state.Publication) (bool, error) {
	if p.Topic != s.topic {
		return false, fmt.Errorf("publication %q belongs to topic %q, not %q", p.ID, p.Topic, s.topic)
	}
	var task taskv1.Task
	if err := proto.Unmarshal(p.Payload, &task); err != nil {
		return false, fmt.Errorf("decoding retained publication %q: %w", p.ID, err)
	}
	publishCtx, stopPublish := context.WithTimeout(ctx, publicationTimeout)
	err := s.producer.Publish(publishCtx, p.ID, &task)
	stopPublish()
	// Confirmation/release must survive cancellation of the submitting RPC.
	finalizeCtx, stopFinalize := context.WithTimeout(context.Background(), publicationFinalizeTimeout)
	defer stopFinalize()
	if err != nil {
		if releaseErr := s.store.ReleasePublication(finalizeCtx, p, publicationRetryDelay); releaseErr != nil {
			return false, errors.Join(err, releaseErr)
		}
		return true, err
	}
	if err := s.store.ConfirmPublication(finalizeCtx, p); err != nil {
		return false, fmt.Errorf("confirming acknowledged task %q: %w", p.ID, err)
	}
	return false, nil
}
