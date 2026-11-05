package state

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

var startAttempt = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 0 then
    return -1
end
local attempts = tonumber(redis.call('HGET', KEYS[1], 'attempts') or '0')
if not attempts or attempts < 0 or attempts % 1 ~= 0 then
    return redis.error_reply('invalid task attempt count')
end
if attempts >= tonumber(ARGV[1]) then
    return 0
end
return redis.call('HINCRBY', KEYS[1], 'attempts', 1)
`)

// StartAttempt atomically reserves one started attempt in an existing task hash.
// It returns the one-based attempt number, or zero when maxAttempts is exhausted.
// An interrupted start remains counted; a missing task is never recreated.
func (s *RedisTaskStore) StartAttempt(ctx context.Context, taskID string, maxAttempts int64) (int64, error) {
	if maxAttempts < 1 {
		return 0, fmt.Errorf("task %q must allow at least one attempt", taskID)
	}
	attempt, err := startAttempt.Run(ctx, s.client, []string{taskKeyPrefix + taskID}, maxAttempts).Int64()
	if err != nil {
		return 0, fmt.Errorf("starting attempt for task %q: %w", taskID, err)
	}
	if attempt == -1 {
		return 0, ErrTaskNotFound
	}
	return attempt, nil
}
