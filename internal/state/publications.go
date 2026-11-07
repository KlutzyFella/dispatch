package state

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const publicationKeyPrefix = "publication:"
const outboxKeyPrefix = "outbox:"

var admitTask = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) ~= 0 then return 0 end
if redis.call('TYPE', KEYS[2]).ok ~= 'none' then
    return redis.error_reply('orphan publication already exists')
end
local indexType = redis.call('TYPE', KEYS[3]).ok
if indexType ~= 'none' and indexType ~= 'zset' then
    return redis.error_reply('publication index has wrong type')
end
local clock = redis.call('TIME')
local now = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)
local untilTime = now + tonumber(ARGV[12])
redis.call('HSET', KEYS[1], 'id', ARGV[1], 'task_type', ARGV[2],
    'priority', ARGV[3], 'max_retries', ARGV[4], 'status', ARGV[5],
    'progress', ARGV[6], 'created_at', ARGV[7], 'updated_at', ARGV[8],
    'publication_state', 'pending', 'publication_topic', ARGV[9])
redis.call('HSET', KEYS[2], 'id', ARGV[1], 'topic', ARGV[9],
    'payload', ARGV[10], 'owner', ARGV[11], 'until', untilTime)
redis.call('ZADD', KEYS[3], untilTime, ARGV[1])
return 1
`)

var confirmPublication = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 0 then return -1 end
if redis.call('HGET', KEYS[1], 'publication_topic') ~= ARGV[2] then return 0 end
if redis.call('HGET', KEYS[1], 'publication_state') == 'published' then return 1 end
if redis.call('HGET', KEYS[2], 'owner') ~= ARGV[3] then return 0 end
local indexType = redis.call('TYPE', KEYS[3]).ok
if indexType ~= 'none' and indexType ~= 'zset' then
    return redis.error_reply('publication index has wrong type')
end
redis.call('HSET', KEYS[1], 'publication_state', 'published')
redis.call('DEL', KEYS[2])
redis.call('ZREM', KEYS[3], ARGV[1])
return 1
`)

var releasePublication = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 0 then return -1 end
if redis.call('HGET', KEYS[1], 'publication_topic') ~= ARGV[2] then return 0 end
if redis.call('HGET', KEYS[1], 'publication_state') == 'published' then return 1 end
if redis.call('HGET', KEYS[2], 'owner') ~= ARGV[3] then return 0 end
local indexType = redis.call('TYPE', KEYS[3]).ok
if indexType ~= 'none' and indexType ~= 'zset' then
    return redis.error_reply('publication index has wrong type')
end
local clock = redis.call('TIME')
local now = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)
redis.call('HSET', KEYS[2], 'owner', '', 'until', 0)
redis.call('ZADD', KEYS[3], now + tonumber(ARGV[4]), ARGV[1])
return 1
`)

var claimPublication = redis.NewScript(`
local clock = redis.call('TIME')
local now = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)
local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now, 'LIMIT', 0, 1)
if #ids == 0 then return {} end
local id = ids[1]
local taskKey = ARGV[1] .. id
local publicationKey = ARGV[2] .. id
if redis.call('HGET', taskKey, 'publication_state') ~= 'pending' then
    return redis.error_reply('indexed publication has no pending canonical task')
end
if redis.call('HGET', taskKey, 'publication_topic') ~= ARGV[3] or
   redis.call('HGET', publicationKey, 'topic') ~= ARGV[3] or
   redis.call('HGET', publicationKey, 'id') ~= id then
    return redis.error_reply('publication destination or identity mismatch')
end
local payload = redis.call('HGET', publicationKey, 'payload')
local untilTime = tonumber(redis.call('HGET', publicationKey, 'until'))
if not payload or not untilTime then
    return redis.error_reply('publication payload or lease is missing')
end
if untilTime > now then
    redis.call('ZADD', KEYS[1], untilTime, id)
    return {}
end
local newUntil = now + tonumber(ARGV[5])
redis.call('HSET', publicationKey, 'owner', ARGV[4], 'until', newUntil)
redis.call('ZADD', KEYS[1], newUntil, id)
return {id, payload}
`)

func publicationKeys(p Publication) []string {
	return []string{taskKeyPrefix + p.ID, publicationKeyPrefix + p.ID, outboxKeyPrefix + p.Topic}
}

// AdmitTask atomically creates the canonical record, retained payload, and
// recovery index entry. Existing tasks, including progress, are never replaced.
func (s *RedisTaskStore) AdmitTask(ctx context.Context, record TaskRecord, p Publication, lease time.Duration) error {
	if p.ID != record.ID || p.Topic == "" || p.Owner == "" || lease.Milliseconds() < 1 {
		return fmt.Errorf("invalid publication admission for task %q", record.ID)
	}
	created, err := admitTask.Run(ctx, s.client, publicationKeys(p), record.ID, record.TaskType,
		record.Priority, record.MaxRetries, record.Status, record.Progress,
		record.CreatedAt.UnixNano(), record.UpdatedAt.UnixNano(), p.Topic, p.Payload,
		p.Owner, lease.Milliseconds()).Int64()
	if err != nil {
		return fmt.Errorf("admitting task %q: %w", p.ID, err)
	}
	if created == 0 {
		return ErrTaskExists
	}
	return nil
}

func (s *RedisTaskStore) ConfirmPublication(ctx context.Context, p Publication) error {
	result, err := confirmPublication.Run(ctx, s.client, publicationKeys(p), p.ID, p.Topic, p.Owner).Int64()
	return publicationResult("confirming", p.ID, result, err)
}

func (s *RedisTaskStore) ReleasePublication(ctx context.Context, p Publication, retryDelay time.Duration) error {
	if retryDelay < 0 {
		return fmt.Errorf("negative publication retry delay")
	}
	result, err := releasePublication.Run(ctx, s.client, publicationKeys(p), p.ID, p.Topic, p.Owner, retryDelay.Milliseconds()).Int64()
	return publicationResult("releasing", p.ID, result, err)
}

func publicationResult(operation, id string, result int64, err error) error {
	if err != nil {
		return fmt.Errorf("%s publication %q: %w", operation, id, err)
	}
	if result == -1 {
		return ErrTaskNotFound
	}
	if result == 0 {
		return ErrPublicationLeaseLost
	}
	return nil
}

// ClaimPublication atomically leases the next due intent for this topic. Redis
// time governs eligibility, so dispatch instances do not compare local clocks.
func (s *RedisTaskStore) ClaimPublication(ctx context.Context, topic, owner string, lease time.Duration) (*Publication, error) {
	if topic == "" || owner == "" || lease.Milliseconds() < 1 {
		return nil, fmt.Errorf("invalid publication recovery claim")
	}
	values, err := claimPublication.Run(ctx, s.client, []string{outboxKeyPrefix + topic},
		taskKeyPrefix, publicationKeyPrefix, topic, owner, lease.Milliseconds()).Slice()
	if err != nil {
		return nil, fmt.Errorf("claiming publication for topic %q: %w", topic, err)
	}
	if len(values) == 0 {
		return nil, nil
	}
	if len(values) != 2 {
		return nil, fmt.Errorf("invalid publication claim response")
	}
	id, idOK := values[0].(string)
	payload, payloadOK := values[1].(string)
	if !idOK || !payloadOK {
		return nil, fmt.Errorf("invalid publication claim types")
	}
	return &Publication{ID: id, Topic: topic, Payload: []byte(payload), Owner: owner}, nil
}
