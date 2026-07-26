package redisstore

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ironclaw/mcp-ironclaw/internal/domain"
	"github.com/redis/go-redis/v9"
)

var (
	ErrLockHeld        = errors.New("redisstore: lock is held by another holder")
	ErrLeaseSuperseded = errors.New("redisstore: lease was superseded by a newer fencing token")
	ErrEmptyResource   = errors.New("redisstore: resource name is required")
)

var _ domain.FencedLocker = (*Store)(nil)

const DefaultLeaseTTL = 15 * time.Second

var acquireLock = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 1 then
  return -1
end

local token = redis.call('INCR', KEYS[2])
redis.call('SET', KEYS[1], token, 'PX', ARGV[1])

return token
`)

var releaseLock = redis.NewScript(`
local held = redis.call('GET', KEYS[1])
if not held then
  return 0
end
if tonumber(held) ~= tonumber(ARGV[1]) then
  return -1
end
redis.call('DEL', KEYS[1])

return 1
`)

var guardLock = redis.NewScript(`
local fence = tonumber(redis.call('GET', KEYS[2]) or '0')
if fence > tonumber(ARGV[1]) then
  return -1
end

local held = redis.call('GET', KEYS[1])
if not held or tonumber(held) ~= tonumber(ARGV[1]) then
  return -1
end

return 1
`)

func (s *Store) lockKey(resource string) string {
	return fmt.Sprintf("%s:lock:{%s}", s.namespace, resource)
}

func (s *Store) fenceKey(resource string) string {
	return fmt.Sprintf("%s:fence:{%s}", s.namespace, resource)
}

func (s *Store) Acquire(ctx context.Context, resource string, ttl time.Duration) (domain.Lease, error) {
	if resource == "" {
		return domain.Lease{}, ErrEmptyResource
	}
	if ttl <= 0 {
		ttl = DefaultLeaseTTL
	}

	token, err := acquireLock.Run(ctx, s.client,
		[]string{s.lockKey(resource), s.fenceKey(resource)},
		ttl.Milliseconds(),
	).Int64()
	if err != nil {
		return domain.Lease{}, fmt.Errorf("redisstore: acquiring lock %s: %w", resource, err)
	}
	if token < 0 {
		return domain.Lease{}, ErrLockHeld
	}

	return domain.Lease{
		Resource:  resource,
		Token:     token,
		ExpiresAt: time.Now().UTC().Add(ttl),
	}, nil
}

func (s *Store) Release(ctx context.Context, lease domain.Lease) error {
	released, err := releaseLock.Run(ctx, s.client,
		[]string{s.lockKey(lease.Resource)},
		lease.Token,
	).Int64()
	if err != nil {
		return fmt.Errorf("redisstore: releasing lock %s: %w", lease.Resource, err)
	}
	if released < 0 {
		return ErrLeaseSuperseded
	}

	return nil
}

func (s *Store) Guard(ctx context.Context, lease domain.Lease) error {
	valid, err := guardLock.Run(ctx, s.client,
		[]string{s.lockKey(lease.Resource), s.fenceKey(lease.Resource)},
		lease.Token,
	).Int64()
	if err != nil {
		return fmt.Errorf("redisstore: guarding lock %s: %w", lease.Resource, err)
	}
	if valid < 0 {
		return ErrLeaseSuperseded
	}

	return nil
}
