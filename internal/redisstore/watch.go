package redisstore

import (
	"context"
	"errors"
	"fmt"

	"github.com/ironclaw/mcp-ironclaw/internal/domain"
	"github.com/redis/go-redis/v9"
)

var ErrEmptyDeviceIdentity = errors.New("redisstore: user id and device id are required")

var _ domain.DeviceWatcher = (*Store)(nil)

const watchBuffer = 8

func (s *Store) updatesChannel(userID, deviceID string) string {
	return fmt.Sprintf("%s:{%s}:device:%s:updates", s.namespace, userID, deviceID)
}

func (s *Store) WatchDevice(ctx context.Context, userID, deviceID string) (<-chan domain.DeviceState, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if userID == "" || deviceID == "" {
		return nil, ErrEmptyDeviceIdentity
	}

	subscription := s.client.SSubscribe(ctx, s.updatesChannel(userID, deviceID))
	if _, err := subscription.Receive(ctx); err != nil {
		_ = subscription.Close()
		return nil, fmt.Errorf("redisstore: subscribing to device updates: %w", err)
	}

	updates := make(chan domain.DeviceState, watchBuffer)

	go func() {
		defer close(updates)
		defer func() { _ = subscription.Close() }()

		incoming := subscription.Channel(redis.WithChannelSize(watchBuffer))

		for {
			select {
			case <-ctx.Done():
				return
			case _, ok := <-incoming:
				if !ok {
					return
				}
			}

			state, err := s.DeviceState(ctx, userID, deviceID)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				continue
			}

			select {
			case <-ctx.Done():
				return
			case updates <- state:
			default:
				s.Increment(MetricWatchDropped, 1)
			}
		}
	}()

	return updates, nil
}
