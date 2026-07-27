package store

import (
	"context"

	"github.com/ironclaw/mcp-ironclaw/internal/domain"
)

var _ domain.DeviceWatcher = (*Memory)(nil)

const watchBuffer = 8

type watcher struct {
	updates chan domain.DeviceState
	closed  bool
}

func (m *Memory) WatchDevice(ctx context.Context, userID, deviceID string) (<-chan domain.DeviceState, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if userID == "" || deviceID == "" {
		return nil, ErrDeviceNotFound
	}

	subscriber := &watcher{updates: make(chan domain.DeviceState, watchBuffer)}
	key := deviceKey(userID, deviceID)

	m.mu.Lock()
	if m.watchers == nil {
		m.watchers = make(map[string][]*watcher)
	}
	m.watchers[key] = append(m.watchers[key], subscriber)
	m.mu.Unlock()

	go func() {
		<-ctx.Done()
		m.unwatch(key, subscriber)
	}()

	return subscriber.updates, nil
}

func (m *Memory) unwatch(key string, subscriber *watcher) {
	m.mu.Lock()
	defer m.mu.Unlock()

	remaining := m.watchers[key][:0]
	for _, existing := range m.watchers[key] {
		if existing != subscriber {
			remaining = append(remaining, existing)
		}
	}
	if len(remaining) == 0 {
		delete(m.watchers, key)
	} else {
		m.watchers[key] = remaining
	}

	if !subscriber.closed {
		subscriber.closed = true
		close(subscriber.updates)
	}
}

func (m *Memory) notify(key string, state domain.DeviceState) {
	for _, subscriber := range m.watchers[key] {
		select {
		case subscriber.updates <- state:
		default:
			m.metrics[MetricWatchDropped]++
		}
	}
}
