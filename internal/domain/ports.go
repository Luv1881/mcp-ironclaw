package domain

import (
	"context"
	"time"
)

type EventSource interface {
	Events(ctx context.Context) (<-chan Event, error)
	Close() error
}

type Transport interface {
	Send(ctx context.Context, batch Batch) error
}

type Publisher interface {
	Publish(ctx context.Context, partitionKey string, batch Batch) error
}

type BatchHandler func(ctx context.Context, batch Batch) error

type Consumer interface {
	Consume(ctx context.Context, handler BatchHandler) error
}

type WindowPublisher interface {
	PublishWindow(ctx context.Context, window AggregateWindow) error
}

type WindowHandler func(ctx context.Context, window AggregateWindow) error

type WindowConsumer interface {
	ConsumeWindows(ctx context.Context, handler WindowHandler) error
}

type StateWriter interface {
	ApplyWindow(ctx context.Context, window AggregateWindow) error
}

type DeviceStateReader interface {
	DeviceState(ctx context.Context, userID, deviceID string) (DeviceState, error)
}

type UserDeviceLister interface {
	UserDevices(ctx context.Context, userID string) ([]string, error)
}

type StateReader interface {
	DeviceStateReader
	UserDeviceLister
}

type DeviceWatcher interface {
	WatchDevice(ctx context.Context, userID, deviceID string) (<-chan DeviceState, error)
}

type Lease struct {
	Resource  string
	Token     int64
	ExpiresAt time.Time
}

type FencedLocker interface {
	Acquire(ctx context.Context, resource string, ttl time.Duration) (Lease, error)
	Release(ctx context.Context, lease Lease) error
}

type MetricsReader interface {
	PipelineMetrics(ctx context.Context) (map[string]int64, error)
}

type MetricsRecorder interface {
	Increment(name string, delta int64)
}

type CommandPublisher interface {
	PublishCommand(ctx context.Context, command Command) error
}

type Clock interface {
	Now() time.Time
}
