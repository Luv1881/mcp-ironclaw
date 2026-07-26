package bus

import (
	"context"

	"github.com/ironclaw/mcp-ironclaw/internal/domain"
)

var (
	_ domain.Publisher       = (*BatchBus)(nil)
	_ domain.Consumer        = (*BatchBus)(nil)
	_ domain.WindowPublisher = (*WindowBus)(nil)
	_ domain.WindowConsumer  = (*WindowBus)(nil)
)

type BatchBus struct {
	*Bus[domain.Batch]
}

func NewBatchBus(config Config) (*BatchBus, error) {
	inner, err := New[domain.Batch](config)
	if err != nil {
		return nil, err
	}
	return &BatchBus{Bus: inner}, nil
}

func (b *BatchBus) Publish(ctx context.Context, partitionKey string, batch domain.Batch) error {
	return b.Send(ctx, partitionKey, batch)
}

func (b *BatchBus) Consume(ctx context.Context, handler domain.BatchHandler) error {
	return b.RunConsumerGroup(ctx, func(ctx context.Context, batch domain.Batch) error {
		return handler(ctx, batch)
	})
}

type WindowBus struct {
	*Bus[domain.AggregateWindow]
}

func NewWindowBus(config Config) (*WindowBus, error) {
	inner, err := New[domain.AggregateWindow](config)
	if err != nil {
		return nil, err
	}
	return &WindowBus{Bus: inner}, nil
}

func (b *WindowBus) PublishWindow(ctx context.Context, window domain.AggregateWindow) error {
	return b.Send(ctx, window.Key.ShardTag(), window)
}

func (b *WindowBus) ConsumeWindows(ctx context.Context, handler domain.WindowHandler) error {
	return b.RunConsumerGroup(ctx, func(ctx context.Context, window domain.AggregateWindow) error {
		return handler(ctx, window)
	})
}
