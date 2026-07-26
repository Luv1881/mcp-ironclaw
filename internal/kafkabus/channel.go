package kafkabus

import (
	"context"

	"github.com/ironclaw/mcp-ironclaw/internal/domain"
)

var (
	_ domain.Publisher        = (*Channel)(nil)
	_ domain.Consumer         = (*Channel)(nil)
	_ domain.WindowPublisher  = (*Channel)(nil)
	_ domain.WindowConsumer   = (*Channel)(nil)
	_ domain.CommandPublisher = (*Channel)(nil)
)

type Channel struct {
	producer *Producer
	consumer *Consumer
}

func NewChannel(config Config) (*Channel, error) {
	producer, err := NewProducer(config)
	if err != nil {
		return nil, err
	}

	consumer, err := NewConsumer(config)
	if err != nil {
		producer.Close()
		return nil, err
	}

	return &Channel{producer: producer, consumer: consumer}, nil
}

func (c *Channel) Publish(ctx context.Context, partitionKey string, batch domain.Batch) error {
	return c.producer.Publish(ctx, partitionKey, batch)
}

func (c *Channel) PublishWindow(ctx context.Context, window domain.AggregateWindow) error {
	return c.producer.PublishWindow(ctx, window)
}

func (c *Channel) PublishCommand(ctx context.Context, command domain.Command) error {
	return c.producer.PublishCommand(ctx, command)
}

func (c *Channel) Consume(ctx context.Context, handler domain.BatchHandler) error {
	return c.consumer.Consume(ctx, handler)
}

func (c *Channel) ConsumeWindows(ctx context.Context, handler domain.WindowHandler) error {
	return c.consumer.ConsumeWindows(ctx, handler)
}

func (c *Channel) ConsumeCommands(ctx context.Context, handler func(context.Context, domain.Command) error) error {
	return c.consumer.ConsumeCommands(ctx, handler)
}

func (c *Channel) Close() {
	c.producer.Close()
	c.consumer.Close()
}
