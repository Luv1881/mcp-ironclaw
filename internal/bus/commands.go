package bus

import (
	"context"

	"github.com/ironclaw/mcp-ironclaw/internal/domain"
)

var _ domain.CommandPublisher = (*CommandBus)(nil)

type CommandBus struct {
	*Bus[domain.Command]
}

func NewCommandBus(config Config) (*CommandBus, error) {
	inner, err := New[domain.Command](config)
	if err != nil {
		return nil, err
	}
	return &CommandBus{Bus: inner}, nil
}

func (b *CommandBus) PublishCommand(ctx context.Context, command domain.Command) error {
	if err := command.Validate(); err != nil {
		return err
	}
	return b.Send(ctx, command.ShardTag(), command)
}

func (b *CommandBus) ConsumeCommands(ctx context.Context, handler func(context.Context, domain.Command) error) error {
	return b.RunConsumerGroup(ctx, handler)
}
