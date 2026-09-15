package app

import (
	"context"

	"github.com/ironclaw/mcp-ironclaw/internal/domain"
)

var _ domain.WindowPublisher = (*fanOutWindows)(nil)

type fanOutWindows struct {
	targets []domain.WindowPublisher
}

func newFanOutWindows(targets ...domain.WindowPublisher) *fanOutWindows {
	return &fanOutWindows{targets: targets}
}

func (f *fanOutWindows) PublishWindow(ctx context.Context, window domain.AggregateWindow) error {
	var firstErr error

	for _, target := range f.targets {
		if err := target.PublishWindow(ctx, window); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}
