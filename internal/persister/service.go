package persister

import (
	"context"
	"errors"

	"github.com/ironclaw/mcp-ironclaw/internal/domain"
)

var ErrNilWriter = errors.New("persister: state writer is nil")

const MetricWindowsPersisted = "persister_windows_persisted"

type Service struct {
	writer  domain.StateWriter
	metrics domain.MetricsRecorder
}

func New(writer domain.StateWriter, metrics domain.MetricsRecorder) (*Service, error) {
	if writer == nil {
		return nil, ErrNilWriter
	}
	return &Service{writer: writer, metrics: metrics}, nil
}

func (s *Service) HandleWindow(ctx context.Context, window domain.AggregateWindow) error {
	if err := s.writer.ApplyWindow(ctx, window); err != nil {
		return err
	}
	if s.metrics != nil {
		s.metrics.Increment(MetricWindowsPersisted, 1)
	}
	return nil
}
