package commands

import (
	"context"
	"errors"
	"fmt"

	"github.com/ironclaw/mcp-ironclaw/internal/domain"
)

var (
	ErrNilResetter     = errors.New("commands: device resetter is nil")
	ErrUnsupportedKind = errors.New("commands: command kind is not supported")
)

const (
	MetricCommandsApplied  = "commands_applied"
	MetricCommandsRejected = "commands_rejected"
)

type DeviceResetter interface {
	ResetDevice(ctx context.Context, userID, deviceID string) error
}

type Service struct {
	resetter DeviceResetter
	metrics  domain.MetricsRecorder
}

func New(resetter DeviceResetter, metrics domain.MetricsRecorder) (*Service, error) {
	if resetter == nil {
		return nil, ErrNilResetter
	}
	return &Service{resetter: resetter, metrics: metrics}, nil
}

func (s *Service) HandleCommand(ctx context.Context, command domain.Command) error {
	if err := command.Validate(); err != nil {
		s.record(MetricCommandsRejected, 1)
		return err
	}

	switch command.Kind {
	case domain.CommandResetCounters:
		if err := s.resetter.ResetDevice(ctx, command.UserID, command.DeviceID); err != nil {
			s.record(MetricCommandsRejected, 1)
			return err
		}

	default:
		s.record(MetricCommandsRejected, 1)
		return fmt.Errorf("%w: %s", ErrUnsupportedKind, command.Kind)
	}

	s.record(MetricCommandsApplied, 1)
	return nil
}

func (s *Service) record(name string, delta int64) {
	if s.metrics == nil {
		return
	}
	s.metrics.Increment(name, delta)
}
