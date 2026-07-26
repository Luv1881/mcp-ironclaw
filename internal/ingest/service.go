package ingest

import (
	"context"
	"errors"

	"github.com/ironclaw/mcp-ironclaw/internal/domain"
)

var (
	ErrNilPublisher    = errors.New("ingest: publisher is nil")
	ErrMissingIdentity = errors.New("ingest: authenticated identity is empty")
)

const (
	MetricBatchesAccepted = "ingest_batches_accepted"
	MetricBatchesRejected = "ingest_batches_rejected"
	MetricEventsAccepted  = "ingest_events_accepted"
)

type Service struct {
	publisher domain.Publisher
	metrics   domain.MetricsRecorder
}

func New(publisher domain.Publisher, metrics domain.MetricsRecorder) (*Service, error) {
	if publisher == nil {
		return nil, ErrNilPublisher
	}
	return &Service{publisher: publisher, metrics: metrics}, nil
}

func (s *Service) Accept(ctx context.Context, authenticatedDeviceID string, batch domain.Batch) error {
	if authenticatedDeviceID == "" {
		s.record(MetricBatchesRejected, 1)
		return ErrMissingIdentity
	}
	if err := batch.VerifyIdentity(authenticatedDeviceID); err != nil {
		s.record(MetricBatchesRejected, 1)
		return err
	}
	if err := batch.Validate(); err != nil {
		s.record(MetricBatchesRejected, 1)
		return err
	}

	if err := s.publisher.Publish(ctx, batch.DeviceID, batch); err != nil {
		s.record(MetricBatchesRejected, 1)
		return err
	}

	s.record(MetricBatchesAccepted, 1)
	s.record(MetricEventsAccepted, int64(batch.Len()))

	return nil
}

func (s *Service) record(name string, delta int64) {
	if s.metrics == nil {
		return
	}
	s.metrics.Increment(name, delta)
}
