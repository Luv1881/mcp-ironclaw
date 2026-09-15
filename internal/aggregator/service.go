package aggregator

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/ironclaw/mcp-ironclaw/internal/domain"
)

var (
	ErrNilAggregator = errors.New("aggregator: window source is nil")
	ErrNilPublisher  = errors.New("aggregator: window publisher is nil")
)

const (
	MetricBatchesConsumed = "aggregator_batches_consumed"
	MetricWindowsEmitted  = "aggregator_windows_emitted"
	MetricWindowsRetained = "aggregator_windows_retained"
	MetricEventsShed      = "aggregator_events_shed"
)

type WindowSource interface {
	IngestBatch(batch domain.Batch) error
	CollectWindowsBefore(watermark time.Time) ([]domain.AggregateWindow, error)
	CollectAll() ([]domain.AggregateWindow, error)
	Commit(windows []domain.AggregateWindow)
}

type shedReporter interface {
	Shed() int64
}

type Service struct {
	source    WindowSource
	publisher domain.WindowPublisher
	metrics   domain.MetricsRecorder

	shedMu   sync.Mutex
	lastShed int64
}

func New(source WindowSource, publisher domain.WindowPublisher, metrics domain.MetricsRecorder) (*Service, error) {
	if source == nil {
		return nil, ErrNilAggregator
	}
	if publisher == nil {
		return nil, ErrNilPublisher
	}
	return &Service{source: source, publisher: publisher, metrics: metrics}, nil
}

func (s *Service) HandleBatch(ctx context.Context, batch domain.Batch) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.source.IngestBatch(batch); err != nil {
		return err
	}
	s.record(MetricBatchesConsumed, 1)
	s.recordShed()
	return nil
}

func (s *Service) recordShed() {
	reporter, ok := s.source.(shedReporter)
	if !ok {
		return
	}

	s.shedMu.Lock()
	shed := reporter.Shed()
	delta := shed - s.lastShed
	s.lastShed = shed
	s.shedMu.Unlock()

	s.record(MetricEventsShed, delta)
}

func (s *Service) EmitClosedWindows(ctx context.Context, watermark time.Time) error {
	windows, err := s.source.CollectWindowsBefore(watermark)
	if err != nil {
		return err
	}
	return s.publish(ctx, windows)
}

func (s *Service) EmitAll(ctx context.Context) error {
	windows, err := s.source.CollectAll()
	if err != nil {
		return err
	}
	return s.publish(ctx, windows)
}

func (s *Service) publish(ctx context.Context, windows []domain.AggregateWindow) error {
	published := make([]domain.AggregateWindow, 0, len(windows))

	var failure error
	for _, window := range windows {
		if err := s.publisher.PublishWindow(ctx, window); err != nil {
			if failure == nil {
				failure = err
			}
			continue
		}
		published = append(published, window)
	}

	s.source.Commit(published)
	s.record(MetricWindowsEmitted, int64(len(published)))

	if failure != nil {
		s.record(MetricWindowsRetained, int64(len(windows)-len(published)))
	}

	return failure
}

func (s *Service) record(name string, delta int64) {
	if s.metrics == nil || delta == 0 {
		return
	}
	s.metrics.Increment(name, delta)
}
