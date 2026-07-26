package aggregator_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ironclaw/mcp-ironclaw/internal/aggregate"
	"github.com/ironclaw/mcp-ironclaw/internal/aggregator"
	"github.com/ironclaw/mcp-ironclaw/internal/domain"
	"github.com/ironclaw/mcp-ironclaw/internal/store"
)

const windowSize = 10 * time.Second

var windowBase = time.Unix(1700000000, 0).UTC()

type flakyPublisher struct {
	published []domain.AggregateWindow
	failAfter int
	err       error
}

func (p *flakyPublisher) PublishWindow(_ context.Context, window domain.AggregateWindow) error {
	if p.err != nil && len(p.published) >= p.failAfter {
		return p.err
	}
	p.published = append(p.published, window)
	return nil
}

func newSource(t *testing.T) *aggregate.Aggregator {
	t.Helper()
	source, err := aggregate.New(aggregate.Config{WindowSize: windowSize, RelativeAccuracy: 0.01})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	return source
}

func batchOf(processIDs ...int32) domain.Batch {
	events := make([]domain.Event, 0, len(processIDs))
	for _, processID := range processIDs {
		events = append(events, domain.Event{
			DeviceID:     "device-1",
			UserID:       "user-1",
			ProcessID:    processID,
			PodID:        "pod-a",
			Kind:         domain.EventKindSyscall,
			ObservedAt:   windowBase,
			LatencyNanos: int64(time.Millisecond),
			Bytes:        100,
		})
	}
	return domain.Batch{DeviceID: "device-1", Events: events}
}

func TestNewRejectsNilCollaborators(t *testing.T) {
	if _, err := aggregator.New(nil, &flakyPublisher{}, nil); !errors.Is(err, aggregator.ErrNilAggregator) {
		t.Fatalf("got %v, want ErrNilAggregator", err)
	}
	if _, err := aggregator.New(newSource(t), nil, nil); !errors.Is(err, aggregator.ErrNilPublisher) {
		t.Fatalf("got %v, want ErrNilPublisher", err)
	}
}

func TestEmitAllPublishesEveryWindow(t *testing.T) {
	source := newSource(t)
	publisher := &flakyPublisher{}
	metrics := store.NewMemory()

	service, err := aggregator.New(source, publisher, metrics)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctx := context.Background()
	if err := service.HandleBatch(ctx, batchOf(1, 2, 3)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := service.EmitAll(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(publisher.published) != 3 {
		t.Fatalf("published %d windows, want 3", len(publisher.published))
	}

	if err := service.EmitAll(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(publisher.published) != 3 {
		t.Fatalf("committed windows were re-emitted: %d", len(publisher.published))
	}
}

func TestPublishFailureRetainsUnpublishedWindows(t *testing.T) {
	source := newSource(t)
	sentinel := errors.New("broker unavailable")
	publisher := &flakyPublisher{failAfter: 1, err: sentinel}
	metrics := store.NewMemory()

	service, err := aggregator.New(source, publisher, metrics)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctx := context.Background()
	if err := service.HandleBatch(ctx, batchOf(1, 2, 3)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := service.EmitAll(ctx); !errors.Is(err, sentinel) {
		t.Fatalf("got %v, want the publisher error", err)
	}
	if len(publisher.published) != 1 {
		t.Fatalf("published %d windows before failing, want 1", len(publisher.published))
	}

	if got := source.OpenWindows(); got != 2 {
		t.Fatalf("retained %d windows after the publish failure, want 2", got)
	}

	publisher.err = nil
	if err := service.EmitAll(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(publisher.published) != 3 {
		t.Fatalf("published %d windows after recovery, want all 3", len(publisher.published))
	}
	if got := source.OpenWindows(); got != 0 {
		t.Fatalf("%d windows remain open after a successful emit, want 0", got)
	}

	recorded, err := metrics.PipelineMetrics(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if recorded[aggregator.MetricWindowsRetained] != 2 {
		t.Fatalf("retained metric %d, want 2", recorded[aggregator.MetricWindowsRetained])
	}
	if recorded[aggregator.MetricWindowsEmitted] != 3 {
		t.Fatalf("emitted metric %d, want 3", recorded[aggregator.MetricWindowsEmitted])
	}
}

func TestEmitClosedWindowsRespectsWatermark(t *testing.T) {
	source := newSource(t)
	publisher := &flakyPublisher{}

	service, err := aggregator.New(source, publisher, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctx := context.Background()

	current := batchOf(1)
	later := batchOf(2)
	later.Events[0].ObservedAt = windowBase.Add(windowSize)

	if err := service.HandleBatch(ctx, current); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := service.HandleBatch(ctx, later); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := service.EmitClosedWindows(ctx, windowBase.Add(windowSize)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(publisher.published) != 1 {
		t.Fatalf("published %d windows, want only the closed one", len(publisher.published))
	}
	if got := source.OpenWindows(); got != 1 {
		t.Fatalf("%d windows open, want the in-flight window to remain", got)
	}
}

func TestHandleBatchRejectsInvalidEventsWithoutPartialApplication(t *testing.T) {
	source := newSource(t)
	service, err := aggregator.New(source, &flakyPublisher{}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	batch := batchOf(1, 2)
	batch.Events[1].UserID = ""

	if err := service.HandleBatch(context.Background(), batch); !errors.Is(err, domain.ErrMissingUserID) {
		t.Fatalf("got %v, want ErrMissingUserID", err)
	}
	if got := source.OpenWindows(); got != 0 {
		t.Fatalf("%d windows were opened from a rejected batch, want 0", got)
	}
}

func TestHandleBatchRejectsCancelledContext(t *testing.T) {
	source := newSource(t)
	service, err := aggregator.New(source, &flakyPublisher{}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := service.HandleBatch(ctx, batchOf(1)); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want context.Canceled", err)
	}
}
