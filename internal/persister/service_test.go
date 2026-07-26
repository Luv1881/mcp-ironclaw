package persister_test

import (
	"context"
	"errors"
	"testing"

	"github.com/ironclaw/mcp-ironclaw/internal/domain"
	"github.com/ironclaw/mcp-ironclaw/internal/persister"
	"github.com/ironclaw/mcp-ironclaw/internal/store"
)

type failingWriter struct{ err error }

func (w failingWriter) ApplyWindow(context.Context, domain.AggregateWindow) error { return w.err }

func sampleWindow() domain.AggregateWindow {
	return domain.AggregateWindow{
		Key:      domain.CorrelationKey{UserID: "user-1", DeviceID: "device-1", ProcessID: 1},
		WindowID: 7,
		Count:    12,
	}
}

func TestNewRejectsNilWriter(t *testing.T) {
	if _, err := persister.New(nil, nil); !errors.Is(err, persister.ErrNilWriter) {
		t.Fatalf("got %v, want ErrNilWriter", err)
	}
}

func TestHandleWindowPersistsAndCounts(t *testing.T) {
	memory := store.NewMemory()

	service, err := persister.New(memory, memory)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctx := context.Background()
	if err := service.HandleWindow(ctx, sampleWindow()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	state, err := memory.DeviceState(ctx, "user-1", "device-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if state.Count != 12 {
		t.Fatalf("persisted count %d, want 12", state.Count)
	}

	metrics, err := memory.PipelineMetrics(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if metrics[persister.MetricWindowsPersisted] != 1 {
		t.Fatalf("persisted metric %d, want 1", metrics[persister.MetricWindowsPersisted])
	}
}

func TestHandleWindowPropagatesWriterFailureWithoutCounting(t *testing.T) {
	sentinel := errors.New("store unavailable")
	metrics := store.NewMemory()

	service, err := persister.New(failingWriter{err: sentinel}, metrics)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := service.HandleWindow(context.Background(), sampleWindow()); !errors.Is(err, sentinel) {
		t.Fatalf("got %v, want the writer error", err)
	}

	recorded, err := metrics.PipelineMetrics(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if recorded[persister.MetricWindowsPersisted] != 0 {
		t.Fatalf("counted %d persisted windows after a failure, want 0", recorded[persister.MetricWindowsPersisted])
	}
}

func TestHandleWindowWorksWithoutMetricsRecorder(t *testing.T) {
	service, err := persister.New(store.NewMemory(), nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := service.HandleWindow(context.Background(), sampleWindow()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
