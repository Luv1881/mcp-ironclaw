package aggregate_test

import (
	"errors"
	"testing"
	"time"

	"github.com/ironclaw/mcp-ironclaw/internal/aggregate"
	"github.com/ironclaw/mcp-ironclaw/internal/domain"
)

const windowSize = 10 * time.Second

var windowBase = time.Unix(1700000000, 0).UTC()

func newAggregator(t *testing.T) *aggregate.Aggregator {
	t.Helper()
	aggregator, err := aggregate.New(aggregate.Config{WindowSize: windowSize, RelativeAccuracy: 0.01})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	return aggregator
}

func event(processID int32, at time.Time, latency time.Duration, failed bool) domain.Event {
	return domain.Event{
		DeviceID:     "device-1",
		UserID:       "user-1",
		ProcessID:    processID,
		PodID:        "pod-a",
		Kind:         domain.EventKindSyscall,
		ObservedAt:   at,
		LatencyNanos: int64(latency),
		Bytes:        100,
		Failed:       failed,
	}
}

func TestAggregatorRejectsInvalidConfig(t *testing.T) {
	if _, err := aggregate.New(aggregate.Config{WindowSize: 0}); !errors.Is(err, aggregate.ErrInvalidWindowSize) {
		t.Fatalf("got %v, want ErrInvalidWindowSize", err)
	}
	if _, err := aggregate.New(aggregate.Config{WindowSize: windowSize, RelativeAccuracy: 1.2}); !errors.Is(err, aggregate.ErrInvalidAccuracy) {
		t.Fatalf("got %v, want ErrInvalidAccuracy", err)
	}
}

func TestAggregatorSeparatesWindowsAndProcesses(t *testing.T) {
	aggregator := newAggregator(t)

	for i := 0; i < 10; i++ {
		if err := aggregator.Ingest(event(1, windowBase, time.Millisecond, false)); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	for i := 0; i < 5; i++ {
		if err := aggregator.Ingest(event(2, windowBase, time.Millisecond, true)); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if err := aggregator.Ingest(event(1, windowBase.Add(windowSize), time.Millisecond, false)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := aggregator.OpenWindows(); got != 3 {
		t.Fatalf("open windows %d, want 3", got)
	}

	windows, err := aggregator.CloseWindowsBefore(windowBase.Add(windowSize))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(windows) != 2 {
		t.Fatalf("closed %d windows, want 2", len(windows))
	}

	byProcess := map[int32]domain.AggregateWindow{}
	for _, window := range windows {
		byProcess[window.Key.ProcessID] = window
	}

	if got := byProcess[1].Count; got != 10 {
		t.Fatalf("process 1 count %d, want 10", got)
	}
	if got := byProcess[1].ErrorCount; got != 0 {
		t.Fatalf("process 1 error count %d, want 0", got)
	}
	if got := byProcess[2].Count; got != 5 {
		t.Fatalf("process 2 count %d, want 5", got)
	}
	if got := byProcess[2].ErrorCount; got != 5 {
		t.Fatalf("process 2 error count %d, want 5", got)
	}
	if got := byProcess[1].Bytes; got != 1000 {
		t.Fatalf("process 1 bytes %d, want 1000", got)
	}

	if got := aggregator.OpenWindows(); got != 1 {
		t.Fatalf("open windows after close %d, want 1", got)
	}
}

func TestAggregatorWindowBoundariesAreTumbling(t *testing.T) {
	aggregator := newAggregator(t)

	if err := aggregator.Ingest(event(1, windowBase.Add(3*time.Second), time.Millisecond, false)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	windows, err := aggregator.Flush()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(windows) != 1 {
		t.Fatalf("flushed %d windows, want 1", len(windows))
	}

	window := windows[0]
	if !window.WindowEnd.Equal(window.WindowStart.Add(windowSize)) {
		t.Fatalf("window %v..%v is not %v wide", window.WindowStart, window.WindowEnd, windowSize)
	}
	if window.WindowStart.After(windowBase.Add(3 * time.Second)) {
		t.Fatalf("window start %v is after the event", window.WindowStart)
	}
	if !window.WindowEnd.After(windowBase.Add(3 * time.Second)) {
		t.Fatalf("window end %v does not contain the event", window.WindowEnd)
	}
}

func TestAggregatorComputesPercentilesWithinAccuracy(t *testing.T) {
	aggregator := newAggregator(t)

	values := make([]int64, 0, 1000)
	for i := 1; i <= 1000; i++ {
		latency := time.Duration(i) * time.Microsecond
		values = append(values, int64(latency))
		if err := aggregator.Ingest(event(1, windowBase, latency, false)); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	windows, err := aggregator.Flush()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(windows) != 1 {
		t.Fatalf("flushed %d windows, want 1", len(windows))
	}

	window := windows[0]
	assertWithinAccuracy(t, window.P95Nanos, exactQuantile(values, 0.95), 0.01)
	assertWithinAccuracy(t, window.P99Nanos, exactQuantile(values, 0.99), 0.01)

	if window.MaxNanos != int64(1000*time.Microsecond) {
		t.Fatalf("max %d, want %d", window.MaxNanos, int64(1000*time.Microsecond))
	}
	if window.P99Nanos < window.P95Nanos {
		t.Fatalf("p99 %d is below p95 %d", window.P99Nanos, window.P95Nanos)
	}
}

func TestAggregatorRejectsInvalidEvent(t *testing.T) {
	aggregator := newAggregator(t)

	invalid := event(1, windowBase, time.Millisecond, false)
	invalid.UserID = ""

	if err := aggregator.Ingest(invalid); !errors.Is(err, domain.ErrMissingUserID) {
		t.Fatalf("got %v, want ErrMissingUserID", err)
	}
	if got := aggregator.OpenWindows(); got != 0 {
		t.Fatalf("open windows %d, want 0", got)
	}
}

func TestAggregatorWindowIdentityIsUniquePerKeyAndWindow(t *testing.T) {
	aggregator := newAggregator(t)

	if err := aggregator.Ingest(event(1, windowBase, time.Millisecond, false)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := aggregator.Ingest(event(2, windowBase, time.Millisecond, false)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := aggregator.Ingest(event(1, windowBase.Add(windowSize), time.Millisecond, false)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	windows, err := aggregator.Flush()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	seen := map[string]bool{}
	for _, window := range windows {
		identity := window.Identity()
		if seen[identity] {
			t.Fatalf("duplicate window identity %q", identity)
		}
		seen[identity] = true
	}
	if len(seen) != 3 {
		t.Fatalf("got %d distinct identities, want 3", len(seen))
	}
}

func TestAggregatorIngestBatch(t *testing.T) {
	aggregator := newAggregator(t)

	batch := domain.Batch{
		DeviceID: "device-1",
		Events: []domain.Event{
			event(1, windowBase, time.Millisecond, false),
			event(1, windowBase, 2*time.Millisecond, true),
		},
	}

	if err := aggregator.IngestBatch(batch); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	windows, err := aggregator.Flush()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(windows) != 1 {
		t.Fatalf("flushed %d windows, want 1", len(windows))
	}
	if windows[0].Count != 2 || windows[0].ErrorCount != 1 {
		t.Fatalf("count %d errors %d, want 2 and 1", windows[0].Count, windows[0].ErrorCount)
	}
}
