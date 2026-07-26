package pipeline_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/ironclaw/mcp-ironclaw/internal/domain"
	"github.com/ironclaw/mcp-ironclaw/internal/pipeline"
)

type fixedClock struct{ at time.Time }

func (c fixedClock) Now() time.Time { return c.at }

type recordingTransport struct {
	mu      sync.Mutex
	batches []domain.Batch
	err     error
	release chan struct{}
}

func (t *recordingTransport) Send(ctx context.Context, batch domain.Batch) error {
	if t.release != nil {
		select {
		case <-t.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.err != nil {
		return t.err
	}
	t.batches = append(t.batches, batch)
	return nil
}

func (t *recordingTransport) snapshot() []domain.Batch {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]domain.Batch(nil), t.batches...)
}

type sliceSource struct {
	events []domain.Event
	closed bool
}

func (s *sliceSource) Events(ctx context.Context) (<-chan domain.Event, error) {
	out := make(chan domain.Event)
	go func() {
		defer close(out)
		for _, event := range s.events {
			select {
			case <-ctx.Done():
				return
			case out <- event:
			}
		}
	}()
	return out, nil
}

func (s *sliceSource) Close() error {
	s.closed = true
	return nil
}

func newEvent(deviceID string, processID int32) domain.Event {
	return domain.Event{
		DeviceID:     deviceID,
		UserID:       "user-1",
		ProcessID:    processID,
		PodID:        "pod-a",
		Kind:         domain.EventKindSyscall,
		ObservedAt:   time.Unix(1700000000, 0),
		LatencyNanos: 1000,
		Bytes:        128,
	}
}

func newEvents(deviceID string, count int) []domain.Event {
	events := make([]domain.Event, 0, count)
	for i := 0; i < count; i++ {
		events = append(events, newEvent(deviceID, int32(i%4)))
	}
	return events
}

func baseConfig() pipeline.BatcherConfig {
	return pipeline.BatcherConfig{
		DeviceID:    "device-1",
		MaxEvents:   10,
		MaxInterval: time.Second,
		QueueDepth:  4,
		Clock:       fixedClock{at: time.Unix(1700000000, 0)},
	}
}

func TestBatcherConfigValidation(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(*pipeline.BatcherConfig)
		wantErr error
	}{
		{"missing device", func(c *pipeline.BatcherConfig) { c.DeviceID = "" }, domain.ErrMissingDeviceID},
		{"zero max events", func(c *pipeline.BatcherConfig) { c.MaxEvents = 0 }, pipeline.ErrInvalidMaxEvents},
		{"zero interval", func(c *pipeline.BatcherConfig) { c.MaxInterval = 0 }, pipeline.ErrInvalidInterval},
		{"zero queue depth", func(c *pipeline.BatcherConfig) { c.QueueDepth = 0 }, pipeline.ErrInvalidQueueDepth},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			config := baseConfig()
			tc.mutate(&config)
			_, err := pipeline.NewBatcher(config)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("got %v, want %v", err, tc.wantErr)
			}
		})
	}
}

func TestBatcherGroupsEventsBySize(t *testing.T) {
	batcher, err := pipeline.NewBatcher(baseConfig())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	transport := &recordingTransport{}
	source := &sliceSource{events: newEvents("device-1", 25)}

	if err := batcher.Run(context.Background(), source, transport); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	batches := transport.snapshot()
	if len(batches) != 3 {
		t.Fatalf("got %d batches, want 3", len(batches))
	}
	if got := batches[0].Len(); got != 10 {
		t.Fatalf("first batch has %d events, want 10", got)
	}
	if got := batches[2].Len(); got != 5 {
		t.Fatalf("final batch has %d events, want 5", got)
	}

	stats := batcher.Stats()
	if stats.EventsAccepted != 25 {
		t.Fatalf("accepted %d, want 25", stats.EventsAccepted)
	}
	if stats.BatchesSent != 3 {
		t.Fatalf("sent %d batches, want 3", stats.BatchesSent)
	}
}

func TestBatcherFlushesPartialBatchOnFlushSignal(t *testing.T) {
	flush := make(chan time.Time, 1)
	config := baseConfig()
	config.FlushSignal = flush
	config.MaxInterval = 0

	batcher, err := pipeline.NewBatcher(config)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	events := make(chan domain.Event)
	source := &channelSource{events: events}
	transport := &recordingTransport{}

	done := make(chan error, 1)
	go func() { done <- batcher.Run(context.Background(), source, transport) }()

	events <- newEvent("device-1", 1)
	events <- newEvent("device-1", 2)
	flush <- time.Unix(1700000001, 0)

	waitFor(t, func() bool { return len(transport.snapshot()) == 1 })

	close(events)
	if err := <-done; err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	batches := transport.snapshot()
	if got := batches[0].Len(); got != 2 {
		t.Fatalf("flushed batch has %d events, want 2", got)
	}
}

func TestBatcherRejectsForeignAndInvalidEvents(t *testing.T) {
	batcher, err := pipeline.NewBatcher(baseConfig())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	invalid := newEvent("device-1", 1)
	invalid.LatencyNanos = -5

	foreign := newEvent("device-2", 1)

	source := &sliceSource{events: []domain.Event{newEvent("device-1", 1), foreign, invalid}}
	transport := &recordingTransport{}

	if err := batcher.Run(context.Background(), source, transport); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	stats := batcher.Stats()
	if stats.EventsAccepted != 1 {
		t.Fatalf("accepted %d, want 1", stats.EventsAccepted)
	}
	if stats.EventsRejected != 2 {
		t.Fatalf("rejected %d, want 2", stats.EventsRejected)
	}

	batches := transport.snapshot()
	if len(batches) != 1 || batches[0].Len() != 1 {
		t.Fatalf("got %d batches, want a single one-event batch", len(batches))
	}
	if err := batches[0].Validate(); err != nil {
		t.Fatalf("emitted batch failed validation: %v", err)
	}
}

func TestBatcherDropOldestAccountsForEveryLostEvent(t *testing.T) {
	config := baseConfig()
	config.MaxEvents = 1
	config.QueueDepth = 1
	config.Policy = pipeline.OverflowDropOldest

	batcher, err := pipeline.NewBatcher(config)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	release := make(chan struct{})
	transport := &recordingTransport{release: release}
	source := &sliceSource{events: newEvents("device-1", 20)}

	done := make(chan error, 1)
	go func() { done <- batcher.Run(context.Background(), source, transport) }()

	waitFor(t, func() bool {
		stats := batcher.Stats()
		return stats.EventsAccepted == 20 && stats.BatchesDropped > 0
	})

	close(release)
	if err := <-done; err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	waitFor(t, func() bool {
		stats := batcher.Stats()
		return stats.BatchesSent+stats.BatchesDropped == stats.EventsAccepted
	})

	stats := batcher.Stats()
	if stats.EventsDropped != stats.BatchesDropped {
		t.Fatalf("dropped %d events across %d batches of one event each", stats.EventsDropped, stats.BatchesDropped)
	}
	if stats.BatchesSent+stats.BatchesDropped != 20 {
		t.Fatalf("sent %d and dropped %d, want them to sum to 20", stats.BatchesSent, stats.BatchesDropped)
	}
}

func TestBatcherCountsSendFailures(t *testing.T) {
	batcher, err := pipeline.NewBatcher(baseConfig())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	transport := &recordingTransport{err: errors.New("transport unavailable")}
	source := &sliceSource{events: newEvents("device-1", 20)}

	if err := batcher.Run(context.Background(), source, transport); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	stats := batcher.Stats()
	if stats.SendFailures != 2 {
		t.Fatalf("send failures %d, want 2", stats.SendFailures)
	}
	if stats.BatchesSent != 0 {
		t.Fatalf("batches sent %d, want 0", stats.BatchesSent)
	}
}

func TestBatcherRejectsNilCollaborators(t *testing.T) {
	batcher, err := pipeline.NewBatcher(baseConfig())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := batcher.Run(context.Background(), nil, &recordingTransport{}); !errors.Is(err, pipeline.ErrNilSource) {
		t.Fatalf("got %v, want ErrNilSource", err)
	}
	if err := batcher.Run(context.Background(), &sliceSource{}, nil); !errors.Is(err, pipeline.ErrNilTransport) {
		t.Fatalf("got %v, want ErrNilTransport", err)
	}
}

type channelSource struct{ events chan domain.Event }

func (s *channelSource) Events(ctx context.Context) (<-chan domain.Event, error) {
	return s.events, nil
}

func (s *channelSource) Close() error { return nil }

func waitFor(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition not met before deadline")
}
