package aggregate_test

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/ironclaw/mcp-ironclaw/internal/aggregate"
	"github.com/ironclaw/mcp-ironclaw/internal/domain"
)

func TestAggregatorConcurrentIngestCountsEveryEvent(t *testing.T) {
	aggregator := newAggregator(t)

	const writers = 8
	const perWriter = 500

	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(id int32) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				if err := aggregator.Ingest(event(id, windowBase, time.Millisecond, false)); err != nil {
					t.Error(err)
					return
				}
			}
		}(int32(w))
	}
	wg.Wait()

	windows, err := aggregator.Flush()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(windows) != writers {
		t.Fatalf("got %d windows, want %d", len(windows), writers)
	}

	var total int64
	for _, window := range windows {
		if window.Count != perWriter {
			t.Fatalf("window %s has count %d, want %d", window.Identity(), window.Count, perWriter)
		}
		total += window.Count
	}
	if total != writers*perWriter {
		t.Fatalf("aggregated %d events, want %d", total, writers*perWriter)
	}
}

func TestAggregatorConcurrentIngestAndFlushLosesNothing(t *testing.T) {
	aggregator := newAggregator(t)

	const total = 4000

	var wg sync.WaitGroup
	var mu sync.Mutex
	var collected int64

	stop := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			windows, err := aggregator.Flush()
			if err != nil {
				t.Error(err)
				return
			}
			mu.Lock()
			for _, window := range windows {
				collected += window.Count
			}
			mu.Unlock()
		}
	}()

	for i := 0; i < total; i++ {
		if err := aggregator.Ingest(event(1, windowBase, time.Millisecond, false)); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	close(stop)
	wg.Wait()

	windows, err := aggregator.Flush()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, window := range windows {
		collected += window.Count
	}

	if collected != total {
		t.Fatalf("collected %d events across concurrent flushes, want %d", collected, total)
	}
}

type stubSketch struct{}

func (s stubSketch) Add(int64)                       {}
func (s stubSketch) Quantile(float64) (int64, error) { return 0, nil }
func (s stubSketch) Count() int64                    { return 0 }
func (s stubSketch) Max() int64                      { return 0 }

func TestAggregatorUsesInjectedSketchFactory(t *testing.T) {
	var created int

	aggregator, err := aggregate.New(aggregate.Config{
		WindowSize: windowSize,
		NewSketch: func() (aggregate.QuantileSketch, error) {
			created++
			return stubSketch{}, nil
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := aggregator.Ingest(event(1, windowBase, time.Millisecond, false)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := aggregator.Ingest(event(1, windowBase, time.Millisecond, false)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := aggregator.Ingest(event(2, windowBase, time.Millisecond, false)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if created != 2 {
		t.Fatalf("factory created %d sketches, want 2", created)
	}
}

func TestAggregatorPropagatesSketchFactoryError(t *testing.T) {
	sentinel := errors.New("factory unavailable")

	aggregator, err := aggregate.New(aggregate.Config{
		WindowSize: windowSize,
		NewSketch:  func() (aggregate.QuantileSketch, error) { return nil, sentinel },
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := aggregator.Ingest(event(1, windowBase, time.Millisecond, false)); !errors.Is(err, sentinel) {
		t.Fatalf("got %v, want the factory error", err)
	}
	if got := aggregator.OpenWindows(); got != 0 {
		t.Fatalf("open windows %d, want 0 after a failed sketch construction", got)
	}
}

func TestSketchTracksNegativeValuesInMinAndMax(t *testing.T) {
	sketch, err := aggregate.NewSketch(0.01)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for _, value := range []int64{-500, -100, -900} {
		sketch.Add(value)
	}

	if got := sketch.Max(); got != -100 {
		t.Fatalf("max %d, want -100", got)
	}
	if got := sketch.Min(); got != -900 {
		t.Fatalf("min %d, want -900", got)
	}
}

func TestKeyCeilingShedsInsteadOfGrowingWithoutBound(t *testing.T) {
	aggregator, err := aggregate.New(aggregate.Config{
		WindowSize:     windowSize,
		MaxOpenWindows: 10,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for i := 0; i < 500; i++ {
		err := aggregator.Ingest(event(int32(i), windowBase, time.Millisecond, false))
		if err != nil && !errors.Is(err, aggregate.ErrTooManyKeys) {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	if got := aggregator.OpenWindows(); got != 10 {
		t.Fatalf("held %d keys, want the ceiling of 10", got)
	}
	if aggregator.Shed() != 490 {
		t.Fatalf("shed %d events, want 490", aggregator.Shed())
	}
}

func TestKeysAlreadyOpenKeepAccumulatingAtTheCeiling(t *testing.T) {
	aggregator, err := aggregate.New(aggregate.Config{
		WindowSize:     windowSize,
		MaxOpenWindows: 2,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for i := 0; i < 5; i++ {
		if err := aggregator.Ingest(event(1, windowBase, time.Millisecond, false)); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if err := aggregator.Ingest(event(2, windowBase, time.Millisecond, false)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := aggregator.Ingest(event(3, windowBase, time.Millisecond, false)); !errors.Is(err, aggregate.ErrTooManyKeys) {
		t.Fatalf("got %v, want ErrTooManyKeys for a new key at the ceiling", err)
	}

	windows, err := aggregator.Flush()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var total int64
	for _, window := range windows {
		total += window.Count
	}
	if total != 6 {
		t.Fatalf("counted %d events, want 6: existing keys must keep accumulating", total)
	}
}

func TestBatchSurvivesShedEvents(t *testing.T) {
	aggregator, err := aggregate.New(aggregate.Config{
		WindowSize:     windowSize,
		MaxOpenWindows: 1,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	batch := domain.Batch{DeviceID: "device-1"}
	for i := 0; i < 4; i++ {
		batch.Events = append(batch.Events, event(int32(i), windowBase, time.Millisecond, false))
	}

	if err := aggregator.IngestBatch(batch); err != nil {
		t.Fatalf("a shed key must not fail the whole batch: %v", err)
	}
	if aggregator.Shed() != 3 {
		t.Fatalf("shed %d, want 3", aggregator.Shed())
	}
}
