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

func syntheticConfig() pipeline.SyntheticConfig {
	return pipeline.SyntheticConfig{
		DeviceID:     "device-1",
		UserID:       "user-1",
		PodID:        "pod-a",
		ProcessIDs:   []int32{1, 2, 3},
		EventCount:   100,
		ErrorRate:    0.1,
		BaseLatency:  time.Millisecond,
		TailLatency:  50 * time.Millisecond,
		TailFraction: 0.05,
		Seed:         42,
		Clock:        fixedClock{at: time.Unix(1700000000, 0)},
	}
}

func TestSyntheticSourceConfigValidation(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(*pipeline.SyntheticConfig)
		wantErr error
	}{
		{"missing device", func(c *pipeline.SyntheticConfig) { c.DeviceID = "" }, domain.ErrMissingDeviceID},
		{"missing user", func(c *pipeline.SyntheticConfig) { c.UserID = "" }, domain.ErrMissingUserID},
		{"zero events", func(c *pipeline.SyntheticConfig) { c.EventCount = 0 }, pipeline.ErrInvalidEventCount},
		{"no processes", func(c *pipeline.SyntheticConfig) { c.ProcessIDs = nil }, pipeline.ErrNoProcesses},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			config := syntheticConfig()
			tc.mutate(&config)
			if _, err := pipeline.NewSyntheticSource(config); !errors.Is(err, tc.wantErr) {
				t.Fatalf("got %v, want %v", err, tc.wantErr)
			}
		})
	}
}

func TestSyntheticSourceProducesValidEvents(t *testing.T) {
	source, err := pipeline.NewSyntheticSource(syntheticConfig())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	events, err := source.Events(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	processes := map[int32]int{}
	count := 0
	for event := range events {
		if err := event.Validate(); err != nil {
			t.Fatalf("invalid synthetic event: %v", err)
		}
		if event.DeviceID != "device-1" || event.UserID != "user-1" {
			t.Fatalf("unexpected identity %q/%q", event.DeviceID, event.UserID)
		}
		processes[event.ProcessID]++
		count++
	}

	if count != 100 {
		t.Fatalf("produced %d events, want 100", count)
	}
	if len(processes) != 3 {
		t.Fatalf("covered %d process ids, want 3", len(processes))
	}
}

func TestSyntheticSourceIsDeterministicForSeed(t *testing.T) {
	collect := func() []domain.Event {
		source, err := pipeline.NewSyntheticSource(syntheticConfig())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		events, err := source.Events(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		var collected []domain.Event
		for event := range events {
			collected = append(collected, event)
		}
		return collected
	}

	first, second := collect(), collect()
	if len(first) != len(second) {
		t.Fatalf("run lengths differ: %d vs %d", len(first), len(second))
	}
	for i := range first {
		if first[i] != second[i] {
			t.Fatalf("event %d differs between seeded runs", i)
		}
	}
}

func TestSyntheticSourceConcurrentStreamsAreRaceFree(t *testing.T) {
	source, err := pipeline.NewSyntheticSource(syntheticConfig())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			events, err := source.Events(context.Background())
			if err != nil {
				return
			}
			for event := range events {
				if event.LatencyNanos < 0 {
					t.Error("negative latency produced")
					return
				}
			}
		}()
	}
	wg.Wait()
}

func TestSyntheticSourceStopsOnClose(t *testing.T) {
	config := syntheticConfig()
	config.EventCount = 1000000
	config.Interval = time.Millisecond

	source, err := pipeline.NewSyntheticSource(config)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	events, err := source.Events(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	<-events
	if err := source.Close(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	drained := make(chan struct{})
	go func() {
		defer close(drained)
		for range events {
		}
	}()

	select {
	case <-drained:
	case <-time.After(5 * time.Second):
		t.Fatal("source did not stop after Close")
	}

	if _, err := source.Events(context.Background()); !errors.Is(err, pipeline.ErrSourceClosed) {
		t.Fatalf("got %v, want ErrSourceClosed", err)
	}
}

func TestSyntheticSourceStopsOnContextCancel(t *testing.T) {
	config := syntheticConfig()
	config.EventCount = 1000000
	config.Interval = time.Millisecond

	source, err := pipeline.NewSyntheticSource(config)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	events, err := source.Events(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	<-events
	cancel()

	drained := make(chan struct{})
	go func() {
		defer close(drained)
		for range events {
		}
	}()

	select {
	case <-drained:
	case <-time.After(5 * time.Second):
		t.Fatal("source did not stop after context cancellation")
	}
}

func TestSyntheticSourceProducesTailLatencies(t *testing.T) {
	config := syntheticConfig()
	config.EventCount = 10000

	source, err := pipeline.NewSyntheticSource(config)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	events, err := source.Events(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var tail, failed int
	for event := range events {
		if event.LatencyNanos >= int64(config.TailLatency) {
			tail++
		}
		if event.Failed {
			failed++
		}
	}

	if tail == 0 {
		t.Fatal("no tail latencies produced despite a non-zero tail fraction")
	}
	if failed == 0 {
		t.Fatal("no failed events produced despite a non-zero error rate")
	}
}
