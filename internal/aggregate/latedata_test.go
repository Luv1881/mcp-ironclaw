package aggregate_test

import (
	"testing"
	"time"

	"github.com/ironclaw/mcp-ironclaw/internal/domain"
	"github.com/ironclaw/mcp-ironclaw/internal/store"
)

func TestLateEventsForAClosedWindowAreNotDiscarded(t *testing.T) {
	aggregator := newAggregator(t)

	for i := 0; i < 10; i++ {
		if err := aggregator.Ingest(event(1, windowBase, time.Millisecond, false)); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	first, err := aggregator.CloseWindowsBefore(windowBase.Add(windowSize))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(first) != 1 || first[0].Count != 10 {
		t.Fatalf("first emission %+v, want one window of 10", first)
	}

	for i := 0; i < 4; i++ {
		if err := aggregator.Ingest(event(1, windowBase, time.Millisecond, false)); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	second, err := aggregator.CloseWindowsBefore(windowBase.Add(windowSize))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(second) != 1 {
		t.Fatalf("second emission produced %d windows, want 1", len(second))
	}
	if second[0].Count != 4 {
		t.Fatalf("late emission carried %d events, want only the 4 late ones", second[0].Count)
	}

	if first[0].WindowIdentity() != second[0].WindowIdentity() {
		t.Fatal("late data was attributed to a different window")
	}
	if first[0].Identity() == second[0].Identity() {
		t.Fatal("the two emissions share an identity, so a store would discard the late one as a duplicate")
	}

	memory := store.NewMemory()
	for _, window := range append(first, second...) {
		if err := memory.ApplyWindow(t.Context(), window); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	state, err := memory.DeviceState(t.Context(), "user-1", "device-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if state.Count != 14 {
		t.Fatalf("store counted %d events, want all 14 including the late arrivals", state.Count)
	}
}

func TestRedeliveryOfTheSameEmissionIsStillIdempotent(t *testing.T) {
	aggregator := newAggregator(t)

	for i := 0; i < 6; i++ {
		if err := aggregator.Ingest(event(1, windowBase, time.Millisecond, false)); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	windows, err := aggregator.Flush()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	memory := store.NewMemory()
	for i := 0; i < 5; i++ {
		for _, window := range windows {
			if err := memory.ApplyWindow(t.Context(), window); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		}
	}

	state, err := memory.DeviceState(t.Context(), "user-1", "device-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if state.Count != 6 {
		t.Fatalf("store counted %d events after five redeliveries, want 6", state.Count)
	}
}

func TestEmissionSequenceIsMonotonic(t *testing.T) {
	aggregator := newAggregator(t)

	seen := map[int64]bool{}
	previous := int64(0)

	for round := 0; round < 3; round++ {
		if err := aggregator.Ingest(event(int32(round), windowBase, time.Millisecond, false)); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		windows, err := aggregator.Flush()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		for _, window := range windows {
			if seen[window.Sequence] {
				t.Fatalf("sequence %d was reused", window.Sequence)
			}
			seen[window.Sequence] = true
			if window.Sequence <= previous {
				t.Fatalf("sequence %d did not advance past %d", window.Sequence, previous)
			}
			previous = window.Sequence
		}
	}

	if len(seen) != 3 {
		t.Fatalf("observed %d distinct sequences, want 3", len(seen))
	}
}

func TestWindowIdentityDistinguishesEmissionsButNotWindows(t *testing.T) {
	base := domain.AggregateWindow{
		Key:      domain.CorrelationKey{UserID: "user-1", DeviceID: "device-1", ProcessID: 1},
		WindowID: 5,
		Sequence: 1,
	}
	later := base
	later.Sequence = 2

	if base.WindowIdentity() != later.WindowIdentity() {
		t.Fatal("emissions of the same window should share a window identity")
	}
	if base.Identity() == later.Identity() {
		t.Fatal("emissions of the same window must have distinct dedupe identities")
	}
}
