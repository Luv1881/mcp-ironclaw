package store_test

import (
	"context"
	"testing"
	"time"

	"github.com/ironclaw/mcp-ironclaw/internal/domain"
	"github.com/ironclaw/mcp-ironclaw/internal/store"
)

func watchedWindow(count int64, windowID int64) domain.AggregateWindow {
	return domain.AggregateWindow{
		Key:         domain.CorrelationKey{UserID: "user-000", DeviceID: "device-000", ProcessID: 1, PodID: "pod-000"},
		WindowID:    windowID,
		Sequence:    windowID,
		WindowStart: time.Unix(1700000000, 0).UTC(),
		WindowEnd:   time.Unix(1700000010, 0).UTC(),
		Count:       count,
		P95Nanos:    1000,
		P99Nanos:    2000,
	}
}

func TestWatchDeviceDeliversAppliedWindows(t *testing.T) {
	memory := store.NewMemory()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	updates, err := memory.WatchDevice(ctx, "user-000", "device-000")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := memory.ApplyWindow(ctx, watchedWindow(10, 1)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	select {
	case state := <-updates:
		if state.Count != 10 {
			t.Fatalf("watcher saw count %d, want 10", state.Count)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("watcher never received the applied window")
	}
}

func TestWatchDeviceIgnoresOtherDevices(t *testing.T) {
	memory := store.NewMemory()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	updates, err := memory.WatchDevice(ctx, "user-000", "device-000")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	other := watchedWindow(10, 1)
	other.Key.DeviceID = "device-999"
	if err := memory.ApplyWindow(ctx, other); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	select {
	case state := <-updates:
		t.Fatalf("watcher received an update for another device: %+v", state)
	case <-time.After(200 * time.Millisecond):
	}
}

func TestDuplicateWindowsDoNotNotify(t *testing.T) {
	memory := store.NewMemory()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	updates, err := memory.WatchDevice(ctx, "user-000", "device-000")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	window := watchedWindow(10, 1)
	for i := 0; i < 3; i++ {
		if err := memory.ApplyWindow(ctx, window); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	<-updates

	select {
	case state := <-updates:
		t.Fatalf("a redelivered window produced a second notification: %+v", state)
	case <-time.After(200 * time.Millisecond):
	}
}

func TestResetNotifiesWatchers(t *testing.T) {
	memory := store.NewMemory()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := memory.ApplyWindow(ctx, watchedWindow(10, 1)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	updates, err := memory.WatchDevice(ctx, "user-000", "device-000")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := memory.ResetDevice(ctx, "user-000", "device-000"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	select {
	case state := <-updates:
		if state.Count != 0 {
			t.Fatalf("watcher saw count %d after a reset, want 0", state.Count)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("watcher never saw the reset")
	}
}

func TestCancellingAWatchClosesItsChannelAndStopsTracking(t *testing.T) {
	memory := store.NewMemory()
	ctx, cancel := context.WithCancel(context.Background())

	updates, err := memory.WatchDevice(ctx, "user-000", "device-000")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	cancel()

	select {
	case _, ok := <-updates:
		if ok {
			t.Fatal("expected the channel to be closed, not to carry a value")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancelling the context did not close the watch channel")
	}

	if err := memory.ApplyWindow(context.Background(), watchedWindow(10, 1)); err != nil {
		t.Fatalf("applying a window after a watch was cancelled must not fail: %v", err)
	}
}

func TestASlowWatcherIsCountedNotBlocking(t *testing.T) {
	memory := store.NewMemory()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if _, err := memory.WatchDevice(ctx, "user-000", "device-000"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 1; i <= 100; i++ {
			if err := memory.ApplyWindow(ctx, watchedWindow(1, int64(i))); err != nil {
				return
			}
		}
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("a watcher that never reads blocked the apply path")
	}

	metrics, err := memory.PipelineMetrics(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if metrics[store.MetricWatchDropped] == 0 {
		t.Fatal("updates were dropped for a slow watcher but the drop was not counted")
	}
	if metrics[store.MetricWindowsApplied] != 100 {
		t.Fatalf("applied %d windows, want 100 regardless of watcher pressure", metrics[store.MetricWindowsApplied])
	}
}

func TestMemorySatisfiesTheDeviceWatcherPort(t *testing.T) {
	var _ domain.DeviceWatcher = store.NewMemory()
}
