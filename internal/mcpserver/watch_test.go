package mcpserver_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ironclaw/mcp-ironclaw/internal/domain"
	"github.com/ironclaw/mcp-ironclaw/internal/mcpserver"
	"github.com/ironclaw/mcp-ironclaw/internal/store"
)

func watchTools(t *testing.T, memory *store.Memory) *mcpserver.Tools {
	t.Helper()

	tools, err := mcpserver.NewTools(mcpserver.Options{
		State:   memory,
		Devices: memory,
		Metrics: memory,
		Watcher: memory,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	return tools
}

func appliedWindow(count int64, windowID int64) domain.AggregateWindow {
	return domain.AggregateWindow{
		Key:         domain.CorrelationKey{UserID: "user-000", DeviceID: "device-000", ProcessID: 1, PodID: "pod-000"},
		WindowID:    windowID,
		Sequence:    windowID,
		WindowStart: time.Unix(1700000000, 0).UTC(),
		WindowEnd:   time.Unix(1700000010, 0).UTC(),
		Count:       count,
		P95Nanos:    45000000,
		P99Nanos:    73000000,
	}
}

func TestWatchDeviceReturnsTheNextUpdate(t *testing.T) {
	memory := store.NewMemory()
	tools := watchTools(t, memory)

	go func() {
		time.Sleep(100 * time.Millisecond)
		memory.ApplyWindow(context.Background(), appliedWindow(42, 1))
	}()

	output, err := tools.WatchDevice(context.Background(), mcpserver.WatchDeviceInput{
		UserID:         "user-000",
		DeviceID:       "device-000",
		TimeoutSeconds: 5,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !output.Updated || output.TimedOut {
		t.Fatalf("expected an update, got %+v", output)
	}
	if output.State.Count != 42 {
		t.Fatalf("returned count %d, want 42", output.State.Count)
	}
	if output.State.P99Nanos != 73000000 {
		t.Fatalf("returned p99 %d, want 73000000", output.State.P99Nanos)
	}
}

func TestWatchDeviceTimesOutWithoutFailing(t *testing.T) {
	tools := watchTools(t, store.NewMemory())

	started := time.Now()
	output, err := tools.WatchDevice(context.Background(), mcpserver.WatchDeviceInput{
		UserID:         "user-000",
		DeviceID:       "device-000",
		TimeoutSeconds: 1,
	})
	if err != nil {
		t.Fatalf("a quiet device must time out cleanly, not error: %v", err)
	}

	if !output.TimedOut || output.Updated {
		t.Fatalf("expected a timeout, got %+v", output)
	}
	if elapsed := time.Since(started); elapsed > 3*time.Second {
		t.Fatalf("waited %s for a 1 second timeout", elapsed)
	}
}

func TestWatchDeviceValidatesItsInput(t *testing.T) {
	tools := watchTools(t, store.NewMemory())

	if _, err := tools.WatchDevice(context.Background(), mcpserver.WatchDeviceInput{DeviceID: "device-000"}); !errors.Is(err, mcpserver.ErrMissingUserID) {
		t.Fatalf("got %v, want ErrMissingUserID", err)
	}
	if _, err := tools.WatchDevice(context.Background(), mcpserver.WatchDeviceInput{UserID: "user-000"}); !errors.Is(err, mcpserver.ErrMissingDevice) {
		t.Fatalf("got %v, want ErrMissingDevice", err)
	}
}

func TestWatchDeviceReportsItsOwnAbsence(t *testing.T) {
	memory := store.NewMemory()

	tools, err := mcpserver.NewTools(mcpserver.Options{State: memory, Devices: memory, Metrics: memory})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	_, err = tools.WatchDevice(context.Background(), mcpserver.WatchDeviceInput{UserID: "user-000", DeviceID: "device-000"})
	if !errors.Is(err, mcpserver.ErrNilDeviceWatcher) {
		t.Fatalf("got %v, want ErrNilDeviceWatcher", err)
	}
}

func TestWatchDeviceCapsAnOverlongTimeout(t *testing.T) {
	memory := store.NewMemory()
	tools := watchTools(t, memory)

	go func() {
		time.Sleep(50 * time.Millisecond)
		memory.ApplyWindow(context.Background(), appliedWindow(1, 1))
	}()

	output, err := tools.WatchDevice(context.Background(), mcpserver.WatchDeviceInput{
		UserID:         "user-000",
		DeviceID:       "device-000",
		TimeoutSeconds: 86400,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if output.WaitedFor != mcpserver.MaxWatchTimeout.String() {
		t.Fatalf("a day-long timeout was accepted as %s, want it capped at %s", output.WaitedFor, mcpserver.MaxWatchTimeout)
	}
}

func TestWatchDeviceRefusesAnotherUsersDevice(t *testing.T) {
	memory := store.NewMemory()

	tools, err := mcpserver.NewTools(mcpserver.Options{
		State:       memory,
		Devices:     memory,
		Metrics:     memory,
		Watcher:     memory,
		RequireAuth: true,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctx := mcpserver.ContextWithPrincipal(context.Background(), mcpserver.Principal{UserID: "user-000"})

	if _, err := tools.WatchDevice(ctx, mcpserver.WatchDeviceInput{UserID: "user-999", DeviceID: "device-000", TimeoutSeconds: 1}); !errors.Is(err, mcpserver.ErrForbidden) {
		t.Fatalf("got %v, want ErrForbidden when watching another tenant's device", err)
	}
}

func TestWatchDeviceStopsWhenTheCallerDisconnects(t *testing.T) {
	tools := watchTools(t, store.NewMemory())

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)

	go func() {
		_, err := tools.WatchDevice(ctx, mcpserver.WatchDeviceInput{UserID: "user-000", DeviceID: "device-000", TimeoutSeconds: 300})
		done <- err
	}()

	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("got %v, want context.Canceled", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("cancelling the caller did not end the watch")
	}
}
