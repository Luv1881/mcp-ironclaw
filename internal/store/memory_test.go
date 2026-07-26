package store_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/ironclaw/mcp-ironclaw/internal/domain"
	"github.com/ironclaw/mcp-ironclaw/internal/store"
)

func window(userID, deviceID string, processID int32, windowID int64, count int64) domain.AggregateWindow {
	return domain.AggregateWindow{
		Key:         domain.CorrelationKey{UserID: userID, DeviceID: deviceID, ProcessID: processID, PodID: "pod-a"},
		WindowID:    windowID,
		WindowStart: time.Unix(windowID*10, 0).UTC(),
		WindowEnd:   time.Unix(windowID*10+10, 0).UTC(),
		Count:       count,
		ErrorCount:  count / 10,
		Bytes:       count * 100,
		P95Nanos:    windowID * 1000,
		P99Nanos:    windowID * 2000,
	}
}

func TestApplyWindowAccumulatesAcrossProcessesAndWindows(t *testing.T) {
	memory := store.NewMemory()
	ctx := context.Background()

	for _, w := range []domain.AggregateWindow{
		window("user-1", "device-1", 1, 1, 100),
		window("user-1", "device-1", 2, 1, 50),
		window("user-1", "device-1", 1, 2, 25),
	} {
		if err := memory.ApplyWindow(ctx, w); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	state, err := memory.DeviceState(ctx, "user-1", "device-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if state.Count != 175 {
		t.Fatalf("count %d, want 175", state.Count)
	}
	if state.Bytes != 17500 {
		t.Fatalf("bytes %d, want 17500", state.Bytes)
	}
	if state.LastWindowID != 2 {
		t.Fatalf("last window %d, want 2", state.LastWindowID)
	}
	if state.P95Nanos != 2000 {
		t.Fatalf("p95 %d, want the latest window's 2000", state.P95Nanos)
	}
}

func TestApplyWindowIgnoresDuplicateWindowIdentity(t *testing.T) {
	memory := store.NewMemory()
	ctx := context.Background()

	target := window("user-1", "device-1", 1, 1, 100)

	for i := 0; i < 4; i++ {
		if err := memory.ApplyWindow(ctx, target); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	state, err := memory.DeviceState(ctx, "user-1", "device-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if state.Count != 100 {
		t.Fatalf("count %d, want 100 after four identical applies", state.Count)
	}

	metrics, err := memory.PipelineMetrics(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if metrics[store.MetricWindowsDuplicate] != 3 {
		t.Fatalf("duplicates %d, want 3", metrics[store.MetricWindowsDuplicate])
	}
	if metrics[store.MetricWindowsApplied] != 1 {
		t.Fatalf("applied %d, want 1", metrics[store.MetricWindowsApplied])
	}
}

func TestOutOfOrderWindowDoesNotRewindPercentiles(t *testing.T) {
	memory := store.NewMemory()
	ctx := context.Background()

	if err := memory.ApplyWindow(ctx, window("user-1", "device-1", 1, 5, 10)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := memory.ApplyWindow(ctx, window("user-1", "device-1", 1, 2, 10)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	state, err := memory.DeviceState(ctx, "user-1", "device-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if state.LastWindowID != 5 {
		t.Fatalf("last window %d, want 5", state.LastWindowID)
	}
	if state.P95Nanos != 5000 {
		t.Fatalf("p95 %d, want the newer window's 5000", state.P95Nanos)
	}
	if state.Count != 20 {
		t.Fatalf("count %d, want both windows counted", state.Count)
	}
}

func TestUserDevicesIsScopedAndSorted(t *testing.T) {
	memory := store.NewMemory()
	ctx := context.Background()

	for _, w := range []domain.AggregateWindow{
		window("user-1", "device-b", 1, 1, 1),
		window("user-1", "device-a", 1, 1, 1),
		window("user-2", "device-z", 1, 1, 1),
	} {
		if err := memory.ApplyWindow(ctx, w); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	devices, err := memory.UserDevices(ctx, "user-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(devices) != 2 || devices[0] != "device-a" || devices[1] != "device-b" {
		t.Fatalf("user-1 devices %v, want sorted [device-a device-b]", devices)
	}

	others, err := memory.UserDevices(ctx, "user-2")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(others) != 1 || others[0] != "device-z" {
		t.Fatalf("user-2 devices %v, want [device-z]", others)
	}

	empty, err := memory.UserDevices(ctx, "user-absent")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(empty) != 0 {
		t.Fatalf("unknown user returned %v, want empty", empty)
	}
}

func TestSameDeviceIdentifierUnderDifferentUsersStaysSeparate(t *testing.T) {
	memory := store.NewMemory()
	ctx := context.Background()

	if err := memory.ApplyWindow(ctx, window("user-1", "shared-id", 1, 1, 10)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := memory.ApplyWindow(ctx, window("user-2", "shared-id", 1, 1, 70)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	first, err := memory.DeviceState(ctx, "user-1", "shared-id")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	second, err := memory.DeviceState(ctx, "user-2", "shared-id")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if first.Count != 10 || second.Count != 70 {
		t.Fatalf("tenant isolation broken: %d and %d, want 10 and 70", first.Count, second.Count)
	}
}

func TestConcurrentApplyWindowCountsEveryWindowOnce(t *testing.T) {
	memory := store.NewMemory()
	ctx := context.Background()

	const writers = 8
	const windowsEach = 100

	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(processID int32) {
			defer wg.Done()
			for i := 0; i < windowsEach; i++ {
				if err := memory.ApplyWindow(ctx, window("user-1", "device-1", processID, int64(i), 1)); err != nil {
					t.Error(err)
					return
				}
			}
		}(int32(w))
	}
	wg.Wait()

	state, err := memory.DeviceState(ctx, "user-1", "device-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if state.Count != writers*windowsEach {
		t.Fatalf("count %d, want %d", state.Count, writers*windowsEach)
	}
}

func TestConcurrentDuplicateApplyIsAppliedOnce(t *testing.T) {
	memory := store.NewMemory()
	ctx := context.Background()

	target := window("user-1", "device-1", 1, 1, 100)

	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := memory.ApplyWindow(ctx, target); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()

	state, err := memory.DeviceState(ctx, "user-1", "device-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if state.Count != 100 {
		t.Fatalf("count %d, want 100 after 32 concurrent identical applies", state.Count)
	}
}

func TestResetDeviceClearsCountersButKeepsRegistration(t *testing.T) {
	memory := store.NewMemory()
	ctx := context.Background()

	if err := memory.ApplyWindow(ctx, window("user-1", "device-1", 1, 4, 100)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := memory.ResetDevice(ctx, "user-1", "device-1"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	state, err := memory.DeviceState(ctx, "user-1", "device-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if state.Count != 0 || state.ErrorCount != 0 || state.Bytes != 0 {
		t.Fatalf("counters not cleared: %+v", state)
	}
	if state.P95Nanos != 0 || state.P99Nanos != 0 {
		t.Fatalf("percentiles not cleared: %+v", state)
	}
	if state.LastWindowID != 4 {
		t.Fatalf("last window %d, want the watermark to survive a reset", state.LastWindowID)
	}

	devices, err := memory.UserDevices(ctx, "user-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(devices) != 1 {
		t.Fatalf("device disappeared from the user index after reset: %v", devices)
	}
}

func TestResetDeviceRejectsUnknownDeviceAndCancelledContext(t *testing.T) {
	memory := store.NewMemory()

	if err := memory.ResetDevice(context.Background(), "user-1", "absent"); !errors.Is(err, store.ErrDeviceNotFound) {
		t.Fatalf("got %v, want ErrDeviceNotFound", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := memory.ResetDevice(ctx, "user-1", "device-1"); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want context.Canceled", err)
	}
}

func TestResetDeviceCountsAWindowAfterReset(t *testing.T) {
	memory := store.NewMemory()
	ctx := context.Background()

	if err := memory.ApplyWindow(ctx, window("user-1", "device-1", 1, 4, 100)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := memory.ResetDevice(ctx, "user-1", "device-1"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := memory.ApplyWindow(ctx, window("user-1", "device-1", 1, 5, 30)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	state, err := memory.DeviceState(ctx, "user-1", "device-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if state.Count != 30 {
		t.Fatalf("count %d, want only the post-reset window", state.Count)
	}
}

func TestCancelledContextIsRejected(t *testing.T) {
	memory := store.NewMemory()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := memory.ApplyWindow(ctx, window("user-1", "device-1", 1, 1, 1)); !errors.Is(err, context.Canceled) {
		t.Fatalf("ApplyWindow got %v, want context.Canceled", err)
	}
	if _, err := memory.DeviceState(ctx, "user-1", "device-1"); !errors.Is(err, context.Canceled) {
		t.Fatalf("DeviceState got %v, want context.Canceled", err)
	}
	if _, err := memory.UserDevices(ctx, "user-1"); !errors.Is(err, context.Canceled) {
		t.Fatalf("UserDevices got %v, want context.Canceled", err)
	}
	if _, err := memory.PipelineMetrics(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("PipelineMetrics got %v, want context.Canceled", err)
	}
}

func TestPipelineMetricsSnapshotIsIsolated(t *testing.T) {
	memory := store.NewMemory()
	ctx := context.Background()

	memory.Increment("custom_metric", 5)

	metrics, err := memory.PipelineMetrics(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	metrics["custom_metric"] = 999

	fresh, err := memory.PipelineMetrics(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if fresh["custom_metric"] != 5 {
		t.Fatalf("metric %d, want 5: the returned snapshot aliases internal state", fresh["custom_metric"])
	}
}

func TestSmallLateDeltaDoesNotOverrideFullWindowPercentiles(t *testing.T) {
	memory := store.NewMemory()
	ctx := context.Background()

	full := window("user-1", "device-1", 1, 9, 1000)
	full.Sequence = 1
	full.P95Nanos = 5_000_000
	full.P99Nanos = 9_000_000

	lateDelta := window("user-1", "device-1", 1, 9, 2)
	lateDelta.Sequence = 2
	lateDelta.P95Nanos = 900_000_000
	lateDelta.P99Nanos = 900_000_000

	if err := memory.ApplyWindow(ctx, full); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := memory.ApplyWindow(ctx, lateDelta); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	state, err := memory.DeviceState(ctx, "user-1", "device-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if state.Count != 1002 {
		t.Fatalf("count %d, want both emissions counted", state.Count)
	}
	if state.P99Nanos != 9_000_000 {
		t.Fatalf("p99 %d, want the 1000-event sample to win over a 2-event late delta", state.P99Nanos)
	}
}

func TestLargerLateEmissionDoesSupplyPercentiles(t *testing.T) {
	memory := store.NewMemory()
	ctx := context.Background()

	small := window("user-1", "device-1", 1, 9, 5)
	small.Sequence = 1
	small.P99Nanos = 1_000

	larger := window("user-1", "device-1", 1, 9, 500)
	larger.Sequence = 2
	larger.P99Nanos = 7_000

	if err := memory.ApplyWindow(ctx, small); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := memory.ApplyWindow(ctx, larger); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	state, err := memory.DeviceState(ctx, "user-1", "device-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if state.P99Nanos != 7_000 {
		t.Fatalf("p99 %d, want the larger sample's value", state.P99Nanos)
	}
}

func TestPercentilesRecoverAfterAResetWithinTheSameWindow(t *testing.T) {
	memory := store.NewMemory()
	ctx := context.Background()

	large := window("user-1", "device-1", 1, 9, 1000)
	large.Sequence = 1
	large.P99Nanos = 9_000_000

	if err := memory.ApplyWindow(ctx, large); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := memory.ResetDevice(ctx, "user-1", "device-1"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	small := window("user-1", "device-1", 1, 9, 5)
	small.Sequence = 2
	small.P99Nanos = 4_000

	if err := memory.ApplyWindow(ctx, small); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	state, err := memory.DeviceState(ctx, "user-1", "device-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if state.P99Nanos != 4_000 {
		t.Fatalf("p99 %d, want 4000: a reset must clear the percentile sample size too", state.P99Nanos)
	}
}
