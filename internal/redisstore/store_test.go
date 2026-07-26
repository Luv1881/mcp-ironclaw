package redisstore_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ironclaw/mcp-ironclaw/internal/domain"
	"github.com/ironclaw/mcp-ironclaw/internal/redisstore"
	"github.com/ironclaw/mcp-ironclaw/internal/testenv"
	"github.com/redis/go-redis/v9"
)

func newStore(t *testing.T) *redisstore.Store {
	t.Helper()

	address := testenv.RedisAddr(t)

	client := redis.NewClient(&redis.Options{Addr: address})
	t.Cleanup(func() { client.Close() })

	namespace := fmt.Sprintf("ironclawtest:%d", time.Now().UnixNano())

	store, err := redisstore.New(redisstore.Options{
		Client:    client,
		Namespace: namespace,
		DedupeTTL: time.Hour,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	t.Cleanup(func() {
		keys, err := client.Keys(context.Background(), namespace+"*").Result()
		if err == nil && len(keys) > 0 {
			client.Del(context.Background(), keys...)
		}
	})

	return store
}

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

func TestNewRejectsNilClient(t *testing.T) {
	if _, err := redisstore.New(redisstore.Options{}); !errors.Is(err, redisstore.ErrNilClient) {
		t.Fatalf("got %v, want ErrNilClient", err)
	}
}

func TestApplyWindowAccumulatesAcrossProcessesAndWindows(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()

	for _, w := range []domain.AggregateWindow{
		window("user-1", "device-1", 1, 1, 100),
		window("user-1", "device-1", 2, 1, 50),
		window("user-1", "device-1", 1, 2, 25),
	} {
		if err := store.ApplyWindow(ctx, w); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	state, err := store.DeviceState(ctx, "user-1", "device-1")
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
		t.Fatalf("p95 %d, want the newest window's 2000", state.P95Nanos)
	}
}

func TestRedeliveredWindowIsAppliedOnce(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()

	target := window("user-1", "device-1", 1, 1, 100)

	for i := 0; i < 5; i++ {
		if err := store.ApplyWindow(ctx, target); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	state, err := store.DeviceState(ctx, "user-1", "device-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if state.Count != 100 {
		t.Fatalf("count %d, want 100 after five identical applies", state.Count)
	}

	metrics, err := store.PipelineMetrics(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if metrics[redisstore.MetricWindowsDuplicate] != 4 {
		t.Fatalf("duplicates %d, want 4", metrics[redisstore.MetricWindowsDuplicate])
	}
	if metrics[redisstore.MetricWindowsApplied] != 1 {
		t.Fatalf("applied %d, want 1", metrics[redisstore.MetricWindowsApplied])
	}
}

func TestConcurrentDuplicateApplyIsAppliedOnce(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()

	target := window("user-1", "device-1", 1, 1, 100)

	var wg sync.WaitGroup
	for i := 0; i < 24; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := store.ApplyWindow(ctx, target); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()

	state, err := store.DeviceState(ctx, "user-1", "device-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if state.Count != 100 {
		t.Fatalf("count %d, want 100 after 24 concurrent identical applies", state.Count)
	}
}

func TestOutOfOrderWindowDoesNotRewindPercentiles(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()

	if err := store.ApplyWindow(ctx, window("user-1", "device-1", 1, 5, 10)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := store.ApplyWindow(ctx, window("user-1", "device-1", 1, 2, 10)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	state, err := store.DeviceState(ctx, "user-1", "device-1")
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

func TestSameWindowKeepsWorstCasePercentileAcrossProcesses(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()

	high := window("user-1", "device-1", 1, 3, 10)
	high.P99Nanos = 90000

	low := window("user-1", "device-1", 2, 3, 10)
	low.P99Nanos = 1000

	if err := store.ApplyWindow(ctx, high); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := store.ApplyWindow(ctx, low); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	state, err := store.DeviceState(ctx, "user-1", "device-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if state.P99Nanos != 90000 {
		t.Fatalf("p99 %d, want the worst case 90000 within the window", state.P99Nanos)
	}
}

func TestUserDevicesIsScopedAndSorted(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()

	for _, w := range []domain.AggregateWindow{
		window("user-1", "device-b", 1, 1, 1),
		window("user-1", "device-a", 1, 1, 1),
		window("user-2", "device-z", 1, 1, 1),
	} {
		if err := store.ApplyWindow(ctx, w); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	devices, err := store.UserDevices(ctx, "user-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(devices) != 2 || devices[0] != "device-a" || devices[1] != "device-b" {
		t.Fatalf("user-1 devices %v, want sorted [device-a device-b]", devices)
	}

	others, err := store.UserDevices(ctx, "user-2")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(others) != 1 || others[0] != "device-z" {
		t.Fatalf("user-2 devices %v, want [device-z]", others)
	}
}

func TestTenantsWithTheSameDeviceIdentifierStaySeparate(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()

	if err := store.ApplyWindow(ctx, window("user-1", "shared-id", 1, 1, 10)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := store.ApplyWindow(ctx, window("user-2", "shared-id", 1, 1, 70)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	first, err := store.DeviceState(ctx, "user-1", "shared-id")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	second, err := store.DeviceState(ctx, "user-2", "shared-id")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if first.Count != 10 || second.Count != 70 {
		t.Fatalf("tenant isolation broken: %d and %d, want 10 and 70", first.Count, second.Count)
	}
}

func TestUnknownDeviceIsReported(t *testing.T) {
	store := newStore(t)

	if _, err := store.DeviceState(context.Background(), "user-1", "absent"); !errors.Is(err, redisstore.ErrDeviceNotFound) {
		t.Fatalf("got %v, want ErrDeviceNotFound", err)
	}
	if err := store.ResetDevice(context.Background(), "user-1", "absent"); !errors.Is(err, redisstore.ErrDeviceNotFound) {
		t.Fatalf("got %v, want ErrDeviceNotFound", err)
	}
}

func TestResetDeviceClearsCountersButKeepsRegistration(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()

	if err := store.ApplyWindow(ctx, window("user-1", "device-1", 1, 4, 100)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := store.ResetDevice(ctx, "user-1", "device-1"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	state, err := store.DeviceState(ctx, "user-1", "device-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if state.Count != 0 || state.ErrorCount != 0 || state.Bytes != 0 {
		t.Fatalf("counters not cleared: %+v", state)
	}
	if state.LastWindowID != 4 {
		t.Fatalf("last window %d, want the watermark to survive a reset", state.LastWindowID)
	}

	devices, err := store.UserDevices(ctx, "user-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(devices) != 1 {
		t.Fatalf("device disappeared from the user index after reset: %v", devices)
	}
}

func TestKeysUseUserHashTagSoAUsersStateSharesASlot(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()

	if err := store.ApplyWindow(ctx, window("user-1", "device-1", 1, 1, 5)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	client := redis.NewClient(&redis.Options{Addr: testenv.RedisAddr(t)})
	defer client.Close()

	keys, err := client.Keys(ctx, "ironclawtest:*{user-1}*").Result()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(keys) < 2 {
		t.Fatalf("found %d hash-tagged keys for user-1, want the state and device index to share the tag", len(keys))
	}

	slots := map[int64]bool{}
	for _, key := range keys {
		slot, err := client.ClusterKeySlot(ctx, key).Result()
		if err != nil {
			t.Skipf("cluster slot lookup unavailable: %v", err)
		}
		slots[slot] = true
	}
	if len(slots) != 1 {
		t.Fatalf("user-1 keys span %d slots, want 1 so multi-key operations stay local", len(slots))
	}
}
