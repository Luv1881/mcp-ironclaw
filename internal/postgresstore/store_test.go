package postgresstore_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/ironclaw/mcp-ironclaw/internal/domain"
	"github.com/ironclaw/mcp-ironclaw/internal/postgresstore"
	"github.com/ironclaw/mcp-ironclaw/internal/store"
	"github.com/ironclaw/mcp-ironclaw/internal/testenv"
	"github.com/jackc/pgx/v5/pgxpool"
)

func newStore(t *testing.T) (*postgresstore.Store, *store.Memory) {
	t.Helper()

	dsn := testenv.PostgresDSN(t)
	ctx := context.Background()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	t.Cleanup(pool.Close)

	if err := pool.Ping(ctx); err != nil {
		t.Skipf("postgres is not accepting connections: %v", err)
	}

	metrics := store.NewMemory()

	target, err := postgresstore.New(pool, metrics)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := target.Migrate(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if _, err := pool.Exec(ctx, "TRUNCATE aggregate_windows"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	return target, metrics
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
		MaxNanos:    windowID * 3000,
	}
}

func TestNewRejectsNilPool(t *testing.T) {
	if _, err := postgresstore.New(nil, nil); !errors.Is(err, postgresstore.ErrNilPool) {
		t.Fatalf("got %v, want ErrNilPool", err)
	}
}

func TestApplyWindowAccumulatesAcrossProcessesAndWindows(t *testing.T) {
	target, _ := newStore(t)
	ctx := context.Background()

	for _, w := range []domain.AggregateWindow{
		window("user-1", "device-1", 1, 1, 100),
		window("user-1", "device-1", 2, 1, 50),
		window("user-1", "device-1", 1, 2, 25),
	} {
		if err := target.ApplyWindow(ctx, w); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	state, err := target.DeviceState(ctx, "user-1", "device-1")
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

func TestRedeliveredWindowDoesNotDoubleCount(t *testing.T) {
	target, metrics := newStore(t)
	ctx := context.Background()

	target1 := window("user-1", "device-1", 1, 1, 100)

	for i := 0; i < 5; i++ {
		if err := target.ApplyWindow(ctx, target1); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	state, err := target.DeviceState(ctx, "user-1", "device-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if state.Count != 100 {
		t.Fatalf("count %d, want 100 after five identical applies", state.Count)
	}

	recorded, err := metrics.PipelineMetrics(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if recorded[postgresstore.MetricWindowsPersisted] != 1 {
		t.Fatalf("persisted %d, want 1", recorded[postgresstore.MetricWindowsPersisted])
	}
	if recorded[postgresstore.MetricWindowsDuplicate] != 4 {
		t.Fatalf("duplicates %d, want 4", recorded[postgresstore.MetricWindowsDuplicate])
	}
}

func TestConcurrentDuplicateApplyIsPersistedOnce(t *testing.T) {
	target, _ := newStore(t)
	ctx := context.Background()

	duplicate := window("user-1", "device-1", 1, 1, 100)

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := target.ApplyWindow(ctx, duplicate); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()

	state, err := target.DeviceState(ctx, "user-1", "device-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if state.Count != 100 {
		t.Fatalf("count %d, want 100 after 16 concurrent identical applies", state.Count)
	}
}

func TestUserDevicesIsScopedAndSorted(t *testing.T) {
	target, _ := newStore(t)
	ctx := context.Background()

	for _, w := range []domain.AggregateWindow{
		window("user-1", "device-b", 1, 1, 1),
		window("user-1", "device-a", 1, 1, 1),
		window("user-2", "device-z", 1, 1, 1),
	} {
		if err := target.ApplyWindow(ctx, w); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	devices, err := target.UserDevices(ctx, "user-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(devices) != 2 || devices[0] != "device-a" || devices[1] != "device-b" {
		t.Fatalf("user-1 devices %v, want sorted [device-a device-b]", devices)
	}
}

func TestUnknownDeviceIsReported(t *testing.T) {
	target, _ := newStore(t)
	ctx := context.Background()

	if _, err := target.DeviceState(ctx, "user-1", "absent"); !errors.Is(err, postgresstore.ErrDeviceNotFound) {
		t.Fatalf("got %v, want ErrDeviceNotFound", err)
	}
	if err := target.ResetDevice(ctx, "user-1", "absent"); !errors.Is(err, postgresstore.ErrDeviceNotFound) {
		t.Fatalf("got %v, want ErrDeviceNotFound", err)
	}
}

func TestTenantsWithTheSameDeviceIdentifierStaySeparate(t *testing.T) {
	target, _ := newStore(t)
	ctx := context.Background()

	if err := target.ApplyWindow(ctx, window("user-1", "shared-id", 1, 1, 10)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := target.ApplyWindow(ctx, window("user-2", "shared-id", 1, 1, 70)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	first, err := target.DeviceState(ctx, "user-1", "shared-id")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	second, err := target.DeviceState(ctx, "user-2", "shared-id")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if first.Count != 10 || second.Count != 70 {
		t.Fatalf("tenant isolation broken: %d and %d, want 10 and 70", first.Count, second.Count)
	}
}

func TestResetDeviceRemovesThatDeviceOnly(t *testing.T) {
	target, _ := newStore(t)
	ctx := context.Background()

	if err := target.ApplyWindow(ctx, window("user-1", "device-1", 1, 1, 10)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := target.ApplyWindow(ctx, window("user-1", "device-2", 1, 1, 20)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := target.ResetDevice(ctx, "user-1", "device-1"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if _, err := target.DeviceState(ctx, "user-1", "device-1"); !errors.Is(err, postgresstore.ErrDeviceNotFound) {
		t.Fatalf("got %v, want the reset device to be gone", err)
	}

	survivor, err := target.DeviceState(ctx, "user-1", "device-2")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if survivor.Count != 20 {
		t.Fatalf("sibling device count %d, want 20", survivor.Count)
	}
}

func TestTableIsPartitionedByDay(t *testing.T) {
	target, _ := newStore(t)
	ctx := context.Background()

	if err := target.EnsurePartitions(ctx, time.Now(), 3); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	today := window("user-1", "device-1", 1, 77, 10)
	today.WindowStart = time.Now().UTC()
	today.WindowEnd = today.WindowStart.Add(10 * time.Second)

	if err := target.ApplyWindow(ctx, today); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	state, err := target.DeviceState(ctx, "user-1", "device-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if state.Count != 10 {
		t.Fatalf("count %d, want 10 through the partitioned table", state.Count)
	}
}

func TestRetentionDropsOldPartitions(t *testing.T) {
	target, _ := newStore(t)
	ctx := context.Background()

	old := time.Now().AddDate(0, 0, -30)
	if err := target.EnsurePartitions(ctx, old, 2); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	dropped, err := target.DropPartitionsBefore(ctx, time.Now().AddDate(0, 0, -7))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if dropped < 2 {
		t.Fatalf("dropped %d partitions, want at least the 2 old ones", dropped)
	}
}

func TestDefaultPartitionAbsorbsOutOfRangeWindows(t *testing.T) {
	target, _ := newStore(t)
	ctx := context.Background()

	ancient := window("user-1", "device-ancient", 1, 1, 5)

	if err := target.ApplyWindow(ctx, ancient); err != nil {
		t.Fatalf("a window outside every explicit partition must still land: %v", err)
	}
}
