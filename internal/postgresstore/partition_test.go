package postgresstore_test

import (
	"context"
	"testing"
	"time"

	"github.com/ironclaw/mcp-ironclaw/internal/domain"
)

func TestAWindowLandingInTheDefaultPartitionDoesNotBlockItsPartition(t *testing.T) {
	store, _ := newStore(t)
	ctx := context.Background()

	future := time.Now().UTC().AddDate(0, 0, 40).Truncate(24 * time.Hour)

	window := domain.AggregateWindow{
		Key:         domain.CorrelationKey{UserID: "default-part-user", DeviceID: "device-000", ProcessID: 1, PodID: "pod-000"},
		WindowID:    future.UnixNano() / int64(10*time.Second),
		Sequence:    1,
		WindowStart: future.Add(3 * time.Hour),
		WindowEnd:   future.Add(3*time.Hour + 10*time.Second),
		Count:       25,
		P95Nanos:    1000,
		P99Nanos:    2000,
	}

	if err := store.ApplyWindow(ctx, window); err != nil {
		t.Fatalf("a window for a day with no partition must still be stored: %v", err)
	}

	if err := store.EnsurePartitions(ctx, future, 1); err != nil {
		t.Fatalf("creating the partition for a day whose rows already sit in the DEFAULT partition failed: %v", err)
	}

	state, err := store.DeviceState(ctx, "default-part-user", "device-000")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if state.Count != 25 {
		t.Fatalf("count is %d after the partition was created, want 25 — the row was lost in the migration", state.Count)
	}

	if err := store.ApplyWindow(ctx, window); err != nil {
		t.Fatalf("re-delivering the same emission after the migration failed: %v", err)
	}
	state, err = store.DeviceState(ctx, "default-part-user", "device-000")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if state.Count != 25 {
		t.Fatalf("count is %d after redelivery, want 25 — the migration broke idempotency", state.Count)
	}
}
