package bus_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/ironclaw/mcp-ironclaw/internal/bus"
	"github.com/ironclaw/mcp-ironclaw/internal/domain"
)

func testConfig() bus.Config {
	return bus.Config{Partitions: 4, Capacity: 16, MaxAttempts: 1}
}

func batchFor(deviceID string, count int) domain.Batch {
	events := make([]domain.Event, 0, count)
	for i := 0; i < count; i++ {
		events = append(events, domain.Event{
			DeviceID:     deviceID,
			UserID:       "user-1",
			ProcessID:    int32(i),
			Kind:         domain.EventKindSyscall,
			ObservedAt:   time.Unix(1700000000, 0),
			LatencyNanos: 1000,
		})
	}
	return domain.Batch{DeviceID: deviceID, Events: events}
}

func TestBusConfigValidation(t *testing.T) {
	cases := []struct {
		name    string
		config  bus.Config
		wantErr error
	}{
		{"zero partitions", bus.Config{Partitions: 0, Capacity: 1, MaxAttempts: 1}, bus.ErrInvalidPartitions},
		{"zero capacity", bus.Config{Partitions: 1, Capacity: 0, MaxAttempts: 1}, bus.ErrInvalidCapacity},
		{"negative attempts", bus.Config{Partitions: 1, Capacity: 1, MaxAttempts: -1}, bus.ErrInvalidAttempts},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := bus.NewBatchBus(tc.config); !errors.Is(err, tc.wantErr) {
				t.Fatalf("got %v, want %v", err, tc.wantErr)
			}
		})
	}
}

func TestPartitionForIsStableAndBounded(t *testing.T) {
	broker, err := bus.NewBatchBus(testConfig())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for _, key := range []string{"device-1", "device-2", "device-3", "user-9"} {
		first := broker.PartitionFor(key)
		if first != broker.PartitionFor(key) {
			t.Fatalf("partition for %q is not stable", key)
		}
		if first < 0 || first >= broker.Partitions() {
			t.Fatalf("partition %d for %q is out of range", first, key)
		}
	}
}

func TestSameKeyAlwaysLandsOnOnePartitionPreservingOrder(t *testing.T) {
	broker, err := bus.NewBatchBus(testConfig())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctx := context.Background()
	for i := 1; i <= 8; i++ {
		if err := broker.Publish(ctx, "device-1", batchFor("device-1", i)); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	broker.Close()

	partition := broker.PartitionFor("device-1")

	var sizes []int
	err = broker.ReceivePartition(ctx, partition, func(_ context.Context, batch domain.Batch) error {
		sizes = append(sizes, batch.Len())
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(sizes) != 8 {
		t.Fatalf("received %d batches, want 8", len(sizes))
	}
	for i, size := range sizes {
		if size != i+1 {
			t.Fatalf("batch %d has size %d, want %d: ordering was not preserved", i, size, i+1)
		}
	}
}

func TestPointToPointDeliversEachBatchToExactlyOneConsumer(t *testing.T) {
	config := testConfig()
	config.Capacity = 64

	broker, err := bus.NewBatchBus(config)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctx := context.Background()
	const total = 200
	for i := 0; i < total; i++ {
		key := string(rune('a' + i%26))
		if err := broker.Publish(ctx, key, batchFor(key, 1)); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	broker.Close()

	var mu sync.Mutex
	received := 0
	perPartition := map[string]int{}

	err = broker.RunConsumerGroup(ctx, func(_ context.Context, batch domain.Batch) error {
		mu.Lock()
		received++
		perPartition[batch.DeviceID]++
		mu.Unlock()
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if received != total {
		t.Fatalf("consumers saw %d batches in total, want exactly %d", received, total)
	}
	if got := broker.Stats().Delivered; got != total {
		t.Fatalf("delivered stat %d, want %d", got, total)
	}
	if len(perPartition) != 26 {
		t.Fatalf("saw %d distinct keys, want 26", len(perPartition))
	}
}

func TestConsumerGroupRefusesMoreConsumersThanPartitions(t *testing.T) {
	config := testConfig()
	config.Partitions = 2

	broker, err := bus.NewBatchBus(config)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	first, err := broker.Claim()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := broker.Claim(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := broker.Claim(); !errors.Is(err, bus.ErrNoFreePartition) {
		t.Fatalf("got %v, want ErrNoFreePartition", err)
	}

	broker.Release(first)
	if _, err := broker.Claim(); err != nil {
		t.Fatalf("released partition was not reclaimable: %v", err)
	}
}

func TestHandlerFailureRetriesThenDeadLetters(t *testing.T) {
	config := testConfig()
	config.MaxAttempts = 3

	broker, err := bus.NewBatchBus(config)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctx := context.Background()
	if err := broker.Publish(ctx, "device-1", batchFor("device-1", 1)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	broker.Close()

	attempts := 0
	err = broker.ReceivePartition(ctx, broker.PartitionFor("device-1"), func(_ context.Context, _ domain.Batch) error {
		attempts++
		return errors.New("poison message")
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if attempts != 3 {
		t.Fatalf("handler called %d times, want 3", attempts)
	}

	stats := broker.Stats()
	if stats.DeadLetter != 1 {
		t.Fatalf("dead letter count %d, want 1", stats.DeadLetter)
	}
	if stats.Delivered != 0 {
		t.Fatalf("delivered %d, want 0", stats.Delivered)
	}
	if len(broker.DeadLettered()) != 1 {
		t.Fatalf("dead letter queue holds %d batches, want 1", len(broker.DeadLettered()))
	}
}

func TestHandlerRecoveryOnRetryIsNotDeadLettered(t *testing.T) {
	config := testConfig()
	config.MaxAttempts = 3

	broker, err := bus.NewBatchBus(config)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctx := context.Background()
	if err := broker.Publish(ctx, "device-1", batchFor("device-1", 1)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	broker.Close()

	attempts := 0
	err = broker.ReceivePartition(ctx, broker.PartitionFor("device-1"), func(_ context.Context, _ domain.Batch) error {
		attempts++
		if attempts < 2 {
			return errors.New("transient failure")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	stats := broker.Stats()
	if stats.Delivered != 1 {
		t.Fatalf("delivered %d, want 1", stats.Delivered)
	}
	if stats.DeadLetter != 0 {
		t.Fatalf("dead letter count %d, want 0", stats.DeadLetter)
	}
	if stats.Retried != 1 {
		t.Fatalf("retried %d, want 1", stats.Retried)
	}
}

func TestPublishAfterCloseIsRejected(t *testing.T) {
	broker, err := bus.NewBatchBus(testConfig())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	broker.Close()
	broker.Close()

	if err := broker.Publish(context.Background(), "device-1", batchFor("device-1", 1)); !errors.Is(err, bus.ErrClosed) {
		t.Fatalf("got %v, want ErrClosed", err)
	}
}

func TestReceiveHonoursContextCancellation(t *testing.T) {
	broker, err := bus.NewBatchBus(testConfig())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- broker.Consume(ctx, func(context.Context, domain.Batch) error { return nil })
	}()

	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("got %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("consumer ignored context cancellation")
	}
}

func TestWindowBusPartitionsByUserShardTag(t *testing.T) {
	broker, err := bus.NewWindowBus(testConfig())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctx := context.Background()

	window := func(deviceID string) domain.AggregateWindow {
		return domain.AggregateWindow{
			Key:      domain.CorrelationKey{UserID: "user-1", DeviceID: deviceID, ProcessID: 1},
			WindowID: 1,
			Count:    1,
		}
	}

	for _, deviceID := range []string{"device-1", "device-2", "device-3"} {
		if err := broker.PublishWindow(ctx, window(deviceID)); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	broker.Close()

	partition := broker.PartitionFor("user-1")
	seen := 0
	err = broker.ReceivePartition(ctx, partition, func(_ context.Context, w domain.AggregateWindow) error {
		if w.Key.UserID != "user-1" {
			t.Errorf("unexpected user %q on the user-1 partition", w.Key.UserID)
		}
		seen++
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if seen != 3 {
		t.Fatalf("a single user's windows landed on %d messages of one partition, want 3", seen)
	}
}
