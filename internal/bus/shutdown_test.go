package bus_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ironclaw/mcp-ironclaw/internal/bus"
	"github.com/ironclaw/mcp-ironclaw/internal/domain"
)

func TestConcurrentSendAndCloseDoesNotPanic(t *testing.T) {
	broker, err := bus.NewBatchBus(bus.Config{Partitions: 4, Capacity: 256, MaxAttempts: 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctx := context.Background()

	var wg sync.WaitGroup
	for producer := 0; producer < 8; producer++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				err := broker.Publish(ctx, string(rune('a'+id)), batchFor("device-1", 1))
				if err != nil && !errors.Is(err, bus.ErrClosed) {
					t.Error(err)
					return
				}
			}
		}(producer)
	}

	go func() {
		time.Sleep(time.Millisecond)
		broker.Close()
	}()

	wg.Wait()
	broker.Close()

	if err := broker.Publish(ctx, "device-1", batchFor("device-1", 1)); !errors.Is(err, bus.ErrClosed) {
		t.Fatalf("got %v, want ErrClosed", err)
	}
}

func TestCloseLetsConsumersDrainBufferedMessages(t *testing.T) {
	broker, err := bus.NewBatchBus(bus.Config{Partitions: 4, Capacity: 256, MaxAttempts: 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctx := context.Background()
	const total = 400
	for i := 0; i < total; i++ {
		if err := broker.Publish(ctx, string(rune('a'+i%26)), batchFor("device-1", 1)); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	var mu sync.Mutex
	received := 0

	done := make(chan error, 1)
	go func() {
		done <- broker.RunConsumerGroup(ctx, func(context.Context, domain.Batch) error {
			mu.Lock()
			received++
			mu.Unlock()
			return nil
		})
	}()

	broker.Close()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("consumer group did not exit after Close")
	}

	mu.Lock()
	defer mu.Unlock()
	if received != total {
		t.Fatalf("drained %d messages after Close, want %d", received, total)
	}
}

func TestConsumerGroupOwnsEveryPartitionExclusively(t *testing.T) {
	broker, err := bus.NewBatchBus(bus.Config{Partitions: 4, Capacity: 16, MaxAttempts: 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		close(started)
		done <- broker.RunConsumerGroup(ctx, func(context.Context, domain.Batch) error { return nil })
	}()

	<-started
	waitUntil(t, func() bool {
		_, err := broker.Claim()
		return errors.Is(err, bus.ErrNoFreePartition)
	})

	cancel()
	<-done

	if _, err := broker.Claim(); err != nil {
		t.Fatalf("partitions were not released after the group exited: %v", err)
	}
}

func TestCancelledContextAbandonsInsteadOfDeadLettering(t *testing.T) {
	broker, err := bus.NewBatchBus(bus.Config{Partitions: 1, Capacity: 16, MaxAttempts: 5})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := broker.Publish(context.Background(), "device-1", batchFor("device-1", 1)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	broker.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	attempts := 0
	err = broker.ReceivePartition(ctx, 0, func(context.Context, domain.Batch) error {
		attempts++
		return errors.New("handler failed")
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("unexpected error: %v", err)
	}

	stats := broker.Stats()
	if stats.DeadLetter != 0 {
		t.Fatalf("dead lettered %d messages during shutdown, want 0", stats.DeadLetter)
	}
	if attempts > 1 {
		t.Fatalf("handler retried %d times against a cancelled context, want at most 1", attempts)
	}
}

func waitUntil(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition not met before deadline")
}

func TestSendOnAFullPartitionReturnsClosedAfterShutdown(t *testing.T) {
	broker, err := bus.NewBatchBus(bus.Config{Partitions: 1, Capacity: 1, MaxAttempts: 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctx := context.Background()
	if err := broker.Publish(ctx, "device-1", batchFor("device-1", 1)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	result := make(chan error, 1)
	go func() { result <- broker.Publish(ctx, "device-1", batchFor("device-1", 2)) }()

	time.Sleep(50 * time.Millisecond)
	broker.Close()

	select {
	case err := <-result:
		if !errors.Is(err, bus.ErrClosed) {
			t.Fatalf("got %v, want ErrClosed once the bus shut down", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Send blocked on a full partition after Close instead of returning ErrClosed")
	}
}

func TestNoAcceptedSendIsStrandedByAConcurrentClose(t *testing.T) {
	for attempt := 0; attempt < 25; attempt++ {
		broker, err := bus.New[domain.Batch](bus.Config{Partitions: 4, Capacity: 64, MaxAttempts: 1})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		var (
			delivered atomic.Int64
			accepted  atomic.Int64
			senders   sync.WaitGroup
			consumers sync.WaitGroup
		)

		ctx, cancel := context.WithCancel(context.Background())

		for partition := 0; partition < 4; partition++ {
			consumers.Add(1)
			go func(p int) {
				defer consumers.Done()
				_ = broker.ReceivePartition(ctx, p, func(context.Context, domain.Batch) error {
					delivered.Add(1)
					return nil
				})
			}(partition)
		}

		start := make(chan struct{})
		for sender := 0; sender < 8; sender++ {
			senders.Add(1)
			go func(id int) {
				defer senders.Done()
				<-start
				for i := 0; i < 20; i++ {
					err := broker.Send(context.Background(), fmt.Sprintf("device-%d-%d", id, i), domain.Batch{DeviceID: "d"})
					if err == nil {
						accepted.Add(1)
					}
				}
			}(sender)
		}

		close(start)
		time.Sleep(time.Duration(attempt%5) * time.Millisecond)
		broker.Close()

		senders.Wait()
		consumers.Wait()
		cancel()

		if got, want := delivered.Load(), accepted.Load(); got != want {
			t.Fatalf("attempt %d: %d sends were accepted but only %d were delivered — a send that returned nil was stranded by Close", attempt, want, got)
		}
	}
}

func TestReceivePartitionRejectsAnOutOfRangePartition(t *testing.T) {
	broker, err := bus.New[domain.Batch](bus.Config{Partitions: 4, Capacity: 8, MaxAttempts: 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer broker.Close()

	for _, partition := range []int{-1, 4, 99} {
		err := broker.ReceivePartition(context.Background(), partition, func(context.Context, domain.Batch) error { return nil })
		if !errors.Is(err, bus.ErrNoPartition) {
			t.Fatalf("partition %d returned %v, want ErrNoPartition instead of an index panic", partition, err)
		}
	}
}

func TestPartitionForIsAlwaysInRange(t *testing.T) {
	broker, err := bus.New[domain.Batch](bus.Config{Partitions: 8, Capacity: 4, MaxAttempts: 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer broker.Close()

	for i := 0; i < 20000; i++ {
		partition := broker.PartitionFor(fmt.Sprintf("device-%d-%x", i, i*2654435761))
		if partition < 0 || partition >= 8 {
			t.Fatalf("PartitionFor returned %d, outside [0,8)", partition)
		}
	}
}
