package pipeline_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/ironclaw/mcp-ironclaw/internal/domain"
	"github.com/ironclaw/mcp-ironclaw/internal/pipeline"
)

func batchOf(deviceID string, count int) domain.Batch {
	return domain.Batch{DeviceID: deviceID, Events: newEvents(deviceID, count)}
}

func TestDropOldestEvictsWhenFull(t *testing.T) {
	strategy := pipeline.StrategyFor(pipeline.OverflowDropOldest)
	queue := make(chan domain.Batch, 1)

	first := batchOf("device-1", 1)
	second := batchOf("device-1", 2)

	if outcome := strategy.Enqueue(context.Background(), queue, first); !outcome.Enqueued {
		t.Fatal("first batch should enqueue into empty queue")
	}

	outcome := strategy.Enqueue(context.Background(), queue, second)
	if !outcome.Enqueued {
		t.Fatal("second batch should enqueue after eviction")
	}
	if len(outcome.Dropped) != 1 || outcome.Dropped[0].Len() != 1 {
		t.Fatalf("expected the oldest one-event batch to be dropped, got %v", outcome.Dropped)
	}

	queued := <-queue
	if queued.Len() != 2 {
		t.Fatalf("queue holds a %d-event batch, want the newest 2-event batch", queued.Len())
	}
}

func TestDropOldestDoesNotBlockWhenQueueDrainsConcurrently(t *testing.T) {
	strategy := pipeline.StrategyFor(pipeline.OverflowDropOldest)
	queue := make(chan domain.Batch, 1)
	queue <- batchOf("device-1", 1)

	go func() { <-queue }()

	done := make(chan struct{})
	go func() {
		defer close(done)
		strategy.Enqueue(context.Background(), queue, batchOf("device-1", 2))
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("drop-oldest strategy blocked when the queue drained concurrently")
	}
}

func TestDropNewestKeepsQueuedBatch(t *testing.T) {
	strategy := pipeline.StrategyFor(pipeline.OverflowDropNewest)
	queue := make(chan domain.Batch, 1)

	strategy.Enqueue(context.Background(), queue, batchOf("device-1", 1))
	outcome := strategy.Enqueue(context.Background(), queue, batchOf("device-1", 2))

	if outcome.Enqueued {
		t.Fatal("newest batch should be dropped, not enqueued")
	}
	if len(outcome.Dropped) != 1 || outcome.Dropped[0].Len() != 2 {
		t.Fatalf("expected the newest batch to be dropped, got %v", outcome.Dropped)
	}

	if queued := <-queue; queued.Len() != 1 {
		t.Fatalf("queue holds a %d-event batch, want the original 1-event batch", queued.Len())
	}
}

func TestBlockingStrategyWaitsForCapacity(t *testing.T) {
	strategy := pipeline.StrategyFor(pipeline.OverflowBlock)
	queue := make(chan domain.Batch, 1)
	queue <- batchOf("device-1", 1)

	result := make(chan pipeline.OverflowOutcome, 1)
	go func() {
		result <- strategy.Enqueue(context.Background(), queue, batchOf("device-1", 2))
	}()

	select {
	case <-result:
		t.Fatal("blocking strategy returned while the queue was full")
	case <-time.After(50 * time.Millisecond):
	}

	<-queue

	select {
	case outcome := <-result:
		if !outcome.Enqueued || len(outcome.Dropped) != 0 {
			t.Fatalf("blocking strategy dropped a batch: %v", outcome)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("blocking strategy did not enqueue after capacity freed")
	}
}

func TestBlockingStrategyReleasesOnContextCancel(t *testing.T) {
	strategy := pipeline.StrategyFor(pipeline.OverflowBlock)
	queue := make(chan domain.Batch, 1)
	queue <- batchOf("device-1", 1)

	ctx, cancel := context.WithCancel(context.Background())

	result := make(chan pipeline.OverflowOutcome, 1)
	go func() { result <- strategy.Enqueue(ctx, queue, batchOf("device-1", 2)) }()

	cancel()

	select {
	case outcome := <-result:
		if outcome.Enqueued {
			t.Fatal("cancelled enqueue reported success")
		}
		if len(outcome.Dropped) != 1 {
			t.Fatalf("expected the batch to be reported as dropped, got %v", outcome.Dropped)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("blocking strategy ignored context cancellation")
	}
}

type countingStrategy struct {
	mu    sync.Mutex
	calls int
}

func (s *countingStrategy) Enqueue(ctx context.Context, queue chan domain.Batch, batch domain.Batch) pipeline.OverflowOutcome {
	s.mu.Lock()
	s.calls++
	s.mu.Unlock()

	select {
	case queue <- batch:
		return pipeline.OverflowOutcome{Enqueued: true}
	default:
		return pipeline.OverflowOutcome{Dropped: []domain.Batch{batch}}
	}
}

func TestBatcherUsesInjectedStrategy(t *testing.T) {
	strategy := &countingStrategy{}
	config := baseConfig()
	config.Strategy = strategy

	batcher, err := pipeline.NewBatcher(config)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	transport := &recordingTransport{}
	source := &sliceSource{events: newEvents("device-1", 20)}

	if err := batcher.Run(context.Background(), source, transport); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	strategy.mu.Lock()
	defer strategy.mu.Unlock()
	if strategy.calls != 2 {
		t.Fatalf("injected strategy called %d times, want 2", strategy.calls)
	}
}
