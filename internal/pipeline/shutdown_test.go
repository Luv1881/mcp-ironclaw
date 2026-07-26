package pipeline_test

import (
	"context"
	"testing"
	"time"

	"github.com/ironclaw/mcp-ironclaw/internal/domain"
	"github.com/ironclaw/mcp-ironclaw/internal/pipeline"
)

type cancelAwareTransport struct {
	recordingTransport
	sawCancelledContext chan struct{}
}

func (t *cancelAwareTransport) Send(ctx context.Context, batch domain.Batch) error {
	if ctx.Err() != nil {
		select {
		case t.sawCancelledContext <- struct{}{}:
		default:
		}
		return ctx.Err()
	}
	return t.recordingTransport.Send(ctx, batch)
}

func TestBatcherDrainsQueuedBatchesAfterContextCancel(t *testing.T) {
	config := baseConfig()
	config.MaxEvents = 1
	config.QueueDepth = 64
	config.ShutdownGrace = 5 * time.Second

	batcher, err := pipeline.NewBatcher(config)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	transport := &cancelAwareTransport{sawCancelledContext: make(chan struct{}, 1)}
	events := make(chan domain.Event)
	source := &channelSource{events: events}

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() { done <- batcher.Run(ctx, source, transport) }()

	for i := 0; i < 8; i++ {
		events <- newEvent("device-1", int32(i))
	}

	waitFor(t, func() bool { return batcher.Stats().EventsAccepted == 8 })

	cancel()

	if err := <-done; err == nil {
		t.Fatal("expected the collect loop to report the cancellation")
	}

	select {
	case <-transport.sawCancelledContext:
		t.Fatal("sender was handed the cancelled collect context")
	default:
	}

	stats := batcher.Stats()
	if stats.SendFailures != 0 {
		t.Fatalf("send failures %d, want 0 after a clean shutdown drain", stats.SendFailures)
	}
	if stats.BatchesSent != stats.BatchesEnqueued {
		t.Fatalf("sent %d of %d enqueued batches", stats.BatchesSent, stats.BatchesEnqueued)
	}
	if stats.BatchesSent != 8 {
		t.Fatalf("sent %d batches, want all 8 to survive shutdown", stats.BatchesSent)
	}
}
