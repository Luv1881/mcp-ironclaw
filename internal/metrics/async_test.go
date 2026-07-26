package metrics_test

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/ironclaw/mcp-ironclaw/internal/metrics"
)

type countingRecorder struct {
	mu      sync.Mutex
	totals  map[string]int64
	block   chan struct{}
	applied int
}

func newCounting() *countingRecorder {
	return &countingRecorder{totals: map[string]int64{}}
}

func (r *countingRecorder) Increment(name string, delta int64) {
	if r.block != nil {
		<-r.block
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.totals[name] += delta
	r.applied++
}

func (r *countingRecorder) total(name string) int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.totals[name]
}

func TestNewAsyncRejectsNilRecorder(t *testing.T) {
	if _, err := metrics.NewAsync(nil, 8); !errors.Is(err, metrics.ErrNilRecorder) {
		t.Fatalf("got %v, want ErrNilRecorder", err)
	}
}

func TestSamplesReachTheUnderlyingRecorder(t *testing.T) {
	inner := newCounting()

	async, err := metrics.NewAsync(inner, 64)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for i := 0; i < 10; i++ {
		async.Increment("events", 3)
	}
	async.Close()

	if got := inner.total("events"); got != 30 {
		t.Fatalf("total %d, want 30", got)
	}
	if async.Dropped() != 0 {
		t.Fatalf("dropped %d, want 0", async.Dropped())
	}
}

func TestIncrementNeverBlocksWhenTheRecorderStalls(t *testing.T) {
	inner := newCounting()
	inner.block = make(chan struct{})

	async, err := metrics.NewAsync(inner, 4)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 5000; i++ {
			async.Increment("events", 1)
		}
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Increment blocked while the underlying recorder was stalled")
	}

	if async.Dropped() == 0 {
		t.Fatal("expected samples to be dropped rather than blocking the caller")
	}

	close(inner.block)
	async.Close()
}

func TestCloseDrainsBufferedSamples(t *testing.T) {
	inner := newCounting()

	async, err := metrics.NewAsync(inner, 1024)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for i := 0; i < 500; i++ {
		async.Increment("events", 1)
	}
	async.Close()

	if got := inner.total("events") + async.Dropped(); got != 500 {
		t.Fatalf("applied plus dropped is %d, want 500", got)
	}
}

func TestIncrementAfterCloseIsCountedNotPanicking(t *testing.T) {
	inner := newCounting()

	async, err := metrics.NewAsync(inner, 8)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	async.Close()
	async.Close()

	async.Increment("events", 1)

	if async.Dropped() == 0 {
		t.Fatal("increments after close should be counted as dropped")
	}
}

func TestConcurrentIncrementIsRaceFree(t *testing.T) {
	inner := newCounting()

	async, err := metrics.NewAsync(inner, 4096)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				async.Increment("events", 1)
			}
		}()
	}
	wg.Wait()
	async.Close()

	if got := inner.total("events") + async.Dropped(); got != 4000 {
		t.Fatalf("applied plus dropped is %d, want 4000", got)
	}
}
