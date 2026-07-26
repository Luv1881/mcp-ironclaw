package app_test

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/ironclaw/mcp-ironclaw/internal/app"
)

func TestStopIsIdempotentAndSafeFromManyGoroutines(t *testing.T) {
	instance, err := app.New(testConfig())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := instance.Start(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	done := make(chan struct{})
	for i := 0; i < 8; i++ {
		go func() {
			instance.Stop()
			done <- struct{}{}
		}()
	}

	for i := 0; i < 8; i++ {
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			t.Fatal("a concurrent Stop call did not return")
		}
	}
}

func TestStopDoesNotLeakTheContextWatcherGoroutine(t *testing.T) {
	before := runtime.NumGoroutine()

	for i := 0; i < 3; i++ {
		instance, err := app.New(testConfig())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		ctx, cancel := context.WithCancel(context.Background())

		if err := instance.Start(ctx); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		instance.Stop()
		cancel()
	}

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if runtime.NumGoroutine() <= before+2 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("goroutines grew from %d to %d across start/stop cycles", before, runtime.NumGoroutine())
}

func TestStartAfterParentCancellationStillShutsDownCleanly(t *testing.T) {
	instance, err := app.New(testConfig())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	if err := instance.Start(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	cancel()

	stopped := make(chan struct{})
	go func() {
		instance.Stop()
		close(stopped)
	}()

	select {
	case <-stopped:
	case <-time.After(30 * time.Second):
		t.Fatal("runtime did not shut down after parent cancellation")
	}
}
