package store_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/ironclaw/mcp-ironclaw/internal/store"
)

func TestWatchersUnderConcurrentApplyAndCancellation(t *testing.T) {
	memory := store.NewMemory()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wait sync.WaitGroup

	for writer := 0; writer < 4; writer++ {
		wait.Add(1)
		go func(offset int) {
			defer wait.Done()
			for i := 1; i <= 200; i++ {
				if err := memory.ApplyWindow(ctx, watchedWindow(1, int64(offset*1000+i))); err != nil {
					return
				}
			}
		}(writer)
	}

	for subscriber := 0; subscriber < 16; subscriber++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for i := 0; i < 25; i++ {
				watchCtx, stop := context.WithCancel(ctx)
				updates, err := memory.WatchDevice(watchCtx, "user-000", "device-000")
				if err != nil {
					stop()
					return
				}
				select {
				case <-updates:
				case <-time.After(time.Millisecond):
				}
				stop()
				for range updates {
				}
			}
		}()
	}

	wait.Add(1)
	go func() {
		defer wait.Done()
		for i := 0; i < 100; i++ {
			memory.ResetDevice(ctx, "user-000", "device-000")
		}
	}()

	done := make(chan struct{})
	go func() {
		wait.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(60 * time.Second):
		t.Fatal("concurrent watch/apply/cancel deadlocked")
	}
}
