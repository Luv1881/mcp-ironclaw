package store_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ironclaw/mcp-ironclaw/internal/domain"
	"github.com/ironclaw/mcp-ironclaw/internal/store"
)

type stubClock struct {
	now atomic.Int64
}

func newStubClock(start time.Time) *stubClock {
	clock := &stubClock{}
	clock.now.Store(start.UnixNano())
	return clock
}

func (c *stubClock) Now() time.Time {
	return time.Unix(0, c.now.Load()).UTC()
}

func (c *stubClock) advance(d time.Duration) {
	c.now.Add(int64(d))
}

func TestSecondAcquireIsRefusedWhileTheLeaseIsLive(t *testing.T) {
	locker := store.NewLocker(newStubClock(time.Unix(1700000000, 0)))

	if _, err := locker.Acquire(context.Background(), "device-owner:device-000", time.Minute); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := locker.Acquire(context.Background(), "device-owner:device-000", time.Minute); !errors.Is(err, store.ErrLockHeld) {
		t.Fatalf("got %v, want ErrLockHeld", err)
	}
}

func TestFencingTokensAreMonotonicPerResource(t *testing.T) {
	clock := newStubClock(time.Unix(1700000000, 0))
	locker := store.NewLocker(clock)
	ctx := context.Background()

	first, err := locker.Acquire(ctx, "device-owner:device-000", time.Minute)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := locker.Release(ctx, first); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	second, err := locker.Acquire(ctx, "device-owner:device-000", time.Minute)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if second.Token <= first.Token {
		t.Fatalf("token did not advance: %d then %d", first.Token, second.Token)
	}
}

func TestAPausedHolderCannotWriteAfterItsLeaseIsTakenOver(t *testing.T) {
	clock := newStubClock(time.Unix(1700000000, 0))
	locker := store.NewLocker(clock)
	ctx := context.Background()

	stale, err := locker.Acquire(ctx, "device-owner:device-000", 10*time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := locker.Guard(ctx, stale); err != nil {
		t.Fatalf("the live holder must be allowed to write: %v", err)
	}

	clock.advance(11 * time.Second)

	fresh, err := locker.Acquire(ctx, "device-owner:device-000", 10*time.Second)
	if err != nil {
		t.Fatalf("an expired lease must be re-acquirable: %v", err)
	}

	if err := locker.Guard(ctx, stale); !errors.Is(err, store.ErrLeaseSuperseded) {
		t.Fatalf("the paused holder was allowed to write with a stale token: %v", err)
	}
	if err := locker.Guard(ctx, fresh); err != nil {
		t.Fatalf("the new holder must be allowed to write: %v", err)
	}
}

func TestReleaseWithAStaleTokenDoesNotUnlockTheNewHolder(t *testing.T) {
	clock := newStubClock(time.Unix(1700000000, 0))
	locker := store.NewLocker(clock)
	ctx := context.Background()

	stale, err := locker.Acquire(ctx, "device-owner:device-000", 10*time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	clock.advance(11 * time.Second)

	if _, err := locker.Acquire(ctx, "device-owner:device-000", 10*time.Second); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := locker.Release(ctx, stale); !errors.Is(err, store.ErrLeaseSuperseded) {
		t.Fatalf("got %v, want ErrLeaseSuperseded", err)
	}
	if _, err := locker.Acquire(ctx, "device-owner:device-000", 10*time.Second); !errors.Is(err, store.ErrLockHeld) {
		t.Fatalf("a stale release unlocked the resource for someone else: %v", err)
	}
}

func TestOnlyOneConcurrentCallerAcquiresTheLock(t *testing.T) {
	locker := store.NewLocker(newStubClock(time.Unix(1700000000, 0)))

	const contenders = 32

	var (
		wait     sync.WaitGroup
		start    = make(chan struct{})
		acquired atomic.Int64
		tokens   sync.Map
	)

	for i := 0; i < contenders; i++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start

			lease, err := locker.Acquire(context.Background(), "device-owner:device-000", time.Minute)
			if err != nil {
				return
			}
			acquired.Add(1)
			tokens.Store(lease.Token, true)
		}()
	}

	close(start)
	wait.Wait()

	if got := acquired.Load(); got != 1 {
		t.Fatalf("%d contenders acquired the lock, want exactly 1", got)
	}
}

func TestEmptyResourceIsRejected(t *testing.T) {
	locker := store.NewLocker(nil)

	if _, err := locker.Acquire(context.Background(), "", time.Minute); !errors.Is(err, store.ErrEmptyResource) {
		t.Fatalf("got %v, want ErrEmptyResource", err)
	}
}

func TestLockerSatisfiesTheFencedLockerPort(t *testing.T) {
	var _ domain.FencedLocker = store.NewLocker(nil)
}
