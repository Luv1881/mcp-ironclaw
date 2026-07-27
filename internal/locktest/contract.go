package locktest

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/ironclaw/mcp-ironclaw/internal/domain"
)

type Guarded interface {
	domain.FencedLocker
	Guard(ctx context.Context, lease domain.Lease) error
}

type Contract struct {
	New        func(t *testing.T) Guarded
	Held       error
	Superseded error
}

func Run(t *testing.T, contract Contract) {
	t.Helper()

	t.Run("one holder at a time", func(t *testing.T) {
		locker := contract.New(t)
		ctx := context.Background()

		lease, err := locker.Acquire(ctx, "owner:device-000", 30*time.Second)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if lease.Token <= 0 {
			t.Fatalf("fencing token %d is not positive", lease.Token)
		}
		if _, err := locker.Acquire(ctx, "owner:device-000", 30*time.Second); !errors.Is(err, contract.Held) {
			t.Fatalf("got %v, want the lock-held error", err)
		}
	})

	t.Run("tokens advance across acquisitions", func(t *testing.T) {
		locker := contract.New(t)
		ctx := context.Background()

		first, err := locker.Acquire(ctx, "owner:device-001", 30*time.Second)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if err := locker.Release(ctx, first); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		second, err := locker.Acquire(ctx, "owner:device-001", 30*time.Second)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if second.Token <= first.Token {
			t.Fatalf("token did not advance: %d then %d", first.Token, second.Token)
		}
	})

	t.Run("the live holder may write", func(t *testing.T) {
		locker := contract.New(t)
		ctx := context.Background()

		lease, err := locker.Acquire(ctx, "owner:device-002", 30*time.Second)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if err := locker.Guard(ctx, lease); err != nil {
			t.Fatalf("the live holder was refused: %v", err)
		}
	})

	t.Run("a released lease may not write", func(t *testing.T) {
		locker := contract.New(t)
		ctx := context.Background()

		lease, err := locker.Acquire(ctx, "owner:device-003", 30*time.Second)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if err := locker.Release(ctx, lease); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if err := locker.Guard(ctx, lease); !errors.Is(err, contract.Superseded) {
			t.Fatalf("a released lease was still allowed to write: %v", err)
		}
	})

	t.Run("an expired lease is refused once someone else acquires", func(t *testing.T) {
		locker := contract.New(t)
		ctx := context.Background()

		stale, err := locker.Acquire(ctx, "owner:device-004", 300*time.Millisecond)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		time.Sleep(600 * time.Millisecond)

		fresh, err := locker.Acquire(ctx, "owner:device-004", 30*time.Second)
		if err != nil {
			t.Fatalf("an expired lease must be re-acquirable: %v", err)
		}
		if err := locker.Guard(ctx, stale); !errors.Is(err, contract.Superseded) {
			t.Fatalf("a paused holder wrote with a stale token: %v", err)
		}
		if err := locker.Guard(ctx, fresh); err != nil {
			t.Fatalf("the new holder was refused: %v", err)
		}
	})

	t.Run("a stale release does not unlock the new holder", func(t *testing.T) {
		locker := contract.New(t)
		ctx := context.Background()

		stale, err := locker.Acquire(ctx, "owner:device-005", 300*time.Millisecond)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		time.Sleep(600 * time.Millisecond)

		fresh, err := locker.Acquire(ctx, "owner:device-005", 30*time.Second)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if err := locker.Release(ctx, stale); !errors.Is(err, contract.Superseded) {
			t.Fatalf("a stale release was accepted: %v", err)
		}
		if err := locker.Guard(ctx, fresh); err != nil {
			t.Fatalf("a stale release stole the lock from the live holder: %v", err)
		}
	})

	t.Run("exactly one contender wins", func(t *testing.T) {
		locker := contract.New(t)

		const contenders = 16

		var (
			wait  sync.WaitGroup
			start = make(chan struct{})
			mu    sync.Mutex
			won   []domain.Lease
		)

		for i := 0; i < contenders; i++ {
			wait.Add(1)
			go func() {
				defer wait.Done()
				<-start

				lease, err := locker.Acquire(context.Background(), "owner:device-006", 30*time.Second)
				if err != nil {
					return
				}
				mu.Lock()
				won = append(won, lease)
				mu.Unlock()
			}()
		}

		close(start)
		wait.Wait()

		if len(won) != 1 {
			t.Fatalf("%d contenders acquired the lock, want exactly 1", len(won))
		}
	})
}
