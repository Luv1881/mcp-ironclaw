package redisstore_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ironclaw/mcp-ironclaw/internal/domain"
	"github.com/ironclaw/mcp-ironclaw/internal/redisstore"
)

func TestRedisWatchDeliversAppliedWindows(t *testing.T) {
	store := newStore(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	updates, err := store.WatchDevice(ctx, "user-000", "device-000")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := store.ApplyWindow(ctx, window("user-000", "device-000", 1, 1, 40)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	select {
	case state := <-updates:
		if state.Count != 40 {
			t.Fatalf("watcher saw count %d, want 40", state.Count)
		}
		if state.DeviceID != "device-000" {
			t.Fatalf("watcher saw device %q", state.DeviceID)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("no update arrived over redis pub/sub")
	}
}

func TestRedisWatchIsScopedToOneDevice(t *testing.T) {
	store := newStore(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	updates, err := store.WatchDevice(ctx, "user-000", "device-000")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := store.ApplyWindow(ctx, window("user-000", "device-999", 1, 1, 40)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := store.ApplyWindow(ctx, window("user-000", "device-000", 1, 1, 7)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	select {
	case state := <-updates:
		if state.Count != 7 {
			t.Fatalf("watcher saw count %d, want only its own device's 7", state.Count)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("no update arrived over redis pub/sub")
	}
}

func TestRedisWatchStopsOnCancellation(t *testing.T) {
	store := newStore(t)
	ctx, cancel := context.WithCancel(context.Background())

	updates, err := store.WatchDevice(ctx, "user-000", "device-000")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	cancel()

	deadline := time.After(10 * time.Second)
	for {
		select {
		case _, ok := <-updates:
			if !ok {
				return
			}
		case <-deadline:
			t.Fatal("cancelling the context did not close the watch channel")
		}
	}
}

func TestRedisWatchRejectsAnEmptyIdentity(t *testing.T) {
	store := newStore(t)

	if _, err := store.WatchDevice(context.Background(), "", "device-000"); !errors.Is(err, redisstore.ErrEmptyDeviceIdentity) {
		t.Fatalf("got %v, want ErrEmptyDeviceIdentity", err)
	}
}

func TestRedisStoreSatisfiesTheWatchAndLockPorts(t *testing.T) {
	store := newStore(t)

	var (
		_ domain.DeviceWatcher = store
		_ domain.FencedLocker  = store
	)
}

func TestRedisLockAdmitsExactlyOneHolder(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()

	lease, err := store.Acquire(ctx, "device-owner:device-000", 30*time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if lease.Token <= 0 {
		t.Fatalf("fencing token %d is not positive", lease.Token)
	}

	if _, err := store.Acquire(ctx, "device-owner:device-000", 30*time.Second); !errors.Is(err, redisstore.ErrLockHeld) {
		t.Fatalf("got %v, want ErrLockHeld", err)
	}

	if err := store.Release(ctx, lease); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	next, err := store.Acquire(ctx, "device-owner:device-000", 30*time.Second)
	if err != nil {
		t.Fatalf("unexpected error after release: %v", err)
	}
	if next.Token <= lease.Token {
		t.Fatalf("fencing token did not advance: %d then %d", lease.Token, next.Token)
	}
}

func TestRedisLockFencesOutAnExpiredHolder(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()

	stale, err := store.Acquire(ctx, "device-owner:device-001", 300*time.Millisecond)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := store.Guard(ctx, stale); err != nil {
		t.Fatalf("the live holder must be allowed to write: %v", err)
	}

	time.Sleep(600 * time.Millisecond)

	fresh, err := store.Acquire(ctx, "device-owner:device-001", 30*time.Second)
	if err != nil {
		t.Fatalf("an expired lease must be re-acquirable: %v", err)
	}

	if err := store.Guard(ctx, stale); !errors.Is(err, redisstore.ErrLeaseSuperseded) {
		t.Fatalf("a paused holder was allowed to write with a stale token: %v", err)
	}
	if err := store.Guard(ctx, fresh); err != nil {
		t.Fatalf("the new holder must be allowed to write: %v", err)
	}
	if err := store.Release(ctx, stale); !errors.Is(err, redisstore.ErrLeaseSuperseded) {
		t.Fatalf("a stale release was accepted: %v", err)
	}
	if err := store.Guard(ctx, fresh); err != nil {
		t.Fatalf("a stale release stole the lock from the live holder: %v", err)
	}
}

func TestRedisLockUnderConcurrentContention(t *testing.T) {
	store := newStore(t)

	const contenders = 16

	var (
		wait   sync.WaitGroup
		start  = make(chan struct{})
		mutex  sync.Mutex
		leases []domain.Lease
	)

	for i := 0; i < contenders; i++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start

			lease, err := store.Acquire(context.Background(), "device-owner:device-002", 30*time.Second)
			if err != nil {
				return
			}
			mutex.Lock()
			leases = append(leases, lease)
			mutex.Unlock()
		}()
	}

	close(start)
	wait.Wait()

	if len(leases) != 1 {
		t.Fatalf("%d contenders acquired the lock, want exactly 1", len(leases))
	}
}

func TestApplyAndResetKeysShareOneClusterSlot(t *testing.T) {
	store := newStore(t)

	tag := func(key string) string {
		open := strings.Index(key, "{")
		close := strings.Index(key, "}")
		if open < 0 || close < open {
			return ""
		}
		return key[open : close+1]
	}

	scripts := map[string][]string{
		"apply": store.ApplyScriptKeys("slot-user", "device-000"),
		"reset": store.ResetScriptKeys("slot-user", "device-000"),
		"lock":  store.LockScriptKeys("owner:device-000"),
	}

	for name, keys := range scripts {
		want := tag(keys[0])
		if want == "" {
			t.Fatalf("%s: key %q carries no hash tag, so a cluster cannot co-locate it", name, keys[0])
		}
		for _, key := range keys {
			if got := tag(key); got != want {
				t.Fatalf("%s: key %q has hash tag %q, want %q — a Redis Cluster would refuse this script with CROSSSLOT", name, key, got, want)
			}
		}
	}
}

func TestResetIsAtomicAgainstConcurrentApplies(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()

	if err := store.ApplyWindow(ctx, window("race-user", "device-000", 1, 1, 10)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var wait sync.WaitGroup
	start := make(chan struct{})

	wait.Add(1)
	go func() {
		defer wait.Done()
		<-start
		for i := 2; i < 40; i++ {
			_ = store.ApplyWindow(ctx, window("race-user", "device-000", 1, int64(i), 5))
		}
	}()

	wait.Add(1)
	go func() {
		defer wait.Done()
		<-start
		for i := 0; i < 20; i++ {
			if err := store.ResetDevice(ctx, "race-user", "device-000"); err != nil {
				return
			}
		}
	}()

	close(start)
	wait.Wait()

	state, err := store.DeviceState(ctx, "race-user", "device-000")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if state.Count < 0 {
		t.Fatalf("count went negative under concurrent reset and apply: %d", state.Count)
	}
}
