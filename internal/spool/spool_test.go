package spool_test

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/ironclaw/mcp-ironclaw/internal/spool"
)

func openSpool(t *testing.T, maxBytes int64) (*spool.Spool, string) {
	t.Helper()

	dir := t.TempDir()
	queue, err := spool.Open(dir, maxBytes)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	return queue, dir
}

func TestOpenValidatesConfiguration(t *testing.T) {
	if _, err := spool.Open("", 10); !errors.Is(err, spool.ErrNoDirectory) {
		t.Fatalf("got %v, want ErrNoDirectory", err)
	}
	if _, err := spool.Open(t.TempDir(), 0); !errors.Is(err, spool.ErrInvalidBudget) {
		t.Fatalf("got %v, want ErrInvalidBudget", err)
	}
}

func TestEntriesComeBackInOrder(t *testing.T) {
	queue, _ := openSpool(t, 1<<20)

	for _, payload := range []string{"one", "two", "three"} {
		if err := queue.Enqueue([]byte(payload)); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	for _, want := range []string{"one", "two", "three"} {
		payload, name, err := queue.Peek()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if string(payload) != want {
			t.Fatalf("got %q, want %q", payload, want)
		}
		queue.Release(name)
	}

	if _, _, err := queue.Peek(); !errors.Is(err, spool.ErrEmpty) {
		t.Fatalf("got %v, want ErrEmpty", err)
	}
}

func TestUnreleasedEntryIsRedelivered(t *testing.T) {
	queue, _ := openSpool(t, 1<<20)

	if err := queue.Enqueue([]byte("retry me")); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for i := 0; i < 3; i++ {
		payload, _, err := queue.Peek()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if string(payload) != "retry me" {
			t.Fatalf("got %q on attempt %d", payload, i)
		}
	}

	if got := queue.Stats().Entries; got != 1 {
		t.Fatalf("entries %d, want the unreleased entry to remain", got)
	}
}

func TestBudgetDropsOldestRatherThanGrowing(t *testing.T) {
	queue, _ := openSpool(t, 30)

	for i := 0; i < 20; i++ {
		if err := queue.Enqueue([]byte("0123456789")); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	stats := queue.Stats()
	if stats.Bytes > 30 {
		t.Fatalf("spool holds %d bytes, want at most the 30 byte budget", stats.Bytes)
	}
	if stats.Dropped == 0 {
		t.Fatal("expected the oldest entries to be dropped once the budget was reached")
	}
	if stats.Entries > 3 {
		t.Fatalf("entries %d, want at most 3 within budget", stats.Entries)
	}
}

func TestPayloadLargerThanTheBudgetIsRefused(t *testing.T) {
	queue, _ := openSpool(t, 8)

	if err := queue.Enqueue([]byte("far too large for the budget")); !errors.Is(err, spool.ErrTooLarge) {
		t.Fatalf("got %v, want ErrTooLarge", err)
	}
	if queue.Stats().Entries != 0 {
		t.Fatal("an oversized payload must not be stored")
	}
}

func TestSurvivesRestart(t *testing.T) {
	dir := t.TempDir()

	first, err := spool.Open(dir, 1<<20)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, payload := range []string{"a", "b"} {
		if err := first.Enqueue([]byte(payload)); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	second, err := spool.Open(dir, 1<<20)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := second.Stats().Entries; got != 2 {
		t.Fatalf("recovered %d entries, want 2", got)
	}

	payload, _, err := second.Peek()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(payload) != "a" {
		t.Fatalf("recovered %q first, want the oldest entry", payload)
	}
}

func TestPartialWritesAreDiscardedOnRecovery(t *testing.T) {
	dir := t.TempDir()

	if err := os.WriteFile(filepath.Join(dir, "00000000000000000001-000001.batch.partial"), []byte("torn"), 0o600); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	queue, err := spool.Open(dir, 1<<20)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := queue.Stats().Entries; got != 0 {
		t.Fatalf("recovered %d entries, want a torn write to be discarded", got)
	}

	remaining, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(remaining) != 0 {
		t.Fatalf("%d files remain, want the partial file removed", len(remaining))
	}
}
