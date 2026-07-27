package spool_test

import (
	"fmt"
	"testing"

	"github.com/ironclaw/mcp-ironclaw/internal/spool"
)

func drainAll(t *testing.T, queue *spool.Spool) []string {
	t.Helper()

	seen := make([]string, 0)
	for {
		payload, name, err := queue.Peek()
		if err != nil {
			return seen
		}
		seen = append(seen, string(payload))
		queue.Release(name)
	}
}

func TestEntriesSurviveARestartFollowedByNewEnqueues(t *testing.T) {
	dir := t.TempDir()

	first, err := spool.Open(dir, 1<<20)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for i := 0; i < 3; i++ {
		if err := first.Enqueue([]byte(fmt.Sprintf("before-restart-%d", i))); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	reopened, err := spool.Open(dir, 1<<20)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := reopened.Stats().Entries; got != 3 {
		t.Fatalf("recovered %d entries, want 3", got)
	}

	for i := 0; i < 3; i++ {
		if err := reopened.Enqueue([]byte(fmt.Sprintf("after-restart-%d", i))); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	if got := reopened.Stats().Entries; got != 6 {
		t.Fatalf("spool holds %d entries after 3 pre-restart and 3 post-restart enqueues, want 6", got)
	}

	drained := drainAll(t, reopened)
	if len(drained) != 6 {
		t.Fatalf("drained %d entries, want 6: %v", len(drained), drained)
	}

	unique := map[string]bool{}
	for _, payload := range drained {
		if unique[payload] {
			t.Fatalf("payload %q came back twice, so a filename collided and overwrote another entry: %v", payload, drained)
		}
		unique[payload] = true
	}

	for i := 0; i < 3; i++ {
		want := fmt.Sprintf("before-restart-%d", i)
		if !unique[want] {
			t.Fatalf("%q was lost across the restart; drained: %v", want, drained)
		}
	}
}

func TestOrderingIsPreservedAcrossManyRestarts(t *testing.T) {
	dir := t.TempDir()

	expected := make([]string, 0, 12)

	for restart := 0; restart < 4; restart++ {
		queue, err := spool.Open(dir, 1<<20)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		for i := 0; i < 3; i++ {
			payload := fmt.Sprintf("restart-%d-entry-%d", restart, i)
			if err := queue.Enqueue([]byte(payload)); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			expected = append(expected, payload)
		}
	}

	final, err := spool.Open(dir, 1<<20)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	drained := drainAll(t, final)
	if len(drained) != len(expected) {
		t.Fatalf("drained %d entries across 4 restarts, want %d: %v", len(drained), len(expected), drained)
	}
	for i := range expected {
		if drained[i] != expected[i] {
			t.Fatalf("entry %d is %q, want %q — FIFO order was not preserved across restarts:\n got %v\nwant %v",
				i, drained[i], expected[i], drained, expected)
		}
	}
}

func TestRecoveredEntriesStillHonourTheByteBudget(t *testing.T) {
	dir := t.TempDir()

	first, err := spool.Open(dir, 400)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for i := 0; i < 6; i++ {
		if err := first.Enqueue([]byte(fmt.Sprintf("%0100d", i))); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	reopened, err := spool.Open(dir, 400)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for i := 6; i < 12; i++ {
		if err := reopened.Enqueue([]byte(fmt.Sprintf("%0100d", i))); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	stats := reopened.Stats()
	if stats.Bytes > 400 {
		t.Fatalf("spool holds %d bytes against a 400 byte budget after a restart", stats.Bytes)
	}

	drained := drainAll(t, reopened)
	unique := map[string]bool{}
	for _, payload := range drained {
		if unique[payload] {
			t.Fatalf("payload %q came back twice after a restart under budget pressure", payload)
		}
		unique[payload] = true
	}
}
