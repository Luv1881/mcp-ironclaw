package spool_test

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/ironclaw/mcp-ironclaw/internal/spool"
)

func TestEmptyPayloadsAreRefused(t *testing.T) {
	queue, _ := openSpool(t, 1<<20)

	if err := queue.Enqueue(nil); !errors.Is(err, spool.ErrEmptyPayload) {
		t.Fatalf("got %v, want ErrEmptyPayload: a zero-length entry consumes no budget but still consumes an inode and an index slot", err)
	}

	if stats := queue.Stats(); stats.Entries != 0 {
		t.Fatalf("queue holds %d entries after refusing an empty payload", stats.Entries)
	}
}

func TestTheIndexIsBoundedIndependentlyOfTheByteBudget(t *testing.T) {
	queue, _ := openSpool(t, 1<<20)

	payload := []byte("x")

	refused := false
	for i := 0; i < 200_000; i++ {
		if err := queue.Enqueue(payload); err != nil {
			if !errors.Is(err, spool.ErrTooMany) {
				t.Fatalf("unexpected error after %d entries: %v", i, err)
			}
			refused = true
			break
		}
	}

	if !refused {
		t.Fatal("a one-megabyte budget accepted 200,000 one-byte entries, so the index is not bounded")
	}
}

func TestRecoveryRefusesToFollowSymlinks(t *testing.T) {
	secret := filepath.Join(t.TempDir(), "secret")
	if err := os.WriteFile(secret, []byte("telemetry-that-is-not-ours"), 0o600); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	dir := t.TempDir()
	name := "00000000000000000001-00000000000000000001.batch"
	if err := os.Symlink(secret, filepath.Join(dir, name)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	queue, err := spool.Open(dir, 1<<20)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if stats := queue.Stats(); stats.Entries != 0 {
		t.Fatalf("recovered %d entries from a symlink, want 0", stats.Entries)
	}

	if _, _, err := queue.Peek(); !errors.Is(err, spool.ErrEmpty) {
		t.Fatalf("got %v, want ErrEmpty: a symlink planted in the spool directory must never be read and shipped", err)
	}
}

func TestAnEntryReplacedByASymlinkIsRefusedAtPeek(t *testing.T) {
	queue, dir := openSpool(t, 1<<20)

	if err := queue.Enqueue([]byte("legitimate")); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	_, name, err := queue.Peek()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	secret := filepath.Join(t.TempDir(), "secret")
	if err := os.WriteFile(secret, []byte("not-telemetry"), 0o600); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	path := filepath.Join(dir, name)
	if err := os.Remove(path); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := os.Symlink(secret, path); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	payload, _, err := queue.Peek()
	if err == nil {
		t.Fatalf("peeked %q through a symlink: an entry whose file was swapped must not be read", payload)
	}
	if !errors.Is(err, spool.ErrInvalidName) {
		t.Fatalf("got %v, want ErrInvalidName", err)
	}
}

func TestTheSpoolDirectoryIsMadeOwnerOnly(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "shared")
	if err := os.MkdirAll(dir, 0o777); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := os.Chmod(dir, 0o777); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if _, err := spool.Open(dir, 1<<20); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	info, err := os.Stat(dir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0o700 {
		t.Fatalf("directory mode is %o, want 0700: spooled telemetry must not be readable by other local users", perm)
	}
}

func TestRecencyOrderSurvivesRecoveryBeyondTheOldSequenceWidth(t *testing.T) {
	dir := t.TempDir()

	files := []string{
		"00000000000000000001-0000000000001000000.batch",
		"00000000000000000001-0000000000000999999.batch",
	}

	for _, name := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(name), 0o600); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	queue, err := spool.Open(dir, 1<<20)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	_, first, err := queue.Peek()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if first != files[1] {
		t.Fatalf("recovered order starts at %s, want %s: ordering must follow the parsed sequence, not the raw filename", first, files[1])
	}
}

func TestReleaseReportsThatTheEntryCouldNotBeRemoved(t *testing.T) {
	queue, dir := openSpool(t, 1<<20)

	if err := queue.Enqueue([]byte("stuck")); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	_, name, err := queue.Peek()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })

	if err := queue.Release(name); err == nil {
		t.Skip("this filesystem permits unlink from a read-only directory, so the failure cannot be provoked here")
	}

	if stats := queue.Stats(); stats.Entries != 1 {
		t.Fatalf("queue holds %d entries after a failed removal, want 1: accounting must not claim an entry left the disk", stats.Entries)
	}
}

func TestRecoveryBringsAnOverfullDirectoryBackInsideTheBudget(t *testing.T) {
	dir := t.TempDir()

	if _, err := spool.Open(dir, 1<<20); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for i := 0; i < 8; i++ {
		name := filepath.Join(dir, "0000000000000000000"+string(rune('1'+i))+"-00000000000000000001.batch")
		if err := os.WriteFile(name, make([]byte, 64<<10), 0o600); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	const budget = 128 << 10
	queue, err := spool.Open(dir, budget)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	stats := queue.Stats()
	if stats.Bytes > budget {
		t.Fatalf("recovered %d bytes against a %d-byte budget: recovery must trim, not merely adopt what it finds", stats.Bytes, budget)
	}

	remaining, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(remaining) != stats.Entries {
		t.Fatalf("%d files remain on disk but the index holds %d entries", len(remaining), stats.Entries)
	}
}
