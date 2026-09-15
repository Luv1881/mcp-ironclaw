package spool

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

var (
	ErrNoDirectory   = errors.New("spool: directory is required")
	ErrInvalidBudget = errors.New("spool: max bytes must be positive")
	ErrEmpty         = errors.New("spool: no entries")
	ErrTooLarge      = errors.New("spool: payload exceeds the entire budget")
	ErrEmptyPayload  = errors.New("spool: refusing to spool an empty payload")
	ErrTooMany       = errors.New("spool: entry count exceeds the budget's index ceiling")
	ErrInvalidName   = errors.New("spool: entry name is not a plain filename")
)

const (
	extension = ".batch"
	pending   = ".partial"

	minBudgetPerEntry = 1024
	minEntries        = 64
)

type Stats struct {
	Enqueued     int64
	Dequeued     int64
	Dropped      int64
	DropFailures int64
	Bytes        int64
	Entries      int
}

type Spool struct {
	dir        string
	maxBytes   int64
	maxEntries int

	mu    sync.Mutex
	order []string
	sizes map[string]int64
	bytes int64
	seq   uint64

	enqueued     atomic.Int64
	dequeued     atomic.Int64
	dropped      atomic.Int64
	dropFailures atomic.Int64
}

func Open(dir string, maxBytes int64) (*Spool, error) {
	if dir == "" {
		return nil, ErrNoDirectory
	}
	if maxBytes <= 0 {
		return nil, ErrInvalidBudget
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("spool: creating directory: %w", err)
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		return nil, fmt.Errorf("spool: restricting directory permissions: %w", err)
	}

	maxEntries := int(maxBytes / minBudgetPerEntry)
	if maxEntries < minEntries {
		maxEntries = minEntries
	}

	s := &Spool{dir: dir, maxBytes: maxBytes, maxEntries: maxEntries, sizes: map[string]int64{}}
	if err := s.recover(); err != nil {
		return nil, err
	}
	s.trimToBudget()

	return s, nil
}

type entry struct {
	name     string
	ordinal  int64
	sequence uint64
}

func (s *Spool) recover() error {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return fmt.Errorf("spool: reading directory: %w", err)
	}

	found := make([]entry, 0, len(entries))
	for _, directoryEntry := range entries {
		name := directoryEntry.Name()
		if filepath.Ext(name) == pending {
			_ = os.Remove(filepath.Join(s.dir, name))
			continue
		}
		if filepath.Ext(name) != extension {
			continue
		}
		if !directoryEntry.Type().IsRegular() {
			continue
		}

		info, err := directoryEntry.Info()
		if err != nil {
			return fmt.Errorf("spool: inspecting entry %s: %w", name, err)
		}

		ordinal, sequence, ok := parseName(name)
		if !ok {
			continue
		}

		s.sizes[name] = info.Size()
		s.bytes += info.Size()
		s.observe(ordinal, sequence)
		found = append(found, entry{name: name, ordinal: ordinal, sequence: sequence})
	}

	sort.Slice(found, func(i, j int) bool {
		if found[i].ordinal != found[j].ordinal {
			return found[i].ordinal < found[j].ordinal
		}
		return found[i].sequence < found[j].sequence
	})

	s.order = make([]string, 0, len(found))
	for _, item := range found {
		s.order = append(s.order, item.name)
	}

	return nil
}

func (s *Spool) trimToBudget() {
	s.mu.Lock()
	defer s.mu.Unlock()

	for (s.bytes > s.maxBytes || len(s.order) > s.maxEntries) && len(s.order) > 0 {
		if err := s.removeLocked(s.order[0]); err != nil {
			s.dropFailures.Add(1)
			return
		}
		s.dropped.Add(1)
	}
}

func (s *Spool) observe(ordinal int64, sequence uint64) {
	if ordinal > s.enqueued.Load() {
		s.enqueued.Store(ordinal)
	}
	if sequence > s.seq {
		s.seq = sequence
	}
}

func parseName(name string) (int64, uint64, bool) {
	trimmed := strings.TrimSuffix(name, extension)

	separator := strings.LastIndex(trimmed, "-")
	if separator < 0 {
		return 0, 0, false
	}

	ordinal, err := strconv.ParseInt(trimmed[:separator], 10, 64)
	if err != nil {
		return 0, 0, false
	}
	sequence, err := strconv.ParseUint(trimmed[separator+1:], 10, 64)
	if err != nil {
		return 0, 0, false
	}

	return ordinal, sequence, true
}

func (s *Spool) nextName() string {
	for {
		s.seq++
		name := fmt.Sprintf("%020d-%020d%s", s.enqueued.Load()+1, s.seq, extension)
		if _, taken := s.sizes[name]; !taken {
			return name
		}
	}
}

func (s *Spool) Enqueue(payload []byte) error {
	if len(payload) == 0 {
		return ErrEmptyPayload
	}

	size := int64(len(payload))
	if size > s.maxBytes {
		s.dropped.Add(1)
		return ErrTooLarge
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for s.bytes+size > s.maxBytes && len(s.order) > 0 {
		if err := s.removeLocked(s.order[0]); err != nil {
			s.dropFailures.Add(1)
			break
		}
		s.dropped.Add(1)
	}

	if len(s.order) >= s.maxEntries {
		s.dropped.Add(1)
		return fmt.Errorf("%w: %d entries", ErrTooMany, s.maxEntries)
	}

	name := s.nextName()
	final := filepath.Join(s.dir, name)
	temp := final + pending

	if err := writeFileSynced(temp, payload); err != nil {
		_ = os.Remove(temp)
		return err
	}
	if err := os.Rename(temp, final); err != nil {
		_ = os.Remove(temp)
		return fmt.Errorf("spool: publishing entry: %w", err)
	}

	s.order = append(s.order, name)
	s.sizes[name] = size
	s.bytes += size
	s.enqueued.Add(1)

	if err := syncDirectory(s.dir); err != nil {
		return fmt.Errorf("spool: syncing directory after publishing %s: %w", name, err)
	}

	return nil
}

func writeFileSynced(path string, payload []byte) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("spool: writing entry: %w", err)
	}

	if _, err := file.Write(payload); err != nil {
		_ = file.Close()
		return fmt.Errorf("spool: writing entry: %w", err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return fmt.Errorf("spool: syncing entry: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("spool: closing entry: %w", err)
	}

	return nil
}

func syncDirectory(dir string) error {
	handle, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer handle.Close()

	return handle.Sync()
}

func (s *Spool) Peek() ([]byte, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.order) == 0 {
		return nil, "", ErrEmpty
	}

	name := s.order[0]
	path := filepath.Join(s.dir, name)

	info, err := os.Lstat(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			_ = s.removeLocked(name)
		}
		return nil, "", fmt.Errorf("spool: reading entry: %w", err)
	}
	if !info.Mode().IsRegular() {
		_ = s.removeLocked(name)
		return nil, "", fmt.Errorf("%w: %s is not a regular file", ErrInvalidName, name)
	}

	payload, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			_ = s.removeLocked(name)
		}
		return nil, "", fmt.Errorf("spool: reading entry: %w", err)
	}

	return payload, name, nil
}

func (s *Spool) Release(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.sizes[name]; !ok {
		return nil
	}
	if err := s.removeLocked(name); err != nil {
		return err
	}

	s.dequeued.Add(1)

	return nil
}

func (s *Spool) removeLocked(name string) error {
	if filepath.Base(name) != name || name == "." || name == string(filepath.Separator) {
		return fmt.Errorf("%w: %q", ErrInvalidName, name)
	}

	if err := os.Remove(filepath.Join(s.dir, name)); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("spool: removing entry %s: %w", name, err)
	}

	size, ok := s.sizes[name]
	if !ok {
		return nil
	}

	s.bytes -= size
	if s.bytes < 0 {
		s.bytes = 0
	}
	delete(s.sizes, name)

	for i, existing := range s.order {
		if existing == name {
			s.order = append(s.order[:i], s.order[i+1:]...)
			break
		}
	}

	return nil
}

func (s *Spool) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()

	return Stats{
		Enqueued:     s.enqueued.Load(),
		Dequeued:     s.dequeued.Load(),
		Dropped:      s.dropped.Load(),
		DropFailures: s.dropFailures.Load(),
		Bytes:        s.bytes,
		Entries:      len(s.order),
	}
}
