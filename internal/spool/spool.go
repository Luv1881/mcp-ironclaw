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
)

const (
	extension = ".batch"
	pending   = ".partial"
)

type Stats struct {
	Enqueued int64
	Dequeued int64
	Dropped  int64
	Bytes    int64
	Entries  int
}

type Spool struct {
	dir      string
	maxBytes int64

	mu    sync.Mutex
	order []string
	sizes map[string]int64
	bytes int64
	seq   uint64

	enqueued atomic.Int64
	dequeued atomic.Int64
	dropped  atomic.Int64
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

	s := &Spool{dir: dir, maxBytes: maxBytes, sizes: map[string]int64{}}
	if err := s.recover(); err != nil {
		return nil, err
	}

	return s, nil
}

func (s *Spool) recover() error {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return fmt.Errorf("spool: reading directory: %w", err)
	}

	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		name := entry.Name()
		if filepath.Ext(name) == pending {
			_ = os.Remove(filepath.Join(s.dir, name))
			continue
		}
		if filepath.Ext(name) != extension {
			continue
		}

		info, err := entry.Info()
		if err != nil {
			continue
		}
		names = append(names, name)
		s.sizes[name] = info.Size()
		s.bytes += info.Size()
		s.observe(name)
	}

	sort.Strings(names)
	s.order = names

	return nil
}

func (s *Spool) observe(name string) {
	ordinal, sequence, ok := parseName(name)
	if !ok {
		return
	}
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
		name := fmt.Sprintf("%020d-%06d%s", s.enqueued.Load()+1, s.seq, extension)
		if _, taken := s.sizes[name]; !taken {
			return name
		}
	}
}

func (s *Spool) Enqueue(payload []byte) error {
	size := int64(len(payload))
	if size > s.maxBytes {
		s.dropped.Add(1)
		return ErrTooLarge
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for s.bytes+size > s.maxBytes && len(s.order) > 0 {
		s.removeLocked(s.order[0])
		s.dropped.Add(1)
	}

	name := s.nextName()
	final := filepath.Join(s.dir, name)
	temp := final + pending

	if err := os.WriteFile(temp, payload, 0o600); err != nil {
		return fmt.Errorf("spool: writing entry: %w", err)
	}
	if err := os.Rename(temp, final); err != nil {
		_ = os.Remove(temp)
		return fmt.Errorf("spool: publishing entry: %w", err)
	}

	s.order = append(s.order, name)
	s.sizes[name] = size
	s.bytes += size
	s.enqueued.Add(1)

	return nil
}

func (s *Spool) Peek() ([]byte, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.order) == 0 {
		return nil, "", ErrEmpty
	}

	name := s.order[0]
	payload, err := os.ReadFile(filepath.Join(s.dir, name))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			s.removeLocked(name)
		}
		return nil, "", fmt.Errorf("spool: reading entry: %w", err)
	}

	return payload, name, nil
}

func (s *Spool) Release(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.sizes[name]; ok {
		s.removeLocked(name)
		s.dequeued.Add(1)
	}
}

func (s *Spool) removeLocked(name string) {
	_ = os.Remove(filepath.Join(s.dir, name))

	s.bytes -= s.sizes[name]
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
}

func (s *Spool) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()

	return Stats{
		Enqueued: s.enqueued.Load(),
		Dequeued: s.dequeued.Load(),
		Dropped:  s.dropped.Load(),
		Bytes:    s.bytes,
		Entries:  len(s.order),
	}
}
