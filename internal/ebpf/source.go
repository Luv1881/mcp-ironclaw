//go:build ebpf && linux

package ebpf

import (
	"bytes"
	"context"
	_ "embed"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/cilium/ebpf"
	"github.com/cilium/ebpf/link"
	"github.com/cilium/ebpf/ringbuf"
	"github.com/cilium/ebpf/rlimit"
	"github.com/ironclaw/mcp-ironclaw/internal/domain"
)

//go:embed ironclaw.bpf.o
var program []byte

const (
	statObserved = iota
	statEmitted
	statDroppedRingbuf
	statDroppedRate
	statDroppedFilter
	statCount
)

var ErrAlreadyStarted = errors.New("ebpf: source already started")

type Settings struct {
	MinLatency               time.Duration
	MaxEventsPerCPUPerSecond uint64
	SampleModulus            uint32
	TargetTGID               uint32
}

type kernelConfig struct {
	MinLatencyNanos          uint64
	MaxEventsPerCPUPerSecond uint64
	SampleModulus            uint32
	TargetTGID               uint32
	Enabled                  uint8
	Pad                      [7]uint8
}

type Stats struct {
	Observed       uint64
	Emitted        uint64
	DroppedRingbuf uint64
	DroppedRate    uint64
	DroppedFilter  uint64
	DecodeFailures uint64
}

type Source struct {
	translator Translator
	settings   Settings

	collection *ebpf.Collection
	links      []link.Link
	reader     *ringbuf.Reader

	started   bool
	closeOnce sync.Once
	mu        sync.Mutex

	decodeFailures uint64
}

func NewSource(translator Translator, settings Settings) (*Source, error) {
	if err := translator.Validate(); err != nil {
		return nil, err
	}
	return &Source{translator: translator, settings: settings}, nil
}

func (s *Source) Events(ctx context.Context) (<-chan domain.Event, error) {
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return nil, ErrAlreadyStarted
	}
	s.started = true
	s.mu.Unlock()

	if err := rlimit.RemoveMemlock(); err != nil {
		return nil, fmt.Errorf("ebpf: raising memlock limit: %w", err)
	}

	spec, err := ebpf.LoadCollectionSpecFromReader(bytes.NewReader(program))
	if err != nil {
		return nil, fmt.Errorf("ebpf: loading collection spec: %w", err)
	}

	collection, err := ebpf.NewCollection(spec)
	if err != nil {
		return nil, fmt.Errorf("ebpf: creating collection: %w", err)
	}

	s.mu.Lock()
	s.collection = collection
	s.mu.Unlock()

	if err := s.applySettings(); err != nil {
		s.Close()
		return nil, err
	}
	if err := s.attach(); err != nil {
		s.Close()
		return nil, err
	}

	reader, err := ringbuf.NewReader(collection.Maps["events"])
	if err != nil {
		s.Close()
		return nil, fmt.Errorf("ebpf: opening ring buffer: %w", err)
	}
	s.reader = reader

	out := make(chan domain.Event)
	go s.pump(ctx, out)

	return out, nil
}

func (s *Source) applySettings() error {
	config := kernelConfig{
		MinLatencyNanos:          uint64(s.settings.MinLatency),
		MaxEventsPerCPUPerSecond: s.settings.MaxEventsPerCPUPerSecond,
		SampleModulus:            s.settings.SampleModulus,
		TargetTGID:               s.settings.TargetTGID,
		Enabled:                  1,
	}

	settings := s.collection.Maps["settings"]
	if settings == nil {
		return errors.New("ebpf: settings map is missing from the object")
	}

	key := uint32(0)
	if err := settings.Put(&key, &config); err != nil {
		return fmt.Errorf("ebpf: writing settings: %w", err)
	}

	return nil
}

func (s *Source) attach() error {
	programs := map[string]string{
		"ironclaw_sys_enter": "sys_enter",
		"ironclaw_sys_exit":  "sys_exit",
	}

	for name, tracepoint := range programs {
		prog := s.collection.Programs[name]
		if prog == nil {
			return fmt.Errorf("ebpf: program %q is missing from the object", name)
		}

		attached, err := link.Tracepoint("raw_syscalls", tracepoint, prog, nil)
		if err != nil {
			return fmt.Errorf("ebpf: attaching %s: %w", tracepoint, err)
		}
		s.links = append(s.links, attached)
	}

	return nil
}

func (s *Source) pump(ctx context.Context, out chan<- domain.Event) {
	defer close(out)

	finished := make(chan struct{})
	defer close(finished)

	go func() {
		select {
		case <-ctx.Done():
			s.reader.Close()
		case <-finished:
		}
	}()

	for {
		sample, err := s.reader.Read()
		if err != nil {
			return
		}

		record, err := Decode(sample.RawSample)
		if err != nil {
			s.mu.Lock()
			s.decodeFailures++
			s.mu.Unlock()
			continue
		}

		event, err := s.translator.Event(record)
		if err != nil {
			s.mu.Lock()
			s.decodeFailures++
			s.mu.Unlock()
			continue
		}

		select {
		case out <- event:
		case <-ctx.Done():
			return
		}
	}
}

func (s *Source) Stats() (Stats, error) {
	s.mu.Lock()
	collection := s.collection
	failures := s.decodeFailures
	s.mu.Unlock()

	if collection == nil {
		return Stats{}, nil
	}

	counters := collection.Maps["stats"]
	if counters == nil {
		return Stats{}, errors.New("ebpf: stats map is missing from the object")
	}

	totals := make([]uint64, statCount)
	for index := range totals {
		var perCPU []uint64
		key := uint32(index)
		if err := counters.Lookup(&key, &perCPU); err != nil {
			return Stats{}, fmt.Errorf("ebpf: reading stat %d: %w", index, err)
		}
		for _, value := range perCPU {
			totals[index] += value
		}
	}

	return Stats{
		Observed:       totals[statObserved],
		Emitted:        totals[statEmitted],
		DroppedRingbuf: totals[statDroppedRingbuf],
		DroppedRate:    totals[statDroppedRate],
		DroppedFilter:  totals[statDroppedFilter],
		DecodeFailures: failures,
	}, nil
}

func (s *Source) Close() error {
	s.closeOnce.Do(func() {
		if s.reader != nil {
			s.reader.Close()
		}
		for _, attached := range s.links {
			attached.Close()
		}
		if s.collection != nil {
			s.collection.Close()
		}
	})
	return nil
}
