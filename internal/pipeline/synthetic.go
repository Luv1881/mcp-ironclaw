package pipeline

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"time"

	"github.com/ironclaw/mcp-ironclaw/internal/domain"
)

var (
	ErrInvalidEventCount = errors.New("pipeline: event count must be positive")
	ErrNoProcesses       = errors.New("pipeline: process id set must not be empty")
	ErrSourceClosed      = errors.New("pipeline: source is closed")
)

type SyntheticConfig struct {
	DeviceID     string
	UserID       string
	PodID        string
	ProcessIDs   []int32
	EventCount   int
	Interval     time.Duration
	ErrorRate    float64
	BaseLatency  time.Duration
	TailLatency  time.Duration
	TailFraction float64
	Seed         int64
	Clock        domain.Clock
}

func (c SyntheticConfig) validate() error {
	if c.DeviceID == "" {
		return domain.ErrMissingDeviceID
	}
	if c.UserID == "" {
		return domain.ErrMissingUserID
	}
	if c.EventCount <= 0 {
		return ErrInvalidEventCount
	}
	if len(c.ProcessIDs) == 0 {
		return ErrNoProcesses
	}
	return nil
}

type SyntheticSource struct {
	config SyntheticConfig
	clock  domain.Clock
	random *rand.Rand
	once   sync.Once
	done   chan struct{}
	closed bool
	mu     sync.Mutex
}

func NewSyntheticSource(config SyntheticConfig) (*SyntheticSource, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}
	clock := config.Clock
	if clock == nil {
		clock = systemClock{}
	}
	return &SyntheticSource{
		config: config,
		clock:  clock,
		random: rand.New(rand.NewSource(config.Seed)),
		done:   make(chan struct{}),
	}, nil
}

func (s *SyntheticSource) DeviceID() string { return s.config.DeviceID }

func (s *SyntheticSource) Events(ctx context.Context) (<-chan domain.Event, error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, ErrSourceClosed
	}
	s.mu.Unlock()

	out := make(chan domain.Event)

	go func() {
		defer close(out)
		for i := 0; i < s.config.EventCount; i++ {
			event := s.next(i)
			if s.stopped() {
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-s.done:
				return
			case out <- event:
			}
			if s.config.Interval > 0 && !s.wait(ctx) {
				return
			}
		}
	}()

	return out, nil
}

func (s *SyntheticSource) stopped() bool {
	select {
	case <-s.done:
		return true
	default:
		return false
	}
}

func (s *SyntheticSource) wait(ctx context.Context) bool {
	timer := time.NewTimer(s.config.Interval)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-s.done:
		return false
	case <-timer.C:
		return true
	}
}

func (s *SyntheticSource) next(index int) domain.Event {
	s.mu.Lock()
	defer s.mu.Unlock()

	processID := s.config.ProcessIDs[index%len(s.config.ProcessIDs)]

	return domain.Event{
		DeviceID:     s.config.DeviceID,
		UserID:       s.config.UserID,
		ProcessID:    processID,
		PodID:        s.config.PodID,
		Kind:         domain.EventKindSyscall,
		ObservedAt:   s.clock.Now(),
		LatencyNanos: s.latency(),
		Bytes:        int64(64 + s.random.Intn(1024)),
		Failed:       s.random.Float64() < s.config.ErrorRate,
	}
}

func (s *SyntheticSource) latency() int64 {
	base := s.config.BaseLatency
	if base <= 0 {
		base = time.Millisecond
	}
	tail := s.config.TailLatency
	if tail <= 0 {
		tail = 50 * time.Millisecond
	}

	if s.config.TailFraction > 0 && s.random.Float64() < s.config.TailFraction {
		return int64(tail) + s.random.Int63n(int64(tail))
	}
	return int64(base) + s.random.Int63n(int64(base))
}

func (s *SyntheticSource) Close() error {
	s.once.Do(func() {
		s.mu.Lock()
		s.closed = true
		s.mu.Unlock()
		close(s.done)
	})
	return nil
}
