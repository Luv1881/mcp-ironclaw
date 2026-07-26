package aggregate

import (
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/ironclaw/mcp-ironclaw/internal/domain"
)

var (
	ErrInvalidWindowSize = errors.New("aggregate: window size must be positive")
	ErrTooManyKeys       = errors.New("aggregate: correlation key ceiling reached, event shed")
)

type QuantileSketch interface {
	Add(value int64)
	Quantile(q float64) (int64, error)
	Count() int64
	Max() int64
}

type SketchFactory func() (QuantileSketch, error)

type Config struct {
	WindowSize       time.Duration
	RelativeAccuracy float64
	NewSketch        SketchFactory
	SequenceSeed     int64
	MaxOpenWindows   int
}

func (c Config) validate() error {
	if c.WindowSize <= 0 {
		return ErrInvalidWindowSize
	}
	if c.RelativeAccuracy < 0 || c.RelativeAccuracy >= 1 {
		return ErrInvalidAccuracy
	}
	return nil
}

type windowKey struct {
	correlation domain.CorrelationKey
	windowID    int64
}

type windowState struct {
	count      int64
	errorCount int64
	bytes      int64
	sketch     QuantileSketch
}

type Aggregator struct {
	config   Config
	open     map[windowKey]*windowState
	pending  map[string]domain.AggregateWindow
	sequence int64
	shed     int64
	mu       sync.Mutex
}

func New(config Config) (*Aggregator, error) {
	if config.RelativeAccuracy == 0 {
		config.RelativeAccuracy = defaultRelativeError
	}
	if err := config.validate(); err != nil {
		return nil, err
	}
	if config.NewSketch == nil {
		accuracy := config.RelativeAccuracy
		config.NewSketch = func() (QuantileSketch, error) { return NewSketch(accuracy) }
	}
	sequence := config.SequenceSeed
	if sequence == 0 {
		sequence = time.Now().UnixNano()
	}

	return &Aggregator{
		config:   config,
		open:     make(map[windowKey]*windowState),
		pending:  make(map[string]domain.AggregateWindow),
		sequence: sequence,
	}, nil
}

func (a *Aggregator) WindowID(at time.Time) int64 {
	return at.UnixNano() / int64(a.config.WindowSize)
}

func (a *Aggregator) Shed() int64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.shed
}

func (a *Aggregator) OpenWindows() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.open) + len(a.pending)
}

func (a *Aggregator) Ingest(event domain.Event) error {
	if err := event.Validate(); err != nil {
		return err
	}

	key := windowKey{
		correlation: event.CorrelationKey(),
		windowID:    a.WindowID(event.ObservedAt),
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	state, ok := a.open[key]
	if !ok {
		if a.config.MaxOpenWindows > 0 && len(a.open) >= a.config.MaxOpenWindows {
			a.shed++
			return ErrTooManyKeys
		}

		sketch, err := a.config.NewSketch()
		if err != nil {
			return err
		}
		state = &windowState{sketch: sketch}
		a.open[key] = state
	}

	state.count++
	state.bytes += event.Bytes
	if event.Failed {
		state.errorCount++
	}
	state.sketch.Add(event.LatencyNanos)

	return nil
}

func (a *Aggregator) IngestBatch(batch domain.Batch) error {
	for _, event := range batch.Events {
		if err := event.Validate(); err != nil {
			return err
		}
	}
	for _, event := range batch.Events {
		if err := a.Ingest(event); err != nil {
			if errors.Is(err, ErrTooManyKeys) {
				continue
			}
			return err
		}
	}
	return nil
}

func (a *Aggregator) CollectWindowsBefore(watermark time.Time) ([]domain.AggregateWindow, error) {
	boundary := a.WindowID(watermark)

	a.mu.Lock()
	defer a.mu.Unlock()

	return a.collect(func(key windowKey) bool { return key.windowID < boundary })
}

func (a *Aggregator) CollectAll() ([]domain.AggregateWindow, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	return a.collect(func(windowKey) bool { return true })
}

func (a *Aggregator) Commit(windows []domain.AggregateWindow) {
	a.mu.Lock()
	defer a.mu.Unlock()

	for _, window := range windows {
		delete(a.pending, window.Identity())
	}
}

func (a *Aggregator) CloseWindowsBefore(watermark time.Time) ([]domain.AggregateWindow, error) {
	windows, err := a.CollectWindowsBefore(watermark)
	if err != nil {
		return nil, err
	}
	a.Commit(windows)
	return windows, nil
}

func (a *Aggregator) Flush() ([]domain.AggregateWindow, error) {
	windows, err := a.CollectAll()
	if err != nil {
		return nil, err
	}
	a.Commit(windows)
	return windows, nil
}

func (a *Aggregator) collect(include func(windowKey) bool) ([]domain.AggregateWindow, error) {
	type sortableKey struct {
		key   windowKey
		label string
	}

	selected := make([]sortableKey, 0, len(a.open))
	for key := range a.open {
		if include(key) {
			selected = append(selected, sortableKey{key: key, label: key.correlation.String()})
		}
	}

	sort.Slice(selected, func(i, j int) bool {
		if selected[i].key.windowID != selected[j].key.windowID {
			return selected[i].key.windowID < selected[j].key.windowID
		}
		return selected[i].label < selected[j].label
	})

	windows := make([]domain.AggregateWindow, 0, len(selected)+len(a.pending))
	for _, window := range a.pending {
		windows = append(windows, window)
	}
	sort.Slice(windows, func(i, j int) bool { return windows[i].Identity() < windows[j].Identity() })

	for _, entry := range selected {
		key := entry.key
		state := a.open[key]

		p95, err := state.sketch.Quantile(0.95)
		if err != nil {
			return nil, err
		}
		p99, err := state.sketch.Quantile(0.99)
		if err != nil {
			return nil, err
		}

		start := time.Unix(0, key.windowID*int64(a.config.WindowSize)).UTC()

		a.sequence++

		window := domain.AggregateWindow{
			Key:         key.correlation,
			WindowID:    key.windowID,
			Sequence:    a.sequence,
			WindowStart: start,
			WindowEnd:   start.Add(a.config.WindowSize),
			Count:       state.count,
			ErrorCount:  state.errorCount,
			Bytes:       state.bytes,
			P95Nanos:    p95,
			P99Nanos:    p99,
			MaxNanos:    state.sketch.Max(),
		}

		delete(a.open, key)
		a.pending[window.Identity()] = window
		windows = append(windows, window)
	}

	return windows, nil
}
