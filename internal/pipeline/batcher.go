package pipeline

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/ironclaw/mcp-ironclaw/internal/domain"
)

type OverflowPolicy uint8

const (
	OverflowDropOldest OverflowPolicy = iota
	OverflowDropNewest
	OverflowBlock
)

const defaultShutdownGrace = 5 * time.Second

var (
	ErrInvalidMaxEvents  = errors.New("pipeline: max events must be positive")
	ErrInvalidInterval   = errors.New("pipeline: max interval must be positive")
	ErrInvalidQueueDepth = errors.New("pipeline: queue depth must be positive")
	ErrNilSource         = errors.New("pipeline: event source is nil")
	ErrNilTransport      = errors.New("pipeline: transport is nil")
)

type BatcherConfig struct {
	DeviceID      string
	MaxEvents     int
	MaxInterval   time.Duration
	QueueDepth    int
	Policy        OverflowPolicy
	Strategy      OverflowStrategy
	ShutdownGrace time.Duration
	Clock         domain.Clock
	FlushSignal   <-chan time.Time
}

func (c BatcherConfig) validate() error {
	if c.DeviceID == "" {
		return domain.ErrMissingDeviceID
	}
	if c.MaxEvents <= 0 {
		return ErrInvalidMaxEvents
	}
	if c.MaxInterval <= 0 && c.FlushSignal == nil {
		return ErrInvalidInterval
	}
	if c.QueueDepth <= 0 {
		return ErrInvalidQueueDepth
	}
	return nil
}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

type Batcher struct {
	config   BatcherConfig
	clock    domain.Clock
	strategy OverflowStrategy
	queue    chan domain.Batch
	stats    counters
	mu       sync.Mutex
}

func NewBatcher(config BatcherConfig) (*Batcher, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}

	clock := config.Clock
	if clock == nil {
		clock = systemClock{}
	}
	strategy := config.Strategy
	if strategy == nil {
		strategy = StrategyFor(config.Policy)
	}
	if config.ShutdownGrace <= 0 {
		config.ShutdownGrace = defaultShutdownGrace
	}

	return &Batcher{
		config:   config,
		clock:    clock,
		strategy: strategy,
		queue:    make(chan domain.Batch, config.QueueDepth),
	}, nil
}

func (b *Batcher) Stats() Stats { return b.stats.snapshot() }

func (b *Batcher) Run(ctx context.Context, source domain.EventSource, transport domain.Transport) error {
	if source == nil {
		return ErrNilSource
	}
	if transport == nil {
		return ErrNilTransport
	}

	events, err := source.Events(ctx)
	if err != nil {
		return err
	}

	drainCtx, cancelDrain := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelDrain()

	sender := newSender(transport, &b.stats)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		sender.run(drainCtx, b.queue)
	}()

	collectErr := b.collect(ctx, events)
	close(b.queue)

	go func() {
		timer := time.NewTimer(b.config.ShutdownGrace)
		defer timer.Stop()
		select {
		case <-timer.C:
			cancelDrain()
		case <-drainCtx.Done():
		}
	}()

	wg.Wait()

	return collectErr
}

func (b *Batcher) collect(ctx context.Context, events <-chan domain.Event) error {
	pending := make([]domain.Event, 0, b.config.MaxEvents)

	flushes, stop := b.flushTicks()
	defer stop()

	for {
		select {
		case <-ctx.Done():
			b.flush(ctx, pending)
			return ctx.Err()

		case event, ok := <-events:
			if !ok {
				b.flush(ctx, pending)
				return nil
			}
			if err := b.accept(event); err != nil {
				continue
			}
			pending = append(pending, event)
			if len(pending) >= b.config.MaxEvents {
				b.flush(ctx, pending)
				pending = make([]domain.Event, 0, b.config.MaxEvents)
			}

		case <-flushes:
			if len(pending) > 0 {
				b.flush(ctx, pending)
				pending = make([]domain.Event, 0, b.config.MaxEvents)
			}
		}
	}
}

func (b *Batcher) accept(event domain.Event) error {
	if err := event.Validate(); err != nil {
		b.stats.eventsRejected.Add(1)
		return err
	}
	if event.DeviceID != b.config.DeviceID {
		b.stats.eventsRejected.Add(1)
		return domain.ErrDeviceMismatch
	}
	b.stats.eventsAccepted.Add(1)
	return nil
}

func (b *Batcher) flushTicks() (<-chan time.Time, func()) {
	if b.config.FlushSignal != nil {
		return b.config.FlushSignal, func() {}
	}
	ticker := time.NewTicker(b.config.MaxInterval)
	return ticker.C, ticker.Stop
}

func (b *Batcher) flush(ctx context.Context, events []domain.Event) {
	if len(events) == 0 {
		return
	}

	batch := domain.Batch{
		DeviceID:  b.config.DeviceID,
		CreatedAt: b.clock.Now(),
		Events:    events,
	}

	b.mu.Lock()
	outcome := b.strategy.Enqueue(ctx, b.queue, batch)
	b.mu.Unlock()

	if outcome.Enqueued {
		b.stats.batchesEnqueued.Add(1)
	}
	for _, dropped := range outcome.Dropped {
		b.stats.batchesDropped.Add(1)
		b.stats.eventsDropped.Add(int64(dropped.Len()))
	}
}
