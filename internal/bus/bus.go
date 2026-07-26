package bus

import (
	"context"
	"errors"
	"hash/fnv"
	"sync"
	"sync/atomic"
)

var (
	ErrInvalidPartitions = errors.New("bus: partition count must be positive")
	ErrInvalidCapacity   = errors.New("bus: partition capacity must be positive")
	ErrInvalidAttempts   = errors.New("bus: max attempts must be positive")
	ErrNoFreePartition   = errors.New("bus: every partition is already claimed")
	ErrClosed            = errors.New("bus: closed")
)

type Config struct {
	Partitions  int
	Capacity    int
	MaxAttempts int
}

func (c Config) validate() error {
	if c.Partitions <= 0 {
		return ErrInvalidPartitions
	}
	if c.Capacity <= 0 {
		return ErrInvalidCapacity
	}
	if c.MaxAttempts <= 0 {
		return ErrInvalidAttempts
	}
	return nil
}

type Stats struct {
	Published  int64
	Delivered  int64
	Retried    int64
	DeadLetter int64
	Abandoned  int64
}

type Bus[T any] struct {
	config     Config
	partitions []chan T
	claimed    []bool
	deadLetter []T
	shutdown   chan struct{}
	mu         sync.Mutex
	closeOnce  sync.Once
	closed     atomic.Bool
	published  atomic.Int64
	delivered  atomic.Int64
	retried    atomic.Int64
	dead       atomic.Int64
	abandoned  atomic.Int64
}

func New[T any](config Config) (*Bus[T], error) {
	if config.MaxAttempts == 0 {
		config.MaxAttempts = 1
	}
	if err := config.validate(); err != nil {
		return nil, err
	}

	partitions := make([]chan T, config.Partitions)
	for i := range partitions {
		partitions[i] = make(chan T, config.Capacity)
	}

	return &Bus[T]{
		config:     config,
		partitions: partitions,
		claimed:    make([]bool, config.Partitions),
		shutdown:   make(chan struct{}),
	}, nil
}

func (b *Bus[T]) Partitions() int { return b.config.Partitions }

func (b *Bus[T]) PartitionFor(key string) int {
	hasher := fnv.New32a()
	_, _ = hasher.Write([]byte(key))
	return int(hasher.Sum32()) % b.config.Partitions
}

func (b *Bus[T]) Stats() Stats {
	return Stats{
		Published:  b.published.Load(),
		Delivered:  b.delivered.Load(),
		Retried:    b.retried.Load(),
		DeadLetter: b.dead.Load(),
		Abandoned:  b.abandoned.Load(),
	}
}

func (b *Bus[T]) DeadLettered() []T {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]T(nil), b.deadLetter...)
}

func (b *Bus[T]) Send(ctx context.Context, key string, value T) error {
	if b.closed.Load() {
		return ErrClosed
	}

	select {
	case b.partitions[b.PartitionFor(key)] <- value:
		b.published.Add(1)
		return nil
	case <-b.shutdown:
		return ErrClosed
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *Bus[T]) Claim() (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for index, taken := range b.claimed {
		if !taken {
			b.claimed[index] = true
			return index, nil
		}
	}
	return 0, ErrNoFreePartition
}

func (b *Bus[T]) claimAll() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, taken := range b.claimed {
		if taken {
			return ErrNoFreePartition
		}
	}
	for index := range b.claimed {
		b.claimed[index] = true
	}
	return nil
}

func (b *Bus[T]) Release(partition int) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if partition >= 0 && partition < len(b.claimed) {
		b.claimed[partition] = false
	}
}

func (b *Bus[T]) releaseAll() {
	b.mu.Lock()
	defer b.mu.Unlock()

	for index := range b.claimed {
		b.claimed[index] = false
	}
}

func (b *Bus[T]) RunConsumerGroup(ctx context.Context, handler func(context.Context, T) error) error {
	if err := b.claimAll(); err != nil {
		return err
	}
	defer b.releaseAll()

	errs := make([]error, b.config.Partitions)

	var wg sync.WaitGroup
	for partition := 0; partition < b.config.Partitions; partition++ {
		wg.Add(1)
		go func(partition int) {
			defer wg.Done()
			errs[partition] = b.consume(ctx, partition, handler)
		}(partition)
	}
	wg.Wait()

	return errors.Join(errs...)
}

func (b *Bus[T]) Receive(ctx context.Context, handler func(context.Context, T) error) error {
	partition, err := b.Claim()
	if err != nil {
		return err
	}
	defer b.Release(partition)

	return b.consume(ctx, partition, handler)
}

func (b *Bus[T]) ReceivePartition(ctx context.Context, partition int, handler func(context.Context, T) error) error {
	return b.consume(ctx, partition, handler)
}

func (b *Bus[T]) consume(ctx context.Context, partition int, handler func(context.Context, T) error) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case value := <-b.partitions[partition]:
			b.deliver(ctx, value, handler)

		case <-b.shutdown:
			return b.drain(ctx, partition, handler)
		}
	}
}

func (b *Bus[T]) drain(ctx context.Context, partition int, handler func(context.Context, T) error) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case value := <-b.partitions[partition]:
			b.deliver(ctx, value, handler)

		default:
			return nil
		}
	}
}

func (b *Bus[T]) deliver(ctx context.Context, value T, handler func(context.Context, T) error) {
	for attempt := 1; attempt <= b.config.MaxAttempts; attempt++ {
		if err := handler(ctx, value); err == nil {
			b.delivered.Add(1)
			return
		}
		if ctx.Err() != nil {
			b.abandoned.Add(1)
			return
		}
		if attempt < b.config.MaxAttempts {
			b.retried.Add(1)
		}
	}

	b.dead.Add(1)
	b.mu.Lock()
	b.deadLetter = append(b.deadLetter, value)
	b.mu.Unlock()
}

func (b *Bus[T]) Close() {
	b.closeOnce.Do(func() {
		b.closed.Store(true)
		close(b.shutdown)
	})
}
