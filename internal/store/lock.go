package store

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/ironclaw/mcp-ironclaw/internal/domain"
)

var (
	ErrLockHeld        = errors.New("store: lock is held by another holder")
	ErrLeaseSuperseded = errors.New("store: lease was superseded by a newer fencing token")
	ErrEmptyResource   = errors.New("store: resource name is required")
)

var _ domain.FencedLocker = (*Locker)(nil)

type heldLock struct {
	token     int64
	expiresAt time.Time
}

type Locker struct {
	clock  domain.Clock
	held   map[string]heldLock
	fences map[string]int64
	mu     sync.Mutex
}

func NewLocker(clock domain.Clock) *Locker {
	if clock == nil {
		clock = realClock{}
	}
	return &Locker{
		clock:  clock,
		held:   make(map[string]heldLock),
		fences: make(map[string]int64),
	}
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now().UTC() }

func (l *Locker) Acquire(ctx context.Context, resource string, ttl time.Duration) (domain.Lease, error) {
	if err := ctx.Err(); err != nil {
		return domain.Lease{}, err
	}
	if resource == "" {
		return domain.Lease{}, ErrEmptyResource
	}
	if ttl <= 0 {
		ttl = DefaultLeaseTTL
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	now := l.clock.Now()
	if current, ok := l.held[resource]; ok && current.expiresAt.After(now) {
		return domain.Lease{}, ErrLockHeld
	}

	l.fences[resource]++
	lease := domain.Lease{
		Resource:  resource,
		Token:     l.fences[resource],
		ExpiresAt: now.Add(ttl),
	}
	l.held[resource] = heldLock{token: lease.Token, expiresAt: lease.ExpiresAt}

	return lease, nil
}

func (l *Locker) Release(ctx context.Context, lease domain.Lease) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	current, ok := l.held[lease.Resource]
	if !ok {
		return nil
	}
	if current.token != lease.Token {
		return ErrLeaseSuperseded
	}
	delete(l.held, lease.Resource)

	return nil
}

func (l *Locker) Guard(ctx context.Context, lease domain.Lease) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	current, ok := l.held[lease.Resource]
	if !ok || current.token != lease.Token {
		return ErrLeaseSuperseded
	}
	if l.fences[lease.Resource] > lease.Token {
		return ErrLeaseSuperseded
	}
	if !l.clock.Now().Before(current.expiresAt) {
		return ErrLeaseSuperseded
	}

	return nil
}

const DefaultLeaseTTL = 15 * time.Second
