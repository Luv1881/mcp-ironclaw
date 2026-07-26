package metrics

import (
	"errors"
	"sync"
	"sync/atomic"

	"github.com/ironclaw/mcp-ironclaw/internal/domain"
)

var ErrNilRecorder = errors.New("metrics: underlying recorder is nil")

const defaultBuffer = 4096

type sample struct {
	name  string
	delta int64
}

type Async struct {
	inner   domain.MetricsRecorder
	samples chan sample
	done    chan struct{}
	wg      sync.WaitGroup
	closed  atomic.Bool
	dropped atomic.Int64
}

func NewAsync(inner domain.MetricsRecorder, buffer int) (*Async, error) {
	if inner == nil {
		return nil, ErrNilRecorder
	}
	if buffer <= 0 {
		buffer = defaultBuffer
	}

	recorder := &Async{
		inner:   inner,
		samples: make(chan sample, buffer),
		done:    make(chan struct{}),
	}

	recorder.wg.Add(1)
	go recorder.drain()

	return recorder, nil
}

func (a *Async) Increment(name string, delta int64) {
	if a.closed.Load() {
		a.dropped.Add(1)
		return
	}

	select {
	case a.samples <- sample{name: name, delta: delta}:
	default:
		a.dropped.Add(1)
	}
}

func (a *Async) Dropped() int64 { return a.dropped.Load() }

func (a *Async) drain() {
	defer a.wg.Done()

	for {
		select {
		case entry := <-a.samples:
			a.inner.Increment(entry.name, entry.delta)
		case <-a.done:
			for {
				select {
				case entry := <-a.samples:
					a.inner.Increment(entry.name, entry.delta)
				default:
					return
				}
			}
		}
	}
}

func (a *Async) Close() {
	if a.closed.Swap(true) {
		return
	}
	close(a.done)
	a.wg.Wait()
}
