package pipeline

import (
	"context"
	"sync/atomic"

	"github.com/ironclaw/mcp-ironclaw/internal/domain"
)

type Stats struct {
	EventsAccepted  int64
	EventsRejected  int64
	EventsDropped   int64
	BatchesEnqueued int64
	BatchesDropped  int64
	BatchesSent     int64
	SendFailures    int64
}

type counters struct {
	eventsAccepted  atomic.Int64
	eventsRejected  atomic.Int64
	eventsDropped   atomic.Int64
	batchesEnqueued atomic.Int64
	batchesDropped  atomic.Int64
	batchesSent     atomic.Int64
	sendFailures    atomic.Int64
}

func (c *counters) snapshot() Stats {
	return Stats{
		EventsAccepted:  c.eventsAccepted.Load(),
		EventsRejected:  c.eventsRejected.Load(),
		EventsDropped:   c.eventsDropped.Load(),
		BatchesEnqueued: c.batchesEnqueued.Load(),
		BatchesDropped:  c.batchesDropped.Load(),
		BatchesSent:     c.batchesSent.Load(),
		SendFailures:    c.sendFailures.Load(),
	}
}

type sender struct {
	transport domain.Transport
	stats     *counters
}

func newSender(transport domain.Transport, stats *counters) *sender {
	return &sender{transport: transport, stats: stats}
}

func (s *sender) run(ctx context.Context, queue <-chan domain.Batch) {
	for batch := range queue {
		if err := s.transport.Send(ctx, batch); err != nil {
			s.stats.sendFailures.Add(1)
			continue
		}
		s.stats.batchesSent.Add(1)
	}
}
