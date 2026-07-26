package pipeline

import (
	"context"

	"github.com/ironclaw/mcp-ironclaw/internal/domain"
)

type OverflowOutcome struct {
	Enqueued bool
	Dropped  []domain.Batch
}

type OverflowStrategy interface {
	Enqueue(ctx context.Context, queue chan domain.Batch, batch domain.Batch) OverflowOutcome
}

type dropOldestStrategy struct{}

func (dropOldestStrategy) Enqueue(_ context.Context, queue chan domain.Batch, batch domain.Batch) OverflowOutcome {
	select {
	case queue <- batch:
		return OverflowOutcome{Enqueued: true}
	default:
	}

	select {
	case evicted := <-queue:
		select {
		case queue <- batch:
			return OverflowOutcome{Enqueued: true, Dropped: []domain.Batch{evicted}}
		default:
			return OverflowOutcome{Dropped: []domain.Batch{evicted, batch}}
		}
	default:
		return OverflowOutcome{Dropped: []domain.Batch{batch}}
	}
}

type dropNewestStrategy struct{}

func (dropNewestStrategy) Enqueue(_ context.Context, queue chan domain.Batch, batch domain.Batch) OverflowOutcome {
	select {
	case queue <- batch:
		return OverflowOutcome{Enqueued: true}
	default:
		return OverflowOutcome{Dropped: []domain.Batch{batch}}
	}
}

type blockingStrategy struct{}

func (blockingStrategy) Enqueue(ctx context.Context, queue chan domain.Batch, batch domain.Batch) OverflowOutcome {
	select {
	case queue <- batch:
		return OverflowOutcome{Enqueued: true}
	case <-ctx.Done():
		return OverflowOutcome{Dropped: []domain.Batch{batch}}
	}
}

func StrategyFor(policy OverflowPolicy) OverflowStrategy {
	switch policy {
	case OverflowDropNewest:
		return dropNewestStrategy{}
	case OverflowBlock:
		return blockingStrategy{}
	default:
		return dropOldestStrategy{}
	}
}
