package app

import (
	"context"
	"errors"
	"testing"

	"github.com/ironclaw/mcp-ironclaw/internal/domain"
)

type recordingPublisher struct {
	received []domain.AggregateWindow
	err      error
}

func (p *recordingPublisher) PublishWindow(_ context.Context, window domain.AggregateWindow) error {
	p.received = append(p.received, window)
	return p.err
}

func sampleWindow(sequence int64) domain.AggregateWindow {
	return domain.AggregateWindow{
		Key:      domain.CorrelationKey{UserID: "user-1", DeviceID: "device-1", ProcessID: 7, PodID: "pod-a"},
		WindowID: 42,
		Sequence: sequence,
		Count:    3,
	}
}

func TestFanOutPublishesEveryWindowToEveryTarget(t *testing.T) {
	hot := &recordingPublisher{}
	archive := &recordingPublisher{}

	publisher := newFanOutWindows(hot, archive)
	window := sampleWindow(1)

	if err := publisher.PublishWindow(context.Background(), window); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for name, target := range map[string]*recordingPublisher{"hot state": hot, "archive": archive} {
		if len(target.received) != 1 {
			t.Fatalf("%s received %d windows, want 1", name, len(target.received))
		}
		if target.received[0].Sequence != window.Sequence {
			t.Fatalf("%s received sequence %d, want %d", name, target.received[0].Sequence, window.Sequence)
		}
	}
}

func TestFanOutReachesLaterTargetsAfterAnEarlierFailure(t *testing.T) {
	failure := errors.New("hot state is unavailable")
	hot := &recordingPublisher{err: failure}
	archive := &recordingPublisher{}

	publisher := newFanOutWindows(hot, archive)

	err := publisher.PublishWindow(context.Background(), sampleWindow(1))
	if !errors.Is(err, failure) {
		t.Fatalf("got %v, want the hot state failure to surface so the offset is not committed", err)
	}

	if len(archive.received) != 1 {
		t.Fatalf("archive received %d windows, want 1: a cache outage must not stop the durable archive in the same attempt", len(archive.received))
	}
}

func TestFanOutReportsTheFirstFailureWhenEveryTargetFails(t *testing.T) {
	first := errors.New("hot state is unavailable")
	second := errors.New("archive is unavailable")

	publisher := newFanOutWindows(
		&recordingPublisher{err: first},
		&recordingPublisher{err: second},
	)

	if err := publisher.PublishWindow(context.Background(), sampleWindow(1)); !errors.Is(err, first) {
		t.Fatalf("got %v, want the first failure", err)
	}
}

func TestFanOutWithNoTargetsAcceptsTheWindow(t *testing.T) {
	publisher := newFanOutWindows()

	if err := publisher.PublishWindow(context.Background(), sampleWindow(1)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
