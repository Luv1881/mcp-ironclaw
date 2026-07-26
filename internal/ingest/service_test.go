package ingest_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ironclaw/mcp-ironclaw/internal/domain"
	"github.com/ironclaw/mcp-ironclaw/internal/ingest"
	"github.com/ironclaw/mcp-ironclaw/internal/store"
)

type capturingPublisher struct {
	keys    []string
	batches []domain.Batch
	err     error
}

func (p *capturingPublisher) Publish(_ context.Context, key string, batch domain.Batch) error {
	if p.err != nil {
		return p.err
	}
	p.keys = append(p.keys, key)
	p.batches = append(p.batches, batch)
	return nil
}

func validBatch(deviceID string, count int) domain.Batch {
	events := make([]domain.Event, 0, count)
	for i := 0; i < count; i++ {
		events = append(events, domain.Event{
			DeviceID:     deviceID,
			UserID:       "user-1",
			ProcessID:    int32(i),
			PodID:        "pod-a",
			Kind:         domain.EventKindSyscall,
			ObservedAt:   time.Unix(1700000000, 0),
			LatencyNanos: 1000,
			Bytes:        64,
		})
	}
	return domain.Batch{DeviceID: deviceID, Events: events}
}

func TestNewRejectsNilPublisher(t *testing.T) {
	if _, err := ingest.New(nil, nil); !errors.Is(err, ingest.ErrNilPublisher) {
		t.Fatalf("got %v, want ErrNilPublisher", err)
	}
}

func TestAcceptPublishesKeyedByDeviceID(t *testing.T) {
	publisher := &capturingPublisher{}
	metrics := store.NewMemory()

	service, err := ingest.New(publisher, metrics)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := service.Accept(context.Background(), "device-1", validBatch("device-1", 3)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(publisher.keys) != 1 || publisher.keys[0] != "device-1" {
		t.Fatalf("partition keys %v, want [device-1]", publisher.keys)
	}

	recorded, err := metrics.PipelineMetrics(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if recorded[ingest.MetricBatchesAccepted] != 1 {
		t.Fatalf("accepted %d batches, want 1", recorded[ingest.MetricBatchesAccepted])
	}
	if recorded[ingest.MetricEventsAccepted] != 3 {
		t.Fatalf("accepted %d events, want 3", recorded[ingest.MetricEventsAccepted])
	}
}

func TestAcceptRejectsIdentityAndValidationFailures(t *testing.T) {
	invalid := validBatch("device-1", 1)
	invalid.Events[0].LatencyNanos = -1

	cases := []struct {
		name     string
		identity string
		batch    domain.Batch
		wantErr  error
	}{
		{"empty identity", "", validBatch("device-1", 1), ingest.ErrMissingIdentity},
		{"spoofed device", "attacker", validBatch("device-1", 1), domain.ErrDeviceMismatch},
		{"invalid event", "device-1", invalid, domain.ErrNegativeLatency},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			publisher := &capturingPublisher{}
			service, err := ingest.New(publisher, nil)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if err := service.Accept(context.Background(), tc.identity, tc.batch); !errors.Is(err, tc.wantErr) {
				t.Fatalf("got %v, want %v", err, tc.wantErr)
			}
			if len(publisher.batches) != 0 {
				t.Fatal("a rejected batch was published downstream")
			}
		})
	}
}

func TestAcceptReportsPublisherFailure(t *testing.T) {
	sentinel := errors.New("broker unavailable")
	metrics := store.NewMemory()

	service, err := ingest.New(&capturingPublisher{err: sentinel}, metrics)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := service.Accept(context.Background(), "device-1", validBatch("device-1", 1)); !errors.Is(err, sentinel) {
		t.Fatalf("got %v, want the publisher error", err)
	}

	recorded, err := metrics.PipelineMetrics(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if recorded[ingest.MetricBatchesAccepted] != 0 {
		t.Fatalf("accepted %d batches despite publisher failure, want 0", recorded[ingest.MetricBatchesAccepted])
	}
	if recorded[ingest.MetricBatchesRejected] != 1 {
		t.Fatalf("rejected %d batches, want 1", recorded[ingest.MetricBatchesRejected])
	}
}

func TestAcceptWorksWithoutMetricsRecorder(t *testing.T) {
	service, err := ingest.New(&capturingPublisher{}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := service.Accept(context.Background(), "device-1", validBatch("device-1", 1)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
