package wire_test

import (
	"strings"
	"testing"
	"time"

	"github.com/ironclaw/mcp-ironclaw/internal/domain"
	"github.com/ironclaw/mcp-ironclaw/internal/wire"
)

func TestEdgeBatchRoundTripsBetweenAgentAndIngest(t *testing.T) {
	original := domain.Batch{
		DeviceID:  "device-000",
		CreatedAt: time.Unix(1700000000, 0).UTC(),
		Events: []domain.Event{
			{
				DeviceID:     "device-000",
				UserID:       "user-000",
				ProcessID:    4242,
				PodID:        "on-prem",
				Kind:         domain.EventKindSyscall,
				ObservedAt:   time.Unix(1700000000, 500).UTC(),
				LatencyNanos: 2_500_000,
				Bytes:        128,
				Failed:       true,
			},
		},
	}

	encoded, err := wire.EncodeEdgeBatch(original)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	decoded, err := wire.DecodeEdgeBatch(encoded)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if decoded.DeviceID != original.DeviceID {
		t.Fatalf("device %q, want %q", decoded.DeviceID, original.DeviceID)
	}
	if len(decoded.Events) != 1 {
		t.Fatalf("decoded %d events, want 1", len(decoded.Events))
	}

	event := decoded.Events[0]
	source := original.Events[0]

	if event.UserID != source.UserID || event.ProcessID != source.ProcessID ||
		event.Kind != uint8(source.Kind) || event.LatencyNanos != source.LatencyNanos ||
		event.Bytes != source.Bytes || event.Failed != source.Failed ||
		event.ObservedAtUnixNanos != source.ObservedAt.UnixNano() {
		t.Fatalf("event changed across the edge encoding: %+v vs %+v", event, source)
	}
}

func TestEdgeBatchDoesNotCarryPodIdentity(t *testing.T) {
	encoded, err := wire.EncodeEdgeBatch(domain.Batch{
		DeviceID: "device-000",
		Events: []domain.Event{{
			DeviceID: "device-000", UserID: "user-000", PodID: "attacker-chosen-pod",
			Kind: domain.EventKindSyscall, ObservedAt: time.Unix(1700000000, 0).UTC(),
		}},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := string(encoded); strings.Contains(got, "pod_id") || strings.Contains(got, "attacker-chosen-pod") {
		t.Fatalf("the edge payload still carries pod identity, which ingest stamps from the downward API and must never read from the body: %s", got)
	}
}
