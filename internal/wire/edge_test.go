package wire_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/ironclaw/mcp-ironclaw/internal/domain"
	"github.com/ironclaw/mcp-ironclaw/internal/wire"
)

func agentEncodedBatch() domain.Batch {
	return domain.Batch{
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
}

func TestEdgeBatchPublishesTheFieldNamesIngestReads(t *testing.T) {
	encoded, err := wire.EncodeEdgeBatch(agentEncodedBatch())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var decoded wire.EdgeBatch
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("the agent's encoding is not decodable as the shared contract: %v", err)
	}

	if decoded.DeviceID != "device-000" {
		t.Fatalf("device %q, want device-000", decoded.DeviceID)
	}
	if len(decoded.Events) != 1 {
		t.Fatalf("decoded %d events, want 1", len(decoded.Events))
	}

	event := decoded.Events[0]
	source := agentEncodedBatch().Events[0]

	if event.UserID != source.UserID ||
		event.ProcessID != source.ProcessID ||
		event.Kind != uint8(source.Kind) ||
		event.LatencyNanos != source.LatencyNanos ||
		event.Bytes != source.Bytes ||
		event.Failed != source.Failed ||
		event.ObservedAtUnixNanos != source.ObservedAt.UnixNano() {
		t.Fatalf("event changed across the edge encoding: %+v vs %+v", event, source)
	}
}

func TestEdgeBatchUsesTheDocumentedWireFieldNames(t *testing.T) {
	encoded, err := wire.EncodeEdgeBatch(agentEncodedBatch())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var fields map[string]any
	if err := json.Unmarshal(encoded, &fields); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for _, name := range []string{"device_id", "events"} {
		if _, ok := fields[name]; !ok {
			t.Fatalf("edge payload is missing the %q field: %s", name, encoded)
		}
	}

	events, ok := fields["events"].([]any)
	if !ok || len(events) != 1 {
		t.Fatalf("events field is %T with %d entries, want one object", fields["events"], len(events))
	}

	event, ok := events[0].(map[string]any)
	if !ok {
		t.Fatalf("event is %T, want an object", events[0])
	}

	for _, name := range []string{
		"user_id", "process_id", "kind", "observed_at_unix_nanos",
		"latency_nanos", "bytes", "failed",
	} {
		if _, ok := event[name]; !ok {
			t.Fatalf("edge event is missing the %q field: %s", name, encoded)
		}
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
