package domain_test

import (
	"errors"
	"testing"
	"time"

	"github.com/ironclaw/mcp-ironclaw/internal/domain"
)

func validEvent() domain.Event {
	return domain.Event{
		DeviceID:     "device-1",
		UserID:       "user-1",
		ProcessID:    42,
		PodID:        "pod-a",
		Kind:         domain.EventKindSyscall,
		ObservedAt:   time.Unix(1700000000, 0),
		LatencyNanos: 1500,
		Bytes:        256,
	}
}

func TestEventValidate(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(*domain.Event)
		wantErr error
	}{
		{"valid", func(*domain.Event) {}, nil},
		{"missing device", func(e *domain.Event) { e.DeviceID = "" }, domain.ErrMissingDeviceID},
		{"missing user", func(e *domain.Event) { e.UserID = "" }, domain.ErrMissingUserID},
		{"invalid kind", func(e *domain.Event) { e.Kind = domain.EventKind(200) }, domain.ErrInvalidKind},
		{"negative latency", func(e *domain.Event) { e.LatencyNanos = -1 }, domain.ErrNegativeLatency},
		{"zero timestamp", func(e *domain.Event) { e.ObservedAt = time.Time{} }, domain.ErrZeroTimestamp},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			event := validEvent()
			tc.mutate(&event)
			err := event.Validate()
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("got %v, want %v", err, tc.wantErr)
			}
		})
	}
}

func TestBatchVerifyIdentityRejectsSpoofedDeviceID(t *testing.T) {
	spoofed := validEvent()
	spoofed.DeviceID = "victim-device"

	batch := domain.Batch{
		DeviceID: "device-1",
		Events:   []domain.Event{validEvent(), spoofed},
	}

	if err := batch.VerifyIdentity("device-1"); !errors.Is(err, domain.ErrDeviceMismatch) {
		t.Fatalf("got %v, want ErrDeviceMismatch", err)
	}
}

func TestBatchVerifyIdentityRejectsMismatchedBatchOwner(t *testing.T) {
	batch := domain.Batch{DeviceID: "device-1", Events: []domain.Event{validEvent()}}

	if err := batch.VerifyIdentity("attacker-device"); !errors.Is(err, domain.ErrDeviceMismatch) {
		t.Fatalf("got %v, want ErrDeviceMismatch", err)
	}
}

func TestBatchVerifyIdentityAcceptsMatchingIdentity(t *testing.T) {
	batch := domain.Batch{DeviceID: "device-1", Events: []domain.Event{validEvent()}}

	if err := batch.VerifyIdentity("device-1"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestBatchValidateRejectsForeignEvent(t *testing.T) {
	foreign := validEvent()
	foreign.DeviceID = "device-2"

	batch := domain.Batch{DeviceID: "device-1", Events: []domain.Event{foreign}}

	if err := batch.Validate(); !errors.Is(err, domain.ErrDeviceMismatch) {
		t.Fatalf("got %v, want ErrDeviceMismatch", err)
	}
}

func TestCorrelationKeyStringIsStableAndShardTagged(t *testing.T) {
	key := validEvent().CorrelationKey()

	if got, want := key.String(), "{user-1}:device-1:42:pod-a"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
	if got, want := key.ShardTag(), "user-1"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
	if got, want := key.DeviceScope(), "{user-1}:device-1"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestCorrelationKeyDistinguishesProcesses(t *testing.T) {
	first := validEvent()
	second := validEvent()
	second.ProcessID = 43

	if first.CorrelationKey() == second.CorrelationKey() {
		t.Fatal("expected distinct correlation keys for distinct process ids")
	}
}
