package domain_test

import (
	"errors"
	"testing"
	"time"

	"github.com/ironclaw/mcp-ironclaw/internal/domain"
)

func validCommand() domain.Command {
	return domain.Command{
		Kind:     domain.CommandResetCounters,
		UserID:   "user-1",
		DeviceID: "device-1",
		Actor:    "operator@example.com",
		IssuedAt: time.Unix(1700000000, 0).UTC(),
	}
}

func TestCommandValidate(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(*domain.Command)
		wantErr error
	}{
		{"valid", func(*domain.Command) {}, nil},
		{"unknown kind", func(c *domain.Command) { c.Kind = domain.CommandUnknown }, domain.ErrInvalidCommand},
		{"out of range kind", func(c *domain.Command) { c.Kind = domain.CommandKind(200) }, domain.ErrInvalidCommand},
		{"missing user", func(c *domain.Command) { c.UserID = "" }, domain.ErrMissingUserID},
		{"missing device", func(c *domain.Command) { c.DeviceID = "" }, domain.ErrMissingDeviceID},
		{"missing actor", func(c *domain.Command) { c.Actor = "" }, domain.ErrMissingActor},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			command := validCommand()
			tc.mutate(&command)

			if err := command.Validate(); !errors.Is(err, tc.wantErr) {
				t.Fatalf("got %v, want %v", err, tc.wantErr)
			}
		})
	}
}

func TestCommandKindNames(t *testing.T) {
	cases := []struct {
		kind  domain.CommandKind
		want  string
		valid bool
	}{
		{domain.CommandResetCounters, "reset_counters", true},
		{domain.CommandQuarantineDevice, "quarantine_device", true},
		{domain.CommandUnknown, "unknown", false},
		{domain.CommandKind(99), "unknown", false},
	}

	for _, tc := range cases {
		t.Run(tc.want, func(t *testing.T) {
			if got := tc.kind.String(); got != tc.want {
				t.Fatalf("name %q, want %q", got, tc.want)
			}
			if got := tc.kind.Valid(); got != tc.valid {
				t.Fatalf("valid %v, want %v", got, tc.valid)
			}
		})
	}
}

func TestCommandShardsByUser(t *testing.T) {
	if got := validCommand().ShardTag(); got != "user-1" {
		t.Fatalf("shard tag %q, want user-1", got)
	}
}

func TestEventKindNames(t *testing.T) {
	cases := []struct {
		kind  domain.EventKind
		want  string
		valid bool
	}{
		{domain.EventKindSyscall, "syscall", true},
		{domain.EventKindNetwork, "network", true},
		{domain.EventKindProcessExec, "process_exec", true},
		{domain.EventKindProcessExit, "process_exit", true},
		{domain.EventKindUnknown, "unknown", true},
		{domain.EventKind(200), "unknown", false},
	}

	for _, tc := range cases {
		t.Run(tc.want, func(t *testing.T) {
			if got := tc.kind.String(); got != tc.want {
				t.Fatalf("name %q, want %q", got, tc.want)
			}
			if got := tc.kind.Valid(); got != tc.valid {
				t.Fatalf("valid %v, want %v", got, tc.valid)
			}
		})
	}
}

func TestAggregateWindowIdentityIncludesWindowKeyAndEmission(t *testing.T) {
	window := domain.AggregateWindow{
		Key:      domain.CorrelationKey{UserID: "user-1", DeviceID: "device-1", ProcessID: 7, PodID: "pod-a"},
		WindowID: 42,
		Sequence: 3,
	}

	if got, want := window.WindowIdentity(), "{user-1}:device-1:7:pod-a#42"; got != want {
		t.Fatalf("window identity %q, want %q", got, want)
	}
	if got, want := window.Identity(), "{user-1}:device-1:7:pod-a#42#3"; got != want {
		t.Fatalf("identity %q, want %q", got, want)
	}

	otherPeriod := window
	otherPeriod.WindowID = 43
	if window.Identity() == otherPeriod.Identity() {
		t.Fatal("windows from different periods share an identity")
	}

	laterEmission := window
	laterEmission.Sequence = 4
	if window.Identity() == laterEmission.Identity() {
		t.Fatal("two emissions of one window share a dedupe identity, so late data would be discarded")
	}
	if window.WindowIdentity() != laterEmission.WindowIdentity() {
		t.Fatal("two emissions of one window should still name the same window")
	}
}

func TestBatchLenCountsEvents(t *testing.T) {
	batch := domain.Batch{DeviceID: "device-1", Events: []domain.Event{validEvent(), validEvent()}}

	if got := batch.Len(); got != 2 {
		t.Fatalf("len %d, want 2", got)
	}
	if got := (domain.Batch{}).Len(); got != 0 {
		t.Fatalf("empty batch len %d, want 0", got)
	}
}

func TestEmptyBatchValidationRequiresDeviceID(t *testing.T) {
	if err := (domain.Batch{}).Validate(); !errors.Is(err, domain.ErrMissingDeviceID) {
		t.Fatalf("got %v, want ErrMissingDeviceID", err)
	}
	if err := (domain.Batch{DeviceID: "device-1"}).Validate(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
