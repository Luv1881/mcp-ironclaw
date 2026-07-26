package mcpserver_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ironclaw/mcp-ironclaw/internal/domain"
	"github.com/ironclaw/mcp-ironclaw/internal/mcpserver"
	"github.com/ironclaw/mcp-ironclaw/internal/store"
)

type fixedClock struct{ at time.Time }

func (c fixedClock) Now() time.Time { return c.at }

type recordingCommands struct {
	commands []domain.Command
	err      error
}

func (r *recordingCommands) PublishCommand(_ context.Context, command domain.Command) error {
	if r.err != nil {
		return r.err
	}
	r.commands = append(r.commands, command)
	return nil
}

func seededStore(t *testing.T) *store.Memory {
	t.Helper()

	memory := store.NewMemory()
	ctx := context.Background()

	windows := []domain.AggregateWindow{
		{
			Key:        domain.CorrelationKey{UserID: "user-1", DeviceID: "device-1", ProcessID: 1},
			WindowID:   3,
			WindowEnd:  time.Unix(1700000030, 0).UTC(),
			Count:      120,
			ErrorCount: 4,
			Bytes:      9000,
			P95Nanos:   5000,
			P99Nanos:   9000,
		},
		{
			Key:       domain.CorrelationKey{UserID: "user-1", DeviceID: "device-2", ProcessID: 1},
			WindowID:  3,
			WindowEnd: time.Unix(1700000030, 0).UTC(),
			Count:     7,
		},
	}

	for _, window := range windows {
		if err := memory.ApplyWindow(ctx, window); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	return memory
}

func newTools(t *testing.T, commands domain.CommandPublisher) *mcpserver.Tools {
	t.Helper()

	memory := seededStore(t)

	tools, err := mcpserver.NewTools(mcpserver.Options{
		State:    memory,
		Devices:  memory,
		Metrics:  memory,
		Commands: commands,
		Clock:    fixedClock{at: time.Unix(1700000100, 0).UTC()},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	return tools
}

func TestNewToolsRequiresStateReader(t *testing.T) {
	if _, err := mcpserver.NewTools(mcpserver.Options{}); !errors.Is(err, mcpserver.ErrNilStateReader) {
		t.Fatalf("got %v, want ErrNilStateReader", err)
	}
}

func TestDeviceStateReturnsAggregatedCounters(t *testing.T) {
	tools := newTools(t, nil)

	output, err := tools.DeviceState(context.Background(), mcpserver.DeviceStateInput{
		UserID:   "user-1",
		DeviceID: "device-1",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if output.Count != 120 || output.ErrorCount != 4 {
		t.Fatalf("count %d errors %d, want 120 and 4", output.Count, output.ErrorCount)
	}
	if output.P95Nanos != 5000 || output.P99Nanos != 9000 {
		t.Fatalf("p95 %d p99 %d, want 5000 and 9000", output.P95Nanos, output.P99Nanos)
	}
	if output.LastSeen != "2023-11-14T22:13:50Z" {
		t.Fatalf("last seen %q, want an RFC3339 timestamp", output.LastSeen)
	}
}

func TestDeviceStateValidatesInput(t *testing.T) {
	tools := newTools(t, nil)
	ctx := context.Background()

	if _, err := tools.DeviceState(ctx, mcpserver.DeviceStateInput{DeviceID: "device-1"}); !errors.Is(err, mcpserver.ErrMissingUserID) {
		t.Fatalf("got %v, want ErrMissingUserID", err)
	}
	if _, err := tools.DeviceState(ctx, mcpserver.DeviceStateInput{UserID: "user-1"}); !errors.Is(err, mcpserver.ErrMissingDevice) {
		t.Fatalf("got %v, want ErrMissingDevice", err)
	}
}

func TestDeviceStateSurfacesUnknownDevice(t *testing.T) {
	tools := newTools(t, nil)

	_, err := tools.DeviceState(context.Background(), mcpserver.DeviceStateInput{
		UserID:   "user-1",
		DeviceID: "absent",
	})
	if !errors.Is(err, store.ErrDeviceNotFound) {
		t.Fatalf("got %v, want ErrDeviceNotFound", err)
	}
}

func TestDeviceStateIsScopedToTheRequestingUser(t *testing.T) {
	tools := newTools(t, nil)

	_, err := tools.DeviceState(context.Background(), mcpserver.DeviceStateInput{
		UserID:   "user-2",
		DeviceID: "device-1",
	})
	if !errors.Is(err, store.ErrDeviceNotFound) {
		t.Fatalf("another user could read device-1: %v", err)
	}
}

func TestUserDevicesListsOnlyThatUsersDevices(t *testing.T) {
	tools := newTools(t, nil)

	output, err := tools.UserDevices(context.Background(), mcpserver.UserDevicesInput{UserID: "user-1"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if output.Count != 2 {
		t.Fatalf("count %d, want 2", output.Count)
	}
	if output.Devices[0] != "device-1" || output.Devices[1] != "device-2" {
		t.Fatalf("devices %v, want sorted device-1 and device-2", output.Devices)
	}

	if _, err := tools.UserDevices(context.Background(), mcpserver.UserDevicesInput{}); !errors.Is(err, mcpserver.ErrMissingUserID) {
		t.Fatalf("got %v, want ErrMissingUserID", err)
	}
}

func TestPipelineMetricsExposesCounters(t *testing.T) {
	tools := newTools(t, nil)

	output, err := tools.PipelineMetrics(context.Background(), mcpserver.PipelineMetricsInput{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if output.Metrics[store.MetricWindowsApplied] != 2 {
		t.Fatalf("windows applied %d, want 2", output.Metrics[store.MetricWindowsApplied])
	}
	if output.Metrics[store.MetricEventsCounted] != 127 {
		t.Fatalf("events counted %d, want 127", output.Metrics[store.MetricEventsCounted])
	}
}

func TestResetCountersPublishesCommandInsteadOfWritingState(t *testing.T) {
	commands := &recordingCommands{}
	tools := newTools(t, commands)

	output, err := tools.ResetCounters(context.Background(), mcpserver.ResetCountersInput{
		UserID:   "user-1",
		DeviceID: "device-1",
		Actor:    "operator@example.com",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !output.Accepted || output.Command != "reset_counters" {
		t.Fatalf("output %+v, want an accepted reset_counters command", output)
	}
	if len(commands.commands) != 1 {
		t.Fatalf("published %d commands, want 1", len(commands.commands))
	}

	command := commands.commands[0]
	if command.Kind != domain.CommandResetCounters {
		t.Fatalf("command kind %v, want reset counters", command.Kind)
	}
	if command.Actor != "operator@example.com" {
		t.Fatalf("actor %q was not recorded on the command", command.Actor)
	}
	if !command.IssuedAt.Equal(time.Unix(1700000100, 0).UTC()) {
		t.Fatalf("issued at %v, want the injected clock time", command.IssuedAt)
	}
}

func TestResetCountersRequiresAnAuditableActor(t *testing.T) {
	commands := &recordingCommands{}
	tools := newTools(t, commands)
	ctx := context.Background()

	cases := []struct {
		name    string
		input   mcpserver.ResetCountersInput
		wantErr error
	}{
		{"missing user", mcpserver.ResetCountersInput{DeviceID: "device-1", Actor: "op"}, mcpserver.ErrMissingUserID},
		{"missing device", mcpserver.ResetCountersInput{UserID: "user-1", Actor: "op"}, mcpserver.ErrMissingDevice},
		{"missing actor", mcpserver.ResetCountersInput{UserID: "user-1", DeviceID: "device-1"}, mcpserver.ErrMissingActor},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := tools.ResetCounters(ctx, tc.input); !errors.Is(err, tc.wantErr) {
				t.Fatalf("got %v, want %v", err, tc.wantErr)
			}
		})
	}

	if len(commands.commands) != 0 {
		t.Fatalf("published %d commands from invalid input, want 0", len(commands.commands))
	}
}

func TestResetCountersIsDisabledWithoutACommandPublisher(t *testing.T) {
	tools := newTools(t, nil)

	_, err := tools.ResetCounters(context.Background(), mcpserver.ResetCountersInput{
		UserID:   "user-1",
		DeviceID: "device-1",
		Actor:    "operator",
	})
	if !errors.Is(err, mcpserver.ErrWritesDisabled) {
		t.Fatalf("got %v, want ErrWritesDisabled", err)
	}
}

func TestResetCountersPropagatesPublisherFailure(t *testing.T) {
	sentinel := errors.New("command bus unavailable")
	tools := newTools(t, &recordingCommands{err: sentinel})

	_, err := tools.ResetCounters(context.Background(), mcpserver.ResetCountersInput{
		UserID:   "user-1",
		DeviceID: "device-1",
		Actor:    "operator",
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("got %v, want the publisher error", err)
	}
}
