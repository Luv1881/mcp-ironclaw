package commands_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ironclaw/mcp-ironclaw/internal/commands"
	"github.com/ironclaw/mcp-ironclaw/internal/domain"
	"github.com/ironclaw/mcp-ironclaw/internal/store"
)

type failingResetter struct{ err error }

func (r failingResetter) ResetDevice(context.Context, string, string) error { return r.err }

func resetCommand() domain.Command {
	return domain.Command{
		Kind:     domain.CommandResetCounters,
		UserID:   "user-1",
		DeviceID: "device-1",
		Actor:    "operator",
		IssuedAt: time.Unix(1700000000, 0).UTC(),
	}
}

func seeded(t *testing.T) *store.Memory {
	t.Helper()

	memory := store.NewMemory()
	err := memory.ApplyWindow(context.Background(), domain.AggregateWindow{
		Key:        domain.CorrelationKey{UserID: "user-1", DeviceID: "device-1", ProcessID: 1},
		WindowID:   1,
		Count:      50,
		ErrorCount: 5,
		Bytes:      900,
		P95Nanos:   1000,
		P99Nanos:   2000,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	return memory
}

func TestNewRejectsNilResetter(t *testing.T) {
	if _, err := commands.New(nil, nil); !errors.Is(err, commands.ErrNilResetter) {
		t.Fatalf("got %v, want ErrNilResetter", err)
	}
}

func TestResetCommandClearsCountersAndCounts(t *testing.T) {
	memory := seeded(t)

	service, err := commands.New(memory, memory)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctx := context.Background()
	if err := service.HandleCommand(ctx, resetCommand()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	state, err := memory.DeviceState(ctx, "user-1", "device-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if state.Count != 0 || state.ErrorCount != 0 || state.Bytes != 0 {
		t.Fatalf("counters were not cleared: %+v", state)
	}

	metrics, err := memory.PipelineMetrics(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if metrics[commands.MetricCommandsApplied] != 1 {
		t.Fatalf("applied %d commands, want 1", metrics[commands.MetricCommandsApplied])
	}
	if metrics[store.MetricDeviceResets] != 1 {
		t.Fatalf("recorded %d device resets, want 1", metrics[store.MetricDeviceResets])
	}
}

func TestInvalidCommandIsRejectedWithoutSideEffects(t *testing.T) {
	memory := seeded(t)

	service, err := commands.New(memory, memory)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctx := context.Background()

	cases := []struct {
		name    string
		mutate  func(*domain.Command)
		wantErr error
	}{
		{"unknown kind", func(c *domain.Command) { c.Kind = domain.CommandUnknown }, domain.ErrInvalidCommand},
		{"missing user", func(c *domain.Command) { c.UserID = "" }, domain.ErrMissingUserID},
		{"missing device", func(c *domain.Command) { c.DeviceID = "" }, domain.ErrMissingDeviceID},
		{"missing actor", func(c *domain.Command) { c.Actor = "" }, domain.ErrMissingActor},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			command := resetCommand()
			tc.mutate(&command)

			if err := service.HandleCommand(ctx, command); !errors.Is(err, tc.wantErr) {
				t.Fatalf("got %v, want %v", err, tc.wantErr)
			}
		})
	}

	state, err := memory.DeviceState(ctx, "user-1", "device-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if state.Count != 50 {
		t.Fatalf("counters changed to %d despite rejected commands, want 50", state.Count)
	}

	metrics, err := memory.PipelineMetrics(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if metrics[commands.MetricCommandsRejected] != 4 {
		t.Fatalf("rejected %d commands, want 4", metrics[commands.MetricCommandsRejected])
	}
}

func TestUnsupportedKindIsReported(t *testing.T) {
	memory := seeded(t)

	service, err := commands.New(memory, memory)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	command := resetCommand()
	command.Kind = domain.CommandQuarantineDevice

	if err := service.HandleCommand(context.Background(), command); !errors.Is(err, commands.ErrUnsupportedKind) {
		t.Fatalf("got %v, want ErrUnsupportedKind", err)
	}
}

func TestResetterFailureIsPropagated(t *testing.T) {
	sentinel := errors.New("store unavailable")
	metrics := store.NewMemory()

	service, err := commands.New(failingResetter{err: sentinel}, metrics)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := service.HandleCommand(context.Background(), resetCommand()); !errors.Is(err, sentinel) {
		t.Fatalf("got %v, want the resetter error", err)
	}

	recorded, err := metrics.PipelineMetrics(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if recorded[commands.MetricCommandsApplied] != 0 {
		t.Fatalf("counted %d applied commands after a failure, want 0", recorded[commands.MetricCommandsApplied])
	}
}

func TestResetOfUnknownDeviceIsReported(t *testing.T) {
	memory := store.NewMemory()

	service, err := commands.New(memory, memory)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := service.HandleCommand(context.Background(), resetCommand()); !errors.Is(err, store.ErrDeviceNotFound) {
		t.Fatalf("got %v, want ErrDeviceNotFound", err)
	}
}
