package app_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ironclaw/mcp-ironclaw/internal/app"
	"github.com/ironclaw/mcp-ironclaw/internal/commands"
	"github.com/ironclaw/mcp-ironclaw/internal/domain"
	"github.com/ironclaw/mcp-ironclaw/internal/ingest"
	"github.com/ironclaw/mcp-ironclaw/internal/mcpserver"
	"github.com/ironclaw/mcp-ironclaw/internal/store"
)

func testConfig() app.Config {
	config := app.DefaultConfig()
	config.Devices = 4
	config.EventsPerDevice = 500
	config.EventInterval = 0
	config.BatchSize = 50
	config.BatchInterval = 10 * time.Millisecond
	config.EmitInterval = 20 * time.Millisecond
	config.WindowSize = 50 * time.Millisecond
	config.Partitions = 4
	return config
}

func TestNewRejectsInvalidDeviceCount(t *testing.T) {
	config := testConfig()
	config.Devices = 0

	if _, err := app.New(config); !errors.Is(err, app.ErrInvalidDeviceCount) {
		t.Fatalf("got %v, want ErrInvalidDeviceCount", err)
	}
}

func TestRuntimeFeedsReadableStateThroughEveryStage(t *testing.T) {
	runtime, err := app.New(testConfig())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := runtime.Start(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	state := runtime.State()

	waitFor(t, func() bool {
		metrics, err := state.PipelineMetrics(ctx)
		if err != nil {
			return false
		}
		return metrics[ingest.MetricEventsAccepted] == int64(testConfig().Devices*testConfig().EventsPerDevice)
	})

	cancel()
	runtime.Stop()

	metrics, err := state.PipelineMetrics(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := int64(testConfig().Devices * testConfig().EventsPerDevice)
	if metrics[ingest.MetricEventsAccepted] != expected {
		t.Fatalf("ingested %d events, want %d", metrics[ingest.MetricEventsAccepted], expected)
	}
	if metrics[ingest.MetricBatchesRejected] != 0 {
		t.Fatalf("%d batches were rejected, want 0", metrics[ingest.MetricBatchesRejected])
	}

	devices, err := state.UserDevices(context.Background(), "user-000")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(devices) == 0 {
		t.Fatal("no devices were registered for user-000")
	}
}

func TestRuntimeAppliesResetCommandAsynchronously(t *testing.T) {
	runtime, err := app.New(testConfig())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := runtime.Start(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	state := runtime.State()

	waitFor(t, func() bool {
		deviceState, err := state.DeviceState(ctx, "user-000", "device-000")
		return err == nil && deviceState.Count > 0
	})

	tools, err := mcpserver.NewTools(mcpserver.Options{
		State:    state,
		Devices:  state,
		Metrics:  state,
		Commands: runtime.Commands(),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	output, err := tools.ResetCounters(ctx, mcpserver.ResetCountersInput{
		UserID:   "user-000",
		DeviceID: "device-000",
		Actor:    "operator",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !output.Accepted {
		t.Fatal("reset command was not accepted")
	}

	waitFor(t, func() bool {
		metrics, err := state.PipelineMetrics(ctx)
		if err != nil {
			return false
		}
		return metrics[commands.MetricCommandsApplied] >= 1 && metrics[store.MetricDeviceResets] >= 1
	})

	cancel()
	runtime.Stop()
}

func TestRuntimeCommandsRejectInvalidPayloads(t *testing.T) {
	runtime, err := app.New(testConfig())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	err = runtime.Commands().PublishCommand(context.Background(), domain.Command{
		Kind:     domain.CommandResetCounters,
		DeviceID: "device-000",
		Actor:    "operator",
	})
	if !errors.Is(err, domain.ErrMissingUserID) {
		t.Fatalf("got %v, want ErrMissingUserID", err)
	}
}

func waitFor(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("condition not met before deadline")
}
