package mcpserver_test

import (
	"context"
	"errors"
	"testing"

	"github.com/ironclaw/mcp-ironclaw/internal/mcpserver"
)

func TestOptionalCapabilitiesReportTheirOwnAbsence(t *testing.T) {
	memory := seededStore(t)

	tools, err := mcpserver.NewTools(mcpserver.Options{State: memory})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctx := context.Background()

	if _, err := tools.UserDevices(ctx, mcpserver.UserDevicesInput{UserID: "user-1"}); !errors.Is(err, mcpserver.ErrNilDeviceLister) {
		t.Fatalf("got %v, want ErrNilDeviceLister", err)
	}
	if _, err := tools.PipelineMetrics(ctx, mcpserver.PipelineMetricsInput{}); !errors.Is(err, mcpserver.ErrNilMetricsReader) {
		t.Fatalf("got %v, want ErrNilMetricsReader", err)
	}
	if _, err := tools.ResetCounters(ctx, mcpserver.ResetCountersInput{
		UserID:   "user-1",
		DeviceID: "device-1",
		Actor:    "operator",
	}); !errors.Is(err, mcpserver.ErrWritesDisabled) {
		t.Fatalf("got %v, want ErrWritesDisabled", err)
	}

	if _, err := tools.DeviceState(ctx, mcpserver.DeviceStateInput{UserID: "user-1", DeviceID: "device-1"}); err != nil {
		t.Fatalf("the mandatory capability should still work: %v", err)
	}
}
