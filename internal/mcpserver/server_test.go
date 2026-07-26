package mcpserver_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/ironclaw/mcp-ironclaw/internal/mcpserver"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func connect(t *testing.T, tools *mcpserver.Tools) *mcp.ClientSession {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)

	serverTransport, clientTransport := mcp.NewInMemoryTransports()

	server := mcpserver.NewServer(tools)

	serverSession, err := server.Connect(ctx, serverTransport, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	t.Cleanup(func() { serverSession.Close() })

	client := mcp.NewClient(&mcp.Implementation{Name: "test-client", Version: "0.1.0"}, nil)
	session, err := client.Connect(ctx, clientTransport, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	t.Cleanup(func() { session.Close() })

	return session
}

func TestServerAdvertisesEveryTool(t *testing.T) {
	session := connect(t, newTools(t, &recordingCommands{}))

	result, err := session.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	advertised := map[string]bool{}
	for _, tool := range result.Tools {
		advertised[tool.Name] = true
		if tool.Description == "" {
			t.Fatalf("tool %q has no description", tool.Name)
		}
	}

	for _, name := range []string{
		"get_device_state",
		"get_user_devices",
		"get_pipeline_metrics",
		"reset_device_counters",
	} {
		if !advertised[name] {
			t.Fatalf("tool %q was not advertised", name)
		}
	}
}

func TestCallDeviceStateOverTheProtocol(t *testing.T) {
	session := connect(t, newTools(t, &recordingCommands{}))

	result, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      "get_device_state",
		Arguments: map[string]any{"user_id": "user-1", "device_id": "device-1"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.IsError {
		t.Fatalf("tool call reported an error: %+v", result.Content)
	}

	var output mcpserver.DeviceStateOutput
	decodeStructured(t, result, &output)

	if output.Count != 120 {
		t.Fatalf("count %d, want 120", output.Count)
	}
	if output.P99Nanos != 9000 {
		t.Fatalf("p99 %d, want 9000", output.P99Nanos)
	}
}

func TestCallUserDevicesOverTheProtocol(t *testing.T) {
	session := connect(t, newTools(t, &recordingCommands{}))

	result, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      "get_user_devices",
		Arguments: map[string]any{"user_id": "user-1"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.IsError {
		t.Fatalf("tool call reported an error: %+v", result.Content)
	}

	var output mcpserver.UserDevicesOutput
	decodeStructured(t, result, &output)

	if output.Count != 2 {
		t.Fatalf("count %d, want 2", output.Count)
	}
}

func TestUnknownDeviceIsReportedAsToolErrorNotTransportError(t *testing.T) {
	session := connect(t, newTools(t, &recordingCommands{}))

	result, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      "get_device_state",
		Arguments: map[string]any{"user_id": "user-1", "device_id": "absent"},
	})
	if err != nil {
		t.Fatalf("transport returned an error for a normal not-found: %v", err)
	}
	if !result.IsError {
		t.Fatal("expected the tool call to be flagged as an error")
	}
}

func TestResetCountersOverTheProtocolPublishesACommand(t *testing.T) {
	commands := &recordingCommands{}
	session := connect(t, newTools(t, commands))

	result, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name: "reset_device_counters",
		Arguments: map[string]any{
			"user_id":   "user-1",
			"device_id": "device-1",
			"actor":     "operator@example.com",
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.IsError {
		t.Fatalf("tool call reported an error: %+v", result.Content)
	}

	if len(commands.commands) != 1 {
		t.Fatalf("published %d commands, want 1", len(commands.commands))
	}
}

func decodeStructured(t *testing.T, result *mcp.CallToolResult, target any) {
	t.Helper()

	encoded, err := json.Marshal(result.StructuredContent)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := json.Unmarshal(encoded, target); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
