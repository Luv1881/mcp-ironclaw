package mcpserver_test

import (
	"context"
	"strings"
	"testing"

	"github.com/ironclaw/mcp-ironclaw/internal/mcpserver"
	"github.com/ironclaw/mcp-ironclaw/internal/store"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func toolsRequiringAuth(t *testing.T) *mcpserver.Tools {
	t.Helper()

	memory := store.NewMemory()

	tools, err := mcpserver.NewTools(mcpserver.Options{
		State:       memory,
		Devices:     memory,
		Metrics:     memory,
		Watcher:     memory,
		RequireAuth: true,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	return tools
}

func callExpectingToolError(t *testing.T, session *mcp.ClientSession, name string, arguments map[string]any) string {
	t.Helper()

	result, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      name,
		Arguments: arguments,
	})
	if err != nil {
		t.Fatalf("%s returned a protocol-level error instead of a tool error result: %v", name, err)
	}
	if !result.IsError {
		t.Fatalf("%s was expected to be refused but succeeded", name)
	}

	var text strings.Builder
	for _, content := range result.Content {
		if textual, ok := content.(*mcp.TextContent); ok {
			text.WriteString(textual.Text)
		}
	}
	return text.String()
}

func TestRefusedToolsReturnAnErrorResultNotASchemaViolation(t *testing.T) {
	session := connect(t, toolsRequiringAuth(t))

	cases := []struct {
		tool      string
		arguments map[string]any
		wantText  string
	}{
		{
			tool:      "get_pipeline_metrics",
			arguments: map[string]any{},
			wantText:  "authenticated",
		},
		{
			tool:      "get_user_devices",
			arguments: map[string]any{"user_id": "user-000"},
			wantText:  "authenticated",
		},
		{
			tool:      "get_device_state",
			arguments: map[string]any{"user_id": "user-000", "device_id": "device-000"},
			wantText:  "authenticated",
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.tool, func(t *testing.T) {
			text := callExpectingToolError(t, session, testCase.tool, testCase.arguments)
			if !strings.Contains(text, testCase.wantText) {
				t.Fatalf("refusal text %q does not mention %q", text, testCase.wantText)
			}
			if strings.Contains(text, "validating") || strings.Contains(text, "schema") {
				t.Fatalf("the refusal surfaced as a schema violation instead of a reason: %q", text)
			}
		})
	}
}

func TestAMapValuedOutputSurvivesAnErrorResult(t *testing.T) {
	memory := store.NewMemory()

	tools, err := mcpserver.NewTools(mcpserver.Options{State: memory, Devices: memory})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	session := connect(t, tools)

	text := callExpectingToolError(t, session, "get_pipeline_metrics", map[string]any{})
	if !strings.Contains(text, "metrics reader") {
		t.Fatalf("expected the absent-dependency reason, got %q", text)
	}
}

func TestASliceValuedOutputSurvivesAnErrorResult(t *testing.T) {
	memory := store.NewMemory()

	tools, err := mcpserver.NewTools(mcpserver.Options{State: memory, Metrics: memory})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	session := connect(t, tools)

	text := callExpectingToolError(t, session, "get_user_devices", map[string]any{"user_id": "user-000"})
	if !strings.Contains(text, "device lister") {
		t.Fatalf("expected the absent-dependency reason, got %q", text)
	}
}
