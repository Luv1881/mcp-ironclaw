package mcpserver

import (
	"context"
	"reflect"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	ServerName    = "ironclaw"
	ServerVersion = "0.1.0"

	ServerInstructions = "Read telemetry from the IronClaw device pipeline. " +
		"Every tool is scoped to one tenant: user_id must be a user the caller's token may read, " +
		"and a token without ironclaw:admin is refused for any other tenant. " +
		"Device identifiers are the ones the pipeline already tracks; call get_user_devices to discover them. " +
		"watch_device blocks until the device changes or its timeout expires, so call it in a loop to follow a device live. " +
		"reset_device_counters is asynchronous: it publishes a command and returns before the reset is applied."
)

var (
	readOnlyHint    = true
	writeHint       = false
	closedWorldHint = false
)

func NewServer(tools *Tools) *mcp.Server {
	server := mcp.NewServer(&mcp.Implementation{Name: ServerName, Version: ServerVersion}, &mcp.ServerOptions{
		Instructions: ServerInstructions,
		Capabilities: &mcp.ServerCapabilities{},
	})
	Register(server, tools)
	return server
}

func Register(server *mcp.Server, tools *Tools) {
	mcp.AddTool(server, &mcp.Tool{
		Name:        "get_device_state",
		Description: "Read the current aggregated counters and latency percentiles for one device belonging to one user.",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: readOnlyHint, OpenWorldHint: &closedWorldHint},
	}, wrap(tools.DeviceState))

	mcp.AddTool(server, &mcp.Tool{
		Name:        "get_user_devices",
		Description: "List the device identifiers currently tracked for a user, up to a limit.",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: readOnlyHint, OpenWorldHint: &closedWorldHint},
	}, wrap(tools.UserDevices))

	mcp.AddTool(server, &mcp.Tool{
		Name:        "get_pipeline_metrics",
		Description: "Read the telemetry pipeline's observability counters across ingest, aggregation and persistence. Requires the ironclaw:admin scope.",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: readOnlyHint, OpenWorldHint: &closedWorldHint},
	}, wrap(tools.PipelineMetrics))

	mcp.AddTool(server, &mcp.Tool{
		Name:        "watch_device",
		Description: "Wait for the next state change on a device and return the updated counters. Blocks until an update arrives or the timeout expires; call it repeatedly to follow a device live.",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: readOnlyHint, OpenWorldHint: &closedWorldHint},
	}, wrap(tools.WatchDevice))

	mcp.AddTool(server, &mcp.Tool{
		Name:        "reset_device_counters",
		Description: "Request a counter reset for a device. The request is published as an asynchronous command event and is not applied inline.",
		Annotations: &mcp.ToolAnnotations{
			ReadOnlyHint:    writeHint,
			DestructiveHint: &readOnlyHint,
			IdempotentHint:  false,
			OpenWorldHint:   &closedWorldHint,
		},
	}, wrap(tools.ResetCounters))
}

func wrap[In, Out any](handler func(context.Context, In) (Out, error)) mcp.ToolHandlerFor[In, Out] {
	return func(ctx context.Context, _ *mcp.CallToolRequest, input In) (*mcp.CallToolResult, Out, error) {
		output, err := handler(ctx, input)
		if err != nil {
			return &mcp.CallToolResult{
				IsError: true,
				Content: []mcp.Content{&mcp.TextContent{Text: err.Error()}},
			}, schemaZero[Out](), nil
		}
		return nil, output, nil
	}
}

func schemaZero[Out any]() Out {
	var zero Out

	fillEmptyCollections(reflect.ValueOf(&zero).Elem(), 0)

	return zero
}

const maxSchemaDepth = 8

func fillEmptyCollections(value reflect.Value, depth int) {
	if depth > maxSchemaDepth || !value.CanSet() {
		return
	}

	switch value.Kind() {
	case reflect.Map:
		if value.IsNil() {
			value.Set(reflect.MakeMap(value.Type()))
		}
	case reflect.Slice:
		if value.IsNil() {
			value.Set(reflect.MakeSlice(value.Type(), 0, 0))
		}
	case reflect.Pointer:
		if value.IsNil() {
			value.Set(reflect.New(value.Type().Elem()))
		}
		fillEmptyCollections(value.Elem(), depth+1)
	case reflect.Struct:
		for i := 0; i < value.NumField(); i++ {
			fillEmptyCollections(value.Field(i), depth+1)
		}
	}
}
