package mcpserver

import (
	"context"
	"reflect"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	ServerName    = "ironclaw"
	ServerVersion = "0.1.0"
)

func NewServer(tools *Tools) *mcp.Server {
	server := mcp.NewServer(&mcp.Implementation{Name: ServerName, Version: ServerVersion}, nil)
	Register(server, tools)
	return server
}

func Register(server *mcp.Server, tools *Tools) {
	mcp.AddTool(server, &mcp.Tool{
		Name:        "get_device_state",
		Description: "Read the current aggregated counters and latency percentiles for one device belonging to one user.",
	}, wrap(tools.DeviceState))

	mcp.AddTool(server, &mcp.Tool{
		Name:        "get_user_devices",
		Description: "List every device identifier currently tracked for a user.",
	}, wrap(tools.UserDevices))

	mcp.AddTool(server, &mcp.Tool{
		Name:        "get_pipeline_metrics",
		Description: "Read the telemetry pipeline's observability counters across ingest, aggregation and persistence.",
	}, wrap(tools.PipelineMetrics))

	mcp.AddTool(server, &mcp.Tool{
		Name:        "watch_device",
		Description: "Wait for the next state change on a device and return the updated counters. Blocks until an update arrives or the timeout expires; call it repeatedly to follow a device live.",
	}, wrap(tools.WatchDevice))

	mcp.AddTool(server, &mcp.Tool{
		Name:        "reset_device_counters",
		Description: "Request a counter reset for a device. The request is published as an asynchronous command event and is not applied inline.",
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
