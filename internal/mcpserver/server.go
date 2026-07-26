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

	value := reflect.ValueOf(&zero).Elem()
	if value.Kind() != reflect.Struct {
		return zero
	}

	for i := 0; i < value.NumField(); i++ {
		field := value.Field(i)
		if !field.CanSet() {
			continue
		}
		switch field.Kind() {
		case reflect.Map:
			if field.IsNil() {
				field.Set(reflect.MakeMap(field.Type()))
			}
		case reflect.Slice:
			if field.IsNil() {
				field.Set(reflect.MakeSlice(field.Type(), 0, 0))
			}
		}
	}

	return zero
}
