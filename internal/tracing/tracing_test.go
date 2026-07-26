package tracing_test

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/ironclaw/mcp-ironclaw/internal/tracing"
)

func TestNewRequiresAServiceName(t *testing.T) {
	if _, err := tracing.New(context.Background(), tracing.Config{}); !errors.Is(err, tracing.ErrNoServiceName) {
		t.Fatalf("got %v, want ErrNoServiceName", err)
	}
}

func TestNoExporterYieldsADisabledProvider(t *testing.T) {
	provider, err := tracing.New(context.Background(), tracing.Config{ServiceName: "ironclaw"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctx, span := provider.Start(context.Background(), "noop")
	span.End()

	if tracing.TraceIDFrom(ctx) != "" {
		t.Fatal("a disabled provider should not record a trace id")
	}
}

func TestSpansAreExportedAndCarryATraceID(t *testing.T) {
	var sink bytes.Buffer

	provider, err := tracing.New(context.Background(), tracing.Config{
		ServiceName: "ironclaw-test",
		Writer:      &sink,
		SampleRatio: 1,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctx, span := provider.Start(context.Background(), "agent.batch")
	traceID := tracing.TraceIDFrom(ctx)
	span.End()

	if traceID == "" {
		t.Fatal("an enabled provider must record a trace id")
	}
	if err := provider.Shutdown(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sink.String(), "agent.batch") {
		t.Fatalf("span was not exported:\n%s", sink.String())
	}
}

func TestTraceContextSurvivesInjectAndExtract(t *testing.T) {
	provider, err := tracing.New(context.Background(), tracing.Config{
		ServiceName: "ironclaw-test",
		Writer:      &bytes.Buffer{},
		SampleRatio: 1,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer provider.Shutdown(context.Background())

	producerCtx, span := provider.Start(context.Background(), "ingest.publish")
	defer span.End()

	original := tracing.TraceIDFrom(producerCtx)
	if original == "" {
		t.Fatal("expected a trace id on the producer side")
	}

	carrier := tracing.Inject(producerCtx)
	if len(carrier) == 0 {
		t.Fatal("nothing was injected into the carrier")
	}

	consumerCtx := tracing.Extract(context.Background(), carrier)

	if got := tracing.TraceIDFrom(consumerCtx); got != original {
		t.Fatalf("trace id %q did not survive the carrier, want %q", got, original)
	}
}

func TestCarrierKeysAreEnumerable(t *testing.T) {
	carrier := tracing.Carrier{"traceparent": "value"}

	if got := carrier.Get("traceparent"); got != "value" {
		t.Fatalf("Get returned %q", got)
	}
	if keys := carrier.Keys(); len(keys) != 1 || keys[0] != "traceparent" {
		t.Fatalf("Keys returned %v", keys)
	}
}

func TestExtractWithoutTraceContextIsHarmless(t *testing.T) {
	ctx := tracing.Extract(context.Background(), tracing.Carrier{})

	if tracing.TraceIDFrom(ctx) != "" {
		t.Fatal("an empty carrier must not fabricate a trace id")
	}
}
