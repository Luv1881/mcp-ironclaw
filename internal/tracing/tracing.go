package tracing

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.41.0"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

var ErrNoServiceName = errors.New("tracing: service name is required")

const ScopeName = "github.com/ironclaw/mcp-ironclaw"

type Config struct {
	ServiceName string
	Endpoint    string
	SampleRatio float64
	Writer      io.Writer
}

type Provider struct {
	tracer   trace.Tracer
	shutdown func(context.Context) error
}

func Disabled() *Provider {
	return &Provider{
		tracer:   noop.NewTracerProvider().Tracer(ScopeName),
		shutdown: func(context.Context) error { return nil },
	}
}

func New(ctx context.Context, config Config) (*Provider, error) {
	if config.ServiceName == "" {
		return nil, ErrNoServiceName
	}
	if config.Endpoint == "" && config.Writer == nil {
		return Disabled(), nil
	}

	exporter, err := buildExporter(ctx, config)
	if err != nil {
		return nil, err
	}

	attributes, err := resource.Merge(resource.Default(), resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceName(config.ServiceName),
	))
	if err != nil {
		return nil, fmt.Errorf("tracing: building resource: %w", err)
	}

	ratio := config.SampleRatio
	if ratio <= 0 {
		ratio = 0.01
	}

	provider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter, sdktrace.WithBatchTimeout(2*time.Second)),
		sdktrace.WithResource(attributes),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(ratio))),
	)

	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return &Provider{
		tracer:   provider.Tracer(ScopeName),
		shutdown: provider.Shutdown,
	}, nil
}

const StdoutEndpoint = "stdout"

func buildExporter(ctx context.Context, config Config) (sdktrace.SpanExporter, error) {
	if config.Writer == nil && config.Endpoint == StdoutEndpoint {
		config.Writer = io.Discard
	}
	if config.Writer != nil {
		exporter, err := stdouttrace.New(stdouttrace.WithWriter(config.Writer))
		if err != nil {
			return nil, fmt.Errorf("tracing: building stdout exporter: %w", err)
		}
		return exporter, nil
	}

	exporter, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(config.Endpoint),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		return nil, fmt.Errorf("tracing: building otlp exporter: %w", err)
	}

	return exporter, nil
}

func (p *Provider) Tracer() trace.Tracer { return p.tracer }

func (p *Provider) Shutdown(ctx context.Context) error { return p.shutdown(ctx) }

func (p *Provider) Start(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	return p.tracer.Start(ctx, name, trace.WithAttributes(attrs...))
}

type Carrier map[string]string

func (c Carrier) Get(key string) string { return c[key] }

func (c Carrier) Set(key, value string) { c[key] = value }

func (c Carrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for key := range c {
		keys = append(keys, key)
	}
	return keys
}

func Inject(ctx context.Context) Carrier {
	carrier := Carrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	return carrier
}

func Extract(ctx context.Context, carrier Carrier) context.Context {
	return otel.GetTextMapPropagator().Extract(ctx, carrier)
}

func TraceIDFrom(ctx context.Context) string {
	span := trace.SpanContextFromContext(ctx)
	if !span.HasTraceID() {
		return ""
	}
	return span.TraceID().String()
}
