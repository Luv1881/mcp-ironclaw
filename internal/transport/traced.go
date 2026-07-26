package transport

import (
	"context"

	"github.com/ironclaw/mcp-ironclaw/internal/domain"
	"github.com/ironclaw/mcp-ironclaw/internal/tracing"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

var _ domain.Transport = (*Traced)(nil)

type Traced struct {
	inner    domain.Transport
	provider *tracing.Provider
}

func NewTraced(inner domain.Transport, provider *tracing.Provider) *Traced {
	if provider == nil {
		provider = tracing.Disabled()
	}
	return &Traced{inner: inner, provider: provider}
}

func (t *Traced) Send(ctx context.Context, batch domain.Batch) error {
	ctx, span := t.provider.Start(ctx, "agent.send",
		attribute.String("ironclaw.device_id", batch.DeviceID),
		attribute.Int("ironclaw.batch_events", batch.Len()),
	)
	defer span.End()

	err := t.inner.Send(ctx, batch)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		span.RecordError(err)
	}

	return err
}
