package transport

import (
	"context"
	"errors"

	"github.com/ironclaw/mcp-ironclaw/internal/domain"
)

var (
	ErrNilService      = errors.New("transport: batch acceptor is nil")
	ErrMissingIdentity = errors.New("transport: client identity is empty")
)

var _ domain.Transport = (*Loopback)(nil)

type BatchAcceptor interface {
	Accept(ctx context.Context, authenticatedDeviceID string, batch domain.Batch) error
}

type Loopback struct {
	service  BatchAcceptor
	identity string
}

func NewLoopback(service BatchAcceptor, identity string) (*Loopback, error) {
	if service == nil {
		return nil, ErrNilService
	}
	if identity == "" {
		return nil, ErrMissingIdentity
	}
	return &Loopback{service: service, identity: identity}, nil
}

func (l *Loopback) Send(ctx context.Context, batch domain.Batch) error {
	return l.service.Accept(ctx, l.identity, batch)
}
