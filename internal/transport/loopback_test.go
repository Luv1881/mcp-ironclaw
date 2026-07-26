package transport_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ironclaw/mcp-ironclaw/internal/domain"
	"github.com/ironclaw/mcp-ironclaw/internal/transport"
)

type recordingAcceptor struct {
	identities []string
	err        error
}

func (a *recordingAcceptor) Accept(_ context.Context, identity string, _ domain.Batch) error {
	if a.err != nil {
		return a.err
	}
	a.identities = append(a.identities, identity)
	return nil
}

func sampleBatch() domain.Batch {
	return domain.Batch{
		DeviceID: "device-1",
		Events: []domain.Event{{
			DeviceID:     "device-1",
			UserID:       "user-1",
			ProcessID:    1,
			Kind:         domain.EventKindSyscall,
			ObservedAt:   time.Unix(1700000000, 0),
			LatencyNanos: 1000,
		}},
	}
}

func TestNewLoopbackValidatesDependencies(t *testing.T) {
	if _, err := transport.NewLoopback(nil, "device-1"); !errors.Is(err, transport.ErrNilService) {
		t.Fatalf("got %v, want ErrNilService", err)
	}
	if _, err := transport.NewLoopback(&recordingAcceptor{}, ""); !errors.Is(err, transport.ErrMissingIdentity) {
		t.Fatalf("got %v, want ErrMissingIdentity", err)
	}
}

func TestSendStampsTheConfiguredIdentity(t *testing.T) {
	acceptor := &recordingAcceptor{}

	loopback, err := transport.NewLoopback(acceptor, "device-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := loopback.Send(context.Background(), sampleBatch()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(acceptor.identities) != 1 || acceptor.identities[0] != "device-1" {
		t.Fatalf("identities %v, want [device-1]", acceptor.identities)
	}
}

func TestSendPropagatesAcceptorFailure(t *testing.T) {
	sentinel := errors.New("ingest unavailable")

	loopback, err := transport.NewLoopback(&recordingAcceptor{err: sentinel}, "device-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := loopback.Send(context.Background(), sampleBatch()); !errors.Is(err, sentinel) {
		t.Fatalf("got %v, want the acceptor error", err)
	}
}
