package app

import (
	"context"
	"errors"
	"testing"

	"github.com/ironclaw/mcp-ironclaw/internal/domain"
)

var errBackendDown = errors.New("connection refused")

type stubReader struct {
	state   domain.DeviceState
	listing domain.DeviceListing
	err     error
	calls   int
}

func (s *stubReader) DeviceState(context.Context, string, string) (domain.DeviceState, error) {
	s.calls++
	return s.state, s.err
}

func (s *stubReader) UserDevices(context.Context, string) (domain.DeviceListing, error) {
	s.calls++
	return s.listing, s.err
}

type stubPrimary struct {
	StateBackend
	reader *stubReader
}

func (s stubPrimary) DeviceState(ctx context.Context, userID, deviceID string) (domain.DeviceState, error) {
	return s.reader.DeviceState(ctx, userID, deviceID)
}

func (s stubPrimary) UserDevices(ctx context.Context, userID string) (domain.DeviceListing, error) {
	return s.reader.UserDevices(ctx, userID)
}

func newFallbackForTest(primary, archive *stubReader) StateBackend {
	return newFallbackState(stubPrimary{reader: primary}, archive)
}

func TestFallbackServesTheArchiveWhenTheHotPathIsDown(t *testing.T) {
	primary := &stubReader{err: errBackendDown}
	archive := &stubReader{state: domain.DeviceState{Count: 42, UserID: "user-1", DeviceID: "device-1"}}

	state, err := newFallbackForTest(primary, archive).DeviceState(context.Background(), "user-1", "device-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if state.Count != 42 {
		t.Fatalf("count %d, want the archived value", state.Count)
	}
	if !state.Stale {
		t.Fatal("a state served from the archive must be marked stale, or a degraded read is indistinguishable from a fresh one")
	}
}

func TestFallbackDoesNotMarkAFreshReadStale(t *testing.T) {
	primary := &stubReader{state: domain.DeviceState{Count: 7}}
	archive := &stubReader{}

	state, err := newFallbackForTest(primary, archive).DeviceState(context.Background(), "user-1", "device-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if state.Stale {
		t.Fatal("a state served by the hot path must not be marked stale")
	}
	if archive.calls != 0 {
		t.Fatalf("the archive was queried %d times while the hot path was healthy", archive.calls)
	}
}

func TestFallbackReportsTheHotPathFailureWhenBothAreDown(t *testing.T) {
	primary := &stubReader{err: errBackendDown}
	archive := &stubReader{err: errors.New("archive is down too")}

	_, err := newFallbackForTest(primary, archive).DeviceState(context.Background(), "user-1", "device-1")
	if !errors.Is(err, errBackendDown) {
		t.Fatalf("got %v, want the hot path failure so the operator learns which dependency broke first", err)
	}
}

func TestFallbackDoesNotQueryTheArchiveForAnUnknownDevice(t *testing.T) {
	primary := &stubReader{err: domain.ErrDeviceNotFound}
	archive := &stubReader{state: domain.DeviceState{Count: 99}}

	_, err := newFallbackForTest(primary, archive).DeviceState(context.Background(), "user-1", "absent")
	if !errors.Is(err, domain.ErrDeviceNotFound) {
		t.Fatalf("got %v, want not found", err)
	}
	if archive.calls != 0 {
		t.Fatalf("the archive was queried %d times for a device the hot path reports as unknown", archive.calls)
	}
}

func TestFallbackMarksADeviceListFromTheArchiveStale(t *testing.T) {
	primary := &stubReader{err: errBackendDown}
	archive := &stubReader{listing: domain.DeviceListing{Devices: []string{"device-1", "device-2"}}}

	listing, err := newFallbackForTest(primary, archive).UserDevices(context.Background(), "user-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(listing.Devices) != 2 {
		t.Fatalf("listed %d devices, want the archived pair", len(listing.Devices))
	}
	if !listing.Stale {
		t.Fatal("a device list served from the archive must be marked stale")
	}
}

func TestFallbackLeavesWritesOnTheHotPath(t *testing.T) {
	primary := &recordingWriter{}
	wrapped := newFallbackState(primary, &stubReader{})

	if err := wrapped.ApplyWindow(context.Background(), domain.AggregateWindow{}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if primary.applied != 1 {
		t.Fatalf("the hot path applied %d windows, want 1: the archive must never take writes", primary.applied)
	}
}

type recordingWriter struct {
	StateBackend
	applied int
}

func (w *recordingWriter) ApplyWindow(context.Context, domain.AggregateWindow) error {
	w.applied++
	return nil
}

func (w *recordingWriter) DeviceState(context.Context, string, string) (domain.DeviceState, error) {
	return domain.DeviceState{}, nil
}

func (w *recordingWriter) UserDevices(context.Context, string) (domain.DeviceListing, error) {
	return domain.DeviceListing{}, nil
}
