package httpingest_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ironclaw/mcp-ironclaw/internal/domain"
	"github.com/ironclaw/mcp-ironclaw/internal/httpingest"
	"github.com/ironclaw/mcp-ironclaw/internal/store"
	"github.com/ironclaw/mcp-ironclaw/internal/wire"
)

type recordingAcceptor struct {
	identities []string
	batches    []domain.Batch
	err        error
}

func (a *recordingAcceptor) Accept(_ context.Context, identity string, batch domain.Batch) error {
	if a.err != nil {
		return a.err
	}
	a.identities = append(a.identities, identity)
	a.batches = append(a.batches, batch)
	return nil
}

type fixedClock struct{ at time.Time }

func (c fixedClock) Now() time.Time { return c.at }

func fixedNow() time.Time { return time.Unix(1700000000, 0).UTC() }

func newServer(t *testing.T, acceptor httpingest.BatchAcceptor, metrics domain.MetricsRecorder) http.Handler {
	t.Helper()

	server, err := httpingest.New(httpingest.Config{
		Acceptor: acceptor,
		Metrics:  metrics,
		Clock:    fixedClock{at: time.Unix(1700000000, 0).UTC()},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	return server.Handler()
}

func batchBody(deviceID string, events int) string {
	var builder strings.Builder
	builder.WriteString(`{`)
	if deviceID != "" {
		fmt.Fprintf(&builder, `"device_id":%q,`, deviceID)
	}
	builder.WriteString(`"events":[`)
	for i := 0; i < events; i++ {
		if i > 0 {
			builder.WriteString(",")
		}
		fmt.Fprintf(&builder,
			`{"user_id":"user-1","process_id":%d,"pod_id":"pod-a","kind":1,"observed_at_unix_nanos":%d,"latency_nanos":%d,"bytes":128,"failed":false}`,
			i, time.Unix(1700000000, 0).UnixNano(), 1000+i)
	}
	builder.WriteString(`]}`)
	return builder.String()
}

func post(handler http.Handler, deviceID, body string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodPost, "/v1/batches", bytes.NewBufferString(body))
	if deviceID != "" {
		request.Header.Set(httpingest.DefaultIdentityHeader, deviceID)
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	return recorder
}

func TestNewRejectsNilAcceptor(t *testing.T) {
	if _, err := httpingest.New(httpingest.Config{}); !errors.Is(err, httpingest.ErrNilAcceptor) {
		t.Fatalf("got %v, want ErrNilAcceptor", err)
	}
}

func TestHealthEndpoint(t *testing.T) {
	handler := newServer(t, &recordingAcceptor{}, nil)

	request := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("health returned %d, want 200", recorder.Code)
	}
}

func TestAcceptedBatchIsStampedWithTheAuthenticatedIdentity(t *testing.T) {
	acceptor := &recordingAcceptor{}
	metrics := store.NewMemory()
	handler := newServer(t, acceptor, metrics)

	recorder := post(handler, "device-000", batchBody("", 3))

	if recorder.Code != http.StatusAccepted {
		t.Fatalf("status %d, want 202: %s", recorder.Code, recorder.Body.String())
	}
	if len(acceptor.batches) != 1 {
		t.Fatalf("accepted %d batches, want 1", len(acceptor.batches))
	}

	batch := acceptor.batches[0]
	if batch.DeviceID != "device-000" {
		t.Fatalf("batch device %q, want device-000", batch.DeviceID)
	}
	for i, event := range batch.Events {
		if event.DeviceID != "device-000" {
			t.Fatalf("event %d device %q, want the authenticated identity", i, event.DeviceID)
		}
	}

	recorded, err := metrics.PipelineMetrics(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if recorded[httpingest.MetricRequestsAccepted] != 1 {
		t.Fatalf("accepted metric %d, want 1", recorded[httpingest.MetricRequestsAccepted])
	}
}

func TestMissingIdentityHeaderIsUnauthorized(t *testing.T) {
	acceptor := &recordingAcceptor{}
	metrics := store.NewMemory()
	handler := newServer(t, acceptor, metrics)

	recorder := post(handler, "", batchBody("", 1))

	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("status %d, want 401", recorder.Code)
	}
	if len(acceptor.batches) != 0 {
		t.Fatal("a batch without an authenticated identity reached the acceptor")
	}

	recorded, err := metrics.PipelineMetrics(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if recorded[httpingest.MetricRequestsUnauthentic] != 1 {
		t.Fatalf("unauthenticated metric %d, want 1", recorded[httpingest.MetricRequestsUnauthentic])
	}
}

func TestBodyDeviceIdCannotOverrideTheCertificateIdentity(t *testing.T) {
	acceptor := &recordingAcceptor{}
	handler := newServer(t, acceptor, nil)

	recorder := post(handler, "device-000", batchBody("victim-device", 1))

	if recorder.Code != http.StatusForbidden {
		t.Fatalf("status %d, want 403 for a spoofed device id", recorder.Code)
	}
	if len(acceptor.batches) != 0 {
		t.Fatal("a spoofed batch reached the acceptor")
	}
}

func TestMatchingBodyDeviceIdIsAccepted(t *testing.T) {
	acceptor := &recordingAcceptor{}
	handler := newServer(t, acceptor, nil)

	recorder := post(handler, "device-000", batchBody("device-000", 2))

	if recorder.Code != http.StatusAccepted {
		t.Fatalf("status %d, want 202: %s", recorder.Code, recorder.Body.String())
	}
}

func TestMalformedPayloadIsRejected(t *testing.T) {
	acceptor := &recordingAcceptor{}
	handler := newServer(t, acceptor, nil)

	recorder := post(handler, "device-000", "{not json")

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status %d, want 400", recorder.Code)
	}
	if len(acceptor.batches) != 0 {
		t.Fatal("a malformed batch reached the acceptor")
	}
}

func TestInvalidEventIsRejected(t *testing.T) {
	acceptor := &recordingAcceptor{}
	handler := newServer(t, acceptor, nil)

	body := `{"events":[{"user_id":"","process_id":1,"latency_nanos":10,"observed_at_unix_nanos":1700000000000000000}]}`
	recorder := post(handler, "device-000", body)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status %d, want 400 for an event missing its user id", recorder.Code)
	}
	if len(acceptor.batches) != 0 {
		t.Fatal("an invalid batch reached the acceptor")
	}
}

func TestNegativeLatencyIsRejected(t *testing.T) {
	acceptor := &recordingAcceptor{}
	handler := newServer(t, acceptor, nil)

	body := `{"events":[{"user_id":"user-1","process_id":1,"latency_nanos":-5,"observed_at_unix_nanos":1700000000000000000}]}`
	recorder := post(handler, "device-000", body)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status %d, want 400 for a negative latency", recorder.Code)
	}
}

func TestAcceptorFailureIsReported(t *testing.T) {
	acceptor := &recordingAcceptor{err: errors.New("publisher unavailable")}
	handler := newServer(t, acceptor, nil)

	recorder := post(handler, "device-000", batchBody("", 1))

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status %d, want 400 when the acceptor fails", recorder.Code)
	}
}

func TestIdentityMismatchFromAcceptorIsForbidden(t *testing.T) {
	acceptor := &recordingAcceptor{err: domain.ErrDeviceMismatch}
	handler := newServer(t, acceptor, nil)

	recorder := post(handler, "device-000", batchBody("", 1))

	if recorder.Code != http.StatusForbidden {
		t.Fatalf("status %d, want 403", recorder.Code)
	}
}

func TestOversizedBodyIsRejected(t *testing.T) {
	server, err := httpingest.New(httpingest.Config{
		Acceptor:     &recordingAcceptor{},
		MaxBodyBytes: 64,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	recorder := post(server.Handler(), "device-000", batchBody("", 50))

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status %d, want 400 for an oversized body", recorder.Code)
	}
}

func TestPodIdentityComesFromTheIngestNotTheRequest(t *testing.T) {
	acceptor := &recordingAcceptor{}

	server, err := httpingest.New(httpingest.Config{
		Acceptor: acceptor,
		PodID:    "ingest-7c9f-abcde",
		Clock:    fixedClock{at: fixedNow()},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	recorder := post(server.Handler(), "device-000", batchBody("", 2))
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("status %d, want 202: %s", recorder.Code, recorder.Body.String())
	}

	for i, event := range acceptor.batches[0].Events {
		if event.PodID != "ingest-7c9f-abcde" {
			t.Fatalf("event %d pod %q, want the ingest's own downward-API identity, not the body's pod-a", i, event.PodID)
		}
	}
}

func TestPodIdentityFallsBackToTheEnvironment(t *testing.T) {
	t.Setenv(httpingest.PodIDEnv, "ingest-from-env")

	acceptor := &recordingAcceptor{}
	server, err := httpingest.New(httpingest.Config{Acceptor: acceptor, Clock: fixedClock{at: fixedNow()}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if recorder := post(server.Handler(), "device-000", batchBody("", 1)); recorder.Code != http.StatusAccepted {
		t.Fatalf("status %d, want 202", recorder.Code)
	}
	if got := acceptor.batches[0].Events[0].PodID; got != "ingest-from-env" {
		t.Fatalf("pod id %q, want ingest-from-env", got)
	}
}

func TestAnAgentEncodedBatchSurvivesTheRealIngestPath(t *testing.T) {
	acceptor := &recordingAcceptor{}
	server, err := httpingest.New(httpingest.Config{
		Acceptor: acceptor,
		PodID:    "ingest-7c9f-abcde",
		Clock:    fixedClock{at: fixedNow()},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	observed := time.Unix(1700000000, 500).UTC()
	encoded, err := wire.EncodeEdgeBatch(domain.Batch{
		DeviceID:  "device-000",
		CreatedAt: observed,
		Events: []domain.Event{
			{
				DeviceID: "device-000", UserID: "user-000", ProcessID: 4242,
				PodID: "agent-claimed-pod", Kind: domain.EventKindNetwork,
				ObservedAt: observed, LatencyNanos: 2_500_000, Bytes: 128, Failed: true,
			},
			{
				DeviceID: "device-000", UserID: "user-000", ProcessID: 4243,
				PodID: "agent-claimed-pod", Kind: domain.EventKindSyscall,
				ObservedAt: observed, LatencyNanos: 1_000_000, Bytes: 64, Failed: false,
			},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	recorder := post(server.Handler(), "device-000", string(encoded))
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("status %d, want 202: %s", recorder.Code, recorder.Body.String())
	}

	if len(acceptor.batches) != 1 {
		t.Fatalf("accepted %d batches, want 1", len(acceptor.batches))
	}

	batch := acceptor.batches[0]
	if batch.DeviceID != "device-000" {
		t.Fatalf("device %q, want the authenticated identity", batch.DeviceID)
	}
	if len(batch.Events) != 2 {
		t.Fatalf("batch holds %d events, want 2", len(batch.Events))
	}

	first := batch.Events[0]
	if first.UserID != "user-000" || first.ProcessID != 4242 || first.Kind != domain.EventKindNetwork {
		t.Fatalf("event identity or kind changed across the edge: %+v", first)
	}
	if first.LatencyNanos != 2_500_000 || first.Bytes != 128 || !first.Failed {
		t.Fatalf("event measurements changed across the edge: %+v", first)
	}
	if !first.ObservedAt.Equal(observed) {
		t.Fatalf("observed at %s, want %s", first.ObservedAt, observed)
	}
	if first.PodID != "ingest-7c9f-abcde" {
		t.Fatalf("pod %q, want the ingest's own identity rather than the agent's claim", first.PodID)
	}
}
