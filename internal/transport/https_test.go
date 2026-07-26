package transport_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ironclaw/mcp-ironclaw/internal/domain"
	"github.com/ironclaw/mcp-ironclaw/internal/spool"
	"github.com/ironclaw/mcp-ironclaw/internal/store"
	"github.com/ironclaw/mcp-ironclaw/internal/tracing"
	"github.com/ironclaw/mcp-ironclaw/internal/transport"
)

func encodeBatch(batch domain.Batch) ([]byte, error) {
	return json.Marshal(map[string]any{"device_id": batch.DeviceID, "events": batch.Len()})
}

func newSpool(t *testing.T) *spool.Spool {
	t.Helper()

	queue, err := spool.Open(t.TempDir(), 1<<20)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	return queue
}

func newHTTPS(t *testing.T, endpoint string, queue *spool.Spool) *transport.HTTPS {
	t.Helper()

	client, err := transport.NewHTTPS(transport.HTTPSConfig{
		Endpoint:    endpoint,
		Encode:      encodeBatch,
		Spool:       queue,
		Client:      &http.Client{Timeout: 5 * time.Second},
		BaseBackoff: time.Millisecond,
		MaxBackoff:  5 * time.Millisecond,
		DrainEvery:  2 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	return client
}

func TestNewHTTPSValidatesConfiguration(t *testing.T) {
	if _, err := transport.NewHTTPS(transport.HTTPSConfig{}); !errors.Is(err, transport.ErrNoEndpoint) {
		t.Fatalf("got %v, want ErrNoEndpoint", err)
	}
	_, err := transport.NewHTTPS(transport.HTTPSConfig{Endpoint: "https://edge/v1/batches", Encode: encodeBatch})
	if !errors.Is(err, transport.ErrNoKeyPair) {
		t.Fatalf("got %v, want ErrNoKeyPair when no client certificate is supplied", err)
	}
}

func TestAcceptedBatchIsNotSpooled(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	queue := newSpool(t)
	client := newHTTPS(t, server.URL, queue)

	if err := client.Send(context.Background(), sampleBatch()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := queue.Stats().Entries; got != 0 {
		t.Fatalf("spooled %d entries after a success, want 0", got)
	}
}

func TestOutageSpoolsInsteadOfLosing(t *testing.T) {
	var accepting atomic.Bool

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if accepting.Load() {
			w.WriteHeader(http.StatusAccepted)
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	queue := newSpool(t)
	client := newHTTPS(t, server.URL, queue)

	for i := 0; i < 5; i++ {
		if err := client.Send(context.Background(), sampleBatch()); err != nil {
			t.Fatalf("an outage must spool rather than fail: %v", err)
		}
	}

	if got := queue.Stats().Entries; got != 5 {
		t.Fatalf("spooled %d batches during the outage, want 5", got)
	}

	accepting.Store(true)

	ctx, cancel := context.WithCancel(context.Background())
	go client.Drain(ctx)

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if queue.Stats().Entries == 0 {
			cancel()
			return
		}
		time.Sleep(5 * time.Millisecond)
	}

	cancel()
	t.Fatalf("spool still holds %d entries after recovery", queue.Stats().Entries)
}

func TestSendFailsWhenNoSpoolIsConfigured(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	client, err := transport.NewHTTPS(transport.HTTPSConfig{
		Endpoint: server.URL,
		Encode:   encodeBatch,
		Client:   &http.Client{Timeout: 5 * time.Second},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := client.Send(context.Background(), sampleBatch()); !errors.Is(err, transport.ErrRejected) {
		t.Fatalf("got %v, want ErrRejected without a spool", err)
	}
}

func TestDrainStopsOnContextCancellation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	queue := newSpool(t)
	client := newHTTPS(t, server.URL, queue)

	if err := client.Send(context.Background(), sampleBatch()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	done := make(chan struct{})
	go func() {
		client.Drain(ctx)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Drain ignored context cancellation")
	}
}

func TestTraceHeadersAreInjectedIntoTheRequest(t *testing.T) {
	provider, err := tracing.New(context.Background(), tracing.Config{
		ServiceName: "ironclaw-test",
		Writer:      io.Discard,
		SampleRatio: 1,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer provider.Shutdown(context.Background())

	seen := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case seen <- r.Header.Get("traceparent"):
		default:
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	client := transport.NewTraced(newHTTPS(t, server.URL, nil), provider)

	if err := client.Send(context.Background(), sampleBatch()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	select {
	case header := <-seen:
		if header == "" {
			t.Fatal("the edge received no traceparent header")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("request never reached the server")
	}
}

func TestSpoolDropsAreCounted(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	queue, err := spool.Open(t.TempDir(), 512)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	metrics := store.NewMemory()

	client, err := transport.NewHTTPS(transport.HTTPSConfig{
		Endpoint:    server.URL,
		Encode:      encodeBatch,
		Spool:       queue,
		Client:      &http.Client{Timeout: 5 * time.Second},
		BaseBackoff: time.Millisecond,
		MaxBackoff:  5 * time.Millisecond,
		Metrics:     metrics,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for i := 0; i < 40; i++ {
		if err := client.Send(context.Background(), sampleBatch()); err != nil {
			t.Fatalf("an outage must spool rather than fail: %v", err)
		}
	}

	if queue.Stats().Dropped == 0 {
		t.Fatal("the spool never hit its byte budget, so this test proves nothing")
	}

	recorded, err := metrics.PipelineMetrics(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if recorded[transport.MetricSpoolDropped] != queue.Stats().Dropped {
		t.Fatalf("counted %d drops, spool reports %d", recorded[transport.MetricSpoolDropped], queue.Stats().Dropped)
	}
}
