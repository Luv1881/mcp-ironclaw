package metrics_test

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ironclaw/mcp-ironclaw/internal/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

func scrape(t *testing.T, registry *prometheus.Registry) string {
	t.Helper()

	recorder := httptest.NewRecorder()
	metrics.Handler(registry).ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))

	body, err := io.ReadAll(recorder.Body)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	return string(body)
}

func TestNewPrometheusRejectsNilRegistry(t *testing.T) {
	if _, err := metrics.NewPrometheus(nil); !errors.Is(err, metrics.ErrNilRegistry) {
		t.Fatalf("got %v, want ErrNilRegistry", err)
	}
}

func TestCountersAppearInTheScrape(t *testing.T) {
	registry := prometheus.NewRegistry()

	recorder, err := metrics.NewPrometheus(registry)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	recorder.Increment("ingest_events_accepted", 5)
	recorder.Increment("ingest_events_accepted", 7)

	body := scrape(t, registry)
	if !strings.Contains(body, "ironclaw_ingest_events_accepted_total 12") {
		t.Fatalf("scrape did not contain the accumulated counter:\n%s", body)
	}
}

func TestNamesAreSanitisedForPrometheus(t *testing.T) {
	registry := prometheus.NewRegistry()

	recorder, err := metrics.NewPrometheus(registry)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	recorder.Increment("kafka.dead-letter", 1)

	body := scrape(t, registry)
	if !strings.Contains(body, "ironclaw_kafka_dead_letter_total 1") {
		t.Fatalf("dotted and dashed names must be sanitised:\n%s", body)
	}
}

func TestZeroDeltaDoesNotCreateSeries(t *testing.T) {
	registry := prometheus.NewRegistry()

	recorder, err := metrics.NewPrometheus(registry)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	recorder.Increment("never_incremented", 0)

	if strings.Contains(scrape(t, registry), "never_incremented") {
		t.Fatal("a zero delta should not register a series")
	}
}

func TestFanoutReachesEveryRecorder(t *testing.T) {
	registry := prometheus.NewRegistry()

	promRecorder, err := metrics.NewPrometheus(registry)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	inner := newCounting()
	fanout := metrics.NewFanout(promRecorder, inner, nil)

	fanout.Increment("events", 3)

	if got := inner.total("events"); got != 3 {
		t.Fatalf("secondary recorder saw %d, want 3", got)
	}
	if !strings.Contains(scrape(t, registry), "ironclaw_events_total 3") {
		t.Fatal("prometheus recorder did not receive the fanned out sample")
	}
}

func TestRepeatedNamesReuseTheSameCounter(t *testing.T) {
	registry := prometheus.NewRegistry()

	first, err := metrics.NewPrometheus(registry)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	second, err := metrics.NewPrometheus(registry)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	first.Increment("shared", 2)
	second.Increment("shared", 3)

	if !strings.Contains(scrape(t, registry), "ironclaw_shared_total 5") {
		t.Fatalf("two recorders on one registry must share the counter:\n%s", scrape(t, registry))
	}
}
