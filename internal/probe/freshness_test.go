package probe_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/ironclaw/mcp-ironclaw/internal/probe"
)

type stepObserver struct {
	mu      sync.Mutex
	count   int64
	afterN  int
	polls   int
	failing error
}

func (o *stepObserver) Count(context.Context, string, string) (int64, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.failing != nil {
		return 0, o.failing
	}

	o.polls++
	if o.afterN > 0 && o.polls >= o.afterN {
		o.count++
		o.polls = 0
	}
	return o.count, nil
}

type neverObserver struct{}

func (neverObserver) Count(context.Context, string, string) (int64, error) { return 0, nil }

func acceptingEdge(t *testing.T, status int) *httptest.Server {
	t.Helper()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(status)
	}))
	t.Cleanup(server.Close)

	return server
}

func baseConfig(endpoint string, observer probe.VisibilityObserver) probe.Config {
	return probe.Config{
		Endpoint:   endpoint,
		UserID:     "probe-user",
		DeviceID:   "probe-device",
		Rounds:     3,
		EventCount: 1,
		PollEvery:  time.Millisecond,
		Timeout:    2 * time.Second,
		Client:     &http.Client{Timeout: 5 * time.Second},
		Observer:   observer,
	}
}

func TestNewValidatesConfig(t *testing.T) {
	if _, err := probe.NewFreshness(probe.Config{}); !errors.Is(err, probe.ErrNilClient) {
		t.Fatalf("got %v, want ErrNilClient", err)
	}
	if _, err := probe.NewFreshness(probe.Config{Client: &http.Client{}}); !errors.Is(err, probe.ErrNilObserver) {
		t.Fatalf("got %v, want ErrNilObserver", err)
	}
	if _, err := probe.NewFreshness(probe.Config{Client: &http.Client{}, Observer: neverObserver{}}); !errors.Is(err, probe.ErrInvalidRounds) {
		t.Fatalf("got %v, want ErrInvalidRounds", err)
	}
}

func TestRunCollectsOneSamplePerRound(t *testing.T) {
	edge := acceptingEdge(t, http.StatusAccepted)

	freshness, err := probe.NewFreshness(baseConfig(edge.URL, &stepObserver{afterN: 2}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	report, err := freshness.Run(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if report.Sent != 3 {
		t.Fatalf("sent %d, want 3", report.Sent)
	}
	if len(report.Samples) != 3 {
		t.Fatalf("collected %d samples, want 3", len(report.Samples))
	}
	if report.Lost != 0 {
		t.Fatalf("lost %d, want 0", report.Lost)
	}
	for i, sample := range report.Samples {
		if sample <= 0 {
			t.Fatalf("sample %d is %v, want a positive latency", i, sample)
		}
	}
}

func TestBatchThatNeverBecomesVisibleIsCountedAsLost(t *testing.T) {
	edge := acceptingEdge(t, http.StatusAccepted)

	config := baseConfig(edge.URL, neverObserver{})
	config.Rounds = 2
	config.Timeout = 100 * time.Millisecond

	freshness, err := probe.NewFreshness(config)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	report, err := freshness.Run(context.Background())
	if !errors.Is(err, probe.ErrNoSamples) {
		t.Fatalf("got %v, want ErrNoSamples", err)
	}
	if report.Lost != 2 {
		t.Fatalf("lost %d, want 2", report.Lost)
	}
}

func TestRejectedBatchIsReportedNotSilentlyCounted(t *testing.T) {
	edge := acceptingEdge(t, http.StatusForbidden)

	freshness, err := probe.NewFreshness(baseConfig(edge.URL, &stepObserver{afterN: 1}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if _, err := freshness.Run(context.Background()); !errors.Is(err, probe.ErrNotAccepted) {
		t.Fatalf("got %v, want ErrNotAccepted", err)
	}
}

func TestObserverFailureIsSurfaced(t *testing.T) {
	edge := acceptingEdge(t, http.StatusAccepted)
	sentinel := errors.New("redis unavailable")

	freshness, err := probe.NewFreshness(baseConfig(edge.URL, &stepObserver{failing: sentinel}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if _, err := freshness.Run(context.Background()); !errors.Is(err, sentinel) {
		t.Fatalf("got %v, want the observer error", err)
	}
}

func TestReportQuantiles(t *testing.T) {
	report := probe.Report{Samples: []time.Duration{
		10 * time.Millisecond,
		20 * time.Millisecond,
		30 * time.Millisecond,
		40 * time.Millisecond,
		1000 * time.Millisecond,
	}}

	if got := report.Quantile(0.5); got != 30*time.Millisecond {
		t.Fatalf("p50 %v, want 30ms", got)
	}
	if got := report.Quantile(0.99); got != 40*time.Millisecond {
		t.Fatalf("p99 %v, want 40ms: with five samples floor(0.99*(n-1)) selects the fourth", got)
	}
	if got := report.Quantile(1); got != 1000*time.Millisecond {
		t.Fatalf("max %v, want 1000ms", got)
	}
	if got := report.Mean(); got != 220*time.Millisecond {
		t.Fatalf("mean %v, want 220ms", got)
	}

	empty := probe.Report{}
	if empty.Quantile(0.99) != 0 || empty.Mean() != 0 {
		t.Fatal("an empty report should report zero rather than panic")
	}
}

func TestRunHonoursContextCancellation(t *testing.T) {
	edge := acceptingEdge(t, http.StatusAccepted)

	config := baseConfig(edge.URL, neverObserver{})
	config.Rounds = 100
	config.Timeout = 5 * time.Second

	freshness, err := probe.NewFreshness(config)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	if _, err := freshness.Run(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v, want context.DeadlineExceeded", err)
	}
}
