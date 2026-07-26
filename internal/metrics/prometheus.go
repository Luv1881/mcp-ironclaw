package metrics

import (
	"errors"
	"net/http"
	"regexp"
	"strings"
	"sync"

	"github.com/ironclaw/mcp-ironclaw/internal/domain"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var ErrNilRegistry = errors.New("metrics: registry is nil")

var invalidName = regexp.MustCompile(`[^a-zA-Z0-9_]`)

const namespace = "ironclaw"

var _ domain.MetricsRecorder = (*Prometheus)(nil)

type Prometheus struct {
	registry *prometheus.Registry
	mu       sync.Mutex
	counters map[string]prometheus.Counter
}

func NewPrometheus(registry *prometheus.Registry) (*Prometheus, error) {
	if registry == nil {
		return nil, ErrNilRegistry
	}
	return &Prometheus{registry: registry, counters: map[string]prometheus.Counter{}}, nil
}

func (p *Prometheus) Preregister(names ...string) {
	for _, name := range names {
		p.counter(name)
	}
}

func (p *Prometheus) Increment(name string, delta int64) {
	if delta == 0 {
		return
	}

	counter := p.counter(name)
	if counter == nil {
		return
	}
	counter.Add(float64(delta))
}

func (p *Prometheus) counter(name string) prometheus.Counter {
	p.mu.Lock()
	defer p.mu.Unlock()

	if existing, ok := p.counters[name]; ok {
		return existing
	}

	counter := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      sanitise(name) + "_total",
		Help:      "IronClaw pipeline counter " + name,
	})

	if err := p.registry.Register(counter); err != nil {
		var already prometheus.AlreadyRegisteredError
		if errors.As(err, &already) {
			if existing, ok := already.ExistingCollector.(prometheus.Counter); ok {
				p.counters[name] = existing
				return existing
			}
		}
		return nil
	}

	p.counters[name] = counter

	return counter
}

func sanitise(name string) string {
	cleaned := invalidName.ReplaceAllString(name, "_")
	cleaned = strings.Trim(cleaned, "_")
	if cleaned == "" {
		return "unnamed"
	}
	return strings.ToLower(cleaned)
}

func Handler(registry *prometheus.Registry) http.Handler {
	return promhttp.HandlerFor(registry, promhttp.HandlerOpts{})
}

type Fanout struct {
	targets []domain.MetricsRecorder
}

func NewFanout(targets ...domain.MetricsRecorder) *Fanout {
	live := make([]domain.MetricsRecorder, 0, len(targets))
	for _, target := range targets {
		if target != nil {
			live = append(live, target)
		}
	}
	return &Fanout{targets: live}
}

func (f *Fanout) Increment(name string, delta int64) {
	for _, target := range f.targets {
		target.Increment(name, delta)
	}
}
