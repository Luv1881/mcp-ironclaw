package metrics_test

import (
	"testing"

	"github.com/ironclaw/mcp-ironclaw/internal/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

func BenchmarkPrometheusIncrementParallel(b *testing.B) {
	registry := prometheus.NewRegistry()

	recorder, err := metrics.NewPrometheus(registry)
	if err != nil {
		b.Fatal(err)
	}
	recorder.Preregister(metrics.KnownCounters()...)

	names := metrics.KnownCounters()

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			recorder.Increment(names[i%len(names)], 1)
			i++
		}
	})
}
