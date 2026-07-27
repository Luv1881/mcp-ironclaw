package aggregate_test

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/ironclaw/mcp-ironclaw/internal/aggregate"
	"github.com/ironclaw/mcp-ironclaw/internal/domain"
)

func benchBatch(events int, keys int) domain.Batch {
	random := rand.New(rand.NewSource(1))

	batch := domain.Batch{
		DeviceID:  "device-000",
		CreatedAt: time.Unix(1700000000, 0).UTC(),
		Events:    make([]domain.Event, 0, events),
	}

	for i := 0; i < events; i++ {
		batch.Events = append(batch.Events, domain.Event{
			DeviceID:     "device-000",
			UserID:       "user-000",
			ProcessID:    int32(i % keys),
			PodID:        "pod-000",
			Kind:         domain.EventKindSyscall,
			ObservedAt:   time.Unix(1700000000, int64(i)).UTC(),
			LatencyNanos: 100_000 + random.Int63n(80_000_000),
			Bytes:        512,
			Failed:       i%37 == 0,
		})
	}

	return batch
}

func benchAggregator(b *testing.B) *aggregate.Aggregator {
	b.Helper()

	aggregator, err := aggregate.New(aggregate.Config{WindowSize: 10 * time.Second})
	if err != nil {
		b.Fatal(err)
	}
	return aggregator
}

func BenchmarkIngestBatch(b *testing.B) {
	batch := benchBatch(500, 4)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		aggregator := benchAggregator(b)
		if err := aggregator.IngestBatch(batch); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkIngestBatchSteadyState(b *testing.B) {
	aggregator := benchAggregator(b)
	batch := benchBatch(500, 4)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := aggregator.IngestBatch(batch); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCollectAll(b *testing.B) {
	batches := make([]domain.Batch, 0, 8)
	for i := 0; i < 8; i++ {
		batches = append(batches, benchBatch(500, 16))
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		aggregator := benchAggregator(b)
		for _, batch := range batches {
			if err := aggregator.IngestBatch(batch); err != nil {
				b.Fatal(err)
			}
		}
		b.StartTimer()

		if _, err := aggregator.CollectAll(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSketchQuantile(b *testing.B) {
	sketch, err := aggregate.NewSketch(0.01)
	if err != nil {
		b.Fatal(err)
	}

	random := rand.New(rand.NewSource(1))
	for i := 0; i < 100000; i++ {
		sketch.Add(100_000 + random.Int63n(80_000_000))
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := sketch.Quantile(0.95); err != nil {
			b.Fatal(err)
		}
		if _, err := sketch.Quantile(0.99); err != nil {
			b.Fatal(err)
		}
	}
}

var _ = fmt.Sprintf
