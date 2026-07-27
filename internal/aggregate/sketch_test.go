package aggregate_test

import (
	"errors"
	"math"
	"math/rand"
	"sort"
	"testing"

	"github.com/ironclaw/mcp-ironclaw/internal/aggregate"
)

func exactQuantile(values []int64, q float64) int64 {
	sorted := append([]int64(nil), values...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	rank := int(math.Floor(q * float64(len(sorted)-1)))
	return sorted[rank]
}

func assertWithinAccuracy(t *testing.T, got, want int64, accuracy float64) {
	t.Helper()
	tolerance := float64(want) * accuracy
	if math.Abs(float64(got-want)) > tolerance {
		t.Fatalf("got %d, want %d within relative accuracy %.4f", got, want, accuracy)
	}
}

func TestNewSketchRejectsInvalidAccuracy(t *testing.T) {
	for _, accuracy := range []float64{0, 1, -0.1, 1.5} {
		if _, err := aggregate.NewSketch(accuracy); !errors.Is(err, aggregate.ErrInvalidAccuracy) {
			t.Fatalf("accuracy %v: got %v, want ErrInvalidAccuracy", accuracy, err)
		}
	}
}

func TestSketchQuantilesMatchKnownDistribution(t *testing.T) {
	const accuracy = 0.01

	sketch, err := aggregate.NewSketch(accuracy)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	random := rand.New(rand.NewSource(7))
	values := make([]int64, 0, 100000)
	for i := 0; i < 100000; i++ {
		value := int64(1000 + random.Intn(9000))
		if random.Float64() < 0.02 {
			value = int64(500000 + random.Intn(500000))
		}
		values = append(values, value)
		sketch.Add(value)
	}

	for _, q := range []float64{0.5, 0.9, 0.95, 0.99} {
		got, err := sketch.Quantile(q)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		assertWithinAccuracy(t, got, exactQuantile(values, q), accuracy)
	}

	if sketch.Count() != int64(len(values)) {
		t.Fatalf("count %d, want %d", sketch.Count(), len(values))
	}
}

func TestSketchMergeMatchesSingleSketch(t *testing.T) {
	const accuracy = 0.01

	combined, err := aggregate.NewSketch(accuracy)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	left, err := aggregate.NewSketch(accuracy)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	right, err := aggregate.NewSketch(accuracy)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	random := rand.New(rand.NewSource(11))
	for i := 0; i < 50000; i++ {
		value := int64(1 + random.Intn(1000000))
		combined.Add(value)
		if i%2 == 0 {
			left.Add(value)
		} else {
			right.Add(value)
		}
	}

	if err := left.Merge(right); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if left.Count() != combined.Count() {
		t.Fatalf("merged count %d, want %d", left.Count(), combined.Count())
	}

	for _, q := range []float64{0.5, 0.95, 0.99} {
		mergedValue, err := left.Quantile(q)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		combinedValue, err := combined.Quantile(q)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if mergedValue != combinedValue {
			t.Fatalf("quantile %v: merged %d, combined %d", q, mergedValue, combinedValue)
		}
	}
}

func TestSketchMergeIsCommutative(t *testing.T) {
	const accuracy = 0.02

	build := func(seed int64, count int) *aggregate.Sketch {
		sketch, err := aggregate.NewSketch(accuracy)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		random := rand.New(rand.NewSource(seed))
		for i := 0; i < count; i++ {
			sketch.Add(int64(1 + random.Intn(100000)))
		}
		return sketch
	}

	first, second := build(1, 5000), build(2, 7000)
	forward, backward := first.Clone(), second.Clone()

	if err := forward.Merge(second); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := backward.Merge(first); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for _, q := range []float64{0.5, 0.95, 0.99} {
		a, err := forward.Quantile(q)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		b, err := backward.Quantile(q)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if a != b {
			t.Fatalf("quantile %v is order dependent: %d vs %d", q, a, b)
		}
	}
}

func TestSketchMergeRejectsMismatchedAccuracy(t *testing.T) {
	coarse, err := aggregate.NewSketch(0.05)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	fine, err := aggregate.NewSketch(0.01)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := coarse.Merge(fine); !errors.Is(err, aggregate.ErrAccuracyMismatch) {
		t.Fatalf("got %v, want ErrAccuracyMismatch", err)
	}
	if err := coarse.Merge(nil); !errors.Is(err, aggregate.ErrNilSketchOperand) {
		t.Fatalf("got %v, want ErrNilSketchOperand", err)
	}
}

func TestSketchHandlesEmptyAndNonPositiveValues(t *testing.T) {
	sketch, err := aggregate.NewSketch(0.01)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	value, err := sketch.Quantile(0.99)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if value != 0 {
		t.Fatalf("empty sketch quantile %d, want 0", value)
	}

	sketch.Add(0)
	sketch.Add(0)
	sketch.Add(1000)

	if _, err := sketch.Quantile(1.5); !errors.Is(err, aggregate.ErrInvalidQuantile) {
		t.Fatalf("got %v, want ErrInvalidQuantile", err)
	}

	median, err := sketch.Quantile(0.5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if median != 0 {
		t.Fatalf("median %d, want 0", median)
	}
}

func TestQuantileReflectsBucketsAddedAfterAnEarlierQuantile(t *testing.T) {
	sketch, err := aggregate.NewSketch(0.01)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for i := 0; i < 100; i++ {
		sketch.Add(1_000_000)
	}

	first, err := sketch.Quantile(0.99)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for i := 0; i < 100; i++ {
		sketch.Add(900_000_000)
	}

	second, err := sketch.Quantile(0.99)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if second <= first {
		t.Fatalf("p99 stayed at %d after adding much slower samples (was %d); a cached bucket ordering went stale", second, first)
	}
}

func TestQuantileReflectsBucketsIntroducedByMerge(t *testing.T) {
	build := func() *aggregate.Sketch {
		sketch, err := aggregate.NewSketch(0.01)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		return sketch
	}

	merged := build()
	reference := build()
	middle := build()

	for i := 0; i < 100; i++ {
		merged.Add(1_000_000)
		merged.Add(900_000_000)
		reference.Add(1_000_000)
		reference.Add(900_000_000)
	}

	if _, err := merged.Quantile(0.5); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for i := 0; i < 800; i++ {
		middle.Add(50_000_000)
		reference.Add(50_000_000)
	}

	if err := merged.Merge(middle); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got, err := merged.Quantile(0.5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want, err := reference.Quantile(0.5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got != want {
		t.Fatalf("merged p50 is %d but the same samples added directly give %d; Merge did not invalidate the cached bucket ordering", got, want)
	}
}
