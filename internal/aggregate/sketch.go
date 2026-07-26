package aggregate

import (
	"errors"
	"math"
	"sort"
)

var (
	ErrInvalidAccuracy   = errors.New("aggregate: relative accuracy must be in (0,1)")
	ErrAccuracyMismatch  = errors.New("aggregate: cannot merge sketches with different relative accuracy")
	ErrInvalidQuantile   = errors.New("aggregate: quantile must be in [0,1]")
	ErrNilSketchOperand  = errors.New("aggregate: sketch operand is nil")
	defaultRelativeError = 0.01
)

type Sketch struct {
	relativeAccuracy float64
	gamma            float64
	logGamma         float64
	buckets          map[int]int64
	zeroCount        int64
	count            int64
	sum              int64
	max              int64
	min              int64
}

func NewSketch(relativeAccuracy float64) (*Sketch, error) {
	if relativeAccuracy <= 0 || relativeAccuracy >= 1 {
		return nil, ErrInvalidAccuracy
	}
	gamma := (1 + relativeAccuracy) / (1 - relativeAccuracy)
	return &Sketch{
		relativeAccuracy: relativeAccuracy,
		gamma:            gamma,
		logGamma:         math.Log(gamma),
		buckets:          make(map[int]int64),
		min:              math.MaxInt64,
		max:              math.MinInt64,
	}, nil
}

func (s *Sketch) RelativeAccuracy() float64 { return s.relativeAccuracy }

func (s *Sketch) Count() int64 { return s.count }

func (s *Sketch) Sum() int64 { return s.sum }

func (s *Sketch) Max() int64 {
	if s.count == 0 {
		return 0
	}
	return s.max
}

func (s *Sketch) Min() int64 {
	if s.count == 0 {
		return 0
	}
	return s.min
}

func (s *Sketch) Add(value int64) {
	s.count++
	s.sum += value
	if value > s.max {
		s.max = value
	}
	if value < s.min {
		s.min = value
	}
	if value <= 0 {
		s.zeroCount++
		return
	}
	s.buckets[s.index(value)]++
}

func (s *Sketch) index(value int64) int {
	return int(math.Ceil(math.Log(float64(value)) / s.logGamma))
}

func (s *Sketch) value(index int) int64 {
	estimate := 2 * math.Pow(s.gamma, float64(index)) / (1 + s.gamma)
	return int64(math.Round(estimate))
}

func (s *Sketch) Merge(other *Sketch) error {
	if other == nil {
		return ErrNilSketchOperand
	}
	if other.relativeAccuracy != s.relativeAccuracy {
		return ErrAccuracyMismatch
	}
	if other.count == 0 {
		return nil
	}

	for index, count := range other.buckets {
		s.buckets[index] += count
	}
	s.zeroCount += other.zeroCount
	s.count += other.count
	s.sum += other.sum
	if other.max > s.max {
		s.max = other.max
	}
	if other.min < s.min {
		s.min = other.min
	}
	return nil
}

func (s *Sketch) Quantile(q float64) (int64, error) {
	if q < 0 || q > 1 {
		return 0, ErrInvalidQuantile
	}
	if s.count == 0 {
		return 0, nil
	}

	rank := int64(math.Floor(q * float64(s.count-1)))
	if rank < s.zeroCount {
		return 0, nil
	}

	indexes := make([]int, 0, len(s.buckets))
	for index := range s.buckets {
		indexes = append(indexes, index)
	}
	sort.Ints(indexes)

	cumulative := s.zeroCount
	for _, index := range indexes {
		cumulative += s.buckets[index]
		if cumulative > rank {
			return s.value(index), nil
		}
	}

	return s.max, nil
}

func (s *Sketch) Clone() *Sketch {
	clone := &Sketch{
		relativeAccuracy: s.relativeAccuracy,
		gamma:            s.gamma,
		logGamma:         s.logGamma,
		buckets:          make(map[int]int64, len(s.buckets)),
		zeroCount:        s.zeroCount,
		count:            s.count,
		sum:              s.sum,
		max:              s.max,
		min:              s.min,
	}
	for index, count := range s.buckets {
		clone.buckets[index] = count
	}
	return clone
}
