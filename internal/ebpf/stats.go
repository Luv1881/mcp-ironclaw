package ebpf

const (
	statObserved = iota
	statEmitted
	statDroppedRingbuf
	statDroppedRate
	statDroppedFilter
	statDroppedUnpaired
	statCount
)

type Stats struct {
	Observed        uint64
	Emitted         uint64
	DroppedRingbuf  uint64
	DroppedRate     uint64
	DroppedFilter   uint64
	DroppedUnpaired uint64
	DecodeFailures  uint64
}
