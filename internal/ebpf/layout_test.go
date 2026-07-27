package ebpf_test

import (
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/ironclaw/mcp-ironclaw/internal/ebpf"
)

type cField struct {
	name  string
	size  int
	align int
	count int
}

var cTypes = map[string]struct{ size, align int }{
	"__u64": {8, 8},
	"__s64": {8, 8},
	"__u32": {4, 4},
	"__s32": {4, 4},
	"__u16": {2, 2},
	"__s16": {2, 2},
	"__u8":  {1, 1},
	"__s8":  {1, 1},
}

var fieldPattern = regexp.MustCompile(`^\s*(__[us]\d+)\s+(\w+)(\[(\d+)\])?;`)

func kernelEventLayout(t *testing.T) ([]cField, int, map[string]int) {
	t.Helper()

	source, err := os.ReadFile("../../bpf/ironclaw.c")
	if err != nil {
		t.Skipf("kernel source unavailable: %v", err)
	}

	lines := strings.Split(string(source), "\n")
	inside := false
	fields := make([]cField, 0)

	for _, line := range lines {
		if strings.HasPrefix(strings.TrimSpace(line), "struct event {") {
			inside = true
			continue
		}
		if inside && strings.HasPrefix(strings.TrimSpace(line), "};") {
			break
		}
		if !inside {
			continue
		}

		match := fieldPattern.FindStringSubmatch(line)
		if match == nil {
			continue
		}

		spec, known := cTypes[match[1]]
		if !known {
			t.Fatalf("unhandled C type %q in struct event", match[1])
		}

		count := 1
		if match[4] != "" {
			count, err = strconv.Atoi(match[4])
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		}

		fields = append(fields, cField{name: match[2], size: spec.size, align: spec.align, count: count})
	}

	if len(fields) == 0 {
		t.Fatal("parsed no fields from struct event, so this test proves nothing")
	}

	offset := 0
	maxAlign := 1
	offsets := map[string]int{}

	for _, field := range fields {
		if field.align > maxAlign {
			maxAlign = field.align
		}
		if remainder := offset % field.align; remainder != 0 {
			offset += field.align - remainder
		}
		offsets[field.name] = offset
		offset += field.size * field.count
	}

	if remainder := offset % maxAlign; remainder != 0 {
		offset += maxAlign - remainder
	}

	return fields, offset, offsets
}

func TestRecordSizeMatchesTheKernelStruct(t *testing.T) {
	_, size, _ := kernelEventLayout(t)

	if ebpf.RecordSize != size {
		t.Fatalf("RecordSize is %d but struct event in bpf/ironclaw.c is %d bytes; the decoder would reject every real kernel record", ebpf.RecordSize, size)
	}
}

func TestDecoderOffsetsMatchTheKernelStruct(t *testing.T) {
	_, _, offsets := kernelEventLayout(t)

	want := map[string]int{
		"timestamp_ns": 0,
		"latency_ns":   8,
		"ret":          16,
		"pid":          24,
		"tgid":         28,
		"syscall_nr":   32,
		"failed":       36,
	}

	for name, expected := range want {
		got, present := offsets[name]
		if !present {
			t.Fatalf("struct event no longer has a %s field; the decoder still reads it", name)
		}
		if got != expected {
			t.Fatalf("field %s moved to offset %d; the decoder reads it at %d", name, got, expected)
		}
	}
}

func TestGoStatIndicesMatchTheKernelEnum(t *testing.T) {
	source, err := os.ReadFile("../../bpf/ironclaw.c")
	if err != nil {
		t.Skipf("kernel source unavailable: %v", err)
	}

	pattern := regexp.MustCompile(`STAT_([A-Z_]+)\s*=\s*(\d+)`)
	kernel := map[string]int{}
	for _, match := range pattern.FindAllStringSubmatch(string(source), -1) {
		index, err := strconv.Atoi(match[2])
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		kernel[match[1]] = index
	}

	if len(kernel) == 0 {
		t.Fatal("parsed no stat indices from the kernel source")
	}

	if got, want := kernel["MAX"], ebpf.StatCountForTest; got != want {
		t.Fatalf("kernel STAT_MAX is %d but the Go side reserves %d slots; per-CPU stat reads would be truncated or out of range", got, want)
	}

	for name, index := range map[string]int{
		"OBSERVED":         ebpf.StatObservedForTest,
		"EMITTED":          ebpf.StatEmittedForTest,
		"DROPPED_RINGBUF":  ebpf.StatDroppedRingbufForTest,
		"DROPPED_RATE":     ebpf.StatDroppedRateForTest,
		"DROPPED_FILTER":   ebpf.StatDroppedFilterForTest,
		"DROPPED_UNPAIRED": ebpf.StatDroppedUnpairedForTest,
	} {
		if kernel[name] != index {
			t.Fatalf("STAT_%s is %d in the kernel but %d in Go; stats would be attributed to the wrong counter", name, kernel[name], index)
		}
	}
}
