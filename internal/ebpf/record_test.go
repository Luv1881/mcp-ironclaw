package ebpf_test

import (
	"encoding/binary"
	"errors"
	"testing"
	"time"

	"github.com/ironclaw/mcp-ironclaw/internal/domain"
	"github.com/ironclaw/mcp-ironclaw/internal/ebpf"
)

func encode(record ebpf.Record) []byte {
	raw := make([]byte, ebpf.RecordSize)

	binary.LittleEndian.PutUint64(raw[0:8], record.TimestampNanos)
	binary.LittleEndian.PutUint64(raw[8:16], record.LatencyNanos)
	binary.LittleEndian.PutUint64(raw[16:24], uint64(record.Return))
	binary.LittleEndian.PutUint32(raw[24:28], record.PID)
	binary.LittleEndian.PutUint32(raw[28:32], record.TGID)
	binary.LittleEndian.PutUint32(raw[32:36], uint32(record.SyscallNumber))
	if record.Failed {
		raw[36] = 1
	}

	return raw
}

func sampleRecord() ebpf.Record {
	return ebpf.Record{
		TimestampNanos: 5_000_000_000,
		LatencyNanos:   1_500_000,
		PID:            4242,
		TGID:           4200,
		SyscallNumber:  257,
		Return:         3,
	}
}

func translator() ebpf.Translator {
	return ebpf.Translator{
		DeviceID: "device-1",
		UserID:   "user-1",
		PodID:    "pod-a",
		BootTime: time.Unix(1700000000, 0).UTC(),
	}
}

func TestDecodeRoundTrip(t *testing.T) {
	want := sampleRecord()

	got, err := ebpf.Decode(encode(want))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != want {
		t.Fatalf("decoded %+v, want %+v", got, want)
	}
}

func TestDecodeRejectsTruncatedRecord(t *testing.T) {
	raw := encode(sampleRecord())

	for _, size := range []int{0, 1, ebpf.RecordSize - 1} {
		if _, err := ebpf.Decode(raw[:size]); !errors.Is(err, ebpf.ErrShortRecord) {
			t.Fatalf("size %d: got %v, want ErrShortRecord", size, err)
		}
	}
}

func TestDecodeHandlesNegativeSyscallReturn(t *testing.T) {
	record := sampleRecord()
	record.Return = -2
	record.Failed = true

	got, err := ebpf.Decode(encode(record))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Return != -2 {
		t.Fatalf("return %d, want -2", got.Return)
	}
	if !got.Failed {
		t.Fatal("failed flag was not decoded")
	}
}

func TestDecodeIgnoresTrailingBytes(t *testing.T) {
	raw := append(encode(sampleRecord()), 0xff, 0xff)

	got, err := ebpf.Decode(raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != sampleRecord() {
		t.Fatalf("decoded %+v, want %+v", got, sampleRecord())
	}
}

func TestEventConvertsMonotonicTimestampToWallClock(t *testing.T) {
	event, err := translator().Event(sampleRecord())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	want := time.Unix(1700000005, 0).UTC()
	if !event.ObservedAt.Equal(want) {
		t.Fatalf("observed at %v, want %v", event.ObservedAt, want)
	}
	if event.LatencyNanos != 1_500_000 {
		t.Fatalf("latency %d, want 1500000", event.LatencyNanos)
	}
	if event.ProcessID != 4200 {
		t.Fatalf("process id %d, want the thread group id 4200", event.ProcessID)
	}
	if event.Kind != domain.EventKindSyscall {
		t.Fatalf("kind %v, want syscall", event.Kind)
	}
}

func TestEventProducesDomainValidEvents(t *testing.T) {
	event, err := translator().Event(sampleRecord())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := event.Validate(); err != nil {
		t.Fatalf("translated event failed domain validation: %v", err)
	}
}

func TestEventRejectsIncompleteTranslator(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(*ebpf.Translator)
		wantErr error
	}{
		{"missing device", func(tr *ebpf.Translator) { tr.DeviceID = "" }, ebpf.ErrMissingIdentity},
		{"missing user", func(tr *ebpf.Translator) { tr.UserID = "" }, ebpf.ErrMissingIdentity},
		{"zero boot time", func(tr *ebpf.Translator) { tr.BootTime = time.Time{} }, ebpf.ErrZeroBootTime},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tr := translator()
			tc.mutate(&tr)

			if _, err := tr.Event(sampleRecord()); !errors.Is(err, tc.wantErr) {
				t.Fatalf("got %v, want %v", err, tc.wantErr)
			}
		})
	}
}

func TestBootTimeDerivesTheMonotonicOrigin(t *testing.T) {
	now := time.Unix(1700000060, 0).UTC()

	boot := ebpf.BootTime(now, 60_000_000_000)

	if !boot.Equal(time.Unix(1700000000, 0).UTC()) {
		t.Fatalf("boot time %v, want 1700000000", boot)
	}
}

func TestTranslatedEventsSurviveTheDomainBatchIdentityCheck(t *testing.T) {
	event, err := translator().Event(sampleRecord())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	batch := domain.Batch{DeviceID: "device-1", Events: []domain.Event{event}}

	if err := batch.VerifyIdentity("device-1"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := batch.Validate(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLargeSyscallReturnIsNotTruncatedOrMisclassified(t *testing.T) {
	record := sampleRecord()
	record.Return = 0x7FFF12345678
	record.Failed = false

	decoded, err := ebpf.Decode(encode(record))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if decoded.Return != 0x7FFF12345678 {
		t.Fatalf("return %#x, want %#x: a 64-bit mmap style address must survive", decoded.Return, int64(0x7FFF12345678))
	}
	if decoded.Failed {
		t.Fatal("a large positive return value must not be classified as a failure")
	}
}

func TestErrnoRangeReturnIsClassifiedAsFailure(t *testing.T) {
	record := sampleRecord()
	record.Return = -2
	record.Failed = true

	decoded, err := ebpf.Decode(encode(record))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if decoded.Return != -2 || !decoded.Failed {
		t.Fatalf("decoded %+v, want a failed syscall with return -2", decoded)
	}
}
