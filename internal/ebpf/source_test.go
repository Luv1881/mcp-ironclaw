//go:build ebpf && linux

package ebpf

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/cilium/ebpf"
)

func TestCompiledObjectExposesExpectedProgramsAndMaps(t *testing.T) {
	spec, err := ebpf.LoadCollectionSpecFromReader(bytes.NewReader(program))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for _, name := range []string{"ironclaw_sys_enter", "ironclaw_sys_exit"} {
		if _, ok := spec.Programs[name]; !ok {
			t.Fatalf("program %q is missing from the compiled object", name)
		}
	}

	for _, name := range []string{"events", "start_times", "settings", "rate_limit", "stats"} {
		if _, ok := spec.Maps[name]; !ok {
			t.Fatalf("map %q is missing from the compiled object", name)
		}
	}
}

func TestRingBufferIsSizedForBurstAbsorption(t *testing.T) {
	spec, err := ebpf.LoadCollectionSpecFromReader(bytes.NewReader(program))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	events := spec.Maps["events"]
	if events.Type != ebpf.RingBuf {
		t.Fatalf("events map is %v, want a ring buffer", events.Type)
	}
	if events.MaxEntries < 64*1024 {
		t.Fatalf("ring buffer is %d bytes, want at least 65536", events.MaxEntries)
	}
}

func TestStatsMapIsPerCPUToAvoidContention(t *testing.T) {
	spec, err := ebpf.LoadCollectionSpecFromReader(bytes.NewReader(program))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for _, name := range []string{"stats", "rate_limit"} {
		if spec.Maps[name].Type != ebpf.PerCPUArray {
			t.Fatalf("map %q is %v, want a per-CPU array", name, spec.Maps[name].Type)
		}
	}
}

func TestNewSourceValidatesIdentity(t *testing.T) {
	_, err := NewSource(Translator{}, Settings{})
	if !errors.Is(err, ErrMissingIdentity) {
		t.Fatalf("got %v, want ErrMissingIdentity", err)
	}
}

func TestSourceRefusesConcurrentStarts(t *testing.T) {
	source, err := NewSource(Translator{
		DeviceID: "device-1",
		UserID:   "user-1",
		BootTime: time.Unix(1700000000, 0),
	}, Settings{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer source.Close()

	source.mu.Lock()
	source.started = true
	source.mu.Unlock()

	if _, err := source.Events(t.Context()); !errors.Is(err, ErrAlreadyStarted) {
		t.Fatalf("got %v, want ErrAlreadyStarted", err)
	}
}
