//go:build !(ebpf && linux)

package main

import (
	"errors"
	"strings"
	"testing"
)

func TestEBPFCaptureExplainsThatTheBinaryLacksTheBuildTag(t *testing.T) {
	settings := validSettings()
	settings.mode = captureEBPF

	_, err := newCaptureSource(settings)
	if !errors.Is(err, ErrEBPFNotCompiled) {
		t.Fatalf("got %v, want ErrEBPFNotCompiled", err)
	}
	if !strings.Contains(err.Error(), "-tags ebpf") {
		t.Fatalf("error %q does not tell the operator how to enable eBPF capture", err)
	}
}
