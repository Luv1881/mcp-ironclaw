//go:build ebpf && linux

package main

import (
	"testing"
	"time"
)

func TestKernelBootTimeYieldsAMonotonicAnchoredWallClock(t *testing.T) {
	now := time.Now()

	boot, err := kernelBootTime(now)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !boot.Before(now) {
		t.Fatalf("boot time %s is not before the current time %s", boot, now)
	}

	uptime := now.Sub(boot)
	if uptime <= 0 {
		t.Fatalf("derived uptime %s is not positive", uptime)
	}
	if uptime > 365*24*time.Hour {
		t.Fatalf("derived uptime %s is implausibly large for a running host", uptime)
	}
}
