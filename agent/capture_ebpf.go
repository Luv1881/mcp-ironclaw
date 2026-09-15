//go:build ebpf && linux

package main

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/ironclaw/mcp-ironclaw/internal/domain"
	"github.com/ironclaw/mcp-ironclaw/internal/ebpf"
)

var ErrProcUptime = errors.New("agent: reading kernel uptime")

func newEBPFSource(settings captureSettings) (domain.EventSource, error) {
	boot, err := kernelBootTime(time.Now())
	if err != nil {
		return nil, err
	}

	return ebpf.NewSource(ebpf.Translator{
		DeviceID: settings.deviceID,
		UserID:   settings.userID,
		PodID:    settings.podID,
		BootTime: boot,
	}, ebpf.Settings{
		MinLatency:               settings.minLatency,
		MaxEventsPerCPUPerSecond: settings.maxEventsPerCPU,
		SampleModulus:            settings.sampleModulus,
		TargetTGID:               settings.targetTGID,
	})
}

func kernelBootTime(now time.Time) (time.Time, error) {
	raw, err := os.ReadFile("/proc/uptime")
	if err != nil {
		return time.Time{}, fmt.Errorf("%w: %w", ErrProcUptime, err)
	}

	fields := strings.Fields(string(raw))
	if len(fields) == 0 {
		return time.Time{}, fmt.Errorf("%w: file is empty", ErrProcUptime)
	}

	seconds, err := strconv.ParseFloat(fields[0], 64)
	if err != nil {
		return time.Time{}, fmt.Errorf("%w: %w", ErrProcUptime, err)
	}

	return ebpf.BootTime(now, uint64(seconds*float64(time.Second))), nil
}
