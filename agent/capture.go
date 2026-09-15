package main

import (
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/ironclaw/mcp-ironclaw/internal/domain"
	"github.com/ironclaw/mcp-ironclaw/internal/pipeline"
)

const (
	captureSynthetic = "synthetic"
	captureEBPF      = "ebpf"
)

var (
	ErrUnknownCaptureSource = errors.New("agent: unknown capture source")
	ErrCaptureSettingRange  = errors.New("agent: capture setting does not fit the kernel field width")
)

type captureSettings struct {
	mode            string
	deviceID        string
	userID          string
	podID           string
	events          int
	interval        time.Duration
	tailFraction    float64
	minLatency      time.Duration
	maxEventsPerCPU uint64
	sampleModulus   uint32
	targetTGID      uint32
}

func narrowUint32(name string, value uint64) (uint32, error) {
	if value > math.MaxUint32 {
		return 0, fmt.Errorf("%w: %s %d exceeds %d", ErrCaptureSettingRange, name, value, uint64(math.MaxUint32))
	}
	return uint32(value), nil
}

func newCaptureSource(settings captureSettings) (domain.EventSource, error) {
	switch settings.mode {
	case captureSynthetic:
		return newSyntheticSource(settings)
	case captureEBPF:
		return newEBPFSource(settings)
	default:
		return nil, fmt.Errorf("%w: %q", ErrUnknownCaptureSource, settings.mode)
	}
}

func newSyntheticSource(settings captureSettings) (domain.EventSource, error) {
	return pipeline.NewSyntheticSource(pipeline.SyntheticConfig{
		DeviceID:     settings.deviceID,
		UserID:       settings.userID,
		PodID:        settings.podID,
		ProcessIDs:   []int32{101, 102, 103, 104},
		EventCount:   settings.events,
		Interval:     settings.interval,
		ErrorRate:    0.05,
		BaseLatency:  time.Millisecond,
		TailLatency:  40 * time.Millisecond,
		TailFraction: settings.tailFraction,
		Seed:         time.Now().UnixNano(),
	})
}
