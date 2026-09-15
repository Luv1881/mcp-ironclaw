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
	sampleModulus   uint
	targetTGID      uint
}

func (s captureSettings) validate() error {
	if s.sampleModulus > math.MaxUint32 {
		return fmt.Errorf("%w: sample modulus %d exceeds %d", ErrCaptureSettingRange, s.sampleModulus, uint64(math.MaxUint32))
	}
	if s.targetTGID > math.MaxUint32 {
		return fmt.Errorf("%w: target tgid %d exceeds %d", ErrCaptureSettingRange, s.targetTGID, uint64(math.MaxUint32))
	}
	return nil
}

func newCaptureSource(settings captureSettings) (domain.EventSource, error) {
	if err := settings.validate(); err != nil {
		return nil, err
	}

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
