package main

import (
	"errors"
	"math"
	"testing"
	"time"
)

func validSettings() captureSettings {
	return captureSettings{
		mode:          captureSynthetic,
		deviceID:      "device-000",
		userID:        "user-000",
		podID:         "on-prem",
		events:        10,
		interval:      time.Millisecond,
		tailFraction:  0.05,
		sampleModulus: 1,
	}
}

func TestSyntheticCaptureIsAvailableWithoutKernelPrivileges(t *testing.T) {
	source, err := newCaptureSource(validSettings())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer source.Close()

	if source == nil {
		t.Fatal("synthetic capture returned no source")
	}
}

func TestUnknownCaptureSourceIsRefused(t *testing.T) {
	settings := validSettings()
	settings.mode = "telepathy"

	_, err := newCaptureSource(settings)
	if !errors.Is(err, ErrUnknownCaptureSource) {
		t.Fatalf("got %v, want ErrUnknownCaptureSource", err)
	}
}

func TestKernelFieldValuesWiderThanAUint32AreRefused(t *testing.T) {
	for name, value := range map[string]uint64{
		"sample modulus": uint64(math.MaxUint32) + 1,
		"target tgid":    uint64(math.MaxUint32) + 1,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := narrowUint32(name, value); !errors.Is(err, ErrCaptureSettingRange) {
				t.Fatalf("got %v, want ErrCaptureSettingRange rather than a silent truncation", err)
			}
		})
	}
}

func TestKernelFieldValuesAtTheFieldWidthAreAccepted(t *testing.T) {
	got, err := narrowUint32("sample modulus", uint64(math.MaxUint32))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != math.MaxUint32 {
		t.Fatalf("narrowed to %d, want %d", got, uint32(math.MaxUint32))
	}
}
