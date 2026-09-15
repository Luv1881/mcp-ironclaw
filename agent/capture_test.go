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

func TestCaptureSettingsRejectValuesWiderThanTheKernelField(t *testing.T) {
	if math.MaxUint32 == ^uint(0) {
		t.Skip("uint is 32 bits wide on this platform, so the overflow cannot be expressed")
	}

	tests := map[string]func(*captureSettings){
		"sample modulus": func(settings *captureSettings) { settings.sampleModulus = math.MaxUint32 + 1 },
		"target tgid":    func(settings *captureSettings) { settings.targetTGID = math.MaxUint32 + 1 },
	}

	for name, corrupt := range tests {
		t.Run(name, func(t *testing.T) {
			settings := validSettings()
			corrupt(&settings)

			if _, err := newCaptureSource(settings); !errors.Is(err, ErrCaptureSettingRange) {
				t.Fatalf("got %v, want ErrCaptureSettingRange rather than a silent truncation", err)
			}
		})
	}
}

func TestCaptureSettingsAtTheFieldWidthAreAccepted(t *testing.T) {
	settings := validSettings()
	settings.mode = captureEBPF
	settings.sampleModulus = math.MaxUint32
	settings.targetTGID = 0

	if err := settings.validate(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
