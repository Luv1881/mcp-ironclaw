package domain

import (
	"errors"
	"fmt"
	"time"
)

type EventKind uint8

const (
	EventKindUnknown EventKind = iota
	EventKindSyscall
	EventKindNetwork
	EventKindProcessExec
	EventKindProcessExit
)

var eventKindNames = map[EventKind]string{
	EventKindUnknown:     "unknown",
	EventKindSyscall:     "syscall",
	EventKindNetwork:     "network",
	EventKindProcessExec: "process_exec",
	EventKindProcessExit: "process_exit",
}

func (k EventKind) String() string {
	if name, ok := eventKindNames[k]; ok {
		return name
	}
	return "unknown"
}

func (k EventKind) Valid() bool {
	_, ok := eventKindNames[k]
	return ok
}

var (
	ErrMissingDeviceID = errors.New("domain: device id is empty")
	ErrMissingUserID   = errors.New("domain: user id is empty")
	ErrInvalidKind     = errors.New("domain: event kind is not recognised")
	ErrNegativeLatency = errors.New("domain: latency must not be negative")
	ErrZeroTimestamp   = errors.New("domain: observed timestamp is zero")
	ErrDeviceMismatch  = errors.New("domain: event device id does not match authenticated identity")
)

type Event struct {
	DeviceID     string
	UserID       string
	ProcessID    int32
	PodID        string
	Kind         EventKind
	ObservedAt   time.Time
	LatencyNanos int64
	Bytes        int64
	Failed       bool
}

func (e Event) Validate() error {
	if e.DeviceID == "" {
		return ErrMissingDeviceID
	}
	if e.UserID == "" {
		return ErrMissingUserID
	}
	if !e.Kind.Valid() {
		return ErrInvalidKind
	}
	if e.LatencyNanos < 0 {
		return ErrNegativeLatency
	}
	if e.ObservedAt.IsZero() {
		return ErrZeroTimestamp
	}
	return nil
}

func (e Event) CorrelationKey() CorrelationKey {
	return CorrelationKey{
		UserID:    e.UserID,
		DeviceID:  e.DeviceID,
		ProcessID: e.ProcessID,
		PodID:     e.PodID,
	}
}

type Batch struct {
	DeviceID  string
	CreatedAt time.Time
	Events    []Event
}

func (b Batch) Validate() error {
	if b.DeviceID == "" {
		return ErrMissingDeviceID
	}
	for i, event := range b.Events {
		if err := event.Validate(); err != nil {
			return fmt.Errorf("domain: batch event %d invalid: %w", i, err)
		}
		if event.DeviceID != b.DeviceID {
			return fmt.Errorf("domain: batch event %d: %w", i, ErrDeviceMismatch)
		}
	}
	return nil
}

func (b Batch) VerifyIdentity(authenticatedDeviceID string) error {
	if b.DeviceID != authenticatedDeviceID {
		return fmt.Errorf("domain: batch claims %q but connection is %q: %w", b.DeviceID, authenticatedDeviceID, ErrDeviceMismatch)
	}
	for i, event := range b.Events {
		if event.DeviceID != authenticatedDeviceID {
			return fmt.Errorf("domain: event %d claims %q but connection is %q: %w", i, event.DeviceID, authenticatedDeviceID, ErrDeviceMismatch)
		}
	}
	return nil
}

func (b Batch) Len() int {
	return len(b.Events)
}
