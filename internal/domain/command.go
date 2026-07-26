package domain

import (
	"errors"
	"time"
)

type CommandKind uint8

const (
	CommandUnknown CommandKind = iota
	CommandResetCounters
	CommandQuarantineDevice
)

var commandKindNames = map[CommandKind]string{
	CommandResetCounters:    "reset_counters",
	CommandQuarantineDevice: "quarantine_device",
}

func (k CommandKind) String() string {
	if name, ok := commandKindNames[k]; ok {
		return name
	}
	return "unknown"
}

func (k CommandKind) Valid() bool {
	_, ok := commandKindNames[k]
	return ok
}

var (
	ErrInvalidCommand = errors.New("domain: command kind is not recognised")
	ErrMissingActor   = errors.New("domain: command actor is empty")
)

type Command struct {
	Kind     CommandKind
	UserID   string
	DeviceID string
	Actor    string
	IssuedAt time.Time
}

func (c Command) Validate() error {
	if !c.Kind.Valid() {
		return ErrInvalidCommand
	}
	if c.UserID == "" {
		return ErrMissingUserID
	}
	if c.DeviceID == "" {
		return ErrMissingDeviceID
	}
	if c.Actor == "" {
		return ErrMissingActor
	}
	return nil
}

func (c Command) ShardTag() string {
	return c.UserID
}
