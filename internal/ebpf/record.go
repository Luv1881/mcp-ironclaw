package ebpf

import (
	"encoding/binary"
	"errors"
	"time"

	"github.com/ironclaw/mcp-ironclaw/internal/domain"
)

const RecordSize = 40

var (
	ErrShortRecord     = errors.New("ebpf: record is shorter than the kernel event layout")
	ErrMissingIdentity = errors.New("ebpf: device and user identity are required")
	ErrZeroBootTime    = errors.New("ebpf: boot time is required to convert monotonic timestamps")
)

type Record struct {
	TimestampNanos uint64
	LatencyNanos   uint64
	Return         int64
	PID            uint32
	TGID           uint32
	SyscallNumber  int32
	Failed         bool
}

func Decode(raw []byte) (Record, error) {
	if len(raw) < RecordSize {
		return Record{}, ErrShortRecord
	}

	return Record{
		TimestampNanos: binary.LittleEndian.Uint64(raw[0:8]),
		LatencyNanos:   binary.LittleEndian.Uint64(raw[8:16]),
		Return:         int64(binary.LittleEndian.Uint64(raw[16:24])),
		PID:            binary.LittleEndian.Uint32(raw[24:28]),
		TGID:           binary.LittleEndian.Uint32(raw[28:32]),
		SyscallNumber:  int32(binary.LittleEndian.Uint32(raw[32:36])),
		Failed:         raw[36] != 0,
	}, nil
}

type Translator struct {
	DeviceID string
	UserID   string
	PodID    string
	BootTime time.Time
}

func (t Translator) Validate() error {
	if t.DeviceID == "" || t.UserID == "" {
		return ErrMissingIdentity
	}
	if t.BootTime.IsZero() {
		return ErrZeroBootTime
	}
	return nil
}

func (t Translator) Event(record Record) (domain.Event, error) {
	if err := t.Validate(); err != nil {
		return domain.Event{}, err
	}

	event := domain.Event{
		DeviceID:     t.DeviceID,
		UserID:       t.UserID,
		PodID:        t.PodID,
		ProcessID:    int32(record.TGID),
		Kind:         domain.EventKindSyscall,
		ObservedAt:   t.BootTime.Add(time.Duration(record.TimestampNanos)),
		LatencyNanos: int64(record.LatencyNanos),
		Failed:       record.Failed,
	}

	if err := event.Validate(); err != nil {
		return domain.Event{}, err
	}

	return event, nil
}

func BootTime(now time.Time, monotonicNanos uint64) time.Time {
	return now.Add(-time.Duration(monotonicNanos))
}
