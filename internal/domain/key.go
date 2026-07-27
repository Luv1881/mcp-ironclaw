package domain

import (
	"strconv"
	"time"
)

type CorrelationKey struct {
	UserID    string
	DeviceID  string
	ProcessID int32
	PodID     string
}

func (k CorrelationKey) String() string {
	return string(k.append(make([]byte, 0, k.formattedLen())))
}

func (k CorrelationKey) formattedLen() int {
	return len(k.UserID) + len(k.DeviceID) + len(k.PodID) + 16
}

func (k CorrelationKey) append(buffer []byte) []byte {
	buffer = append(buffer, '{')
	buffer = append(buffer, k.UserID...)
	buffer = append(buffer, '}', ':')
	buffer = append(buffer, k.DeviceID...)
	buffer = append(buffer, ':')
	buffer = strconv.AppendInt(buffer, int64(k.ProcessID), 10)
	buffer = append(buffer, ':')

	return append(buffer, k.PodID...)
}

func (k CorrelationKey) ShardTag() string {
	return k.UserID
}

func (k CorrelationKey) DeviceScope() string {
	buffer := make([]byte, 0, len(k.UserID)+len(k.DeviceID)+4)
	buffer = append(buffer, '{')
	buffer = append(buffer, k.UserID...)
	buffer = append(buffer, '}', ':')
	buffer = append(buffer, k.DeviceID...)

	return string(buffer)
}

type AggregateWindow struct {
	Key         CorrelationKey
	WindowID    int64
	Sequence    int64
	WindowStart time.Time
	WindowEnd   time.Time
	Count       int64
	ErrorCount  int64
	Bytes       int64
	P95Nanos    int64
	P99Nanos    int64
	MaxNanos    int64
}

func (w AggregateWindow) Identity() string {
	buffer := w.Key.append(make([]byte, 0, w.Key.formattedLen()+42))
	buffer = append(buffer, '#')
	buffer = strconv.AppendInt(buffer, w.WindowID, 10)
	buffer = append(buffer, '#')
	buffer = strconv.AppendInt(buffer, w.Sequence, 10)

	return string(buffer)
}

func (w AggregateWindow) WindowIdentity() string {
	buffer := w.Key.append(make([]byte, 0, w.Key.formattedLen()+21))
	buffer = append(buffer, '#')
	buffer = strconv.AppendInt(buffer, w.WindowID, 10)

	return string(buffer)
}

type DeviceState struct {
	UserID       string
	DeviceID     string
	Count        int64
	ErrorCount   int64
	Bytes        int64
	P95Nanos     int64
	P99Nanos     int64
	LastWindowID int64
	LastSeen     time.Time
	Stale        bool
}
