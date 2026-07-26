package domain

import (
	"fmt"
	"time"
)

type CorrelationKey struct {
	UserID    string
	DeviceID  string
	ProcessID int32
	PodID     string
}

func (k CorrelationKey) String() string {
	return fmt.Sprintf("{%s}:%s:%d:%s", k.UserID, k.DeviceID, k.ProcessID, k.PodID)
}

func (k CorrelationKey) ShardTag() string {
	return k.UserID
}

func (k CorrelationKey) DeviceScope() string {
	return fmt.Sprintf("{%s}:%s", k.UserID, k.DeviceID)
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
	return fmt.Sprintf("%s#%d#%d", w.Key.String(), w.WindowID, w.Sequence)
}

func (w AggregateWindow) WindowIdentity() string {
	return fmt.Sprintf("%s#%d", w.Key.String(), w.WindowID)
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
