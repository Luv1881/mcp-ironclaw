package store

import (
	"context"
	"errors"
	"sort"
	"sync"

	"github.com/ironclaw/mcp-ironclaw/internal/domain"
)

var ErrDeviceNotFound = errors.New("store: device not found")

const (
	MetricWindowsApplied   = "windows_applied"
	MetricWindowsDuplicate = "windows_duplicate"
	MetricEventsCounted    = "events_counted"
	MetricErrorsCounted    = "errors_counted"
	MetricDeviceResets     = "device_resets"
	MetricWatchDropped     = "watch_updates_dropped"
)

var (
	_ domain.StateWriter     = (*Memory)(nil)
	_ domain.StateReader     = (*Memory)(nil)
	_ domain.MetricsReader   = (*Memory)(nil)
	_ domain.MetricsRecorder = (*Memory)(nil)
)

type deviceRecord struct {
	state            domain.DeviceState
	applied          map[string]bool
	percentileSample int64
}

type Memory struct {
	devices  map[string]*deviceRecord
	byUser   map[string]map[string]bool
	metrics  map[string]int64
	watchers map[string][]*watcher
	mu       sync.RWMutex
}

func NewMemory() *Memory {
	return &Memory{
		devices:  make(map[string]*deviceRecord),
		byUser:   make(map[string]map[string]bool),
		metrics:  make(map[string]int64),
		watchers: make(map[string][]*watcher),
	}
}

func deviceKey(userID, deviceID string) string {
	return "{" + userID + "}:" + deviceID
}

func (m *Memory) ApplyWindow(ctx context.Context, window domain.AggregateWindow) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	key := deviceKey(window.Key.UserID, window.Key.DeviceID)

	record, ok := m.devices[key]
	if !ok {
		record = &deviceRecord{
			state: domain.DeviceState{
				UserID:   window.Key.UserID,
				DeviceID: window.Key.DeviceID,
			},
			applied: make(map[string]bool),
		}
		m.devices[key] = record

		if m.byUser[window.Key.UserID] == nil {
			m.byUser[window.Key.UserID] = make(map[string]bool)
		}
		m.byUser[window.Key.UserID][window.Key.DeviceID] = true
	}

	identity := window.Identity()
	if record.applied[identity] {
		m.metrics[MetricWindowsDuplicate]++
		return nil
	}
	record.applied[identity] = true

	record.state.Count += window.Count
	record.state.ErrorCount += window.ErrorCount
	record.state.Bytes += window.Bytes

	switch {
	case window.WindowID > record.state.LastWindowID:
		record.state.LastWindowID = window.WindowID
		record.state.P95Nanos = window.P95Nanos
		record.state.P99Nanos = window.P99Nanos
		record.state.LastSeen = window.WindowEnd
		record.percentileSample = window.Count

	case window.WindowID == record.state.LastWindowID:
		if window.Count > record.percentileSample {
			record.state.P95Nanos = window.P95Nanos
			record.state.P99Nanos = window.P99Nanos
			record.percentileSample = window.Count
		}
		if window.WindowEnd.After(record.state.LastSeen) {
			record.state.LastSeen = window.WindowEnd
		}
	}

	m.metrics[MetricWindowsApplied]++
	m.metrics[MetricEventsCounted] += window.Count
	m.metrics[MetricErrorsCounted] += window.ErrorCount
	m.notify(key, record.state)

	return nil
}

func (m *Memory) ResetDevice(ctx context.Context, userID, deviceID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	record, ok := m.devices[deviceKey(userID, deviceID)]
	if !ok {
		return ErrDeviceNotFound
	}

	record.state.Count = 0
	record.state.ErrorCount = 0
	record.state.Bytes = 0
	record.state.P95Nanos = 0
	record.state.P99Nanos = 0
	record.percentileSample = 0
	m.metrics[MetricDeviceResets]++
	m.notify(deviceKey(userID, deviceID), record.state)

	return nil
}

func (m *Memory) DeviceState(ctx context.Context, userID, deviceID string) (domain.DeviceState, error) {
	if err := ctx.Err(); err != nil {
		return domain.DeviceState{}, err
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	record, ok := m.devices[deviceKey(userID, deviceID)]
	if !ok {
		return domain.DeviceState{}, ErrDeviceNotFound
	}
	return record.state, nil
}

func (m *Memory) UserDevices(ctx context.Context, userID string) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	devices := make([]string, 0, len(m.byUser[userID]))
	for deviceID := range m.byUser[userID] {
		devices = append(devices, deviceID)
	}
	sort.Strings(devices)

	return devices, nil
}

func (m *Memory) PipelineMetrics(ctx context.Context) (map[string]int64, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	metrics := make(map[string]int64, len(m.metrics))
	for name, value := range m.metrics {
		metrics[name] = value
	}
	return metrics, nil
}

func (m *Memory) Increment(name string, delta int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.metrics[name] += delta
}
