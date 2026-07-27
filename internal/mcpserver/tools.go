package mcpserver

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ironclaw/mcp-ironclaw/internal/domain"
)

var (
	ErrNilStateReader   = errors.New("mcpserver: device state reader is not configured")
	ErrNilDeviceLister  = errors.New("mcpserver: user device lister is not configured")
	ErrNilMetricsReader = errors.New("mcpserver: pipeline metrics reader is not configured")
	ErrMissingUserID    = errors.New("mcpserver: user id is required")
	ErrMissingDevice    = errors.New("mcpserver: device id is required")
	ErrMissingActor     = errors.New("mcpserver: actor is required")
	ErrWritesDisabled   = errors.New("mcpserver: command publishing is not configured")
	ErrNilDeviceWatcher = errors.New("mcpserver: device watcher is not configured")
	ErrWatchClosed      = errors.New("mcpserver: device watch closed before an update arrived")
)

type DeviceStateInput struct {
	UserID   string `json:"user_id"`
	DeviceID string `json:"device_id"`
}

type DeviceStateOutput struct {
	UserID       string `json:"user_id"`
	DeviceID     string `json:"device_id"`
	Count        int64  `json:"count"`
	ErrorCount   int64  `json:"error_count"`
	Bytes        int64  `json:"bytes"`
	P95Nanos     int64  `json:"p95_nanos"`
	P99Nanos     int64  `json:"p99_nanos"`
	LastWindowID int64  `json:"last_window_id"`
	LastSeen     string `json:"last_seen"`
	Stale        bool   `json:"stale"`
}

type UserDevicesInput struct {
	UserID string `json:"user_id"`
}

type UserDevicesOutput struct {
	UserID  string   `json:"user_id"`
	Devices []string `json:"devices"`
	Count   int      `json:"count"`
}

type PipelineMetricsInput struct{}

type PipelineMetricsOutput struct {
	Metrics map[string]int64 `json:"metrics"`
}

const (
	DefaultWatchTimeout = 30 * time.Second
	MaxWatchTimeout     = 5 * time.Minute
)

type WatchDeviceInput struct {
	UserID         string `json:"user_id"`
	DeviceID       string `json:"device_id"`
	TimeoutSeconds int    `json:"timeout_seconds"`
}

type WatchDeviceOutput struct {
	UserID    string            `json:"user_id"`
	DeviceID  string            `json:"device_id"`
	Updated   bool              `json:"updated"`
	TimedOut  bool              `json:"timed_out"`
	WaitedFor string            `json:"waited_for"`
	State     DeviceStateOutput `json:"state"`
}

type ResetCountersInput struct {
	UserID   string `json:"user_id"`
	DeviceID string `json:"device_id"`
	Actor    string `json:"actor"`
}

type ResetCountersOutput struct {
	Accepted bool   `json:"accepted"`
	Command  string `json:"command"`
}

type Tools struct {
	state       domain.DeviceStateReader
	devices     domain.UserDeviceLister
	metrics     domain.MetricsReader
	commands    domain.CommandPublisher
	watcher     domain.DeviceWatcher
	clock       domain.Clock
	requireAuth bool
}

type Options struct {
	State       domain.DeviceStateReader
	Devices     domain.UserDeviceLister
	Metrics     domain.MetricsReader
	Commands    domain.CommandPublisher
	Watcher     domain.DeviceWatcher
	Clock       domain.Clock
	RequireAuth bool
}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now().UTC() }

func NewTools(options Options) (*Tools, error) {
	if options.State == nil {
		return nil, ErrNilStateReader
	}
	clock := options.Clock
	if clock == nil {
		clock = systemClock{}
	}
	return &Tools{
		state:       options.State,
		devices:     options.Devices,
		metrics:     options.Metrics,
		commands:    options.Commands,
		watcher:     options.Watcher,
		clock:       clock,
		requireAuth: options.RequireAuth,
	}, nil
}

func (t *Tools) DeviceState(ctx context.Context, input DeviceStateInput) (DeviceStateOutput, error) {
	if input.UserID == "" {
		return DeviceStateOutput{}, ErrMissingUserID
	}
	if input.DeviceID == "" {
		return DeviceStateOutput{}, ErrMissingDevice
	}
	if err := t.authorise(ctx, input.UserID); err != nil {
		return DeviceStateOutput{}, err
	}

	state, err := t.state.DeviceState(ctx, input.UserID, input.DeviceID)
	if err != nil {
		return DeviceStateOutput{}, err
	}

	return stateOutput(state), nil
}

func stateOutput(state domain.DeviceState) DeviceStateOutput {
	lastSeen := ""
	if !state.LastSeen.IsZero() {
		lastSeen = state.LastSeen.UTC().Format(time.RFC3339)
	}

	return DeviceStateOutput{
		UserID:       state.UserID,
		DeviceID:     state.DeviceID,
		Count:        state.Count,
		ErrorCount:   state.ErrorCount,
		Bytes:        state.Bytes,
		P95Nanos:     state.P95Nanos,
		P99Nanos:     state.P99Nanos,
		LastWindowID: state.LastWindowID,
		LastSeen:     lastSeen,
		Stale:        state.Stale,
	}
}

func (t *Tools) UserDevices(ctx context.Context, input UserDevicesInput) (UserDevicesOutput, error) {
	if input.UserID == "" {
		return UserDevicesOutput{}, ErrMissingUserID
	}
	if t.devices == nil {
		return UserDevicesOutput{}, ErrNilDeviceLister
	}
	if err := t.authorise(ctx, input.UserID); err != nil {
		return UserDevicesOutput{}, err
	}

	devices, err := t.devices.UserDevices(ctx, input.UserID)
	if err != nil {
		return UserDevicesOutput{}, err
	}

	return UserDevicesOutput{UserID: input.UserID, Devices: devices, Count: len(devices)}, nil
}

func (t *Tools) PipelineMetrics(ctx context.Context, _ PipelineMetricsInput) (PipelineMetricsOutput, error) {
	if t.metrics == nil {
		return PipelineMetricsOutput{}, ErrNilMetricsReader
	}
	if principal, ok := PrincipalFrom(ctx); ok {
		if !principal.hasScope(ScopeAdmin) {
			return PipelineMetricsOutput{}, fmt.Errorf("%w: fleet metrics require %s", ErrForbidden, ScopeAdmin)
		}
	} else if t.requireAuth {
		return PipelineMetricsOutput{}, ErrUnauthenticated
	}

	metrics, err := t.metrics.PipelineMetrics(ctx)
	if err != nil {
		return PipelineMetricsOutput{}, err
	}

	return PipelineMetricsOutput{Metrics: metrics}, nil
}

func (t *Tools) ResetCounters(ctx context.Context, input ResetCountersInput) (ResetCountersOutput, error) {
	if t.commands == nil {
		return ResetCountersOutput{}, ErrWritesDisabled
	}
	if input.UserID == "" {
		return ResetCountersOutput{}, ErrMissingUserID
	}
	if input.DeviceID == "" {
		return ResetCountersOutput{}, ErrMissingDevice
	}
	if input.Actor == "" {
		return ResetCountersOutput{}, ErrMissingActor
	}
	if err := t.authorise(ctx, input.UserID); err != nil {
		return ResetCountersOutput{}, err
	}

	command := domain.Command{
		Kind:     domain.CommandResetCounters,
		UserID:   input.UserID,
		DeviceID: input.DeviceID,
		Actor:    t.actor(ctx, input.Actor),
		IssuedAt: t.clock.Now(),
	}

	if err := command.Validate(); err != nil {
		return ResetCountersOutput{}, err
	}
	if err := t.commands.PublishCommand(ctx, command); err != nil {
		return ResetCountersOutput{}, err
	}

	return ResetCountersOutput{Accepted: true, Command: command.Kind.String()}, nil
}

func (t *Tools) actor(ctx context.Context, claimed string) string {
	principal, ok := PrincipalFrom(ctx)
	if !ok || principal.UserID == "" {
		return claimed
	}
	if principal.hasScope(ScopeAdmin) && claimed != "" {
		return claimed
	}
	return principal.UserID
}

func (t *Tools) WatchDevice(ctx context.Context, input WatchDeviceInput) (WatchDeviceOutput, error) {
	if input.UserID == "" {
		return WatchDeviceOutput{}, ErrMissingUserID
	}
	if input.DeviceID == "" {
		return WatchDeviceOutput{}, ErrMissingDevice
	}
	if t.watcher == nil {
		return WatchDeviceOutput{}, ErrNilDeviceWatcher
	}
	if err := t.authorise(ctx, input.UserID); err != nil {
		return WatchDeviceOutput{}, err
	}

	wait := time.Duration(input.TimeoutSeconds) * time.Second
	if wait <= 0 {
		wait = DefaultWatchTimeout
	}
	if wait > MaxWatchTimeout {
		wait = MaxWatchTimeout
	}

	watchCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	updates, err := t.watcher.WatchDevice(watchCtx, input.UserID, input.DeviceID)
	if err != nil {
		return WatchDeviceOutput{}, err
	}

	timer := time.NewTimer(wait)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return WatchDeviceOutput{}, ctx.Err()

	case <-timer.C:
		return WatchDeviceOutput{
			UserID:    input.UserID,
			DeviceID:  input.DeviceID,
			TimedOut:  true,
			WaitedFor: wait.String(),
		}, nil

	case state, ok := <-updates:
		if !ok {
			return WatchDeviceOutput{}, ErrWatchClosed
		}
		return WatchDeviceOutput{
			UserID:    input.UserID,
			DeviceID:  input.DeviceID,
			Updated:   true,
			State:     stateOutput(state),
			WaitedFor: wait.String(),
		}, nil
	}
}
