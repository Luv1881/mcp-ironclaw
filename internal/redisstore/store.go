package redisstore

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/ironclaw/mcp-ironclaw/internal/domain"
	"github.com/redis/go-redis/v9"
)

var (
	ErrNilClient      = errors.New("redisstore: client is nil")
	ErrDeviceNotFound = errors.New("redisstore: device not found")
)

const (
	MetricWindowsApplied   = "windows_applied"
	MetricWindowsDuplicate = "windows_duplicate"
	MetricEventsCounted    = "events_counted"
	MetricErrorsCounted    = "errors_counted"
	MetricDeviceResets     = "device_resets"
	MetricWatchDropped     = "watch_updates_dropped"
)

const (
	defaultDedupeTTL    = 6 * time.Hour
	metricsWriteTimeout = 250 * time.Millisecond
)

var applyWindow = redis.NewScript(`
local identity = ARGV[1]
if redis.call('ZSCORE', KEYS[2], identity) then
  return 0
end

local now = tonumber(ARGV[12])
redis.call('ZADD', KEYS[2], now, identity)
redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', now - tonumber(ARGV[10]))
redis.call('EXPIRE', KEYS[2], ARGV[10])
redis.call('SADD', KEYS[3], ARGV[2])

redis.call('HSETNX', KEYS[1], 'user_id', ARGV[11])
redis.call('HSETNX', KEYS[1], 'device_id', ARGV[2])

redis.call('HINCRBY', KEYS[1], 'count', ARGV[3])
redis.call('HINCRBY', KEYS[1], 'error_count', ARGV[4])
redis.call('HINCRBY', KEYS[1], 'bytes', ARGV[5])

local incoming = tonumber(ARGV[8])
local current = tonumber(redis.call('HGET', KEYS[1], 'last_window_id') or '-1')
local incomingCount = tonumber(ARGV[3])

if incoming > current then
  redis.call('HSET', KEYS[1], 'last_window_id', ARGV[8], 'p95_nanos', ARGV[6], 'p99_nanos', ARGV[7],
             'last_seen', ARGV[9], 'percentile_sample', ARGV[3])
elseif incoming == current then
  local sample = tonumber(redis.call('HGET', KEYS[1], 'percentile_sample') or '0')
  if incomingCount > sample then
    redis.call('HSET', KEYS[1], 'p95_nanos', ARGV[6], 'p99_nanos', ARGV[7], 'percentile_sample', ARGV[3])
  end
  if ARGV[9] ~= '' then
    redis.call('HSET', KEYS[1], 'last_seen', ARGV[9])
  end
end

redis.call('SPUBLISH', KEYS[4], ARGV[8])

return 1
`)

var (
	_ domain.StateWriter     = (*Store)(nil)
	_ domain.StateReader     = (*Store)(nil)
	_ domain.MetricsReader   = (*Store)(nil)
	_ domain.MetricsRecorder = (*Store)(nil)
)

type Options struct {
	Client     redis.UniversalClient
	Namespace  string
	DedupeTTL  time.Duration
	MetricsKey string
	Recorder   domain.MetricsRecorder
}

type Store struct {
	client     redis.UniversalClient
	namespace  string
	dedupeTTL  time.Duration
	metricsKey string
	recorder   domain.MetricsRecorder
}

func New(options Options) (*Store, error) {
	if options.Client == nil {
		return nil, ErrNilClient
	}
	namespace := options.Namespace
	if namespace == "" {
		namespace = "ironclaw"
	}
	dedupeTTL := options.DedupeTTL
	if dedupeTTL <= 0 {
		dedupeTTL = defaultDedupeTTL
	}
	metricsKey := options.MetricsKey
	if metricsKey == "" {
		metricsKey = namespace + ":metrics"
	}

	return &Store{
		client:     options.Client,
		namespace:  namespace,
		dedupeTTL:  dedupeTTL,
		metricsKey: metricsKey,
		recorder:   options.Recorder,
	}, nil
}

func (s *Store) stateKey(userID, deviceID string) string {
	return fmt.Sprintf("%s:{%s}:device:%s:state", s.namespace, userID, deviceID)
}

const DedupeSchemaVersion = "v2"

func (s *Store) dedupeKey(userID, deviceID string) string {
	return fmt.Sprintf("%s:{%s}:device:%s:applied:%s", s.namespace, userID, deviceID, DedupeSchemaVersion)
}

func (s *Store) devicesKey(userID string) string {
	return fmt.Sprintf("%s:{%s}:devices", s.namespace, userID)
}

func (s *Store) ApplyWindow(ctx context.Context, window domain.AggregateWindow) error {
	userID := window.Key.UserID
	deviceID := window.Key.DeviceID

	lastSeen := ""
	if !window.WindowEnd.IsZero() {
		lastSeen = window.WindowEnd.UTC().Format(time.RFC3339)
	}

	keys := []string{
		s.stateKey(userID, deviceID),
		s.dedupeKey(userID, deviceID),
		s.devicesKey(userID),
		s.updatesChannel(userID, deviceID),
	}

	applied, err := applyWindow.Run(ctx, s.client, keys,
		window.Identity(),
		deviceID,
		window.Count,
		window.ErrorCount,
		window.Bytes,
		window.P95Nanos,
		window.P99Nanos,
		window.WindowID,
		lastSeen,
		int64(s.dedupeTTL.Seconds()),
		userID,
		time.Now().Unix(),
	).Int64()
	if err != nil {
		return fmt.Errorf("redisstore: applying window %s: %w", window.Identity(), err)
	}

	if applied == 0 {
		s.Increment(MetricWindowsDuplicate, 1)
		return nil
	}

	s.Increment(MetricWindowsApplied, 1)
	s.Increment(MetricEventsCounted, window.Count)
	s.Increment(MetricErrorsCounted, window.ErrorCount)

	return nil
}

func (s *Store) DeviceState(ctx context.Context, userID, deviceID string) (domain.DeviceState, error) {
	fields, err := s.client.HGetAll(ctx, s.stateKey(userID, deviceID)).Result()
	if err != nil {
		return domain.DeviceState{}, fmt.Errorf("redisstore: reading device state: %w", err)
	}
	if len(fields) == 0 {
		return domain.DeviceState{}, ErrDeviceNotFound
	}

	state := domain.DeviceState{
		UserID:       userID,
		DeviceID:     deviceID,
		Count:        parseInt(fields["count"]),
		ErrorCount:   parseInt(fields["error_count"]),
		Bytes:        parseInt(fields["bytes"]),
		P95Nanos:     parseInt(fields["p95_nanos"]),
		P99Nanos:     parseInt(fields["p99_nanos"]),
		LastWindowID: parseInt(fields["last_window_id"]),
	}

	if raw := fields["last_seen"]; raw != "" {
		if parsed, err := time.Parse(time.RFC3339, raw); err == nil {
			state.LastSeen = parsed
		}
	}

	return state, nil
}

func (s *Store) UserDevices(ctx context.Context, userID string) ([]string, error) {
	devices, err := s.client.SMembers(ctx, s.devicesKey(userID)).Result()
	if err != nil {
		return nil, fmt.Errorf("redisstore: listing devices: %w", err)
	}
	sort.Strings(devices)
	return devices, nil
}

func (s *Store) PipelineMetrics(ctx context.Context) (map[string]int64, error) {
	fields, err := s.client.HGetAll(ctx, s.metricsKey).Result()
	if err != nil {
		return nil, fmt.Errorf("redisstore: reading metrics: %w", err)
	}

	metrics := make(map[string]int64, len(fields))
	for name, value := range fields {
		metrics[name] = parseInt(value)
	}
	return metrics, nil
}

func (s *Store) Increment(name string, delta int64) {
	if s.recorder != nil {
		s.recorder.Increment(name, delta)
		return
	}
	s.IncrementInRedis(name, delta)
}

func (s *Store) AttachRecorder(recorder domain.MetricsRecorder) {
	s.recorder = recorder
}

func (s *Store) RedisRecorder() domain.MetricsRecorder {
	return redisRecorder{store: s}
}

type redisRecorder struct{ store *Store }

func (r redisRecorder) Increment(name string, delta int64) {
	r.store.IncrementInRedis(name, delta)
}

func (s *Store) IncrementInRedis(name string, delta int64) {
	ctx, cancel := context.WithTimeout(context.Background(), metricsWriteTimeout)
	defer cancel()

	s.client.HIncrBy(ctx, s.metricsKey, name, delta)
}

func (s *Store) ResetDevice(ctx context.Context, userID, deviceID string) error {
	key := s.stateKey(userID, deviceID)

	exists, err := s.client.Exists(ctx, key).Result()
	if err != nil {
		return fmt.Errorf("redisstore: checking device: %w", err)
	}
	if exists == 0 {
		return ErrDeviceNotFound
	}

	if err := s.client.HSet(ctx, key,
		"count", 0,
		"error_count", 0,
		"bytes", 0,
		"p95_nanos", 0,
		"p99_nanos", 0,
		"percentile_sample", 0,
	).Err(); err != nil {
		return fmt.Errorf("redisstore: resetting device: %w", err)
	}

	s.client.SPublish(ctx, s.updatesChannel(userID, deviceID), "reset")
	s.Increment(MetricDeviceResets, 1)

	return nil
}

func parseInt(value string) int64 {
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0
	}
	return parsed
}
