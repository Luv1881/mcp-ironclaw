package postgresstore

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ironclaw/mcp-ironclaw/internal/domain"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrNilPool        = errors.New("postgresstore: pool is nil")
	ErrDeviceNotFound = errors.New("postgresstore: device not found")
)

const schema = `
CREATE TABLE IF NOT EXISTS aggregate_windows (
    user_id      TEXT        NOT NULL,
    device_id    TEXT        NOT NULL,
    process_id   INTEGER     NOT NULL,
    pod_id       TEXT        NOT NULL,
    window_id    BIGINT      NOT NULL,
    sequence     BIGINT      NOT NULL DEFAULT 0,
    window_start TIMESTAMPTZ NOT NULL,
    window_end   TIMESTAMPTZ NOT NULL,
    event_count  BIGINT      NOT NULL,
    error_count  BIGINT      NOT NULL,
    byte_count   BIGINT      NOT NULL,
    p95_nanos    BIGINT      NOT NULL,
    p99_nanos    BIGINT      NOT NULL,
    max_nanos    BIGINT      NOT NULL,
    applied_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, device_id, process_id, pod_id, window_id, sequence, window_start)
) PARTITION BY RANGE (window_start);

CREATE TABLE IF NOT EXISTS aggregate_windows_default PARTITION OF aggregate_windows DEFAULT;

CREATE INDEX IF NOT EXISTS aggregate_windows_device_idx
    ON aggregate_windows (user_id, device_id, window_id DESC);

CREATE INDEX IF NOT EXISTS aggregate_windows_window_start_idx
    ON aggregate_windows (window_start);
`

const insertWindow = `
INSERT INTO aggregate_windows (
    user_id, device_id, process_id, pod_id, window_id, sequence,
    window_start, window_end,
    event_count, error_count, byte_count,
    p95_nanos, p99_nanos, max_nanos
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
ON CONFLICT (user_id, device_id, process_id, pod_id, window_id, sequence, window_start) DO NOTHING
`

const selectDeviceState = `
SELECT
    COALESCE(SUM(event_count), 0),
    COALESCE(SUM(error_count), 0),
    COALESCE(SUM(byte_count), 0),
    COALESCE(MAX(window_id), 0)
FROM aggregate_windows
WHERE user_id = $1 AND device_id = $2
`

const selectLatestPercentiles = `
SELECT
    COALESCE(p95_nanos, 0),
    COALESCE(p99_nanos, 0),
    COALESCE(window_end, to_timestamp(0))
FROM aggregate_windows
WHERE user_id = $1 AND device_id = $2 AND window_id = $3
ORDER BY event_count DESC, sequence DESC
LIMIT 1
`

const selectUserDevices = `
SELECT DISTINCT device_id
FROM aggregate_windows
WHERE user_id = $1
ORDER BY device_id
`

const deleteDevice = `
DELETE FROM aggregate_windows
WHERE user_id = $1 AND device_id = $2
`

var (
	_ domain.StateWriter = (*Store)(nil)
	_ domain.StateReader = (*Store)(nil)
)

type Store struct {
	pool    *pgxpool.Pool
	metrics domain.MetricsRecorder
}

const (
	MetricWindowsPersisted = "postgres_windows_persisted"
	MetricWindowsDuplicate = "postgres_windows_duplicate"
)

func New(pool *pgxpool.Pool, metrics domain.MetricsRecorder) (*Store, error) {
	if pool == nil {
		return nil, ErrNilPool
	}
	return &Store{pool: pool, metrics: metrics}, nil
}

func (s *Store) Migrate(ctx context.Context) error {
	if _, err := s.pool.Exec(ctx, schema); err != nil {
		return fmt.Errorf("postgresstore: applying schema: %w", err)
	}
	return s.EnsurePartitions(ctx, time.Now().AddDate(0, 0, -1), 8)
}

func (s *Store) ApplyWindow(ctx context.Context, window domain.AggregateWindow) error {
	tag, err := s.pool.Exec(ctx, insertWindow,
		window.Key.UserID,
		window.Key.DeviceID,
		window.Key.ProcessID,
		window.Key.PodID,
		window.WindowID,
		window.Sequence,
		window.WindowStart,
		window.WindowEnd,
		window.Count,
		window.ErrorCount,
		window.Bytes,
		window.P95Nanos,
		window.P99Nanos,
		window.MaxNanos,
	)
	if err != nil {
		return fmt.Errorf("postgresstore: inserting window %s: %w", window.Identity(), err)
	}

	if tag.RowsAffected() == 0 {
		s.record(MetricWindowsDuplicate, 1)
		return nil
	}

	s.record(MetricWindowsPersisted, 1)

	return nil
}

func (s *Store) DeviceState(ctx context.Context, userID, deviceID string) (domain.DeviceState, error) {
	state := domain.DeviceState{UserID: userID, DeviceID: deviceID}

	err := s.pool.QueryRow(ctx, selectDeviceState, userID, deviceID).Scan(
		&state.Count, &state.ErrorCount, &state.Bytes, &state.LastWindowID,
	)
	if err != nil {
		return domain.DeviceState{}, fmt.Errorf("postgresstore: reading device state: %w", err)
	}

	if state.Count == 0 && state.LastWindowID == 0 {
		var exists bool
		if err := s.pool.QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM aggregate_windows WHERE user_id=$1 AND device_id=$2)`,
			userID, deviceID).Scan(&exists); err != nil {
			return domain.DeviceState{}, fmt.Errorf("postgresstore: checking device: %w", err)
		}
		if !exists {
			return domain.DeviceState{}, ErrDeviceNotFound
		}
	}

	var lastSeen time.Time
	err = s.pool.QueryRow(ctx, selectLatestPercentiles, userID, deviceID, state.LastWindowID).Scan(
		&state.P95Nanos, &state.P99Nanos, &lastSeen,
	)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return domain.DeviceState{}, fmt.Errorf("postgresstore: reading percentiles: %w", err)
	}
	if !lastSeen.IsZero() && lastSeen.Unix() > 0 {
		state.LastSeen = lastSeen
	}

	return state, nil
}

func (s *Store) UserDevices(ctx context.Context, userID string) ([]string, error) {
	rows, err := s.pool.Query(ctx, selectUserDevices, userID)
	if err != nil {
		return nil, fmt.Errorf("postgresstore: listing devices: %w", err)
	}
	defer rows.Close()

	devices := []string{}
	for rows.Next() {
		var deviceID string
		if err := rows.Scan(&deviceID); err != nil {
			return nil, fmt.Errorf("postgresstore: scanning device: %w", err)
		}
		devices = append(devices, deviceID)
	}

	return devices, rows.Err()
}

func (s *Store) ResetDevice(ctx context.Context, userID, deviceID string) error {
	tag, err := s.pool.Exec(ctx, deleteDevice, userID, deviceID)
	if err != nil {
		return fmt.Errorf("postgresstore: resetting device: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrDeviceNotFound
	}
	return nil
}

func (s *Store) record(name string, delta int64) {
	if s.metrics == nil {
		return
	}
	s.metrics.Increment(name, delta)
}

const ensurePartition = `
CREATE TABLE IF NOT EXISTS %s PARTITION OF aggregate_windows
    FOR VALUES FROM ('%s') TO ('%s')
`

const dropPartition = `DROP TABLE IF EXISTS %s`

func partitionName(day time.Time) string {
	return "aggregate_windows_" + day.UTC().Format("20060102")
}

func (s *Store) EnsurePartition(ctx context.Context, day time.Time) error {
	start := day.UTC().Truncate(24 * time.Hour)
	end := start.Add(24 * time.Hour)

	statement := fmt.Sprintf(ensurePartition,
		partitionName(start),
		start.Format("2006-01-02 15:04:05Z07:00"),
		end.Format("2006-01-02 15:04:05Z07:00"))

	if _, err := s.pool.Exec(ctx, statement); err != nil {
		if !defaultPartitionBlocked(err) {
			return fmt.Errorf("postgresstore: creating partition for %s: %w", start.Format("2006-01-02"), err)
		}
		if err := s.adoptFromDefaultPartition(ctx, start, end); err != nil {
			return err
		}
	}

	return nil
}

func defaultPartitionBlocked(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == pgerrcode.CheckViolation
}

func (s *Store) adoptFromDefaultPartition(ctx context.Context, start, end time.Time) error {
	transaction, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("postgresstore: starting partition migration: %w", err)
	}
	defer func() { _ = transaction.Rollback(ctx) }()

	steps := []string{
		"ALTER TABLE aggregate_windows DETACH PARTITION aggregate_windows_default",
		fmt.Sprintf(ensurePartition,
			partitionName(start),
			start.Format("2006-01-02 15:04:05Z07:00"),
			end.Format("2006-01-02 15:04:05Z07:00")),
	}

	for _, step := range steps {
		if _, err := transaction.Exec(ctx, step); err != nil {
			return fmt.Errorf("postgresstore: migrating rows out of the default partition: %w", err)
		}
	}

	if _, err := transaction.Exec(ctx, `
        INSERT INTO aggregate_windows
        SELECT * FROM aggregate_windows_default
        WHERE window_start >= $1 AND window_start < $2
        ON CONFLICT DO NOTHING`, start, end); err != nil {
		return fmt.Errorf("postgresstore: moving rows into the new partition: %w", err)
	}

	if _, err := transaction.Exec(ctx, `
        DELETE FROM aggregate_windows_default
        WHERE window_start >= $1 AND window_start < $2`, start, end); err != nil {
		return fmt.Errorf("postgresstore: clearing migrated rows: %w", err)
	}

	if _, err := transaction.Exec(ctx,
		"ALTER TABLE aggregate_windows ATTACH PARTITION aggregate_windows_default DEFAULT"); err != nil {
		return fmt.Errorf("postgresstore: reattaching the default partition: %w", err)
	}

	if err := transaction.Commit(ctx); err != nil {
		return fmt.Errorf("postgresstore: committing partition migration: %w", err)
	}

	return nil
}

func (s *Store) EnsurePartitions(ctx context.Context, from time.Time, days int) error {
	for offset := 0; offset < days; offset++ {
		if err := s.EnsurePartition(ctx, from.AddDate(0, 0, offset)); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) DropPartitionsBefore(ctx context.Context, cutoff time.Time) (int, error) {
	rows, err := s.pool.Query(ctx, `
        SELECT c.relname
        FROM pg_class c
        JOIN pg_inherits i ON c.oid = i.inhrelid
        JOIN pg_class parent ON parent.oid = i.inhparent
        WHERE parent.relname = 'aggregate_windows'
    `)
	if err != nil {
		return 0, fmt.Errorf("postgresstore: listing partitions: %w", err)
	}

	names := []string{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			rows.Close()
			return 0, fmt.Errorf("postgresstore: scanning partition: %w", err)
		}
		names = append(names, name)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}

	dropped := 0
	boundary := partitionName(cutoff)

	for _, name := range names {
		if name == "aggregate_windows_default" || name >= boundary {
			continue
		}
		if _, err := s.pool.Exec(ctx, fmt.Sprintf(dropPartition, name)); err != nil {
			return dropped, fmt.Errorf("postgresstore: dropping %s: %w", name, err)
		}
		dropped++
	}

	return dropped, nil
}
