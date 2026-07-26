package app

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/ironclaw/mcp-ironclaw/internal/bus"
	"github.com/ironclaw/mcp-ironclaw/internal/commands"
	"github.com/ironclaw/mcp-ironclaw/internal/domain"
	"github.com/ironclaw/mcp-ironclaw/internal/kafkabus"
	"github.com/ironclaw/mcp-ironclaw/internal/metrics"
	"github.com/ironclaw/mcp-ironclaw/internal/postgresstore"
	"github.com/ironclaw/mcp-ironclaw/internal/redisstore"
	"github.com/ironclaw/mcp-ironclaw/internal/store"
	"github.com/ironclaw/mcp-ironclaw/internal/wire"
	"github.com/jackc/pgx/v5/pgxpool"
)

type BatchChannel interface {
	domain.Publisher
	domain.Consumer
	Close()
}

type WindowChannel interface {
	domain.WindowPublisher
	domain.WindowConsumer
	Close()
}

type CommandChannel interface {
	domain.CommandPublisher
	ConsumeCommands(ctx context.Context, handler func(context.Context, domain.Command) error) error
	Close()
}

type StateBackend interface {
	domain.StateWriter
	domain.StateReader
	domain.MetricsReader
	domain.MetricsRecorder
	domain.DeviceWatcher
	commands.DeviceResetter
}

type Backends struct {
	RedisAddr      string
	PostgresDSN    string
	KafkaBrokers   string
	TopicPrefix    string
	RequireTLS     bool
	CAFile         string
	CertFile       string
	KeyFile        string
	RedisUsername  string
	RedisPassword  string
	KafkaSASLUser  string
	KafkaSASLPass  string
	PostgresRootCA string
	WireFormat     string
}

func (b Backends) codec() (wire.Codec, error) {
	return wire.For(wire.Format(b.WireFormat))
}

func (b Backends) kafkaSecurity() kafkabus.Security {
	return kafkabus.Security{
		Enabled:      b.RequireTLS,
		CAFile:       b.CAFile,
		CertFile:     b.CertFile,
		KeyFile:      b.KeyFile,
		SASLUser:     b.KafkaSASLUser,
		SASLPassword: b.KafkaSASLPass,
	}
}

func (b Backends) redisSecurity() redisstore.Security {
	return redisstore.Security{
		Enabled:  b.RequireTLS,
		CAFile:   b.CAFile,
		CertFile: b.CertFile,
		KeyFile:  b.KeyFile,
		Username: b.RedisUsername,
		Password: b.RedisPassword,
	}
}

func (b Backends) brokers() []string {
	if b.KafkaBrokers == "" {
		return nil
	}
	return strings.Split(b.KafkaBrokers, ",")
}

func (b Backends) topic(name string) string {
	prefix := b.TopicPrefix
	if prefix == "" {
		prefix = "ironclaw"
	}
	return prefix + "." + name
}

type Dependencies struct {
	State          StateBackend
	Archive        domain.StateWriter
	Batches        BatchChannel
	Windows        WindowChannel
	ArchiveWindows WindowChannel
	Commands       CommandChannel
	FanOutWindows  bool
	Extra          []func()
	Describing     string
}

func (d *Dependencies) closeAll() {
	for _, release := range d.Extra {
		release()
	}
	d.Batches.Close()
	d.Windows.Close()
	if d.ArchiveWindows != nil {
		d.ArchiveWindows.Close()
	}
	d.Commands.Close()
}

func buildDependencies(ctx context.Context, config Config) (*Dependencies, error) {
	deps := &Dependencies{Describing: "in-memory"}

	if err := buildState(ctx, config, deps); err != nil {
		deps.closeExtras()
		return nil, err
	}
	if err := buildChannels(config, deps); err != nil {
		deps.closeExtras()
		return nil, err
	}

	return deps, nil
}

func (d *Dependencies) closeExtras() {
	for _, release := range d.Extra {
		release()
	}
	d.Extra = nil
}

func buildState(ctx context.Context, config Config, deps *Dependencies) error {
	if config.Backends.RedisAddr == "" {
		memory := store.NewMemory()
		deps.State = stateWithExtraRecorder{StateBackend: memory, recorder: metrics.NewFanout(memory, config.MetricsRecorder)}
		return nil
	}

	client, err := redisstore.Dial(config.Backends.RedisAddr, config.Backends.redisSecurity())
	if err != nil {
		return err
	}
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return fmt.Errorf("app: connecting to redis at %s: %w", config.Backends.RedisAddr, err)
	}

	state, err := redisstore.New(redisstore.Options{Client: client})
	if err != nil {
		_ = client.Close()
		return err
	}

	async, err := metrics.NewAsync(state.RedisRecorder(), 0)
	if err != nil {
		_ = client.Close()
		return err
	}

	recorder := metrics.NewFanout(async, config.MetricsRecorder)
	state.AttachRecorder(recorder)

	deps.State = stateWithAsyncMetrics{Store: state, recorder: recorder}
	deps.Extra = append(deps.Extra, async.Close)
	deps.Extra = append(deps.Extra, func() { _ = client.Close() })
	deps.Describing = "redis"

	if config.Backends.PostgresDSN == "" {
		return nil
	}

	dsn := config.Backends.PostgresDSN
	if config.Backends.RequireTLS {
		secured, err := postgresstore.RequireTLS(dsn, config.Backends.PostgresRootCA)
		if err != nil {
			return err
		}
		dsn = secured
	}

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return fmt.Errorf("app: connecting to postgres: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return fmt.Errorf("app: pinging postgres: %w", err)
	}

	archive, err := postgresstore.New(pool, deps.State)
	if err != nil {
		pool.Close()
		return err
	}
	if err := archive.Migrate(ctx); err != nil {
		pool.Close()
		return err
	}

	deps.Archive = archive
	deps.Extra = append(deps.Extra, pool.Close)
	deps.Describing = "redis+postgres"

	return nil
}

func buildChannels(config Config, deps *Dependencies) error {
	brokers := config.Backends.brokers()

	if len(brokers) == 0 {
		busConfig := bus.Config{
			Partitions:  config.Partitions,
			Capacity:    config.QueueCapacity,
			MaxAttempts: 3,
		}

		batches, err := bus.NewBatchBus(busConfig)
		if err != nil {
			return err
		}
		windows, err := bus.NewWindowBus(busConfig)
		if err != nil {
			return err
		}
		commandBus, err := bus.NewCommandBus(busConfig)
		if err != nil {
			return err
		}

		deps.Batches = batches
		deps.Windows = windows
		deps.Commands = commandBus

		if deps.Archive != nil {
			archive, err := bus.NewWindowBus(busConfig)
			if err != nil {
				batches.Close()
				windows.Close()
				commandBus.Close()
				return err
			}
			deps.ArchiveWindows = archive
			deps.FanOutWindows = true
		}

		return nil
	}

	codec, err := config.Backends.codec()
	if err != nil {
		return err
	}

	channel := func(name, group string) (*kafkabus.Channel, error) {
		return kafkabus.NewChannel(kafkabus.Config{
			Brokers:         brokers,
			Topic:           config.Backends.topic(name),
			Group:           config.Backends.topic(name) + "." + group,
			DeadLetterTopic: config.Backends.topic(name) + ".dlq",
			MaxAttempts:     3,
			Security:        config.Backends.kafkaSecurity(),
			Codec:           codec,
			OnHandlerError:  handlerErrorLogger(deps),
			Metrics:         deps.State,
		})
	}

	batches, err := channel("events.raw", "aggregators")
	if err != nil {
		return err
	}
	windows, err := channel("events.aggregated", "hot-state")
	if err != nil {
		batches.Close()
		return err
	}
	commandChannel, err := channel("commands", "appliers")
	if err != nil {
		batches.Close()
		windows.Close()
		return err
	}

	deps.Batches = batches
	deps.Windows = windows
	deps.Commands = commandChannel
	deps.Describing += "+kafka"

	if deps.Archive != nil {
		archive, err := channel("events.aggregated", "archive")
		if err != nil {
			batches.Close()
			windows.Close()
			commandChannel.Close()
			return err
		}
		deps.ArchiveWindows = archive
	}

	return nil
}

func handlerErrorLogger(deps *Dependencies) func(string, int, error) {
	return func(topic string, attempt int, err error) {
		if deps != nil && deps.State != nil {
			deps.State.Increment(MetricHandlerErrors, 1)
		}
		log.Printf("ironclaw: handling a record from %s failed on attempt %d: %v", topic, attempt, err)
	}
}

type stateWithExtraRecorder struct {
	StateBackend
	recorder domain.MetricsRecorder
}

func (s stateWithExtraRecorder) Increment(name string, delta int64) {
	s.recorder.Increment(name, delta)
}

type stateWithAsyncMetrics struct {
	*redisstore.Store
	recorder domain.MetricsRecorder
}

func (s stateWithAsyncMetrics) Increment(name string, delta int64) {
	s.recorder.Increment(name, delta)
}
