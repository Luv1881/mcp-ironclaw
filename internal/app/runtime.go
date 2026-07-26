package app

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ironclaw/mcp-ironclaw/internal/aggregate"
	"github.com/ironclaw/mcp-ironclaw/internal/aggregator"
	"github.com/ironclaw/mcp-ironclaw/internal/commands"
	"github.com/ironclaw/mcp-ironclaw/internal/domain"
	"github.com/ironclaw/mcp-ironclaw/internal/ingest"
	"github.com/ironclaw/mcp-ironclaw/internal/persister"
	"github.com/ironclaw/mcp-ironclaw/internal/pipeline"
	"github.com/ironclaw/mcp-ironclaw/internal/transport"
)

var ErrInvalidDeviceCount = errors.New("app: device count must be positive")

type Config struct {
	Devices          int
	EventsPerDevice  int
	EventInterval    time.Duration
	BatchSize        int
	BatchInterval    time.Duration
	WindowSize       time.Duration
	EmitInterval     time.Duration
	Partitions       int
	QueueCapacity    int
	RelativeAccuracy float64
	Seed             int64
	EmitOpenWindows  bool
	MaxOpenWindows   int
	Backends         Backends
	MetricsRecorder  domain.MetricsRecorder
}

func DefaultConfig() Config {
	return Config{
		Devices:          8,
		EventsPerDevice:  100000,
		EventInterval:    time.Millisecond,
		BatchSize:        200,
		BatchInterval:    100 * time.Millisecond,
		WindowSize:       10 * time.Second,
		EmitInterval:     time.Second,
		Partitions:       8,
		QueueCapacity:    4096,
		RelativeAccuracy: 0.01,
		Seed:             1,
		EmitOpenWindows:  true,
		MaxOpenWindows:   500000,
	}
}

func (c Config) validate() error {
	if c.Devices <= 0 {
		return ErrInvalidDeviceCount
	}
	return nil
}

type Runtime struct {
	config     Config
	deps       *Dependencies
	aggService *aggregator.Service
	sources    []*pipeline.SyntheticSource
	batchers   []*pipeline.Batcher

	runCtx    context.Context
	cancelRun context.CancelFunc

	agents    sync.WaitGroup
	consumers sync.WaitGroup
	persisted sync.WaitGroup
	archived  sync.WaitGroup
	commanded sync.WaitGroup
	emitter   sync.WaitGroup

	emitStop     chan struct{}
	stopped      chan struct{}
	shutdownOnce sync.Once
	failures     atomic.Int64
}

const (
	MetricStageFailures = "runtime_stage_failures"
	MetricHandlerErrors = "consumer_handler_errors"
)

func New(config Config) (*Runtime, error) {
	return NewWithContext(context.Background(), config)
}

func NewWithContext(ctx context.Context, config Config) (*Runtime, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}

	deps, err := buildDependencies(ctx, config)
	if err != nil {
		return nil, err
	}

	inner, err := aggregate.New(aggregate.Config{
		WindowSize:       config.WindowSize,
		RelativeAccuracy: config.RelativeAccuracy,
		MaxOpenWindows:   config.MaxOpenWindows,
	})
	if err != nil {
		deps.closeAll()
		return nil, err
	}
	var windowTarget domain.WindowPublisher = deps.Windows
	if deps.FanOutWindows && deps.ArchiveWindows != nil {
		windowTarget = newFanOutWindows(deps.Windows, deps.ArchiveWindows)
	}

	aggService, err := aggregator.New(inner, windowTarget, deps.State)
	if err != nil {
		deps.closeAll()
		return nil, err
	}

	runtime := &Runtime{
		config:     config,
		deps:       deps,
		aggService: aggService,
		emitStop:   make(chan struct{}),
		stopped:    make(chan struct{}),
	}

	if err := runtime.buildAgents(); err != nil {
		deps.closeAll()
		return nil, err
	}

	return runtime, nil
}

func (r *Runtime) Backend() string { return r.deps.Describing }

func (r *Runtime) buildAgents() error {
	for i := 0; i < r.config.Devices; i++ {
		deviceID := fmt.Sprintf("device-%03d", i)
		userID := fmt.Sprintf("user-%03d", i%4)

		source, err := pipeline.NewSyntheticSource(pipeline.SyntheticConfig{
			DeviceID:     deviceID,
			UserID:       userID,
			PodID:        fmt.Sprintf("pod-%03d", i%3),
			ProcessIDs:   []int32{int32(100 + i), int32(200 + i)},
			EventCount:   r.config.EventsPerDevice,
			Interval:     r.config.EventInterval,
			ErrorRate:    0.05,
			BaseLatency:  time.Millisecond,
			TailLatency:  40 * time.Millisecond,
			TailFraction: 0.05,
			Seed:         r.config.Seed + int64(i),
		})
		if err != nil {
			return err
		}

		batcher, err := pipeline.NewBatcher(pipeline.BatcherConfig{
			DeviceID:    deviceID,
			MaxEvents:   r.config.BatchSize,
			MaxInterval: r.config.BatchInterval,
			QueueDepth:  256,
			Policy:      pipeline.OverflowBlock,
		})
		if err != nil {
			return err
		}

		r.sources = append(r.sources, source)
		r.batchers = append(r.batchers, batcher)
	}

	return nil
}

func (r *Runtime) State() StateBackend { return r.deps.State }

func (r *Runtime) Commands() domain.CommandPublisher { return r.deps.Commands }

func (r *Runtime) Start(ctx context.Context) error {
	ingestService, err := ingest.New(r.deps.Batches, r.deps.State)
	if err != nil {
		return err
	}
	persistService, err := persister.New(r.deps.State, r.deps.State)
	if err != nil {
		return err
	}

	var archiveService *persister.Service
	if r.deps.Archive != nil && r.deps.ArchiveWindows != nil {
		archiveService, err = persister.New(r.deps.Archive, r.deps.State)
		if err != nil {
			return err
		}
	}
	commandService, err := commands.New(r.deps.State, r.deps.State)
	if err != nil {
		return err
	}

	transports := make([]domain.Transport, 0, len(r.batchers))
	for i := range r.batchers {
		loopback, err := transport.NewLoopback(ingestService, r.sources[i].DeviceID())
		if err != nil {
			return err
		}
		transports = append(transports, loopback)
	}

	r.runCtx, r.cancelRun = context.WithCancel(context.WithoutCancel(ctx))

	for i, batcher := range r.batchers {
		spawn(&r.agents, func() { r.report("agent", batcher.Run(r.runCtx, r.sources[i], transports[i])) })
	}

	spawn(&r.consumers, func() {
		r.report("aggregator", r.deps.Batches.Consume(r.runCtx, r.aggService.HandleBatch))
	})
	spawn(&r.persisted, func() {
		r.report("persister", r.deps.Windows.ConsumeWindows(r.runCtx, persistService.HandleWindow))
	})
	if archiveService != nil {
		spawn(&r.archived, func() {
			r.report("archive", r.deps.ArchiveWindows.ConsumeWindows(r.runCtx, archiveService.HandleWindow))
		})
	}
	spawn(&r.commanded, func() {
		r.report("commands", r.deps.Commands.ConsumeCommands(r.runCtx, commandService.HandleCommand))
	})
	spawn(&r.emitter, r.emitLoop)

	go func() {
		select {
		case <-ctx.Done():
			r.Stop()
		case <-r.stopped:
		}
	}()

	return nil
}

func (r *Runtime) report(stage string, err error) {
	if err == nil || errors.Is(err, context.Canceled) {
		return
	}

	r.failures.Add(1)
	if r.deps != nil && r.deps.State != nil {
		r.deps.State.Increment(MetricStageFailures, 1)
	}
	log.Printf("ironclaw: %s stopped with error: %v", stage, err)
}

func (r *Runtime) Failures() int64 { return r.failures.Load() }

func spawn(group *sync.WaitGroup, work func()) {
	group.Add(1)
	go func() {
		defer group.Done()
		work()
	}()
}

func (r *Runtime) emitLoop() {
	ticker := time.NewTicker(r.config.EmitInterval)
	defer ticker.Stop()

	for {
		select {
		case <-r.emitStop:
			return

		case now := <-ticker.C:
			if r.config.EmitOpenWindows {
				r.report("emit", r.aggService.EmitAll(r.runCtx))
				continue
			}
			r.report("emit", r.aggService.EmitClosedWindows(r.runCtx, now))
		}
	}
}

func (r *Runtime) Stop() {
	r.shutdownOnce.Do(func() {
		r.shutdown()
		close(r.stopped)
	})
	<-r.stopped
}

func (r *Runtime) shutdown() {
	for _, source := range r.sources {
		r.report("source close", source.Close())
	}
	r.agents.Wait()

	r.deps.Batches.Close()
	r.consumers.Wait()

	close(r.emitStop)
	r.emitter.Wait()
	r.report("final emit", r.aggService.EmitAll(r.runCtx))

	r.deps.Windows.Close()
	r.persisted.Wait()

	if r.deps.ArchiveWindows != nil {
		r.deps.ArchiveWindows.Close()
		r.archived.Wait()
	}

	r.deps.Commands.Close()
	r.commanded.Wait()

	r.deps.closeExtras()
	r.cancelRun()
}
