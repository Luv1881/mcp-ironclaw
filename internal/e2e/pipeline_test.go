package e2e_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ironclaw/mcp-ironclaw/internal/aggregate"
	"github.com/ironclaw/mcp-ironclaw/internal/aggregator"
	"github.com/ironclaw/mcp-ironclaw/internal/bus"
	"github.com/ironclaw/mcp-ironclaw/internal/domain"
	"github.com/ironclaw/mcp-ironclaw/internal/ingest"
	"github.com/ironclaw/mcp-ironclaw/internal/persister"
	"github.com/ironclaw/mcp-ironclaw/internal/pipeline"
	"github.com/ironclaw/mcp-ironclaw/internal/store"
	"github.com/ironclaw/mcp-ironclaw/internal/transport"
)

const (
	deviceID   = "device-1"
	userID     = "user-1"
	eventCount = 5000
	windowSize = 10 * time.Second
)

type harness struct {
	batchBus  *bus.BatchBus
	windowBus *bus.WindowBus
	state     *store.Memory
	agg       *aggregator.Service
	batcher   *pipeline.Batcher
	source    *pipeline.SyntheticSource
	transport domain.Transport
}

func newHarness(t *testing.T) *harness {
	t.Helper()

	state := store.NewMemory()

	batchBus, err := bus.NewBatchBus(bus.Config{Partitions: 4, Capacity: 4096, MaxAttempts: 3})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	windowBus, err := bus.NewWindowBus(bus.Config{Partitions: 4, Capacity: 4096, MaxAttempts: 3})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ingestService, err := ingest.New(batchBus, state)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	loopback, err := transport.NewLoopback(ingestService, deviceID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	inner, err := aggregate.New(aggregate.Config{WindowSize: windowSize, RelativeAccuracy: 0.01})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	aggService, err := aggregator.New(inner, windowBus, state)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	source, err := pipeline.NewSyntheticSource(pipeline.SyntheticConfig{
		DeviceID:     deviceID,
		UserID:       userID,
		PodID:        "pod-a",
		ProcessIDs:   []int32{1, 2},
		EventCount:   eventCount,
		ErrorRate:    0.1,
		BaseLatency:  time.Millisecond,
		TailLatency:  40 * time.Millisecond,
		TailFraction: 0.05,
		Seed:         99,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	batcher, err := pipeline.NewBatcher(pipeline.BatcherConfig{
		DeviceID:    deviceID,
		MaxEvents:   250,
		MaxInterval: 50 * time.Millisecond,
		QueueDepth:  256,
		Policy:      pipeline.OverflowBlock,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	return &harness{
		batchBus:  batchBus,
		windowBus: windowBus,
		state:     state,
		agg:       aggService,
		batcher:   batcher,
		source:    source,
		transport: loopback,
	}
}

func (h *harness) run(t *testing.T) {
	t.Helper()

	ctx := context.Background()

	if err := h.batcher.Run(ctx, h.source, h.transport); err != nil {
		t.Fatalf("agent run failed: %v", err)
	}
	h.batchBus.Close()

	if err := h.batchBus.RunConsumerGroup(ctx, h.agg.HandleBatch); err != nil {
		t.Fatalf("aggregator consumer failed: %v", err)
	}
	if err := h.agg.EmitAll(ctx); err != nil {
		t.Fatalf("window emission failed: %v", err)
	}
	h.windowBus.Close()

	persistService, err := persister.New(h.state, h.state)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := h.windowBus.RunConsumerGroup(ctx, persistService.HandleWindow); err != nil {
		t.Fatalf("persister consumer failed: %v", err)
	}
}

func TestPipelineDeliversEveryEventToReadableState(t *testing.T) {
	h := newHarness(t)
	h.run(t)

	ctx := context.Background()

	state, err := h.state.DeviceState(ctx, userID, deviceID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	agentStats := h.batcher.Stats()
	if agentStats.EventsAccepted != eventCount {
		t.Fatalf("agent accepted %d events, want %d", agentStats.EventsAccepted, eventCount)
	}
	if agentStats.EventsDropped != 0 {
		t.Fatalf("agent dropped %d events under a blocking policy, want 0", agentStats.EventsDropped)
	}

	if state.Count != eventCount {
		t.Fatalf("state counted %d events end to end, want %d", state.Count, eventCount)
	}
	if state.ErrorCount == 0 {
		t.Fatal("state recorded no errors despite a non-zero synthetic error rate")
	}
	if state.P99Nanos < state.P95Nanos {
		t.Fatalf("p99 %d is below p95 %d", state.P99Nanos, state.P95Nanos)
	}
	if state.P95Nanos <= 0 {
		t.Fatalf("p95 %d is not positive", state.P95Nanos)
	}

	devices, err := h.state.UserDevices(ctx, userID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(devices) != 1 || devices[0] != deviceID {
		t.Fatalf("user devices %v, want [%s]", devices, deviceID)
	}
}

func TestPipelineMetricsCoverEveryStage(t *testing.T) {
	h := newHarness(t)
	h.run(t)

	metrics, err := h.state.PipelineMetrics(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	required := []string{
		ingest.MetricBatchesAccepted,
		ingest.MetricEventsAccepted,
		aggregator.MetricBatchesConsumed,
		aggregator.MetricWindowsEmitted,
		persister.MetricWindowsPersisted,
		store.MetricWindowsApplied,
	}

	for _, name := range required {
		if metrics[name] == 0 {
			t.Fatalf("metric %q was never recorded", name)
		}
	}

	if metrics[ingest.MetricEventsAccepted] != eventCount {
		t.Fatalf("ingest counted %d events, want %d", metrics[ingest.MetricEventsAccepted], eventCount)
	}
	if metrics[ingest.MetricBatchesRejected] != 0 {
		t.Fatalf("ingest rejected %d batches, want 0", metrics[ingest.MetricBatchesRejected])
	}
	if metrics[store.MetricWindowsDuplicate] != 0 {
		t.Fatalf("store saw %d duplicate windows on a clean run, want 0", metrics[store.MetricWindowsDuplicate])
	}
}

func TestPipelineRedeliveryIsIdempotent(t *testing.T) {
	h := newHarness(t)
	h.run(t)

	ctx := context.Background()

	before, err := h.state.DeviceState(ctx, userID, deviceID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	replayBus, err := bus.NewWindowBus(bus.Config{Partitions: 2, Capacity: 64, MaxAttempts: 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	replayed := domain.AggregateWindow{
		Key:        domain.CorrelationKey{UserID: userID, DeviceID: deviceID, ProcessID: 1, PodID: "pod-a"},
		WindowID:   1,
		Count:      10,
		ErrorCount: 2,
		Bytes:      512,
		P95Nanos:   1000,
		P99Nanos:   2000,
	}

	persistService, err := persister.New(h.state, h.state)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := persistService.HandleWindow(ctx, replayed); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	afterFirst, err := h.state.DeviceState(ctx, userID, deviceID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if afterFirst.Count != before.Count+10 {
		t.Fatalf("first apply produced count %d, want %d", afterFirst.Count, before.Count+10)
	}

	for i := 0; i < 5; i++ {
		if err := persistService.HandleWindow(ctx, replayed); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	afterReplay, err := h.state.DeviceState(ctx, userID, deviceID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if afterReplay.Count != afterFirst.Count {
		t.Fatalf("redelivery changed count from %d to %d", afterFirst.Count, afterReplay.Count)
	}
	if afterReplay.ErrorCount != afterFirst.ErrorCount {
		t.Fatalf("redelivery changed error count from %d to %d", afterFirst.ErrorCount, afterReplay.ErrorCount)
	}

	metrics, err := h.state.PipelineMetrics(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if metrics[store.MetricWindowsDuplicate] != 5 {
		t.Fatalf("store recorded %d duplicates, want 5", metrics[store.MetricWindowsDuplicate])
	}

	replayBus.Close()
}

func TestIngestRejectsSpoofedDeviceIdentity(t *testing.T) {
	state := store.NewMemory()

	batchBus, err := bus.NewBatchBus(bus.Config{Partitions: 2, Capacity: 16, MaxAttempts: 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	service, err := ingest.New(batchBus, state)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	spoofed := domain.Batch{
		DeviceID: "victim-device",
		Events: []domain.Event{{
			DeviceID:     "victim-device",
			UserID:       userID,
			ProcessID:    1,
			Kind:         domain.EventKindSyscall,
			ObservedAt:   time.Unix(1700000000, 0),
			LatencyNanos: 1000,
		}},
	}

	err = service.Accept(context.Background(), "attacker-device", spoofed)
	if !errors.Is(err, domain.ErrDeviceMismatch) {
		t.Fatalf("got %v, want ErrDeviceMismatch", err)
	}

	if err := service.Accept(context.Background(), "", spoofed); !errors.Is(err, ingest.ErrMissingIdentity) {
		t.Fatalf("got %v, want ErrMissingIdentity", err)
	}

	metrics, err := state.PipelineMetrics(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if metrics[ingest.MetricBatchesRejected] != 2 {
		t.Fatalf("ingest rejected %d batches, want 2", metrics[ingest.MetricBatchesRejected])
	}
	if metrics[ingest.MetricBatchesAccepted] != 0 {
		t.Fatalf("ingest accepted %d spoofed batches, want 0", metrics[ingest.MetricBatchesAccepted])
	}
}

func TestUnknownDeviceReadReturnsNotFound(t *testing.T) {
	state := store.NewMemory()

	if _, err := state.DeviceState(context.Background(), userID, "missing-device"); !errors.Is(err, store.ErrDeviceNotFound) {
		t.Fatalf("got %v, want ErrDeviceNotFound", err)
	}
}
