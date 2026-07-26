package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ironclaw/mcp-ironclaw/internal/domain"
	"github.com/ironclaw/mcp-ironclaw/internal/pipeline"
	"github.com/ironclaw/mcp-ironclaw/internal/spool"
	"github.com/ironclaw/mcp-ironclaw/internal/store"
	"github.com/ironclaw/mcp-ironclaw/internal/tracing"
	"github.com/ironclaw/mcp-ironclaw/internal/transport"
)

type options struct {
	endpoint     string
	serverName   string
	certFile     string
	keyFile      string
	caFile       string
	deviceID     string
	userID       string
	podID        string
	spoolDir     string
	spoolBytes   int64
	events       int
	interval     time.Duration
	batchSize    int
	batchWindow  time.Duration
	reportEvery  time.Duration
	baseBackoff  time.Duration
	maxBackoff   time.Duration
	queueDepth   int
	tailFraction float64
	otlpEndpoint string
	sampleRatio  float64
}

func main() {
	var opts options

	flag.StringVar(&opts.endpoint, "endpoint", "https://edge.ironclaw.internal/v1/batches", "edge batch endpoint")
	flag.StringVar(&opts.serverName, "server-name", "", "TLS server name when dialling an address that differs from the certificate")
	flag.StringVar(&opts.certFile, "cert", "", "device client certificate")
	flag.StringVar(&opts.keyFile, "key", "", "device client key")
	flag.StringVar(&opts.caFile, "ca", "", "CA bundle verifying the edge")
	flag.StringVar(&opts.deviceID, "device", "", "device identity; must match the certificate common name")
	flag.StringVar(&opts.userID, "user", "", "user the device belongs to")
	flag.StringVar(&opts.podID, "pod", "on-prem", "pod or host label attached to events")
	flag.StringVar(&opts.spoolDir, "spool", "", "directory holding batches during an outage; empty disables spooling")
	flag.Int64Var(&opts.spoolBytes, "spool-bytes", 64<<20, "maximum bytes the spool may occupy before dropping the oldest batches")
	flag.IntVar(&opts.events, "events", 100000, "events to produce before exiting")
	flag.DurationVar(&opts.interval, "interval", time.Millisecond, "delay between events")
	flag.IntVar(&opts.batchSize, "batch-size", 500, "events per batch")
	flag.DurationVar(&opts.batchWindow, "batch-window", 200*time.Millisecond, "flush a partial batch after this long")
	flag.IntVar(&opts.queueDepth, "queue-depth", 256, "in-memory batch queue depth before backpressure")
	flag.DurationVar(&opts.reportEvery, "report-every", 30*time.Second, "how often to log agent statistics")
	flag.DurationVar(&opts.baseBackoff, "base-backoff", 250*time.Millisecond, "initial retry delay when the edge is unreachable")
	flag.DurationVar(&opts.maxBackoff, "max-backoff", 30*time.Second, "ceiling on the retry delay")
	flag.Float64Var(&opts.tailFraction, "tail-fraction", 0.05, "fraction of events drawn from the slow tail")
	flag.StringVar(&opts.otlpEndpoint, "otlp", os.Getenv("IRONCLAW_OTLP"), "OTLP gRPC endpoint receiving traces; empty disables tracing")
	flag.Float64Var(&opts.sampleRatio, "trace-sample", 0.01, "fraction of batches traced")
	flag.Parse()

	if err := run(opts); err != nil {
		log.Fatalf("agent: %v", err)
	}
}

func run(opts options) error {
	if opts.deviceID == "" {
		return fmt.Errorf("agent: -device is required")
	}
	if opts.userID == "" {
		return fmt.Errorf("agent: -user is required")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	metrics := store.NewMemory()

	tracer, err := tracing.New(ctx, tracing.Config{
		ServiceName: "ironclaw-agent",
		Endpoint:    opts.otlpEndpoint,
		SampleRatio: opts.sampleRatio,
	})
	if err != nil {
		return err
	}
	defer func() {
		shutdown, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = tracer.Shutdown(shutdown)
	}()

	var queue *spool.Spool
	if opts.spoolDir != "" {
		opened, err := spool.Open(opts.spoolDir, opts.spoolBytes)
		if err != nil {
			return err
		}
		queue = opened
	}

	shipperHTTPS, err := transport.NewHTTPS(transport.HTTPSConfig{
		Endpoint:    opts.endpoint,
		ServerName:  opts.serverName,
		CertFile:    opts.certFile,
		KeyFile:     opts.keyFile,
		CAFile:      opts.caFile,
		Encode:      encodeBatch,
		Spool:       queue,
		BaseBackoff: opts.baseBackoff,
		MaxBackoff:  opts.maxBackoff,
		Metrics:     metrics,
	})
	if err != nil {
		return err
	}

	shipper := transport.NewTraced(shipperHTTPS, tracer)

	source, err := pipeline.NewSyntheticSource(pipeline.SyntheticConfig{
		DeviceID:     opts.deviceID,
		UserID:       opts.userID,
		PodID:        opts.podID,
		ProcessIDs:   []int32{101, 102, 103, 104},
		EventCount:   opts.events,
		Interval:     opts.interval,
		ErrorRate:    0.05,
		BaseLatency:  time.Millisecond,
		TailLatency:  40 * time.Millisecond,
		TailFraction: opts.tailFraction,
		Seed:         time.Now().UnixNano(),
	})
	if err != nil {
		return err
	}
	defer source.Close()

	batcher, err := pipeline.NewBatcher(pipeline.BatcherConfig{
		DeviceID:    opts.deviceID,
		MaxEvents:   opts.batchSize,
		MaxInterval: opts.batchWindow,
		QueueDepth:  opts.queueDepth,
		Policy:      pipeline.OverflowDropOldest,
	})
	if err != nil {
		return err
	}

	if queue != nil {
		go shipperHTTPS.Drain(ctx)
	}
	go report(ctx, opts.reportEvery, batcher, queue)

	log.Printf("agent %s streaming to %s (spool=%v)", opts.deviceID, opts.endpoint, queue != nil)

	if err := batcher.Run(ctx, source, shipper); err != nil && ctx.Err() == nil {
		return err
	}

	logStats(batcher, queue)

	return nil
}

func report(ctx context.Context, every time.Duration, batcher *pipeline.Batcher, queue *spool.Spool) {
	if every <= 0 {
		return
	}

	ticker := time.NewTicker(every)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			logStats(batcher, queue)
		}
	}
}

func logStats(batcher *pipeline.Batcher, queue *spool.Spool) {
	stats := batcher.Stats()

	spooled := 0
	dropped := int64(0)
	if queue != nil {
		queued := queue.Stats()
		spooled = queued.Entries
		dropped = queued.Dropped
	}

	log.Printf("agent stats: accepted=%d sent=%d dropped=%d send_failures=%d spooled=%d spool_dropped=%d",
		stats.EventsAccepted, stats.BatchesSent, stats.EventsDropped, stats.SendFailures, spooled, dropped)
}

func encodeBatch(batch domain.Batch) ([]byte, error) {
	type wireEvent struct {
		UserID              string `json:"user_id"`
		ProcessID           int32  `json:"process_id"`
		PodID               string `json:"pod_id"`
		Kind                uint8  `json:"kind"`
		ObservedAtUnixNanos int64  `json:"observed_at_unix_nanos"`
		LatencyNanos        int64  `json:"latency_nanos"`
		Bytes               int64  `json:"bytes"`
		Failed              bool   `json:"failed"`
	}

	payload := struct {
		DeviceID string      `json:"device_id"`
		Events   []wireEvent `json:"events"`
	}{
		DeviceID: batch.DeviceID,
		Events:   make([]wireEvent, 0, batch.Len()),
	}

	for _, event := range batch.Events {
		payload.Events = append(payload.Events, wireEvent{
			UserID:              event.UserID,
			ProcessID:           event.ProcessID,
			PodID:               event.PodID,
			Kind:                uint8(event.Kind),
			ObservedAtUnixNanos: event.ObservedAt.UnixNano(),
			LatencyNanos:        event.LatencyNanos,
			Bytes:               event.Bytes,
			Failed:              event.Failed,
		})
	}

	return json.Marshal(payload)
}
