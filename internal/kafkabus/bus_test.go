package kafkabus_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ironclaw/mcp-ironclaw/internal/domain"
	"github.com/ironclaw/mcp-ironclaw/internal/kafkabus"
	"github.com/ironclaw/mcp-ironclaw/internal/store"
	"github.com/ironclaw/mcp-ironclaw/internal/testenv"
	"github.com/ironclaw/mcp-ironclaw/internal/tracing"
	"github.com/ironclaw/mcp-ironclaw/internal/wire"
	"github.com/twmb/franz-go/pkg/kgo"
)

func uniqueTopic(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
}

func batchOf(deviceID string, count int) domain.Batch {
	events := make([]domain.Event, 0, count)
	for i := 0; i < count; i++ {
		events = append(events, domain.Event{
			DeviceID:     deviceID,
			UserID:       "user-1",
			ProcessID:    int32(i),
			PodID:        "pod-a",
			Kind:         domain.EventKindSyscall,
			ObservedAt:   time.Unix(1700000000, int64(i)).UTC(),
			LatencyNanos: int64(1000 + i),
			Bytes:        int64(64 + i),
			Failed:       i%3 == 0,
		})
	}
	return domain.Batch{DeviceID: deviceID, CreatedAt: time.Unix(1700000000, 0).UTC(), Events: events}
}

func TestConfigValidation(t *testing.T) {
	if _, err := kafkabus.NewProducer(kafkabus.Config{}); !errors.Is(err, kafkabus.ErrNoBrokers) {
		t.Fatalf("got %v, want ErrNoBrokers", err)
	}
	if _, err := kafkabus.NewProducer(kafkabus.Config{Brokers: []string{"localhost:9092"}}); !errors.Is(err, kafkabus.ErrNoTopic) {
		t.Fatalf("got %v, want ErrNoTopic", err)
	}
	if _, err := kafkabus.NewConsumer(kafkabus.Config{Brokers: []string{"localhost:9092"}, Topic: "t"}); !errors.Is(err, kafkabus.ErrNoGroup) {
		t.Fatalf("got %v, want ErrNoGroup", err)
	}
}

func TestBatchCodecRoundTrip(t *testing.T) {
	original := batchOf("device-1", 4)

	encoded, err := wire.JSON{}.EncodeBatch(original)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	decoded, err := wire.JSON{}.DecodeBatch(encoded)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if decoded.DeviceID != original.DeviceID || decoded.Len() != original.Len() {
		t.Fatalf("decoded %s/%d, want %s/%d", decoded.DeviceID, decoded.Len(), original.DeviceID, original.Len())
	}
	for i := range original.Events {
		if decoded.Events[i] != original.Events[i] {
			t.Fatalf("event %d changed across the wire:\n got %+v\nwant %+v", i, decoded.Events[i], original.Events[i])
		}
	}
	if err := decoded.Validate(); err != nil {
		t.Fatalf("decoded batch failed domain validation: %v", err)
	}
}

func TestWindowCodecRoundTrip(t *testing.T) {
	original := domain.AggregateWindow{
		Key:         domain.CorrelationKey{UserID: "user-1", DeviceID: "device-1", ProcessID: 7, PodID: "pod-a"},
		WindowID:    42,
		WindowStart: time.Unix(420, 0).UTC(),
		WindowEnd:   time.Unix(430, 0).UTC(),
		Count:       100,
		ErrorCount:  3,
		Bytes:       9000,
		P95Nanos:    5000,
		P99Nanos:    9000,
		MaxNanos:    12000,
	}

	encoded, err := wire.JSON{}.EncodeWindow(original)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	decoded, err := wire.JSON{}.DecodeWindow(encoded)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if decoded != original {
		t.Fatalf("decoded %+v, want %+v", decoded, original)
	}
	if decoded.Identity() != original.Identity() {
		t.Fatalf("identity changed across the wire: %s vs %s", decoded.Identity(), original.Identity())
	}
}

func TestDecodeRejectsMalformedPayload(t *testing.T) {
	if _, err := (wire.JSON{}).DecodeBatch([]byte("{not json")); err == nil {
		t.Fatal("expected a decode error for malformed batch json")
	}
	if _, err := (wire.JSON{}).DecodeWindow([]byte("{not json")); err == nil {
		t.Fatal("expected a decode error for malformed window json")
	}
}

func TestPublishAndConsumeRoundTripThroughKafka(t *testing.T) {
	brokers := testenv.KafkaBrokers(t)
	topic := uniqueTopic("ironclaw-batches")
	metrics := store.NewMemory()

	producer, err := kafkabus.NewProducer(kafkabus.Config{Brokers: brokers, Topic: topic, Metrics: metrics})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer producer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const total = 20
	for i := 0; i < total; i++ {
		if err := producer.Publish(ctx, "device-1", batchOf("device-1", i+1)); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	consumer, err := kafkabus.NewConsumer(kafkabus.Config{
		Brokers: brokers, Topic: topic, Group: "ironclaw-test-" + topic, Metrics: metrics,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer consumer.Close()

	var mu sync.Mutex
	sizes := []int{}

	consumeCtx, stop := context.WithCancel(ctx)
	defer stop()

	done := make(chan error, 1)
	go func() {
		done <- consumer.Consume(consumeCtx, func(_ context.Context, batch domain.Batch) error {
			mu.Lock()
			sizes = append(sizes, batch.Len())
			complete := len(sizes) == total
			mu.Unlock()
			if complete {
				stop()
			}
			return nil
		})
	}()

	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("timed out waiting for kafka round trip")
	}

	mu.Lock()
	defer mu.Unlock()

	if len(sizes) != total {
		t.Fatalf("consumed %d batches, want %d", len(sizes), total)
	}
	for i, size := range sizes {
		if size != i+1 {
			t.Fatalf("batch %d has %d events, want %d: per-device ordering was not preserved", i, size, i+1)
		}
	}
}

func TestSameDeviceKeyAlwaysLandsOnOnePartition(t *testing.T) {
	brokers := testenv.KafkaBrokers(t)
	topic := uniqueTopic("ironclaw-partitioning")

	producer, err := kafkabus.NewProducer(kafkabus.Config{Brokers: brokers, Topic: topic})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer producer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	for i := 0; i < 30; i++ {
		device := fmt.Sprintf("device-%d", i%3)
		if err := producer.Publish(ctx, device, batchOf(device, 1)); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	consumer, err := kafkabus.NewConsumer(kafkabus.Config{
		Brokers: brokers, Topic: topic, Group: "ironclaw-test-" + topic,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer consumer.Close()

	var mu sync.Mutex
	seen := 0

	consumeCtx, stop := context.WithCancel(ctx)
	defer stop()

	go func() {
		consumer.Consume(consumeCtx, func(_ context.Context, batch domain.Batch) error {
			mu.Lock()
			seen++
			complete := seen == 30
			mu.Unlock()
			if complete {
				stop()
			}
			return nil
		})
	}()

	<-consumeCtx.Done()

	mu.Lock()
	defer mu.Unlock()
	if seen != 30 {
		t.Fatalf("consumed %d records, want 30", seen)
	}
}

func TestPoisonRecordIsDeadLetteredAfterRetries(t *testing.T) {
	brokers := testenv.KafkaBrokers(t)
	topic := uniqueTopic("ironclaw-poison")
	deadTopic := topic + "-dlq"
	metrics := store.NewMemory()

	producer, err := kafkabus.NewProducer(kafkabus.Config{Brokers: brokers, Topic: topic})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer producer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if err := producer.Publish(ctx, "device-1", batchOf("device-1", 1)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	consumer, err := kafkabus.NewConsumer(kafkabus.Config{
		Brokers:         brokers,
		Topic:           topic,
		Group:           "ironclaw-test-" + topic,
		DeadLetterTopic: deadTopic,
		MaxAttempts:     3,
		Metrics:         metrics,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer consumer.Close()

	var mu sync.Mutex
	attempts := 0

	consumeCtx, stop := context.WithCancel(ctx)
	defer stop()

	go func() {
		consumer.Consume(consumeCtx, func(context.Context, domain.Batch) error {
			mu.Lock()
			attempts++
			done := attempts >= 3
			mu.Unlock()
			if done {
				go func() {
					time.Sleep(500 * time.Millisecond)
					stop()
				}()
			}
			return errors.New("poison record")
		})
	}()

	<-consumeCtx.Done()

	mu.Lock()
	defer mu.Unlock()
	if attempts != 3 {
		t.Fatalf("handler called %d times, want exactly MaxAttempts=3", attempts)
	}

	recorded, err := metrics.PipelineMetrics(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if recorded[kafkabus.MetricDeadLetter] != 1 {
		t.Fatalf("dead lettered %d records, want 1", recorded[kafkabus.MetricDeadLetter])
	}
	if recorded[kafkabus.MetricConsumed] != 0 {
		t.Fatalf("counted %d successful deliveries for a poison record, want 0", recorded[kafkabus.MetricConsumed])
	}
}

func TestTraceContextSurvivesARealKafkaRoundTrip(t *testing.T) {
	brokers := testenv.KafkaBrokers(t)
	topic := uniqueTopic("ironclaw-trace")

	provider, err := tracing.New(context.Background(), tracing.Config{
		ServiceName: "ironclaw-test",
		Writer:      io.Discard,
		SampleRatio: 1,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer provider.Shutdown(context.Background())

	producer, err := kafkabus.NewProducer(kafkabus.Config{Brokers: brokers, Topic: topic})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer producer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	produceCtx, span := provider.Start(ctx, "ingest.publish")
	sent := tracing.TraceIDFrom(produceCtx)
	if sent == "" {
		t.Fatal("no trace id on the producing side")
	}

	if err := producer.Publish(produceCtx, "device-1", batchOf("device-1", 1)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	span.End()

	consumer, err := kafkabus.NewConsumer(kafkabus.Config{
		Brokers: brokers, Topic: topic, Group: "ironclaw-trace-" + topic,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer consumer.Close()

	observed := make(chan string, 1)
	consumeCtx, stop := context.WithCancel(ctx)
	defer stop()

	go func() {
		_ = consumer.Consume(consumeCtx, func(inner context.Context, _ domain.Batch) error {
			select {
			case observed <- tracing.TraceIDFrom(inner):
			default:
			}
			stop()
			return nil
		})
	}()

	select {
	case got := <-observed:
		if got != sent {
			t.Fatalf("consumer saw trace id %q, want the producer's %q", got, sent)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for the traced record")
	}
}

func TestDeadLetteredRecordCarriesItsReason(t *testing.T) {
	brokers := testenv.KafkaBrokers(t)
	topic := uniqueTopic("ironclaw-dlq-reason")
	deadTopic := topic + "-dlq"

	producer, err := kafkabus.NewProducer(kafkabus.Config{Brokers: brokers, Topic: topic})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer producer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if err := producer.Publish(ctx, "device-1", batchOf("device-1", 1)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var (
		mu       sync.Mutex
		observed []string
	)

	consumer, err := kafkabus.NewConsumer(kafkabus.Config{
		Brokers:         brokers,
		Topic:           topic,
		Group:           "ironclaw-test-" + topic,
		DeadLetterTopic: deadTopic,
		MaxAttempts:     2,
		OnHandlerError: func(failedTopic string, attempt int, err error) {
			mu.Lock()
			observed = append(observed, fmt.Sprintf("%s/%d/%v", failedTopic, attempt, err))
			mu.Unlock()
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer consumer.Close()

	consumeCtx, stop := context.WithCancel(ctx)
	defer stop()

	go func() {
		_ = consumer.Consume(consumeCtx, func(context.Context, domain.Batch) error {
			go func() {
				time.Sleep(700 * time.Millisecond)
				stop()
			}()
			return errors.New("redis is holding the wrong type")
		})
	}()

	<-consumeCtx.Done()

	mu.Lock()
	seen := append([]string(nil), observed...)
	mu.Unlock()

	if len(seen) == 0 {
		t.Fatal("a failing handler produced no error report, so the reason is invisible to an operator")
	}
	if !strings.Contains(seen[0], "redis is holding the wrong type") {
		t.Fatalf("reported %q, want the handler's own error", seen[0])
	}

	reader, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(deadTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer reader.Close()

	readCtx, cancelRead := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelRead()

	fetches := reader.PollRecords(readCtx, 1)
	if err := fetches.Err(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	records := fetches.Records()
	if len(records) == 0 {
		t.Fatal("nothing arrived on the dead letter topic")
	}

	headers := map[string]string{}
	for _, header := range records[0].Headers {
		headers[header.Key] = string(header.Value)
	}

	if !strings.Contains(headers[kafkabus.DeadLetterReasonHeader], "redis is holding the wrong type") {
		t.Fatalf("dead letter reason header is %q, want the handler's error", headers[kafkabus.DeadLetterReasonHeader])
	}
	if headers[kafkabus.DeadLetterOriginHeader] != topic {
		t.Fatalf("dead letter origin header is %q, want %q", headers[kafkabus.DeadLetterOriginHeader], topic)
	}
}

func TestRetryingConsumerRequiresADeadLetterTopic(t *testing.T) {
	_, err := kafkabus.NewConsumer(kafkabus.Config{
		Brokers:     []string{"localhost:19092"},
		Topic:       "irrelevant",
		Group:       "irrelevant",
		MaxAttempts: 3,
	})
	if !errors.Is(err, kafkabus.ErrNoDeadTopic) {
		t.Fatalf("got %v, want ErrNoDeadTopic at construction rather than a runtime failure on the first poison message", err)
	}
}

func TestSingleAttemptConsumerNeedsNoDeadLetterTopic(t *testing.T) {
	consumer, err := kafkabus.NewConsumer(kafkabus.Config{
		Brokers:     []string{"localhost:19092"},
		Topic:       "irrelevant",
		Group:       "irrelevant",
		MaxAttempts: 1,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	consumer.Close()
}

func TestOffsetsAdvanceOnlyOverHandledRecords(t *testing.T) {
	brokers := testenv.KafkaBrokers(t)
	topic := uniqueTopic("ironclaw-partial")
	group := "ironclaw-test-" + topic

	producer, err := kafkabus.NewProducer(kafkabus.Config{Brokers: brokers, Topic: topic})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer producer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	const published = 6
	for i := 0; i < published; i++ {
		if err := producer.Publish(ctx, "one-partition", batchOf(fmt.Sprintf("device-%d", i), 1)); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	failAt := "device-3"

	consumer, err := kafkabus.NewConsumer(kafkabus.Config{
		Brokers:     brokers,
		Topic:       topic,
		Group:       group,
		MaxAttempts: 1,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var (
		mu    sync.Mutex
		first []string
	)

	consumeCtx, stop := context.WithCancel(ctx)
	go func() {
		_ = consumer.Consume(consumeCtx, func(_ context.Context, batch domain.Batch) error {
			mu.Lock()
			defer mu.Unlock()
			if batch.DeviceID == failAt {
				return errors.New("refusing this batch")
			}
			first = append(first, batch.DeviceID)
			return nil
		})
		stop()
	}()

	<-consumeCtx.Done()
	consumer.Close()

	mu.Lock()
	handled := append([]string(nil), first...)
	mu.Unlock()

	if len(handled) == 0 {
		t.Fatal("the first consumer handled nothing, so this test proves nothing")
	}
	for _, device := range handled {
		if device == failAt {
			t.Fatalf("%s was reported handled but the handler refused it", failAt)
		}
	}

	resumed, err := kafkabus.NewConsumer(kafkabus.Config{
		Brokers:     brokers,
		Topic:       topic,
		Group:       group,
		MaxAttempts: 1,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer resumed.Close()

	seen := make(chan string, published)
	resumeCtx, stopResume := context.WithTimeout(ctx, 25*time.Second)
	defer stopResume()

	go func() {
		_ = resumed.Consume(resumeCtx, func(_ context.Context, batch domain.Batch) error {
			select {
			case seen <- batch.DeviceID:
			default:
			}
			return nil
		})
	}()

	deadline := time.After(25 * time.Second)
	for {
		select {
		case device := <-seen:
			if device == failAt {
				return
			}
		case <-deadline:
			t.Fatalf("%s was never redelivered: its offset was committed even though the handler refused it", failAt)
		}
	}
}
