package kafkabus

import (
	"context"
	"errors"
	"fmt"

	"github.com/ironclaw/mcp-ironclaw/internal/domain"
	"github.com/ironclaw/mcp-ironclaw/internal/tracing"
	"github.com/ironclaw/mcp-ironclaw/internal/wire"
	"github.com/twmb/franz-go/pkg/kgo"
)

var (
	ErrNoBrokers   = errors.New("kafkabus: at least one broker is required")
	ErrNoTopic     = errors.New("kafkabus: topic is required")
	ErrNoGroup     = errors.New("kafkabus: consumer group is required")
	ErrNoDeadTopic = errors.New("kafkabus: dead letter topic is required when max attempts is exceeded")
)

const (
	MetricPublished  = "kafka_published"
	MetricConsumed   = "kafka_consumed"
	MetricRetried    = "kafka_retried"
	MetricDeadLetter = "kafka_dead_letter"
)

const (
	DeadLetterReasonHeader = "ironclaw-dlq-reason"
	DeadLetterOriginHeader = "ironclaw-dlq-origin"
)

type Config struct {
	Brokers         []string
	Topic           string
	Group           string
	DeadLetterTopic string
	MaxAttempts     int
	Security        Security
	Codec           wire.Codec
	Metrics         domain.MetricsRecorder
	OnHandlerError  func(topic string, attempt int, err error)
}

func (c Config) codec() wire.Codec {
	if c.Codec == nil {
		return wire.JSON{}
	}
	return c.Codec
}

func (c Config) validateProducer() error {
	if len(c.Brokers) == 0 {
		return ErrNoBrokers
	}
	if c.Topic == "" {
		return ErrNoTopic
	}
	return nil
}

func (c Config) validateConsumer() error {
	if err := c.validateProducer(); err != nil {
		return err
	}
	if c.Group == "" {
		return ErrNoGroup
	}
	return nil
}

type Producer struct {
	client  *kgo.Client
	topic   string
	codec   wire.Codec
	metrics domain.MetricsRecorder
}

func NewProducer(config Config) (*Producer, error) {
	if err := config.validateProducer(); err != nil {
		return nil, err
	}

	secure, err := config.Security.options()
	if err != nil {
		return nil, err
	}

	client, err := kgo.NewClient(append([]kgo.Opt{
		kgo.SeedBrokers(config.Brokers...),
		kgo.DefaultProduceTopic(config.Topic),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerBatchCompression(kgo.SnappyCompression()),
		kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)),
		kgo.AllowAutoTopicCreation(),
	}, secure...)...)
	if err != nil {
		return nil, fmt.Errorf("kafkabus: creating producer: %w", err)
	}

	return &Producer{client: client, topic: config.Topic, codec: config.codec(), metrics: config.Metrics}, nil
}

func (p *Producer) Publish(ctx context.Context, partitionKey string, batch domain.Batch) error {
	payload, err := p.codec.EncodeBatch(batch)
	if err != nil {
		return err
	}
	return p.send(ctx, partitionKey, payload)
}

func (p *Producer) PublishCommand(ctx context.Context, command domain.Command) error {
	if err := command.Validate(); err != nil {
		return err
	}
	payload, err := EncodeCommand(command)
	if err != nil {
		return err
	}
	return p.send(ctx, command.ShardTag(), payload)
}

func (p *Producer) PublishWindow(ctx context.Context, window domain.AggregateWindow) error {
	payload, err := p.codec.EncodeWindow(window)
	if err != nil {
		return err
	}
	return p.send(ctx, window.Key.ShardTag(), payload)
}

func (p *Producer) send(ctx context.Context, partitionKey string, payload []byte) error {
	record := &kgo.Record{
		Topic:   p.topic,
		Key:     []byte(partitionKey),
		Value:   payload,
		Headers: traceHeaders(ctx),
	}

	if err := p.client.ProduceSync(ctx, record).FirstErr(); err != nil {
		return fmt.Errorf("kafkabus: producing to %s: %w", p.topic, err)
	}

	p.record(MetricPublished, 1)

	return nil
}

func (p *Producer) record(name string, delta int64) {
	if p.metrics == nil {
		return
	}
	p.metrics.Increment(name, delta)
}

func (p *Producer) Close() {
	p.client.Close()
}

type Consumer struct {
	client          *kgo.Client
	deadLetter      *kgo.Client
	deadLetterTopic string
	maxAttempts     int
	codec           wire.Codec
	metrics         domain.MetricsRecorder
	onError         func(topic string, attempt int, err error)
}

func NewConsumer(config Config) (*Consumer, error) {
	if err := config.validateConsumer(); err != nil {
		return nil, err
	}
	if config.MaxAttempts <= 0 {
		config.MaxAttempts = 1
	}
	if config.MaxAttempts > 1 && config.DeadLetterTopic == "" {
		return nil, ErrNoDeadTopic
	}

	secure, err := config.Security.options()
	if err != nil {
		return nil, err
	}

	client, err := kgo.NewClient(append([]kgo.Opt{
		kgo.SeedBrokers(config.Brokers...),
		kgo.ConsumerGroup(config.Group),
		kgo.ConsumeTopics(config.Topic),
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	}, secure...)...)
	if err != nil {
		return nil, fmt.Errorf("kafkabus: creating consumer: %w", err)
	}

	consumer := &Consumer{
		client:          client,
		deadLetterTopic: config.DeadLetterTopic,
		maxAttempts:     config.MaxAttempts,
		codec:           config.codec(),
		metrics:         config.Metrics,
		onError:         config.OnHandlerError,
	}

	if config.DeadLetterTopic != "" {
		deadLetter, err := kgo.NewClient(append([]kgo.Opt{
			kgo.SeedBrokers(config.Brokers...),
			kgo.DefaultProduceTopic(config.DeadLetterTopic),
			kgo.AllowAutoTopicCreation(),
		}, secure...)...)
		if err != nil {
			client.Close()
			return nil, fmt.Errorf("kafkabus: creating dead letter producer: %w", err)
		}
		consumer.deadLetter = deadLetter
	}

	return consumer, nil
}

func (c *Consumer) Consume(ctx context.Context, handler domain.BatchHandler) error {
	return c.poll(ctx, func(ctx context.Context, record *kgo.Record) error {
		batch, err := c.codec.DecodeBatch(record.Value)
		if err != nil {
			return err
		}
		return handler(ctx, batch)
	})
}

func (c *Consumer) ConsumeCommands(ctx context.Context, handler func(context.Context, domain.Command) error) error {
	return c.poll(ctx, func(ctx context.Context, record *kgo.Record) error {
		command, err := DecodeCommand(record.Value)
		if err != nil {
			return err
		}
		return handler(ctx, command)
	})
}

func (c *Consumer) ConsumeWindows(ctx context.Context, handler domain.WindowHandler) error {
	return c.poll(ctx, func(ctx context.Context, record *kgo.Record) error {
		window, err := c.codec.DecodeWindow(record.Value)
		if err != nil {
			return err
		}
		return handler(ctx, window)
	})
}

func (c *Consumer) poll(ctx context.Context, handle func(context.Context, *kgo.Record) error) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		fetches := c.client.PollFetches(ctx)
		if fetches.IsClientClosed() {
			return nil
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("kafkabus: fetching: %w", errs[0].Err)
		}

		var (
			failure error
			handled []*kgo.Record
		)

		fetches.EachRecord(func(record *kgo.Record) {
			if failure != nil {
				return
			}
			if err := c.deliver(ctx, record, handle); err != nil {
				failure = err
				return
			}
			handled = append(handled, record)
		})

		if len(handled) > 0 {
			if err := c.client.CommitRecords(ctx, handled...); err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				return fmt.Errorf("kafkabus: committing offsets: %w", err)
			}
		}

		if failure != nil {
			return failure
		}
	}
}

func (c *Consumer) deliver(ctx context.Context, record *kgo.Record, handle func(context.Context, *kgo.Record) error) error {
	ctx = traceContext(ctx, record)

	var last error

	for attempt := 1; attempt <= c.maxAttempts; attempt++ {
		err := handle(ctx, record)
		if err == nil {
			c.record(MetricConsumed, 1)
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		last = err
		if c.onError != nil {
			c.onError(record.Topic, attempt, err)
		}
		if attempt < c.maxAttempts {
			c.record(MetricRetried, 1)
		}
	}

	return c.sendToDeadLetter(ctx, record, last)
}

func (c *Consumer) sendToDeadLetter(ctx context.Context, record *kgo.Record, cause error) error {
	if c.deadLetter == nil {
		return ErrNoDeadTopic
	}

	dead := &kgo.Record{
		Topic:   c.deadLetterTopic,
		Key:     record.Key,
		Value:   record.Value,
		Headers: deadLetterHeaders(record, cause),
	}
	if err := c.deadLetter.ProduceSync(ctx, dead).FirstErr(); err != nil {
		return fmt.Errorf("kafkabus: dead lettering: %w", err)
	}

	c.record(MetricDeadLetter, 1)

	return nil
}

func deadLetterHeaders(record *kgo.Record, cause error) []kgo.RecordHeader {
	headers := []kgo.RecordHeader{
		{Key: DeadLetterOriginHeader, Value: []byte(record.Topic)},
	}
	if cause != nil {
		headers = append(headers, kgo.RecordHeader{Key: DeadLetterReasonHeader, Value: []byte(cause.Error())})
	}
	return headers
}

func (c *Consumer) record(name string, delta int64) {
	if c.metrics == nil {
		return
	}
	c.metrics.Increment(name, delta)
}

func (c *Consumer) Close() {
	c.client.Close()
	if c.deadLetter != nil {
		c.deadLetter.Close()
	}
}

func traceHeaders(ctx context.Context) []kgo.RecordHeader {
	carrier := tracing.Inject(ctx)
	if len(carrier) == 0 {
		return nil
	}

	headers := make([]kgo.RecordHeader, 0, len(carrier))
	for key, value := range carrier {
		headers = append(headers, kgo.RecordHeader{Key: key, Value: []byte(value)})
	}

	return headers
}

func traceContext(ctx context.Context, record *kgo.Record) context.Context {
	if len(record.Headers) == 0 {
		return ctx
	}

	carrier := make(tracing.Carrier, len(record.Headers))
	for _, header := range record.Headers {
		carrier[header.Key] = string(header.Value)
	}

	return tracing.Extract(ctx, carrier)
}
