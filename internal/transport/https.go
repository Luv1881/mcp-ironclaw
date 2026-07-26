package transport

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/ironclaw/mcp-ironclaw/internal/domain"
	"github.com/ironclaw/mcp-ironclaw/internal/spool"
	"github.com/ironclaw/mcp-ironclaw/internal/tracing"
)

var (
	ErrNoEndpoint    = errors.New("transport: endpoint is required")
	ErrNoKeyPair     = errors.New("transport: a client certificate and key are required")
	ErrEmptyCABundle = errors.New("transport: CA bundle contained no certificates")
	ErrRejected      = errors.New("transport: edge rejected the batch")
)

var _ domain.Transport = (*HTTPS)(nil)

type Encoder func(domain.Batch) ([]byte, error)

type HTTPSConfig struct {
	Endpoint     string
	CertFile     string
	KeyFile      string
	CAFile       string
	ServerName   string
	Encode       Encoder
	Spool        *spool.Spool
	Client       *http.Client
	BaseBackoff  time.Duration
	MaxBackoff   time.Duration
	DrainEvery   time.Duration
	Metrics      domain.MetricsRecorder
	RandomSource *rand.Rand
}

const (
	MetricSent         = "agent_batches_sent"
	MetricSpooled      = "agent_batches_spooled"
	MetricDrained      = "agent_batches_drained"
	MetricSpoolDropped = "agent_spool_dropped"
)

type HTTPS struct {
	endpoint string
	client   *http.Client
	encode   Encoder
	queue    *spool.Spool
	metrics  domain.MetricsRecorder

	baseBackoff time.Duration
	maxBackoff  time.Duration
	drainEvery  time.Duration

	mu          sync.Mutex
	random      *rand.Rand
	lastDropped int64
}

func NewHTTPS(config HTTPSConfig) (*HTTPS, error) {
	if config.Endpoint == "" {
		return nil, ErrNoEndpoint
	}
	if config.Encode == nil {
		return nil, errors.New("transport: an encoder is required")
	}

	client := config.Client
	if client == nil {
		tlsConfig, err := buildClientTLS(config)
		if err != nil {
			return nil, err
		}
		client = &http.Client{
			Timeout:   15 * time.Second,
			Transport: &http.Transport{TLSClientConfig: tlsConfig, MaxIdleConnsPerHost: 4},
		}
	}

	base := config.BaseBackoff
	if base <= 0 {
		base = 250 * time.Millisecond
	}
	max := config.MaxBackoff
	if max <= 0 {
		max = 30 * time.Second
	}
	drain := config.DrainEvery
	if drain <= 0 {
		drain = 2 * time.Second
	}
	random := config.RandomSource
	if random == nil {
		random = rand.New(rand.NewSource(time.Now().UnixNano()))
	}

	return &HTTPS{
		endpoint:    config.Endpoint,
		client:      client,
		encode:      config.Encode,
		queue:       config.Spool,
		metrics:     config.Metrics,
		baseBackoff: base,
		maxBackoff:  max,
		drainEvery:  drain,
		random:      random,
	}, nil
}

func buildClientTLS(config HTTPSConfig) (*tls.Config, error) {
	if config.CertFile == "" || config.KeyFile == "" {
		return nil, ErrNoKeyPair
	}

	certificate, err := tls.LoadX509KeyPair(config.CertFile, config.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("transport: loading client certificate: %w", err)
	}

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{certificate},
		MinVersion:   tls.VersionTLS13,
		ServerName:   config.ServerName,
	}

	if config.CAFile != "" {
		pem, err := os.ReadFile(config.CAFile)
		if err != nil {
			return nil, fmt.Errorf("transport: reading CA bundle: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, ErrEmptyCABundle
		}
		tlsConfig.RootCAs = pool
	}

	return tlsConfig, nil
}

func (h *HTTPS) Send(ctx context.Context, batch domain.Batch) error {
	payload, err := h.encode(batch)
	if err != nil {
		return err
	}

	if err := h.post(ctx, payload); err != nil {
		if h.queue == nil {
			return err
		}
		h.record(MetricSpooled, 1)
		enqueued := h.queue.Enqueue(payload)
		h.recordSpoolDrops()
		return enqueued
	}

	h.record(MetricSent, 1)

	return nil
}

func (h *HTTPS) post(ctx context.Context, payload []byte) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, h.endpoint, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	for key, value := range tracing.Inject(ctx) {
		request.Header.Set(key, value)
	}

	response, err := h.client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusAccepted {
		return fmt.Errorf("%w: status %d", ErrRejected, response.StatusCode)
	}

	return nil
}

func (h *HTTPS) Drain(ctx context.Context) {
	if h.queue == nil {
		return
	}

	failures := 0

	for {
		if err := ctx.Err(); err != nil {
			return
		}

		payload, name, err := h.queue.Peek()
		if err != nil {
			if !h.wait(ctx, h.drainEvery) {
				return
			}
			failures = 0
			continue
		}

		if err := h.post(ctx, payload); err != nil {
			failures++
			if !h.wait(ctx, h.backoff(failures)) {
				return
			}
			continue
		}

		h.queue.Release(name)
		h.record(MetricDrained, 1)
		failures = 0
	}
}

func (h *HTTPS) backoff(attempt int) time.Duration {
	delay := h.baseBackoff << min(attempt-1, 16)
	if delay > h.maxBackoff || delay <= 0 {
		delay = h.maxBackoff
	}

	h.mu.Lock()
	jitter := h.random.Float64()
	h.mu.Unlock()

	return time.Duration(float64(delay) * (0.5 + 0.5*jitter))
}

func (h *HTTPS) wait(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (h *HTTPS) recordSpoolDrops() {
	dropped := h.queue.Stats().Dropped

	h.mu.Lock()
	delta := dropped - h.lastDropped
	h.lastDropped = dropped
	h.mu.Unlock()

	if delta > 0 {
		h.record(MetricSpoolDropped, delta)
	}
}

func (h *HTTPS) record(name string, delta int64) {
	if h.metrics == nil {
		return
	}
	h.metrics.Increment(name, delta)
}
