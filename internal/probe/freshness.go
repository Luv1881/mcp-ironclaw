package probe

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"math"
	"net/http"
	"sort"
	"time"
)

var (
	ErrNilClient     = errors.New("probe: http client is nil")
	ErrNilObserver   = errors.New("probe: visibility observer is nil")
	ErrNotAccepted   = errors.New("probe: edge did not accept the batch")
	ErrNeverVisible  = errors.New("probe: batch never became visible before the deadline")
	ErrNoSamples     = errors.New("probe: no samples were collected")
	ErrInvalidRounds = errors.New("probe: rounds must be positive")
)

type VisibilityObserver interface {
	Count(ctx context.Context, userID, deviceID string) (int64, error)
}

type Config struct {
	Endpoint   string
	UserID     string
	DeviceID   string
	Rounds     int
	EventCount int
	PollEvery  time.Duration
	Timeout    time.Duration
	Client     *http.Client
	Observer   VisibilityObserver
}

func (c Config) validate() error {
	if c.Client == nil {
		return ErrNilClient
	}
	if c.Observer == nil {
		return ErrNilObserver
	}
	if c.Rounds <= 0 {
		return ErrInvalidRounds
	}
	return nil
}

type Report struct {
	Samples []time.Duration
	Sent    int
	Lost    int
}

func (r Report) Quantile(q float64) time.Duration {
	if len(r.Samples) == 0 {
		return 0
	}

	sorted := append([]time.Duration(nil), r.Samples...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	rank := int(math.Floor(q * float64(len(sorted)-1)))
	return sorted[rank]
}

func (r Report) Mean() time.Duration {
	if len(r.Samples) == 0 {
		return 0
	}

	var total time.Duration
	for _, sample := range r.Samples {
		total += sample
	}
	return total / time.Duration(len(r.Samples))
}

type Freshness struct {
	config Config
}

func NewFreshness(config Config) (*Freshness, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}
	if config.EventCount <= 0 {
		config.EventCount = 1
	}
	if config.PollEvery <= 0 {
		config.PollEvery = 20 * time.Millisecond
	}
	if config.Timeout <= 0 {
		config.Timeout = 30 * time.Second
	}
	return &Freshness{config: config}, nil
}

func (f *Freshness) Run(ctx context.Context) (Report, error) {
	report := Report{}

	for round := 0; round < f.config.Rounds; round++ {
		if err := ctx.Err(); err != nil {
			return report, err
		}

		sample, err := f.measure(ctx)
		report.Sent++

		switch {
		case errors.Is(err, ErrNeverVisible):
			report.Lost++
		case err != nil:
			return report, err
		default:
			report.Samples = append(report.Samples, sample)
		}
	}

	if len(report.Samples) == 0 {
		return report, ErrNoSamples
	}

	return report, nil
}

func (f *Freshness) measure(ctx context.Context) (time.Duration, error) {
	before, err := f.config.Observer.Count(ctx, f.config.UserID, f.config.DeviceID)
	if err != nil {
		return 0, err
	}

	sentAt := time.Now()
	if err := f.post(ctx, sentAt); err != nil {
		return 0, err
	}

	deadline := time.Now().Add(f.config.Timeout)
	for time.Now().Before(deadline) {
		if err := ctx.Err(); err != nil {
			return 0, err
		}

		current, err := f.config.Observer.Count(ctx, f.config.UserID, f.config.DeviceID)
		if err == nil && current >= before+int64(f.config.EventCount) {
			return time.Since(sentAt), nil
		}

		time.Sleep(f.config.PollEvery)
	}

	return 0, ErrNeverVisible
}

func (f *Freshness) post(ctx context.Context, at time.Time) error {
	var body bytes.Buffer
	body.WriteString(`{"events":[`)
	for i := 0; i < f.config.EventCount; i++ {
		if i > 0 {
			body.WriteString(",")
		}
		fmt.Fprintf(&body,
			`{"user_id":%q,"process_id":%d,"pod_id":"probe","kind":1,"observed_at_unix_nanos":%d,"latency_nanos":1000000,"bytes":64,"failed":false}`,
			f.config.UserID, i, at.UnixNano())
	}
	body.WriteString(`]}`)

	request, err := http.NewRequestWithContext(ctx, http.MethodPost, f.config.Endpoint, &body)
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")

	response, err := f.config.Client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusAccepted {
		return fmt.Errorf("%w: status %d", ErrNotAccepted, response.StatusCode)
	}

	return nil
}

func TLSClient(certificate tls.Certificate, roots *tls.Config) *http.Client {
	config := roots.Clone()
	config.Certificates = []tls.Certificate{certificate}

	return &http.Client{
		Timeout: 15 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig:     config,
			MaxIdleConnsPerHost: 4,
		},
	}
}
