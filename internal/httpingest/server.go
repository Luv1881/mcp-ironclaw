package httpingest

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"time"

	"github.com/ironclaw/mcp-ironclaw/internal/domain"
	"github.com/ironclaw/mcp-ironclaw/internal/wire"
)

var ErrNilAcceptor = errors.New("httpingest: batch acceptor is nil")

const (
	DefaultIdentityHeader = "X-Device-Id"
	DefaultMaxBodyBytes   = 8 << 20

	MetricRequestsAccepted    = "http_ingest_accepted"
	MetricRequestsRejected    = "http_ingest_rejected"
	MetricRequestsUnauthentic = "http_ingest_unauthenticated"
)

const PodIDEnv = "IRONCLAW_POD_ID"

func LocalPodID() string {
	if pod := os.Getenv(PodIDEnv); pod != "" {
		return pod
	}
	if host, err := os.Hostname(); err == nil {
		return host
	}
	return "unknown-pod"
}

type BatchAcceptor interface {
	Accept(ctx context.Context, authenticatedDeviceID string, batch domain.Batch) error
}

type Config struct {
	Acceptor         BatchAcceptor
	IdentityHeader   string
	MaxBodyBytes     int64
	AllowedClientCNs []string
	PodID            string
	Metrics          domain.MetricsRecorder
	Clock            domain.Clock
}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now().UTC() }

type Server struct {
	acceptor       BatchAcceptor
	identityHeader string
	maxBodyBytes   int64
	allowedClients map[string]bool
	podID          string
	metrics        domain.MetricsRecorder
	clock          domain.Clock
}

func New(config Config) (*Server, error) {
	if config.Acceptor == nil {
		return nil, ErrNilAcceptor
	}
	if config.IdentityHeader == "" {
		config.IdentityHeader = DefaultIdentityHeader
	}
	if config.MaxBodyBytes <= 0 {
		config.MaxBodyBytes = DefaultMaxBodyBytes
	}
	clock := config.Clock
	if clock == nil {
		clock = systemClock{}
	}
	podID := config.PodID
	if podID == "" {
		podID = LocalPodID()
	}

	allowed := make(map[string]bool, len(config.AllowedClientCNs))
	for _, name := range config.AllowedClientCNs {
		if name != "" {
			allowed[name] = true
		}
	}

	return &Server{
		acceptor:       config.Acceptor,
		identityHeader: config.IdentityHeader,
		maxBodyBytes:   config.MaxBodyBytes,
		allowedClients: allowed,
		podID:          podID,
		metrics:        config.Metrics,
		clock:          clock,
	}, nil
}

func (s *Server) peerIsAuthorisedEdge(r *http.Request) bool {
	if len(s.allowedClients) == 0 {
		return true
	}
	if r.TLS == nil || len(r.TLS.PeerCertificates) == 0 {
		return false
	}
	return s.allowedClients[r.TLS.PeerCertificates[0].Subject.CommonName]
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.health)
	mux.HandleFunc("POST /v1/batches", s.ingest)
	return mux
}

func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func (s *Server) ingest(w http.ResponseWriter, r *http.Request) {
	if !s.peerIsAuthorisedEdge(r) {
		s.record(MetricRequestsUnauthentic, 1)
		http.Error(w, "client certificate is not an authorised edge", http.StatusForbidden)
		return
	}

	deviceID := r.Header.Get(s.identityHeader)
	if deviceID == "" {
		s.record(MetricRequestsUnauthentic, 1)
		http.Error(w, "missing authenticated device identity", http.StatusUnauthorized)
		return
	}

	body := http.MaxBytesReader(w, r.Body, s.maxBodyBytes)
	defer body.Close()

	var payload wire.EdgeBatch
	if err := json.NewDecoder(body).Decode(&payload); err != nil {
		s.record(MetricRequestsRejected, 1)
		http.Error(w, "malformed batch payload", http.StatusBadRequest)
		return
	}

	if payload.DeviceID != "" && payload.DeviceID != deviceID {
		s.record(MetricRequestsRejected, 1)
		http.Error(w, "batch device id does not match the authenticated identity", http.StatusForbidden)
		return
	}

	batch, err := s.toBatch(deviceID, payload)
	if err != nil {
		s.record(MetricRequestsRejected, 1)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err := s.acceptor.Accept(r.Context(), deviceID, batch); err != nil {
		if errors.Is(err, domain.ErrDeviceMismatch) {
			s.record(MetricRequestsRejected, 1)
			http.Error(w, "batch rejected", http.StatusForbidden)
			return
		}
		s.record(MetricRequestsRejected, 1)
		http.Error(w, "batch rejected", http.StatusBadRequest)
		return
	}

	s.record(MetricRequestsAccepted, 1)
	w.WriteHeader(http.StatusAccepted)
}

func (s *Server) toBatch(deviceID string, payload wire.EdgeBatch) (domain.Batch, error) {
	batch := domain.Batch{
		DeviceID:  deviceID,
		CreatedAt: s.clock.Now(),
		Events:    make([]domain.Event, 0, len(payload.Events)),
	}

	for i := range payload.Events {
		event := &payload.Events[i]
		observed := s.clock.Now()
		if event.ObservedAtUnixNanos > 0 {
			observed = time.Unix(0, event.ObservedAtUnixNanos).UTC()
		}

		kind := domain.EventKind(event.Kind)
		if !kind.Valid() {
			kind = domain.EventKindSyscall
		}

		batch.Events = append(batch.Events, domain.Event{
			DeviceID:     deviceID,
			UserID:       event.UserID,
			ProcessID:    event.ProcessID,
			PodID:        s.podID,
			Kind:         kind,
			ObservedAt:   observed,
			LatencyNanos: event.LatencyNanos,
			Bytes:        event.Bytes,
			Failed:       event.Failed,
		})
	}

	if err := batch.Validate(); err != nil {
		return domain.Batch{}, err
	}

	return batch, nil
}

func (s *Server) record(name string, delta int64) {
	if s.metrics == nil {
		return
	}
	s.metrics.Increment(name, delta)
}
