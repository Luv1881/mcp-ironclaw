package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/ironclaw/mcp-ironclaw/internal/domain"
	"github.com/ironclaw/mcp-ironclaw/internal/httpingest"
	"github.com/ironclaw/mcp-ironclaw/internal/ingest"
	"github.com/ironclaw/mcp-ironclaw/internal/kafkabus"
	ironmetrics "github.com/ironclaw/mcp-ironclaw/internal/metrics"
	"github.com/ironclaw/mcp-ironclaw/internal/redisstore"
	"github.com/ironclaw/mcp-ironclaw/internal/store"
	"github.com/ironclaw/mcp-ironclaw/internal/tracing"
	"github.com/ironclaw/mcp-ironclaw/internal/wire"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/attribute"
)

type options struct {
	addr         string
	certFile     string
	keyFile      string
	clientCA     string
	allowedCNs   string
	healthAddr   string
	brokers      string
	topic        string
	redisAddr    string
	requireMTLS  bool
	otlpEndpoint string
	sampleRatio  float64
	wireFormat   string
}

func (o options) allowedClientCNs() []string {
	if o.allowedCNs == "" {
		return nil
	}
	return strings.Split(o.allowedCNs, ",")
}

func main() {
	var opts options

	flag.StringVar(&opts.addr, "addr", ":8443", "listen address")
	flag.StringVar(&opts.certFile, "cert", "", "server certificate file")
	flag.StringVar(&opts.keyFile, "key", "", "server private key file")
	flag.StringVar(&opts.clientCA, "client-ca", "", "CA bundle used to verify the edge client certificate")
	flag.StringVar(&opts.brokers, "kafka", "localhost:19092", "comma separated kafka brokers")
	flag.StringVar(&opts.topic, "topic", "ironclaw.events.raw", "topic receiving raw device batches")
	flag.StringVar(&opts.redisAddr, "redis", "", "redis address for pipeline metrics; empty keeps metrics in memory")
	flag.StringVar(&opts.allowedCNs, "allowed-client-cn", "edge.ironclaw.internal", "comma separated client certificate common names permitted to submit batches; empty disables the check")
	flag.StringVar(&opts.healthAddr, "health-addr", "", "optional plaintext address serving only /healthz, for orchestrator probes that cannot present a client certificate")
	flag.BoolVar(&opts.requireMTLS, "require-mtls", true, "require and verify a client certificate")
	flag.StringVar(&opts.otlpEndpoint, "otlp", os.Getenv("IRONCLAW_OTLP"), "OTLP gRPC endpoint receiving traces; empty disables tracing")
	flag.Float64Var(&opts.sampleRatio, "trace-sample", 0.01, "fraction of requests traced")
	flag.StringVar(&opts.wireFormat, "wire-format", "json", "kafka payload encoding: json or protobuf")
	flag.Parse()

	if err := run(opts); err != nil {
		log.Fatalf("ingest: %v", err)
	}
}

func run(opts options) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	tracer, err := tracing.New(ctx, tracing.Config{
		ServiceName: "ironclaw-ingest",
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

	registry := prometheus.NewRegistry()
	promRecorder, err := ironmetrics.NewPrometheus(registry)
	if err != nil {
		return err
	}

	backing, release, err := buildMetrics(ctx, opts.redisAddr)
	if err != nil {
		return err
	}
	defer release()

	promRecorder.Preregister(ironmetrics.KnownCounters()...)

	metrics := ironmetrics.NewFanout(promRecorder, backing)

	codec, err := wire.For(wire.Format(opts.wireFormat))
	if err != nil {
		return err
	}

	producer, err := kafkabus.NewProducer(kafkabus.Config{
		Brokers: strings.Split(opts.brokers, ","),
		Topic:   opts.topic,
		Codec:   codec,
		Metrics: metrics,
	})
	if err != nil {
		return err
	}
	defer producer.Close()

	service, err := ingest.New(producer, metrics)
	if err != nil {
		return err
	}

	handler, err := httpingest.New(httpingest.Config{
		Acceptor:         service,
		Metrics:          metrics,
		AllowedClientCNs: opts.allowedClientCNs(),
	})
	if err != nil {
		return err
	}

	server := &http.Server{
		Addr:              opts.addr,
		Handler:           traced(handler.Handler(), tracer),
		ReadHeaderTimeout: 10 * time.Second,
	}

	if opts.certFile != "" {
		tlsConfig, err := buildTLS(opts)
		if err != nil {
			return err
		}
		server.TLSConfig = tlsConfig
	}

	var health *http.Server
	if opts.healthAddr != "" {
		mux := http.NewServeMux()
		mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
		})
		mux.Handle("GET /metrics", ironmetrics.Handler(registry))

		health = &http.Server{Addr: opts.healthAddr, Handler: mux, ReadHeaderTimeout: 10 * time.Second}

		go func() {
			if err := health.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Printf("ingest: health listener stopped: %v", err)
			}
		}()
	}

	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		if health != nil {
			_ = health.Shutdown(shutdown)
		}
		_ = server.Shutdown(shutdown)
	}()

	log.Printf("ingest listening on %s (mtls=%v, topic=%s)", opts.addr, server.TLSConfig != nil, opts.topic)

	if server.TLSConfig != nil {
		err = server.ListenAndServeTLS("", "")
	} else {
		err = server.ListenAndServe()
	}
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}

	return nil
}

func buildTLS(opts options) (*tls.Config, error) {
	certificate, err := tls.LoadX509KeyPair(opts.certFile, opts.keyFile)
	if err != nil {
		return nil, fmt.Errorf("loading server certificate: %w", err)
	}

	config := &tls.Config{
		Certificates: []tls.Certificate{certificate},
		MinVersion:   tls.VersionTLS13,
	}

	if opts.requireMTLS && opts.clientCA == "" {
		return nil, errors.New("require-mtls is set but no client CA was provided, so client certificates cannot be verified")
	}

	if opts.clientCA != "" && len(opts.allowedClientCNs()) == 0 {
		return nil, errors.New("a client CA is configured but allowed-client-cn is empty: the CA that signs the edge also signs device certificates, so without pinning a device could bypass the edge and submit telemetry as another device")
	}

	if opts.clientCA != "" {
		pem, err := os.ReadFile(opts.clientCA)
		if err != nil {
			return nil, fmt.Errorf("reading client CA: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, errors.New("client CA bundle contained no certificates")
		}
		config.ClientCAs = pool
		if opts.requireMTLS {
			config.ClientAuth = tls.RequireAndVerifyClientCert
		}
	}

	allowed := opts.allowedClientCNs()
	if len(allowed) > 0 {
		permitted := make(map[string]bool, len(allowed))
		for _, name := range allowed {
			permitted[name] = true
		}

		refuse := errors.New("client certificate common name is not an authorised edge")

		config.VerifyPeerCertificate = func(_ [][]byte, chains [][]*x509.Certificate) error {
			for _, chain := range chains {
				if len(chain) > 0 && permitted[chain[0].Subject.CommonName] {
					return nil
				}
			}
			return refuse
		}

		config.VerifyConnection = func(state tls.ConnectionState) error {
			if len(state.PeerCertificates) > 0 && permitted[state.PeerCertificates[0].Subject.CommonName] {
				return nil
			}
			return refuse
		}
	}

	return config, nil
}

func buildMetrics(ctx context.Context, address string) (domain.MetricsRecorder, func(), error) {
	if address == "" {
		return store.NewMemory(), func() {}, nil
	}

	client := redis.NewClient(&redis.Options{
		Addr:         address,
		DialTimeout:  2 * time.Second,
		ReadTimeout:  2 * time.Second,
		WriteTimeout: 2 * time.Second,
		MaxRetries:   1,
	})

	probe, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	if err := client.Ping(probe).Err(); err != nil {
		log.Printf("ingest: redis at %s is unreachable, serving with in-memory metrics only: %v", address, err)
		_ = client.Close()
		return store.NewMemory(), func() {}, nil
	}

	recorder, err := redisstore.New(redisstore.Options{Client: client})
	if err != nil {
		_ = client.Close()
		return nil, nil, err
	}

	async, err := ironmetrics.NewAsync(recorder, 0)
	if err != nil {
		_ = client.Close()
		return nil, nil, err
	}

	return async, func() { async.Close(); _ = client.Close() }, nil
}

func traced(next http.Handler, provider *tracing.Provider) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" || r.URL.Path == "/metrics" {
			next.ServeHTTP(w, r)
			return
		}

		ctx := tracing.Extract(r.Context(), headerCarrier(r))
		ctx, span := provider.Start(ctx, "ingest.batch",
			attribute.String("ironclaw.device_id", r.Header.Get(httpingest.DefaultIdentityHeader)),
		)
		defer span.End()

		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func headerCarrier(r *http.Request) tracing.Carrier {
	carrier := tracing.Carrier{}
	for _, key := range []string{"traceparent", "tracestate", "baggage"} {
		if value := r.Header.Get(key); value != "" {
			carrier[key] = value
		}
	}
	return carrier
}
