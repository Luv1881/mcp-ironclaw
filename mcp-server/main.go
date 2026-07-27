package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ironclaw/mcp-ironclaw/internal/app"
	"github.com/ironclaw/mcp-ironclaw/internal/mcpserver"
	ironmetrics "github.com/ironclaw/mcp-ironclaw/internal/metrics"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/prometheus/client_golang/prometheus"
)

func main() {
	config := app.DefaultConfig()

	flag.IntVar(&config.Devices, "devices", config.Devices, "number of simulated devices feeding the pipeline")
	flag.IntVar(&config.EventsPerDevice, "events", config.EventsPerDevice, "events produced per simulated device")
	flag.DurationVar(&config.EventInterval, "event-interval", config.EventInterval, "delay between events on each device")
	flag.DurationVar(&config.WindowSize, "window", config.WindowSize, "tumbling aggregation window size")
	flag.IntVar(&config.Partitions, "partitions", config.Partitions, "bus partition count")
	flag.IntVar(&config.MaxOpenWindows, "max-open-windows", config.MaxOpenWindows, "ceiling on in-flight correlation keys; further keys are shed rather than growing memory without bound")
	flag.StringVar(&config.Backends.RedisAddr, "redis", os.Getenv("IRONCLAW_REDIS_ADDR"), "redis address for hot state; empty uses in-memory state")
	flag.StringVar(&config.Backends.PostgresDSN, "postgres", os.Getenv("IRONCLAW_POSTGRES_DSN"), "postgres dsn for the durable archive; requires -redis")
	flag.StringVar(&config.Backends.KafkaBrokers, "kafka", os.Getenv("IRONCLAW_KAFKA_BROKERS"), "comma separated kafka brokers; empty uses the in-memory bus")
	flag.StringVar(&config.Backends.TopicPrefix, "topic-prefix", "ironclaw", "kafka topic prefix")
	flag.BoolVar(&config.Backends.RequireTLS, "require-tls", false, "require TLS to kafka, redis and postgres")
	flag.StringVar(&config.Backends.CAFile, "ca", os.Getenv("IRONCLAW_CA"), "CA bundle verifying kafka and redis")
	flag.StringVar(&config.Backends.CertFile, "client-cert", os.Getenv("IRONCLAW_CLIENT_CERT"), "client certificate for mTLS to kafka and redis")
	flag.StringVar(&config.Backends.KeyFile, "client-key", os.Getenv("IRONCLAW_CLIENT_KEY"), "client key for mTLS to kafka and redis")
	flag.StringVar(&config.Backends.RedisUsername, "redis-user", os.Getenv("IRONCLAW_REDIS_USER"), "redis ACL username")
	flag.StringVar(&config.Backends.RedisPassword, "redis-password", os.Getenv("IRONCLAW_REDIS_PASSWORD"), "redis AUTH password; requires -require-tls")
	flag.StringVar(&config.Backends.KafkaSASLUser, "kafka-user", os.Getenv("IRONCLAW_KAFKA_USER"), "kafka SASL SCRAM username")
	flag.StringVar(&config.Backends.KafkaSASLPass, "kafka-password", os.Getenv("IRONCLAW_KAFKA_PASSWORD"), "kafka SASL SCRAM password; requires -require-tls")
	flag.StringVar(&config.Backends.WireFormat, "wire-format", "json", "kafka payload encoding: json or protobuf")
	flag.StringVar(&config.Backends.PostgresRootCA, "postgres-root-ca", os.Getenv("IRONCLAW_POSTGRES_ROOT_CA"), "CA bundle for postgres sslrootcert")
	flag.BoolVar(&config.EmitOpenWindows, "emit-open-windows", config.EmitOpenWindows, "emit the in-flight window as a delta on every tick so freshness tracks the emit interval instead of the window size")

	var headless bool
	var healthAddr string
	var httpAddr string
	var staticTokens string
	var requireAuth bool
	var allowAnonymous bool
	var tlsCert, tlsKey, clientCA string
	flag.BoolVar(&headless, "headless", false, "run the pipeline without the stdio MCP server, for deployment as an aggregator or persister")
	flag.StringVar(&healthAddr, "health-addr", ":8080", "address serving /healthz when headless")
	flag.StringVar(&httpAddr, "http-addr", "", "serve MCP over streamable HTTP on this address instead of stdio")
	flag.BoolVar(&requireAuth, "require-auth", true, "require a bearer token on the HTTP transport")
	flag.BoolVar(&allowAnonymous, "allow-anonymous-http", false, "serve the HTTP transport with no authentication; every tool then reads every tenant")
	flag.StringVar(&staticTokens, "tokens", os.Getenv("IRONCLAW_MCP_TOKENS"), "static bearer tokens as token:user[:scope|scope], comma separated")
	flag.StringVar(&tlsCert, "tls-cert", os.Getenv("IRONCLAW_MCP_CERT"), "server certificate for the HTTP transport")
	flag.StringVar(&tlsKey, "tls-key", os.Getenv("IRONCLAW_MCP_KEY"), "server key for the HTTP transport")
	flag.StringVar(&clientCA, "tls-client-ca", os.Getenv("IRONCLAW_MCP_CLIENT_CA"), "CA bundle requiring client certificates on the HTTP transport")
	flag.Parse()

	serve := serveOptions{
		headless:       headless,
		healthAddr:     healthAddr,
		httpAddr:       httpAddr,
		staticTokens:   staticTokens,
		requireAuth:    requireAuth,
		allowAnonymous: allowAnonymous,
		tlsCert:        tlsCert,
		tlsKey:         tlsKey,
		clientCA:       clientCA,
	}

	if err := run(config, serve); err != nil {
		log.Fatalf("ironclaw: %v", err)
	}
}

type serveOptions struct {
	headless       bool
	healthAddr     string
	httpAddr       string
	staticTokens   string
	requireAuth    bool
	allowAnonymous bool
	tlsCert        string
	tlsKey         string
	clientCA       string
}

func run(config app.Config, serve serveOptions) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	registry := prometheus.NewRegistry()
	promRecorder, err := ironmetrics.NewPrometheus(registry)
	if err != nil {
		return err
	}
	promRecorder.Preregister(ironmetrics.KnownCounters()...)
	config.MetricsRecorder = promRecorder

	runtime, err := app.NewWithContext(ctx, config)
	if err != nil {
		return err
	}

	if err := runtime.Start(ctx); err != nil {
		return err
	}
	defer runtime.Stop()

	state := runtime.State()

	tools, err := mcpserver.NewTools(mcpserver.Options{
		State:       state,
		Devices:     state,
		Metrics:     state,
		Commands:    runtime.Commands(),
		Watcher:     state,
		RequireAuth: serve.httpAddr != "" && serve.requireAuth,
	})
	if err != nil {
		return err
	}

	log.SetOutput(os.Stderr)

	if serve.headless {
		log.Printf("ironclaw pipeline ready (headless): %d devices, %s windows, %s backend", config.Devices, config.WindowSize, runtime.Backend())
		return serveHealth(ctx, serve.healthAddr, registry)
	}

	if serve.httpAddr != "" {
		return serveHTTP(ctx, tools, serve, config, runtime.Backend())
	}

	server := mcpserver.NewServer(tools)

	log.Printf("ironclaw mcp server ready: %d devices, %s windows, %s backend", config.Devices, config.WindowSize, runtime.Backend())

	if err := server.Run(ctx, &mcp.StdioTransport{}); err != nil && ctx.Err() == nil {
		return err
	}

	return nil
}

func serveHealth(ctx context.Context, addr string, registry *prometheus.Registry) error {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.Handle("GET /metrics", ironmetrics.Handler(registry))

	server := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 10 * time.Second}

	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdown)
	}()

	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}

	return nil
}

func serveHTTP(ctx context.Context, tools *mcpserver.Tools, serve serveOptions, config app.Config, backend string) error {
	options := mcpserver.HTTPOptions{
		Addr:         serve.httpAddr,
		Tools:        tools,
		RequireAuth:  serve.requireAuth,
		CertFile:     serve.tlsCert,
		KeyFile:      serve.tlsKey,
		ClientCAFile: serve.clientCA,
	}

	if !serve.requireAuth && !serve.allowAnonymous {
		return errors.New("ironclaw: -require-auth=false serves every tenant's telemetry to any caller; pass -allow-anonymous-http to accept that explicitly")
	}

	if serve.requireAuth {
		tokens := mcpserver.ParseStaticTokens(serve.staticTokens)
		if len(tokens) == 0 {
			return errors.New("ironclaw: -require-auth is set but no tokens were supplied")
		}
		options.Verifier = mcpserver.StaticTokenVerifier(tokens)
	}

	server, err := mcpserver.NewHTTPServer(options)
	if err != nil {
		return err
	}

	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdown)
	}()

	log.Printf("ironclaw mcp server on %s (tls=%v, auth=%v, %s backend, %d devices)",
		serve.httpAddr, server.TLSConfig != nil, serve.requireAuth, backend, config.Devices)

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
