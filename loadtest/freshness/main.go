package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/ironclaw/mcp-ironclaw/internal/probe"
	"github.com/ironclaw/mcp-ironclaw/internal/redisstore"
	"github.com/redis/go-redis/v9"
)

type observer struct {
	store *redisstore.Store
}

func (o observer) Count(ctx context.Context, userID, deviceID string) (int64, error) {
	state, err := o.store.DeviceState(ctx, userID, deviceID)
	if errors.Is(err, redisstore.ErrDeviceNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return state.Count, nil
}

func main() {
	endpoint := flag.String("endpoint", "https://edge.ironclaw.internal:9443/v1/batches", "edge batch endpoint")
	certFile := flag.String("cert", "deploy/pki/out/device-000.crt", "device client certificate")
	keyFile := flag.String("key", "deploy/pki/out/device-000.key", "device client key")
	caFile := flag.String("ca", "deploy/pki/out/ca.crt", "certificate authority bundle")
	redisAddr := flag.String("redis", "localhost:16379", "redis address holding hot state")
	userID := flag.String("user", "freshness-probe", "user id stamped on probe events")
	deviceID := flag.String("device", "device-000", "device id, must match the client certificate CN")
	rounds := flag.Int("rounds", 50, "number of probe batches")
	budget := flag.Duration("budget", 5*time.Second, "freshness objective the p99 must stay under")
	serverName := flag.String("server-name", "", "TLS server name to verify against when dialling an address that differs from the certificate")
	flag.Parse()

	settings := probeSettings{
		endpoint:   *endpoint,
		certFile:   *certFile,
		keyFile:    *keyFile,
		caFile:     *caFile,
		redisAddr:  *redisAddr,
		userID:     *userID,
		deviceID:   *deviceID,
		rounds:     *rounds,
		budget:     *budget,
		serverName: *serverName,
	}

	if err := run(settings); err != nil {
		log.Fatalf("freshness: %v", err)
	}
}

type probeSettings struct {
	endpoint   string
	certFile   string
	keyFile    string
	caFile     string
	redisAddr  string
	userID     string
	deviceID   string
	rounds     int
	budget     time.Duration
	serverName string
}

func run(settings probeSettings) error {
	endpoint := settings.endpoint
	certFile := settings.certFile
	keyFile := settings.keyFile
	caFile := settings.caFile
	redisAddr := settings.redisAddr
	userID := settings.userID
	deviceID := settings.deviceID
	rounds := settings.rounds
	budget := settings.budget
	certificate, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return fmt.Errorf("loading device certificate: %w", err)
	}

	pem, err := os.ReadFile(caFile)
	if err != nil {
		return fmt.Errorf("reading ca bundle: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return errors.New("ca bundle contained no certificates")
	}

	client := probe.TLSClient(certificate, &tls.Config{
		RootCAs:    pool,
		MinVersion: tls.VersionTLS13,
		ServerName: settings.serverName,
	})

	redisClient := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer redisClient.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	if err := redisClient.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("connecting to redis at %s: %w", redisAddr, err)
	}

	store, err := redisstore.New(redisstore.Options{Client: redisClient})
	if err != nil {
		return err
	}

	freshness, err := probe.NewFreshness(probe.Config{
		Endpoint:   endpoint,
		UserID:     userID,
		DeviceID:   deviceID,
		Rounds:     rounds,
		EventCount: 1,
		PollEvery:  20 * time.Millisecond,
		Timeout:    30 * time.Second,
		Client:     client,
		Observer:   observer{store: store},
	})
	if err != nil {
		return err
	}

	report, err := freshness.Run(ctx)
	if err != nil {
		return err
	}

	fmt.Printf("samples=%d lost=%d\n", len(report.Samples), report.Lost)
	fmt.Printf("mean=%v p50=%v p95=%v p99=%v max=%v\n",
		report.Mean().Round(time.Millisecond),
		report.Quantile(0.50).Round(time.Millisecond),
		report.Quantile(0.95).Round(time.Millisecond),
		report.Quantile(0.99).Round(time.Millisecond),
		report.Quantile(1).Round(time.Millisecond))

	p99 := report.Quantile(0.99)
	if report.Lost > 0 {
		return fmt.Errorf("%d of %d probe batches never became visible", report.Lost, report.Sent)
	}
	if p99 > budget {
		return fmt.Errorf("p99 %v exceeds the %v freshness objective", p99.Round(time.Millisecond), budget)
	}

	fmt.Printf("PASS: p99 %v is within the %v freshness objective\n", p99.Round(time.Millisecond), budget)

	return nil
}
