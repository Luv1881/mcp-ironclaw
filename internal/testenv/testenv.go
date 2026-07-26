package testenv

import (
	"net"
	"os"
	"strings"
	"testing"
	"time"
)

const dialTimeout = 2 * time.Second

func lookup(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func requireReachable(t *testing.T, service, address string) {
	t.Helper()

	conn, err := net.DialTimeout("tcp", address, dialTimeout)
	if err != nil {
		t.Skipf("%s is not reachable at %s (start it with: docker compose -f deploy/docker-compose.yml up -d): %v", service, address, err)
	}
	_ = conn.Close()
}

func RedisAddr(t *testing.T) string {
	t.Helper()

	address := lookup("IRONCLAW_REDIS_ADDR", "localhost:16379")
	requireReachable(t, "redis", address)
	return address
}

func PostgresDSN(t *testing.T) string {
	t.Helper()

	dsn := lookup("IRONCLAW_POSTGRES_DSN", "postgres://ironclaw:ironclaw@localhost:15432/ironclaw?sslmode=disable")
	requireReachable(t, "postgres", lookup("IRONCLAW_POSTGRES_ADDR", "localhost:15432"))
	return dsn
}

func KafkaBrokers(t *testing.T) []string {
	t.Helper()

	raw := lookup("IRONCLAW_KAFKA_BROKERS", "localhost:19092")
	brokers := strings.Split(raw, ",")
	requireReachable(t, "kafka", brokers[0])
	return brokers
}
