package testenv

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
	"os"
	"strings"
	"testing"
	"time"
)

const (
	dialTimeout = 2 * time.Second
	readTimeout = 2 * time.Second
)

var ErrNoProtocolReply = errors.New("testenv: address accepted a connection but never answered the protocol handshake")

func lookup(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func requireReachable(t *testing.T, service, address string, probe func(net.Conn) error) {
	t.Helper()

	conn, err := net.DialTimeout("tcp", address, dialTimeout)
	if err != nil {
		t.Skipf("%s is not reachable at %s (start it with: docker compose -f deploy/docker-compose.yml up -d): %v", service, address, err)
	}
	defer conn.Close()

	if err := probe(conn); err != nil {
		t.Skipf("%s accepted a connection at %s but did not complete a %s handshake, so the integration tests would fail rather than skip: %v", service, address, service, err)
	}
}

func probeRedis(conn net.Conn) error {
	if err := conn.SetDeadline(time.Now().Add(readTimeout)); err != nil {
		return err
	}
	if _, err := conn.Write([]byte("*1\r\n$4\r\nPING\r\n")); err != nil {
		return err
	}

	reply := make([]byte, 7)
	if _, err := io.ReadFull(conn, reply); err != nil {
		return err
	}
	if string(reply) != "+PONG\r\n" {
		return errors.New("redis did not answer PING with PONG")
	}

	return nil
}

func probePostgres(conn net.Conn) error {
	if err := conn.SetDeadline(time.Now().Add(readTimeout)); err != nil {
		return err
	}

	request := make([]byte, 8)
	binary.BigEndian.PutUint32(request[0:4], 8)
	binary.BigEndian.PutUint32(request[4:8], 80877103)
	if _, err := conn.Write(request); err != nil {
		return err
	}

	reply := make([]byte, 1)
	if _, err := io.ReadFull(conn, reply); err != nil {
		return err
	}
	if reply[0] != 'S' && reply[0] != 'N' {
		return errors.New("postgres did not answer an SSLRequest with S or N")
	}

	return nil
}

func probeKafka(conn net.Conn) error {
	if err := conn.SetDeadline(time.Now().Add(readTimeout)); err != nil {
		return err
	}

	request := make([]byte, 14)
	binary.BigEndian.PutUint16(request[0:2], 3)
	binary.BigEndian.PutUint16(request[2:4], 0)
	binary.BigEndian.PutUint32(request[4:8], 1)
	binary.BigEndian.PutUint16(request[8:10], 0xffff)
	binary.BigEndian.PutUint32(request[10:14], 0)
	if _, err := conn.Write(request); err != nil {
		return err
	}

	size := make([]byte, 4)
	if _, err := io.ReadFull(conn, size); err != nil {
		return err
	}
	if length := binary.BigEndian.Uint32(size); length == 0 || length > 1<<20 {
		return ErrNoProtocolReply
	}

	return nil
}

func RedisAddr(t *testing.T) string {
	t.Helper()

	address := lookup("IRONCLAW_REDIS_ADDR", "localhost:16379")
	requireReachable(t, "redis", address, probeRedis)
	return address
}

func PostgresDSN(t *testing.T) string {
	t.Helper()

	dsn := lookup("IRONCLAW_POSTGRES_DSN", "postgres://ironclaw:ironclaw@localhost:15432/ironclaw?sslmode=disable")
	requireReachable(t, "postgres", lookup("IRONCLAW_POSTGRES_ADDR", "localhost:15432"), probePostgres)
	return dsn
}

func KafkaBrokers(t *testing.T) []string {
	t.Helper()

	raw := lookup("IRONCLAW_KAFKA_BROKERS", "localhost:19092")
	brokers := strings.Split(raw, ",")
	requireReachable(t, "kafka", brokers[0], probeKafka)
	return brokers
}
