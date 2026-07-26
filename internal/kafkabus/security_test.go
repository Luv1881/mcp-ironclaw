package kafkabus_test

import (
	"errors"
	"testing"

	"github.com/ironclaw/mcp-ironclaw/internal/kafkabus"
)

func TestSASLCredentialsRequireTLS(t *testing.T) {
	_, err := kafkabus.NewProducer(kafkabus.Config{
		Brokers: []string{"localhost:9092"},
		Topic:   "t",
		Security: kafkabus.Security{
			Enabled:      false,
			SASLUser:     "ironclaw",
			SASLPassword: "hunter2",
		},
	})
	if !errors.Is(err, kafkabus.ErrInsecureWithoutTLS) {
		t.Fatalf("got %v, want ErrInsecureWithoutTLS", err)
	}
}

func TestHalfConfiguredKeyPairIsRejected(t *testing.T) {
	_, err := kafkabus.NewProducer(kafkabus.Config{
		Brokers:  []string{"localhost:9092"},
		Topic:    "t",
		Security: kafkabus.Security{Enabled: true, CertFile: "client.crt"},
	})
	if !errors.Is(err, kafkabus.ErrIncompleteKeyPair) {
		t.Fatalf("got %v, want ErrIncompleteKeyPair", err)
	}
}

func TestHalfConfiguredSASLIsRejected(t *testing.T) {
	_, err := kafkabus.NewProducer(kafkabus.Config{
		Brokers:  []string{"localhost:9092"},
		Topic:    "t",
		Security: kafkabus.Security{Enabled: true, SASLUser: "ironclaw"},
	})
	if !errors.Is(err, kafkabus.ErrIncompleteSASL) {
		t.Fatalf("got %v, want ErrIncompleteSASL", err)
	}
}

func TestUnreadableCABundleIsReported(t *testing.T) {
	_, err := kafkabus.NewProducer(kafkabus.Config{
		Brokers:  []string{"localhost:9092"},
		Topic:    "t",
		Security: kafkabus.Security{Enabled: true, CAFile: "/nonexistent/ca.crt"},
	})
	if err == nil {
		t.Fatal("expected an error for an unreadable CA bundle")
	}
}

func TestConsumerAppliesTheSameSecurityRules(t *testing.T) {
	_, err := kafkabus.NewConsumer(kafkabus.Config{
		Brokers: []string{"localhost:9092"},
		Topic:   "t",
		Group:   "g",
		Security: kafkabus.Security{
			Enabled:      false,
			SASLUser:     "ironclaw",
			SASLPassword: "hunter2",
		},
	})
	if !errors.Is(err, kafkabus.ErrInsecureWithoutTLS) {
		t.Fatalf("got %v, want ErrInsecureWithoutTLS on the consumer too", err)
	}
}

func TestPlaintextRemainsAvailableForLocalDevelopment(t *testing.T) {
	producer, err := kafkabus.NewProducer(kafkabus.Config{
		Brokers: []string{"localhost:19092"},
		Topic:   "t",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	producer.Close()
}
