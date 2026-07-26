package redisstore_test

import (
	"errors"
	"testing"

	"github.com/ironclaw/mcp-ironclaw/internal/redisstore"
)

func TestDialRefusesAPasswordWithoutTLS(t *testing.T) {
	_, err := redisstore.Dial("localhost:6379", redisstore.Security{
		Enabled:  false,
		Password: "hunter2",
	})
	if !errors.Is(err, redisstore.ErrPasswordInClear) {
		t.Fatalf("got %v, want ErrPasswordInClear", err)
	}
}

func TestDialRefusesAHalfConfiguredKeyPair(t *testing.T) {
	_, err := redisstore.Dial("localhost:6379", redisstore.Security{
		Enabled:  true,
		CertFile: "/etc/ironclaw/tls/client.crt",
	})
	if !errors.Is(err, redisstore.ErrIncompleteKeyPair) {
		t.Fatalf("got %v, want ErrIncompleteKeyPair", err)
	}
}

func TestDialAppliesTLSAndCredentials(t *testing.T) {
	client, err := redisstore.Dial("localhost:6379", redisstore.Security{
		Enabled:            true,
		Username:           "ironclaw",
		Password:           "hunter2",
		InsecureSkipVerify: true,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer client.Close()

	options := client.Options()
	if options.TLSConfig == nil {
		t.Fatal("TLS was requested but no TLS config was applied")
	}
	if options.Username != "ironclaw" || options.Password != "hunter2" {
		t.Fatalf("credentials not applied: %q/%q", options.Username, options.Password)
	}
}

func TestDialWithoutSecurityStaysPlaintext(t *testing.T) {
	client, err := redisstore.Dial("localhost:6379", redisstore.Security{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer client.Close()

	if client.Options().TLSConfig != nil {
		t.Fatal("no TLS was requested but a TLS config was applied")
	}
}

func TestDialRejectsAnUnreadableCABundle(t *testing.T) {
	_, err := redisstore.Dial("localhost:6379", redisstore.Security{
		Enabled: true,
		CAFile:  "/nonexistent/ca.crt",
	})
	if err == nil {
		t.Fatal("expected an error for an unreadable CA bundle")
	}
}
