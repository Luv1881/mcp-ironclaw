package postgresstore_test

import (
	"errors"
	"net/url"
	"testing"

	"github.com/ironclaw/mcp-ironclaw/internal/postgresstore"
)

func sslMode(t *testing.T, dsn string) string {
	t.Helper()

	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	return parsed.Query().Get("sslmode")
}

func TestRequireTLSRefusesDisabledSSL(t *testing.T) {
	for _, mode := range []string{"disable", "allow", "prefer"} {
		_, err := postgresstore.RequireTLS("postgres://u:p@host:5432/db?sslmode="+mode, "")
		if !errors.Is(err, postgresstore.ErrPlaintextRefused) {
			t.Fatalf("sslmode=%s: got %v, want ErrPlaintextRefused", mode, err)
		}
	}
}

func TestRequireTLSDefaultsToVerifyFull(t *testing.T) {
	dsn, err := postgresstore.RequireTLS("postgres://u:p@host:5432/db", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := sslMode(t, dsn); got != "verify-full" {
		t.Fatalf("sslmode %q, want verify-full when unspecified", got)
	}
}

func TestRequireTLSKeepsStrongerModesAndAttachesRootCert(t *testing.T) {
	dsn, err := postgresstore.RequireTLS("postgres://u:p@host:5432/db?sslmode=verify-ca", "/etc/ironclaw/tls/ca.crt")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := sslMode(t, dsn); got != "verify-ca" {
		t.Fatalf("sslmode %q, want verify-ca to be preserved", got)
	}

	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := parsed.Query().Get("sslrootcert"); got != "/etc/ironclaw/tls/ca.crt" {
		t.Fatalf("sslrootcert %q, want the supplied CA path", got)
	}
}

func TestRequireTLSRejectsEmptyDSN(t *testing.T) {
	if _, err := postgresstore.RequireTLS("", ""); !errors.Is(err, postgresstore.ErrEmptyDSN) {
		t.Fatalf("got %v, want ErrEmptyDSN", err)
	}
}
