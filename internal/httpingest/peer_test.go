package httpingest_test

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ironclaw/mcp-ironclaw/internal/httpingest"
)

func peerRequest(deviceID, peerCN, body string) *http.Request {
	request := httptest.NewRequest(http.MethodPost, "/v1/batches", bytes.NewBufferString(body))
	request.Header.Set(httpingest.DefaultIdentityHeader, deviceID)

	if peerCN != "" {
		request.TLS = &tls.ConnectionState{
			PeerCertificates: []*x509.Certificate{{Subject: pkix.Name{CommonName: peerCN}}},
		}
	}

	return request
}

func edgeRestrictedServer(t *testing.T, acceptor httpingest.BatchAcceptor) http.Handler {
	t.Helper()

	server, err := httpingest.New(httpingest.Config{
		Acceptor:         acceptor,
		AllowedClientCNs: []string{"edge.ironclaw.internal"},
		Clock:            fixedClock{at: fixedNow()},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	return server.Handler()
}

func TestDeviceCertificateCannotBypassTheEdgeAndImpersonate(t *testing.T) {
	acceptor := &recordingAcceptor{}
	handler := edgeRestrictedServer(t, acceptor)

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, peerRequest("device-017", "device-000", batchBody("", 1)))

	if recorder.Code != http.StatusForbidden {
		t.Fatalf("status %d, want 403: a device certificate must not be able to submit as another device", recorder.Code)
	}
	if len(acceptor.batches) != 0 {
		t.Fatal("an impersonated batch reached the acceptor")
	}
}

func TestAuthorisedEdgeCertificateIsAccepted(t *testing.T) {
	acceptor := &recordingAcceptor{}
	handler := edgeRestrictedServer(t, acceptor)

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, peerRequest("device-017", "edge.ironclaw.internal", batchBody("", 1)))

	if recorder.Code != http.StatusAccepted {
		t.Fatalf("status %d, want 202: %s", recorder.Code, recorder.Body.String())
	}
	if len(acceptor.identities) != 1 || acceptor.identities[0] != "device-017" {
		t.Fatalf("identities %v, want the header identity from a trusted edge", acceptor.identities)
	}
}

func TestPlaintextConnectionIsRefusedWhenAnEdgeIsRequired(t *testing.T) {
	acceptor := &recordingAcceptor{}
	handler := edgeRestrictedServer(t, acceptor)

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, peerRequest("device-017", "", batchBody("", 1)))

	if recorder.Code != http.StatusForbidden {
		t.Fatalf("status %d, want 403 for a connection with no client certificate", recorder.Code)
	}
}

func TestEmptyAllowlistKeepsTheServerUsableBehindATrustedProxy(t *testing.T) {
	acceptor := &recordingAcceptor{}
	handler := newServer(t, acceptor, nil)

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, peerRequest("device-017", "", batchBody("", 1)))

	if recorder.Code != http.StatusAccepted {
		t.Fatalf("status %d, want 202 when no allowlist is configured", recorder.Code)
	}
}
