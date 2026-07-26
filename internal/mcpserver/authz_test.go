package mcpserver_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ironclaw/mcp-ironclaw/internal/mcpserver"
	"github.com/modelcontextprotocol/go-sdk/auth"
)

func asPrincipal(userID string, scopes ...string) context.Context {
	return mcpserver.ContextWithPrincipal(context.Background(), mcpserver.Principal{
		UserID: userID,
		Scopes: scopes,
	})
}

func authenticatedTools(t *testing.T) *mcpserver.Tools {
	t.Helper()

	memory := seededStore(t)

	tools, err := mcpserver.NewTools(mcpserver.Options{
		State:       memory,
		Devices:     memory,
		Metrics:     memory,
		Commands:    &recordingCommands{},
		Clock:       fixedClock{at: time.Unix(1700000100, 0).UTC()},
		RequireAuth: true,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	return tools
}

func TestPrincipalCannotReadAnotherTenantsDevice(t *testing.T) {
	tools := authenticatedTools(t)

	_, err := tools.DeviceState(asPrincipal("user-2"), mcpserver.DeviceStateInput{
		UserID:   "user-1",
		DeviceID: "device-1",
	})
	if !errors.Is(err, mcpserver.ErrForbidden) {
		t.Fatalf("got %v, want ErrForbidden: user-2 must not read user-1's device", err)
	}
}

func TestPrincipalCanReadItsOwnDevice(t *testing.T) {
	tools := authenticatedTools(t)

	state, err := tools.DeviceState(asPrincipal("user-1"), mcpserver.DeviceStateInput{
		UserID:   "user-1",
		DeviceID: "device-1",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if state.Count != 120 {
		t.Fatalf("count %d, want 120", state.Count)
	}
}

func TestAdminScopeCrossesTenants(t *testing.T) {
	tools := authenticatedTools(t)

	if _, err := tools.DeviceState(asPrincipal("ops", mcpserver.ScopeAdmin), mcpserver.DeviceStateInput{
		UserID:   "user-1",
		DeviceID: "device-1",
	}); err != nil {
		t.Fatalf("admin scope should permit cross-tenant reads: %v", err)
	}
}

func TestUnauthenticatedRequestIsRefusedWhenAuthIsRequired(t *testing.T) {
	tools := authenticatedTools(t)

	if _, err := tools.DeviceState(context.Background(), mcpserver.DeviceStateInput{
		UserID:   "user-1",
		DeviceID: "device-1",
	}); !errors.Is(err, mcpserver.ErrUnauthenticated) {
		t.Fatalf("got %v, want ErrUnauthenticated", err)
	}
}

func TestUserDeviceListingIsScopedToThePrincipal(t *testing.T) {
	tools := authenticatedTools(t)

	if _, err := tools.UserDevices(asPrincipal("user-2"), mcpserver.UserDevicesInput{UserID: "user-1"}); !errors.Is(err, mcpserver.ErrForbidden) {
		t.Fatalf("got %v, want ErrForbidden", err)
	}
	if _, err := tools.UserDevices(asPrincipal("user-1"), mcpserver.UserDevicesInput{UserID: "user-1"}); err != nil {
		t.Fatalf("unexpected error for own listing: %v", err)
	}
}

func TestResetIsScopedToThePrincipal(t *testing.T) {
	tools := authenticatedTools(t)

	_, err := tools.ResetCounters(asPrincipal("user-2"), mcpserver.ResetCountersInput{
		UserID:   "user-1",
		DeviceID: "device-1",
		Actor:    "attacker",
	})
	if !errors.Is(err, mcpserver.ErrForbidden) {
		t.Fatalf("got %v, want ErrForbidden: a principal must not reset another tenant's counters", err)
	}
}

func TestFleetMetricsRequireAdminScope(t *testing.T) {
	tools := authenticatedTools(t)

	if _, err := tools.PipelineMetrics(asPrincipal("user-1"), mcpserver.PipelineMetricsInput{}); !errors.Is(err, mcpserver.ErrForbidden) {
		t.Fatalf("got %v, want ErrForbidden for a non-admin principal", err)
	}
	if _, err := tools.PipelineMetrics(asPrincipal("ops", mcpserver.ScopeAdmin), mcpserver.PipelineMetricsInput{}); err != nil {
		t.Fatalf("admin should read fleet metrics: %v", err)
	}
}

func TestStaticTokenVerifier(t *testing.T) {
	verify := mcpserver.StaticTokenVerifier(map[string]mcpserver.StaticToken{
		"good":    {UserID: "user-1", Scopes: []string{"read"}},
		"expired": {UserID: "user-9", Expiry: time.Now().Add(-time.Minute)},
	})

	info, err := verify(context.Background(), "good", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if info.UserID != "user-1" {
		t.Fatalf("user %q, want user-1", info.UserID)
	}

	if _, err := verify(context.Background(), "expired", nil); !errors.Is(err, mcpserver.ErrTokenExpired) {
		t.Fatalf("got %v, want ErrTokenExpired", err)
	}
	if _, err := verify(context.Background(), "absent", nil); !errors.Is(err, mcpserver.ErrUnknownToken) {
		t.Fatalf("got %v, want ErrUnknownToken", err)
	}
}

func TestParseStaticTokens(t *testing.T) {
	tokens := mcpserver.ParseStaticTokens("t1:user-1,t2:ops:ironclaw:admin|read, ,bad")

	if len(tokens) != 2 {
		t.Fatalf("parsed %d tokens, want 2", len(tokens))
	}
	if tokens["t1"].UserID != "user-1" {
		t.Fatalf("t1 user %q, want user-1", tokens["t1"].UserID)
	}
}

func TestHTTPServerRefusesAuthWithoutAVerifier(t *testing.T) {
	_, err := mcpserver.NewHTTPServer(mcpserver.HTTPOptions{
		Addr:        ":0",
		Tools:       authenticatedTools(t),
		RequireAuth: true,
	})
	if !errors.Is(err, mcpserver.ErrNoVerifier) {
		t.Fatalf("got %v, want ErrNoVerifier", err)
	}
}

func TestRejectedTokensMapToUnauthorizedNotServerError(t *testing.T) {
	for _, err := range []error{mcpserver.ErrUnknownToken, mcpserver.ErrTokenExpired} {
		if !errors.Is(err, auth.ErrInvalidToken) {
			t.Fatalf("%v must wrap auth.ErrInvalidToken so the transport answers 401 rather than 500", err)
		}
	}
}

func TestParseStaticTokensKeepsColonsInsideScopes(t *testing.T) {
	tokens := mcpserver.ParseStaticTokens("tok-ops:ops:" + mcpserver.ScopeAdmin + "|read")

	entry, ok := tokens["tok-ops"]
	if !ok {
		t.Fatal("tok-ops was not parsed")
	}
	if entry.UserID != "ops" {
		t.Fatalf("user %q, want ops", entry.UserID)
	}
	if len(entry.Scopes) != 2 || entry.Scopes[0] != mcpserver.ScopeAdmin {
		t.Fatalf("scopes %v, want the admin scope intact despite its colon", entry.Scopes)
	}
}
