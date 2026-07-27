package mcpserver_test

import (
	"context"
	"errors"
	"testing"

	"github.com/ironclaw/mcp-ironclaw/internal/mcpserver"
	"github.com/ironclaw/mcp-ironclaw/internal/store"
)

func TestStaticTokensRejectMalformedEntries(t *testing.T) {
	cases := []struct {
		name string
		raw  string
	}{
		{"leading colon registers an empty token", ":alice:ironclaw:admin"},
		{"empty user id", "sometoken::ironclaw:admin"},
		{"token only", "sometoken"},
		{"empty entry", ""},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			tokens := mcpserver.ParseStaticTokens(testCase.raw)
			if _, present := tokens[""]; present {
				t.Fatal("an empty bearer token was registered; an unauthenticated caller would match it")
			}
			for token, entry := range tokens {
				if token == "" || entry.UserID == "" {
					t.Fatalf("registered token %q for user %q", token, entry.UserID)
				}
			}
		})
	}
}

func TestStaticTokensKeepScopesContainingColons(t *testing.T) {
	tokens := mcpserver.ParseStaticTokens("t1:alice:ironclaw:admin")

	entry, ok := tokens["t1"]
	if !ok {
		t.Fatal("token t1 was not registered")
	}
	if entry.UserID != "alice" {
		t.Fatalf("user %q, want alice", entry.UserID)
	}
	if len(entry.Scopes) != 1 || entry.Scopes[0] != mcpserver.ScopeAdmin {
		t.Fatalf("scopes %v, want [%s]", entry.Scopes, mcpserver.ScopeAdmin)
	}
}

func TestUnknownTokenIsRefused(t *testing.T) {
	verify := mcpserver.StaticTokenVerifier(mcpserver.ParseStaticTokens("t1:alice"))

	if _, err := verify(context.Background(), "not-a-token", nil); !errors.Is(err, mcpserver.ErrUnknownToken) {
		t.Fatalf("got %v, want ErrUnknownToken", err)
	}
	if _, err := verify(context.Background(), "", nil); !errors.Is(err, mcpserver.ErrUnknownToken) {
		t.Fatalf("an empty token got %v, want ErrUnknownToken", err)
	}
	info, err := verify(context.Background(), "t1", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if info.UserID != "alice" {
		t.Fatalf("user %q, want alice", info.UserID)
	}
}

func TestResetActorIsTakenFromThePrincipalNotTheRequest(t *testing.T) {
	memory := store.NewMemory()
	commands := &recordingCommands{}

	tools, err := mcpserver.NewTools(mcpserver.Options{
		State:       memory,
		Devices:     memory,
		Metrics:     memory,
		Commands:    commands,
		RequireAuth: true,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctx := mcpserver.ContextWithPrincipal(context.Background(), mcpserver.Principal{UserID: "alice"})

	if _, err := tools.ResetCounters(ctx, mcpserver.ResetCountersInput{
		UserID:   "alice",
		DeviceID: "device-000",
		Actor:    "admin",
	}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(commands.commands) != 1 {
		t.Fatalf("published %d commands, want 1", len(commands.commands))
	}
	if actor := commands.commands[0].Actor; actor != "alice" {
		t.Fatalf("command actor is %q; a non-admin spoofed the audit trail as %q", actor, "admin")
	}
}

func TestAdminMayAttributeAResetToAnotherActor(t *testing.T) {
	memory := store.NewMemory()
	commands := &recordingCommands{}

	tools, err := mcpserver.NewTools(mcpserver.Options{
		State:       memory,
		Devices:     memory,
		Metrics:     memory,
		Commands:    commands,
		RequireAuth: true,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctx := mcpserver.ContextWithPrincipal(context.Background(), mcpserver.Principal{
		UserID: "fleet-admin",
		Scopes: []string{mcpserver.ScopeAdmin},
	})

	if _, err := tools.ResetCounters(ctx, mcpserver.ResetCountersInput{
		UserID:   "alice",
		DeviceID: "device-000",
		Actor:    "oncall-runbook",
	}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if actor := commands.commands[0].Actor; actor != "oncall-runbook" {
		t.Fatalf("admin attribution was overwritten to %q", actor)
	}
}
