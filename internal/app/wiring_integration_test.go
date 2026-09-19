package app

import (
	"context"
	"testing"

	"github.com/ironclaw/mcp-ironclaw/internal/store"
	"github.com/ironclaw/mcp-ironclaw/internal/testenv"
)

func TestBuildStateWiresTheArchiveAsAReadFallback(t *testing.T) {
	ctx := context.Background()

	config := Config{
		Backends: Backends{
			RedisAddr:   testenv.RedisAddr(t),
			PostgresDSN: testenv.PostgresDSN(t),
		},
		MetricsRecorder: store.NewMemory(),
	}

	deps := &Dependencies{}
	if err := buildState(ctx, config, deps); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	t.Cleanup(deps.closeAll)

	if _, ok := deps.State.(fallbackState); !ok {
		t.Fatalf("state backend is %T, want the archive wired as a read fallback: without it the read path fails outright when Redis is down, which is not what docs/availability.md documents",
			deps.State)
	}
}

func TestBuildStateWithoutAnArchiveHasNoFallback(t *testing.T) {
	ctx := context.Background()

	config := Config{
		Backends:        Backends{RedisAddr: testenv.RedisAddr(t)},
		MetricsRecorder: store.NewMemory(),
	}

	deps := &Dependencies{}
	if err := buildState(ctx, config, deps); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	t.Cleanup(deps.closeAll)

	if _, ok := deps.State.(fallbackState); ok {
		t.Fatal("a fallback was installed with no archive to fall back to")
	}
}
