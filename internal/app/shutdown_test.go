package app

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/ironclaw/mcp-ironclaw/internal/domain"
	"github.com/ironclaw/mcp-ironclaw/internal/store"
)

type recordingChannel struct {
	name    string
	order   *[]string
	closed  int
	batches int
}

func (c *recordingChannel) Publish(_ context.Context, _ string, _ domain.Batch) error {
	c.batches++
	return nil
}

func (c *recordingChannel) PublishWindow(_ context.Context, _ domain.AggregateWindow) error {
	c.batches++
	return nil
}

func (c *recordingChannel) PublishCommand(_ context.Context, _ domain.Command) error {
	c.batches++
	return nil
}

func (c *recordingChannel) Consume(_ context.Context, _ domain.BatchHandler) error { return nil }

func (c *recordingChannel) ConsumeWindows(_ context.Context, _ domain.WindowHandler) error {
	return nil
}

func (c *recordingChannel) ConsumeCommands(_ context.Context, _ func(context.Context, domain.Command) error) error {
	return nil
}

func (c *recordingChannel) Close() {
	c.closed++
	*c.order = append(*c.order, c.name)
}

func TestCloseAllReleasesEveryChannelAndExtra(t *testing.T) {
	order := make([]string, 0, 6)

	batches := &recordingChannel{name: "batches", order: &order}
	windows := &recordingChannel{name: "windows", order: &order}
	archive := &recordingChannel{name: "archive", order: &order}
	commands := &recordingChannel{name: "commands", order: &order}

	deps := &Dependencies{
		Batches:        batches,
		Windows:        windows,
		ArchiveWindows: archive,
		Commands:       commands,
		Extra: []func(){
			func() { order = append(order, "extra-1") },
			func() { order = append(order, "extra-2") },
		},
	}

	deps.closeAll()

	got := strings.Join(order, ",")
	want := "extra-1,extra-2,batches,windows,archive,commands"
	if got != want {
		t.Fatalf("release order %q, want %q: shutdown drains the injected resources before the channels", got, want)
	}

	for name, channel := range map[string]*recordingChannel{
		"batches": batches, "windows": windows, "archive": archive, "commands": commands,
	} {
		if channel.closed != 1 {
			t.Fatalf("%s closed %d times, want exactly once", name, channel.closed)
		}
	}
}

func TestCloseAllToleratesAMissingArchive(t *testing.T) {
	order := make([]string, 0, 3)

	deps := &Dependencies{
		Batches:  &recordingChannel{name: "batches", order: &order},
		Windows:  &recordingChannel{name: "windows", order: &order},
		Commands: &recordingChannel{name: "commands", order: &order},
	}

	deps.closeAll()

	if got := strings.Join(order, ","); got != "batches,windows,commands" {
		t.Fatalf("release order %q, want the archive to be skipped when absent", got)
	}
}

func TestHandlerErrorsAreCountedAndNamed(t *testing.T) {
	state := store.NewMemory()

	logger := handlerErrorLogger(&Dependencies{State: state})
	logger("ironclaw.events.raw", 3, errors.New("redis is holding the wrong type"))

	recorded, err := state.PipelineMetrics(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if recorded[MetricHandlerErrors] != 1 {
		t.Fatalf("counter is %d, want 1: a handler failure must be counted", recorded[MetricHandlerErrors])
	}
}

func TestHandlerLoggerToleratesMissingDependencies(t *testing.T) {
	logger := handlerErrorLogger(nil)
	logger("ironclaw.events.raw", 1, errors.New("boom"))

	empty := handlerErrorLogger(&Dependencies{})
	empty("ironclaw.events.raw", 1, errors.New("boom"))
}

func TestCloseAllToleratesAPartiallyBuiltDependencySet(t *testing.T) {
	order := make([]string, 0, 2)

	deps := &Dependencies{
		Extra: []func(){
			func() { order = append(order, "release") },
			nil,
		},
		Windows: &recordingChannel{name: "windows", order: &order},
	}

	deps.closeAll()

	if got := strings.Join(order, ","); got != "release,windows" {
		t.Fatalf("release order %q, want the populated resources closed and the empty ones skipped", got)
	}
}
