package mcpserver_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ironclaw/mcp-ironclaw/internal/domain"
	"github.com/ironclaw/mcp-ironclaw/internal/mcpserver"
	"github.com/ironclaw/mcp-ironclaw/internal/store"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func harden(t *testing.T, memory *store.Memory, maxWatches int) *mcpserver.Tools {
	t.Helper()

	tools, err := mcpserver.NewTools(mcpserver.Options{
		State:      memory,
		Devices:    memory,
		Metrics:    memory,
		Commands:   &recordingCommands{},
		Watcher:    memory,
		MaxWatches: maxWatches,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	return tools
}

func TestOversizedIdentifiersAreRefused(t *testing.T) {
	memory := seededStore(t)
	tools := harden(t, memory, 4)

	long := strings.Repeat("a", mcpserver.MaxIdentifierBytes+1)

	cases := map[string]func() error{
		"get_device_state user": func() error {
			_, err := tools.DeviceState(context.Background(), mcpserver.DeviceStateInput{UserID: long, DeviceID: "device-1"})
			return err
		},
		"get_device_state device": func() error {
			_, err := tools.DeviceState(context.Background(), mcpserver.DeviceStateInput{UserID: "user-1", DeviceID: long})
			return err
		},
		"get_user_devices": func() error {
			_, err := tools.UserDevices(context.Background(), mcpserver.UserDevicesInput{UserID: long})
			return err
		},
	}

	for name, call := range cases {
		t.Run(name, func(t *testing.T) {
			if err := call(); !errors.Is(err, mcpserver.ErrIdentifierTooLong) {
				t.Fatalf("got %v, want ErrIdentifierTooLong: an unbounded identifier becomes an unbounded Redis key", err)
			}
		})
	}
}

func TestIdentifiersWithControlCharactersAreRefused(t *testing.T) {
	memory := seededStore(t)
	tools := harden(t, memory, 4)

	_, err := tools.DeviceState(context.Background(), mcpserver.DeviceStateInput{
		UserID:   "user-1",
		DeviceID: "device\n1",
	})

	if !errors.Is(err, mcpserver.ErrIdentifierInvalid) {
		t.Fatalf("got %v, want ErrIdentifierInvalid", err)
	}
}

func TestAnIdentifierAtTheLimitIsAccepted(t *testing.T) {
	memory := seededStore(t)
	tools := harden(t, memory, 4)

	atLimit := strings.Repeat("a", mcpserver.MaxIdentifierBytes)

	_, err := tools.DeviceState(context.Background(), mcpserver.DeviceStateInput{UserID: "user-1", DeviceID: atLimit})
	if errors.Is(err, mcpserver.ErrIdentifierTooLong) {
		t.Fatalf("an identifier of exactly %d bytes was refused", mcpserver.MaxIdentifierBytes)
	}
}

type signallingWatcher struct {
	entered chan struct{}
}

func (w *signallingWatcher) WatchDevice(ctx context.Context, _, _ string) (<-chan domain.DeviceState, error) {
	w.entered <- struct{}{}

	updates := make(chan domain.DeviceState)
	go func() {
		defer close(updates)
		<-ctx.Done()
	}()

	return updates, nil
}

func watchToolsWith(t *testing.T, watcher *signallingWatcher, maxWatches int) *mcpserver.Tools {
	t.Helper()

	memory := seededStore(t)
	tools, err := mcpserver.NewTools(mcpserver.Options{
		State:      memory,
		Devices:    memory,
		Metrics:    memory,
		Watcher:    watcher,
		MaxWatches: maxWatches,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	return tools
}

func TestWatchesAreCappedPerServer(t *testing.T) {
	watcher := &signallingWatcher{entered: make(chan struct{}, 4)}
	tools := watchToolsWith(t, watcher, 1)

	firstCtx, cancelFirst := context.WithCancel(context.Background())
	defer cancelFirst()

	firstDone := make(chan error, 1)
	go func() {
		_, err := tools.WatchDevice(firstCtx, mcpserver.WatchDeviceInput{
			UserID: "user-000", DeviceID: "device-000", TimeoutSeconds: 30,
		})
		firstDone <- err
	}()

	select {
	case <-watcher.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("the first watch never reached the watcher")
	}

	_, err := tools.WatchDevice(context.Background(), mcpserver.WatchDeviceInput{
		UserID: "user-000", DeviceID: "device-000", TimeoutSeconds: 30,
	})
	if !errors.Is(err, mcpserver.ErrTooManyWatches) {
		t.Fatalf("got %v, want ErrTooManyWatches: each watch holds a subscription for up to five minutes, so an unbounded count is a cross-tenant deny of service", err)
	}

	cancelFirst()
	select {
	case <-firstDone:
	case <-time.After(5 * time.Second):
		t.Fatal("the first watch did not stop when its caller disconnected")
	}
}

func TestAWatchSlotIsReleasedWhenTheWatchEnds(t *testing.T) {
	watcher := &signallingWatcher{entered: make(chan struct{}, 4)}
	tools := watchToolsWith(t, watcher, 1)

	firstCtx, cancelFirst := context.WithCancel(context.Background())
	firstDone := make(chan struct{})
	go func() {
		defer close(firstDone)
		_, _ = tools.WatchDevice(firstCtx, mcpserver.WatchDeviceInput{
			UserID: "user-000", DeviceID: "device-000", TimeoutSeconds: 30,
		})
	}()

	<-watcher.entered
	cancelFirst()
	<-firstDone

	// the cancellation is observed asynchronously, so retry briefly
	deadline := time.Now().Add(2 * time.Second)
	for {
		secondCtx, cancelSecond := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			_, err := tools.WatchDevice(secondCtx, mcpserver.WatchDeviceInput{
				UserID: "user-000", DeviceID: "device-000", TimeoutSeconds: 30,
			})
			done <- err
		}()

		select {
		case <-watcher.entered:
			cancelSecond()
			<-done
			return
		case err := <-done:
			cancelSecond()
			if !errors.Is(err, mcpserver.ErrTooManyWatches) {
				t.Fatalf("unexpected error: %v", err)
			}
			if time.Now().After(deadline) {
				t.Fatal("the watch slot leaked: a finished watch must free its slot for the next caller")
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func TestDeviceListsAreCappedAndSaySo(t *testing.T) {
	memory := store.NewMemory()

	for i := 0; i < 20; i++ {
		window := appliedWindow(1, int64(i))
		window.Key.DeviceID = fmt.Sprintf("device-%03d", i)
		if err := memory.ApplyWindow(context.Background(), window); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	tools := harden(t, memory, 4)

	output, err := tools.UserDevices(context.Background(), mcpserver.UserDevicesInput{UserID: "user-000", Limit: 5})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(output.Devices) != 5 {
		t.Fatalf("returned %d devices, want the requested 5", len(output.Devices))
	}
	if !output.Truncated {
		t.Fatal("a capped list must report that it was truncated, or the caller cannot tell a small fleet from a clipped answer")
	}
}

func TestDeviceListLimitIsBoundedAbove(t *testing.T) {
	memory := seededStore(t)
	tools := harden(t, memory, 4)

	output, err := tools.UserDevices(context.Background(), mcpserver.UserDevicesInput{UserID: "user-1", Limit: 1_000_000})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if output.Truncated {
		t.Fatal("a limit above the ceiling must be clamped, not reported as truncating a one-device list")
	}
}

func TestUnknownDeviceReadsAsNotFoundNotAsABackendError(t *testing.T) {
	memory := seededStore(t)
	tools := harden(t, memory, 4)

	_, err := tools.DeviceState(context.Background(), mcpserver.DeviceStateInput{UserID: "user-1", DeviceID: "absent"})
	if !errors.Is(err, domain.ErrDeviceNotFound) {
		t.Fatalf("got %v, want domain.ErrDeviceNotFound", err)
	}
	if strings.Contains(err.Error(), "store:") || strings.Contains(err.Error(), "redisstore:") {
		t.Fatalf("error %q names the storage backend, which a client should not have to know", err)
	}
}

func TestTheToolCatalogueDeclaresWhatEachToolDoes(t *testing.T) {
	session := connect(t, harden(t, seededStore(t), 4))

	catalogue, err := session.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	annotations := map[string]*mcp.ToolAnnotations{}
	for _, tool := range catalogue.Tools {
		annotations[tool.Name] = tool.Annotations
	}

	for _, name := range []string{"get_device_state", "get_user_devices", "get_pipeline_metrics", "watch_device"} {
		declared, ok := annotations[name]
		if !ok || declared == nil {
			t.Fatalf("%s declares no annotations, so a host cannot tell it only reads", name)
		}
		if !declared.ReadOnlyHint {
			t.Fatalf("%s is not marked read-only", name)
		}
		if declared.OpenWorldHint == nil || *declared.OpenWorldHint {
			t.Fatalf("%s does not declare a closed world; the default is open, which hosts treat as more dangerous", name)
		}
	}

	reset := annotations["reset_device_counters"]
	if reset == nil {
		t.Fatal("reset_device_counters declares no annotations")
	}
	if reset.ReadOnlyHint {
		t.Fatal("reset_device_counters is marked read-only")
	}
	if reset.DestructiveHint == nil || !*reset.DestructiveHint {
		t.Fatal("reset_device_counters zeroes accumulated counters but does not declare destruction")
	}
	if reset.IdempotentHint {
		t.Fatal("reset_device_counters discards whatever accumulated between calls, so it is not idempotent")
	}
}

func TestTheServerAdvertisesOnlyWhatItImplements(t *testing.T) {
	session := connect(t, harden(t, seededStore(t), 4))

	init := session.InitializeResult()
	if init == nil {
		t.Fatal("no initialize result")
	}
	if init.ServerInfo == nil || init.ServerInfo.Name != mcpserver.ServerName {
		t.Fatalf("server info is %+v", init.ServerInfo)
	}
	if init.Instructions == "" {
		t.Fatal("the server advertises no instructions, so a host has no guidance on tenant scoping or the blocking watch")
	}
	if init.Capabilities == nil || init.Capabilities.Tools == nil {
		t.Fatal("the server does not advertise the tools capability it implements")
	}
	if init.Capabilities.Logging != nil {
		t.Fatal("the server advertises logging but never emits a log notification")
	}
}

func mcpRequest(t *testing.T, handler http.Handler, origin string) *httptest.ResponseRecorder {
	t.Helper()

	body := `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"test","version":"1"}}}`

	request := httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json, text/event-stream")
	if origin != "" {
		request.Header.Set("Origin", origin)
	}

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	return recorder
}

func TestACrossOriginRequestIsRefused(t *testing.T) {
	server, err := mcpserver.NewHTTPServer(mcpserver.HTTPOptions{Addr: ":0", Tools: harden(t, seededStore(t), 4)})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	allowed := mcpRequest(t, server.Handler, "")
	if allowed.Code != http.StatusOK {
		t.Fatalf("a request with no Origin header got %d, want 200: non-browser MCP clients do not send one", allowed.Code)
	}

	refused := mcpRequest(t, server.Handler, "https://attacker.example")
	if refused.Code == http.StatusOK {
		t.Fatalf("a cross-origin request got %d, want a refusal: without origin validation a page the operator visits can drive this endpoint", refused.Code)
	}
}

func TestIdleSessionsAreClosed(t *testing.T) {
	server, err := mcpserver.NewHTTPServer(mcpserver.HTTPOptions{
		Addr:       ":0",
		Tools:      harden(t, seededStore(t), 4),
		SessionTTL: 150 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	opened := mcpRequest(t, server.Handler, "")
	if opened.Code != http.StatusOK {
		t.Fatalf("initialize got %d, want 200", opened.Code)
	}

	session := opened.Header().Get("Mcp-Session-Id")
	if session == "" {
		t.Fatal("the server issued no session id, so this test cannot observe expiry")
	}

	time.Sleep(600 * time.Millisecond)

	request := httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(`{"jsonrpc":"2.0","id":2,"method":"tools/list"}`))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json, text/event-stream")
	request.Header.Set("Mcp-Session-Id", session)

	recorder := httptest.NewRecorder()
	server.Handler.ServeHTTP(recorder, request)

	if recorder.Code == http.StatusOK {
		t.Fatalf("a session idle past its %s timeout was still served: abandoned sessions would accumulate for the life of the process", mcpserver.DefaultSessionTTL)
	}
}
