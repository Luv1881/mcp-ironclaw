package domain_test

import (
	"fmt"
	"testing"

	"github.com/ironclaw/mcp-ironclaw/internal/domain"
)

func referenceKey(k domain.CorrelationKey) string {
	return fmt.Sprintf("{%s}:%s:%d:%s", k.UserID, k.DeviceID, k.ProcessID, k.PodID)
}

func referenceIdentity(w domain.AggregateWindow) string {
	return fmt.Sprintf("%s#%d#%d", referenceKey(w.Key), w.WindowID, w.Sequence)
}

func referenceWindowIdentity(w domain.AggregateWindow) string {
	return fmt.Sprintf("%s#%d", referenceKey(w.Key), w.WindowID)
}

func referenceScope(k domain.CorrelationKey) string {
	return fmt.Sprintf("{%s}:%s", k.UserID, k.DeviceID)
}

func FuzzIdentityMatchesTheOriginalFormatting(f *testing.F) {
	f.Add("user-000", "device-000", int32(4242), "pod-000", int64(892519720), int64(1785000000000000000))
	f.Add("", "", int32(0), "", int64(0), int64(0))
	f.Add("u", "d", int32(-2147483648), "p", int64(-9223372036854775808), int64(9223372036854775807))
	f.Add("ünïcødé", "デバイス", int32(-1), "pod/with:colons", int64(-1), int64(-1))

	f.Fuzz(func(t *testing.T, userID, deviceID string, processID int32, podID string, windowID, sequence int64) {
		key := domain.CorrelationKey{UserID: userID, DeviceID: deviceID, ProcessID: processID, PodID: podID}
		window := domain.AggregateWindow{Key: key, WindowID: windowID, Sequence: sequence}

		if got, want := key.String(), referenceKey(key); got != want {
			t.Fatalf("CorrelationKey.String() = %q, original formatting gives %q", got, want)
		}
		if got, want := key.DeviceScope(), referenceScope(key); got != want {
			t.Fatalf("DeviceScope() = %q, original formatting gives %q", got, want)
		}
		if got, want := window.Identity(), referenceIdentity(window); got != want {
			t.Fatalf("Identity() = %q, original formatting gives %q", got, want)
		}
		if got, want := window.WindowIdentity(), referenceWindowIdentity(window); got != want {
			t.Fatalf("WindowIdentity() = %q, original formatting gives %q", got, want)
		}
	})
}
