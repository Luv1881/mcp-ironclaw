package domain_test

import (
	"testing"
	"time"

	"github.com/ironclaw/mcp-ironclaw/internal/domain"
)

func benchWindow() domain.AggregateWindow {
	return domain.AggregateWindow{
		Key:         domain.CorrelationKey{UserID: "user-000", DeviceID: "device-000", ProcessID: 4242, PodID: "ingest-7c9f-abcde"},
		WindowID:    892519720,
		Sequence:    1785000000000000000,
		WindowStart: time.Unix(1700000000, 0).UTC(),
		WindowEnd:   time.Unix(1700000010, 0).UTC(),
		Count:       1000,
	}
}

func BenchmarkWindowIdentity(b *testing.B) {
	window := benchWindow()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if window.Identity() == "" {
			b.Fatal("empty identity")
		}
	}
}

func BenchmarkCorrelationKeyString(b *testing.B) {
	key := benchWindow().Key

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if key.String() == "" {
			b.Fatal("empty key")
		}
	}
}
