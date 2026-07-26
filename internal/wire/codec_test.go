package wire_test

import (
	"errors"
	"testing"
	"time"

	"github.com/ironclaw/mcp-ironclaw/internal/domain"
	"github.com/ironclaw/mcp-ironclaw/internal/wire"
)

func sampleBatch(events int) domain.Batch {
	batch := domain.Batch{
		DeviceID:  "device-000",
		CreatedAt: time.Unix(1700000000, 0).UTC(),
		Events:    make([]domain.Event, 0, events),
	}

	for i := 0; i < events; i++ {
		batch.Events = append(batch.Events, domain.Event{
			DeviceID:     "device-000",
			UserID:       "user-000",
			ProcessID:    int32(100 + i%4),
			PodID:        "pod-000",
			Kind:         domain.EventKindSyscall,
			ObservedAt:   time.Unix(1700000000, int64(i)).UTC(),
			LatencyNanos: int64(1_000_000 + i),
			Bytes:        int64(64 + i),
			Failed:       i%20 == 0,
		})
	}

	return batch
}

func sampleWindow() domain.AggregateWindow {
	return domain.AggregateWindow{
		Key:         domain.CorrelationKey{UserID: "user-000", DeviceID: "device-000", ProcessID: 7, PodID: "pod-000"},
		WindowID:    892519720,
		Sequence:    1785000000000000000,
		WindowStart: time.Unix(1700000000, 0).UTC(),
		WindowEnd:   time.Unix(1700000010, 0).UTC(),
		Count:       1000,
		ErrorCount:  17,
		Bytes:       640000,
		P95Nanos:    45377988,
		P99Nanos:    73335378,
		MaxNanos:    91000000,
	}
}

func codecs(t *testing.T) []wire.Codec {
	t.Helper()

	list := make([]wire.Codec, 0, 2)
	for _, format := range []wire.Format{wire.FormatJSON, wire.FormatProtobuf} {
		codec, err := wire.For(format)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		list = append(list, codec)
	}
	return list
}

func TestForRejectsUnknownFormats(t *testing.T) {
	if _, err := wire.For("avro"); !errors.Is(err, wire.ErrUnknownFormat) {
		t.Fatalf("got %v, want ErrUnknownFormat", err)
	}
	codec, err := wire.For("")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if codec.Name() != wire.FormatJSON {
		t.Fatalf("empty format resolved to %s, want json for backwards compatibility", codec.Name())
	}
}

func TestBatchRoundTripsThroughEveryCodec(t *testing.T) {
	original := sampleBatch(50)

	for _, codec := range codecs(t) {
		t.Run(string(codec.Name()), func(t *testing.T) {
			encoded, err := codec.EncodeBatch(original)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			decoded, err := codec.DecodeBatch(encoded)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if decoded.DeviceID != original.DeviceID || decoded.Len() != original.Len() {
				t.Fatalf("decoded %s/%d, want %s/%d", decoded.DeviceID, decoded.Len(), original.DeviceID, original.Len())
			}
			for i := range original.Events {
				if decoded.Events[i] != original.Events[i] {
					t.Fatalf("event %d changed:\n got %+v\nwant %+v", i, decoded.Events[i], original.Events[i])
				}
			}
			if err := decoded.Validate(); err != nil {
				t.Fatalf("decoded batch failed domain validation: %v", err)
			}
		})
	}
}

func TestWindowRoundTripsThroughEveryCodec(t *testing.T) {
	original := sampleWindow()

	for _, codec := range codecs(t) {
		t.Run(string(codec.Name()), func(t *testing.T) {
			encoded, err := codec.EncodeWindow(original)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			decoded, err := codec.DecodeWindow(encoded)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if decoded != original {
				t.Fatalf("decoded %+v, want %+v", decoded, original)
			}
			if decoded.Identity() != original.Identity() {
				t.Fatalf("identity changed: %s vs %s", decoded.Identity(), original.Identity())
			}
		})
	}
}

func TestMalformedPayloadsAreRejected(t *testing.T) {
	json, err := wire.For(wire.FormatJSON)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if _, err := json.DecodeBatch([]byte("{not json")); err == nil {
		t.Fatal("expected a decode error for malformed json")
	}
	if _, err := json.DecodeWindow([]byte("{not json")); err == nil {
		t.Fatal("expected a decode error for malformed json")
	}
}

func TestProtobufIsSubstantiallySmallerThanJSON(t *testing.T) {
	batch := sampleBatch(500)

	jsonCodec, err := wire.For(wire.FormatJSON)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	protoCodec, err := wire.For(wire.FormatProtobuf)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	asJSON, err := jsonCodec.EncodeBatch(batch)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	asProto, err := protoCodec.EncodeBatch(batch)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ratio := float64(len(asJSON)) / float64(len(asProto))
	t.Logf("500-event batch: json=%d bytes protobuf=%d bytes ratio=%.2fx (json bytes per event=%.1f, protobuf=%.1f)",
		len(asJSON), len(asProto), ratio, float64(len(asJSON))/500, float64(len(asProto))/500)

	if len(asProto) >= len(asJSON) {
		t.Fatalf("protobuf (%d) is not smaller than json (%d)", len(asProto), len(asJSON))
	}
	if ratio < 1.5 {
		t.Fatalf("protobuf is only %.2fx smaller; the capacity model assumed a material saving", ratio)
	}
}

func BenchmarkEncodeBatchJSON(b *testing.B) {
	codec := wire.JSON{}
	batch := sampleBatch(500)

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := codec.EncodeBatch(batch); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEncodeBatchProtobuf(b *testing.B) {
	codec := wire.Protobuf{}
	batch := sampleBatch(500)

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := codec.EncodeBatch(batch); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodeBatchJSON(b *testing.B) {
	codec := wire.JSON{}
	encoded, err := codec.EncodeBatch(sampleBatch(500))
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := codec.DecodeBatch(encoded); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodeBatchProtobuf(b *testing.B) {
	codec := wire.Protobuf{}
	encoded, err := codec.EncodeBatch(sampleBatch(500))
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := codec.DecodeBatch(encoded); err != nil {
			b.Fatal(err)
		}
	}
}
