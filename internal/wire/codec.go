package wire

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/ironclaw/mcp-ironclaw/internal/domain"
	pb "github.com/ironclaw/mcp-ironclaw/internal/wire/ironclawpb"
	"google.golang.org/protobuf/proto"
)

var (
	ErrUnknownFormat  = errors.New("wire: unknown codec format")
	ErrKindOutOfRange = errors.New("wire: event kind is outside the domain range")
)

type Format string

const (
	FormatJSON     Format = "json"
	FormatProtobuf Format = "protobuf"
)

type Codec interface {
	Name() Format
	EncodeBatch(domain.Batch) ([]byte, error)
	DecodeBatch([]byte) (domain.Batch, error)
	EncodeWindow(domain.AggregateWindow) ([]byte, error)
	DecodeWindow([]byte) (domain.AggregateWindow, error)
}

func For(format Format) (Codec, error) {
	switch format {
	case FormatJSON, "":
		return JSON{}, nil
	case FormatProtobuf:
		return Protobuf{}, nil
	default:
		return nil, fmt.Errorf("%w: %s", ErrUnknownFormat, format)
	}
}

type Protobuf struct{}

func (Protobuf) Name() Format { return FormatProtobuf }

func (Protobuf) EncodeBatch(batch domain.Batch) ([]byte, error) {
	message := &pb.Batch{
		DeviceId:           batch.DeviceID,
		CreatedAtUnixNanos: batch.CreatedAt.UnixNano(),
		Events:             make([]*pb.Event, 0, len(batch.Events)),
	}

	for _, event := range batch.Events {
		message.Events = append(message.Events, &pb.Event{
			DeviceId:            event.DeviceID,
			UserId:              event.UserID,
			ProcessId:           event.ProcessID,
			PodId:               event.PodID,
			Kind:                uint32(event.Kind),
			ObservedAtUnixNanos: event.ObservedAt.UnixNano(),
			LatencyNanos:        event.LatencyNanos,
			Bytes:               event.Bytes,
			Failed:              event.Failed,
		})
	}

	encoded, err := proto.Marshal(message)
	if err != nil {
		return nil, fmt.Errorf("wire: encoding batch: %w", err)
	}
	return encoded, nil
}

func (Protobuf) DecodeBatch(raw []byte) (domain.Batch, error) {
	var message pb.Batch
	if err := proto.Unmarshal(raw, &message); err != nil {
		return domain.Batch{}, fmt.Errorf("wire: decoding batch: %w", err)
	}

	batch := domain.Batch{
		DeviceID: message.DeviceId,
		Events:   make([]domain.Event, 0, len(message.Events)),
	}
	if message.CreatedAtUnixNanos > 0 {
		batch.CreatedAt = time.Unix(0, message.CreatedAtUnixNanos).UTC()
	}

	for _, event := range message.Events {
		if event.Kind > math.MaxUint8 {
			return domain.Batch{}, fmt.Errorf("%w: %d", ErrKindOutOfRange, event.Kind)
		}
		batch.Events = append(batch.Events, domain.Event{
			DeviceID:     event.DeviceId,
			UserID:       event.UserId,
			ProcessID:    event.ProcessId,
			PodID:        event.PodId,
			Kind:         domain.EventKind(event.Kind),
			ObservedAt:   time.Unix(0, event.ObservedAtUnixNanos).UTC(),
			LatencyNanos: event.LatencyNanos,
			Bytes:        event.Bytes,
			Failed:       event.Failed,
		})
	}

	return batch, nil
}

func (Protobuf) EncodeWindow(window domain.AggregateWindow) ([]byte, error) {
	encoded, err := proto.Marshal(&pb.AggregateWindow{
		UserId:               window.Key.UserID,
		DeviceId:             window.Key.DeviceID,
		ProcessId:            window.Key.ProcessID,
		PodId:                window.Key.PodID,
		WindowId:             window.WindowID,
		Sequence:             window.Sequence,
		WindowStartUnixNanos: window.WindowStart.UnixNano(),
		WindowEndUnixNanos:   window.WindowEnd.UnixNano(),
		Count:                window.Count,
		ErrorCount:           window.ErrorCount,
		Bytes:                window.Bytes,
		P95Nanos:             window.P95Nanos,
		P99Nanos:             window.P99Nanos,
		MaxNanos:             window.MaxNanos,
	})
	if err != nil {
		return nil, fmt.Errorf("wire: encoding window: %w", err)
	}
	return encoded, nil
}

func (Protobuf) DecodeWindow(raw []byte) (domain.AggregateWindow, error) {
	var message pb.AggregateWindow
	if err := proto.Unmarshal(raw, &message); err != nil {
		return domain.AggregateWindow{}, fmt.Errorf("wire: decoding window: %w", err)
	}

	return domain.AggregateWindow{
		Key: domain.CorrelationKey{
			UserID:    message.UserId,
			DeviceID:  message.DeviceId,
			ProcessID: message.ProcessId,
			PodID:     message.PodId,
		},
		WindowID:    message.WindowId,
		Sequence:    message.Sequence,
		WindowStart: time.Unix(0, message.WindowStartUnixNanos).UTC(),
		WindowEnd:   time.Unix(0, message.WindowEndUnixNanos).UTC(),
		Count:       message.Count,
		ErrorCount:  message.ErrorCount,
		Bytes:       message.Bytes,
		P95Nanos:    message.P95Nanos,
		P99Nanos:    message.P99Nanos,
		MaxNanos:    message.MaxNanos,
	}, nil
}

type JSON struct{}

func (JSON) Name() Format { return FormatJSON }

type jsonEvent struct {
	DeviceID     string `json:"device_id"`
	UserID       string `json:"user_id"`
	ProcessID    int32  `json:"process_id"`
	PodID        string `json:"pod_id"`
	Kind         uint8  `json:"kind"`
	ObservedAt   int64  `json:"observed_at_unix_nanos"`
	LatencyNanos int64  `json:"latency_nanos"`
	Bytes        int64  `json:"bytes"`
	Failed       bool   `json:"failed"`
}

type jsonBatch struct {
	DeviceID  string      `json:"device_id"`
	CreatedAt int64       `json:"created_at_unix_nanos"`
	Events    []jsonEvent `json:"events"`
}

type jsonWindow struct {
	UserID      string `json:"user_id"`
	DeviceID    string `json:"device_id"`
	ProcessID   int32  `json:"process_id"`
	PodID       string `json:"pod_id"`
	WindowID    int64  `json:"window_id"`
	Sequence    int64  `json:"sequence"`
	WindowStart int64  `json:"window_start_unix_nanos"`
	WindowEnd   int64  `json:"window_end_unix_nanos"`
	Count       int64  `json:"count"`
	ErrorCount  int64  `json:"error_count"`
	Bytes       int64  `json:"bytes"`
	P95Nanos    int64  `json:"p95_nanos"`
	P99Nanos    int64  `json:"p99_nanos"`
	MaxNanos    int64  `json:"max_nanos"`
}

func (JSON) EncodeBatch(batch domain.Batch) ([]byte, error) {
	payload := jsonBatch{
		DeviceID:  batch.DeviceID,
		CreatedAt: batch.CreatedAt.UnixNano(),
		Events:    make([]jsonEvent, 0, len(batch.Events)),
	}

	for _, event := range batch.Events {
		payload.Events = append(payload.Events, jsonEvent{
			DeviceID:     event.DeviceID,
			UserID:       event.UserID,
			ProcessID:    event.ProcessID,
			PodID:        event.PodID,
			Kind:         uint8(event.Kind),
			ObservedAt:   event.ObservedAt.UnixNano(),
			LatencyNanos: event.LatencyNanos,
			Bytes:        event.Bytes,
			Failed:       event.Failed,
		})
	}

	encoded, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("wire: encoding batch: %w", err)
	}
	return encoded, nil
}

func (JSON) DecodeBatch(raw []byte) (domain.Batch, error) {
	var payload jsonBatch
	if err := json.Unmarshal(raw, &payload); err != nil {
		return domain.Batch{}, fmt.Errorf("wire: decoding batch: %w", err)
	}

	batch := domain.Batch{
		DeviceID: payload.DeviceID,
		Events:   make([]domain.Event, 0, len(payload.Events)),
	}
	if payload.CreatedAt > 0 {
		batch.CreatedAt = time.Unix(0, payload.CreatedAt).UTC()
	}

	for _, event := range payload.Events {
		batch.Events = append(batch.Events, domain.Event{
			DeviceID:     event.DeviceID,
			UserID:       event.UserID,
			ProcessID:    event.ProcessID,
			PodID:        event.PodID,
			Kind:         domain.EventKind(event.Kind),
			ObservedAt:   time.Unix(0, event.ObservedAt).UTC(),
			LatencyNanos: event.LatencyNanos,
			Bytes:        event.Bytes,
			Failed:       event.Failed,
		})
	}

	return batch, nil
}

func (JSON) EncodeWindow(window domain.AggregateWindow) ([]byte, error) {
	encoded, err := json.Marshal(jsonWindow{
		UserID:      window.Key.UserID,
		DeviceID:    window.Key.DeviceID,
		ProcessID:   window.Key.ProcessID,
		PodID:       window.Key.PodID,
		WindowID:    window.WindowID,
		Sequence:    window.Sequence,
		WindowStart: window.WindowStart.UnixNano(),
		WindowEnd:   window.WindowEnd.UnixNano(),
		Count:       window.Count,
		ErrorCount:  window.ErrorCount,
		Bytes:       window.Bytes,
		P95Nanos:    window.P95Nanos,
		P99Nanos:    window.P99Nanos,
		MaxNanos:    window.MaxNanos,
	})
	if err != nil {
		return nil, fmt.Errorf("wire: encoding window: %w", err)
	}
	return encoded, nil
}

func (JSON) DecodeWindow(raw []byte) (domain.AggregateWindow, error) {
	var payload jsonWindow
	if err := json.Unmarshal(raw, &payload); err != nil {
		return domain.AggregateWindow{}, fmt.Errorf("wire: decoding window: %w", err)
	}

	return domain.AggregateWindow{
		Key: domain.CorrelationKey{
			UserID:    payload.UserID,
			DeviceID:  payload.DeviceID,
			ProcessID: payload.ProcessID,
			PodID:     payload.PodID,
		},
		WindowID:    payload.WindowID,
		Sequence:    payload.Sequence,
		WindowStart: time.Unix(0, payload.WindowStart).UTC(),
		WindowEnd:   time.Unix(0, payload.WindowEnd).UTC(),
		Count:       payload.Count,
		ErrorCount:  payload.ErrorCount,
		Bytes:       payload.Bytes,
		P95Nanos:    payload.P95Nanos,
		P99Nanos:    payload.P99Nanos,
		MaxNanos:    payload.MaxNanos,
	}, nil
}
