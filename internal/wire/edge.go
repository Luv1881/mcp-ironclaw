package wire

import (
	"encoding/json"
	"fmt"

	"github.com/ironclaw/mcp-ironclaw/internal/domain"
)

type EdgeEvent struct {
	UserID              string `json:"user_id"`
	ProcessID           int32  `json:"process_id"`
	Kind                uint8  `json:"kind"`
	ObservedAtUnixNanos int64  `json:"observed_at_unix_nanos"`
	LatencyNanos        int64  `json:"latency_nanos"`
	Bytes               int64  `json:"bytes"`
	Failed              bool   `json:"failed"`
}

type EdgeBatch struct {
	DeviceID string      `json:"device_id"`
	Events   []EdgeEvent `json:"events"`
}

func EncodeEdgeBatch(batch domain.Batch) ([]byte, error) {
	payload := EdgeBatch{
		DeviceID: batch.DeviceID,
		Events:   make([]EdgeEvent, 0, batch.Len()),
	}

	for i := range batch.Events {
		event := &batch.Events[i]
		payload.Events = append(payload.Events, EdgeEvent{
			UserID:              event.UserID,
			ProcessID:           event.ProcessID,
			Kind:                uint8(event.Kind),
			ObservedAtUnixNanos: event.ObservedAt.UnixNano(),
			LatencyNanos:        event.LatencyNanos,
			Bytes:               event.Bytes,
			Failed:              event.Failed,
		})
	}

	encoded, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("wire: encoding edge batch: %w", err)
	}

	return encoded, nil
}

func DecodeEdgeBatch(raw []byte) (EdgeBatch, error) {
	var payload EdgeBatch
	if err := json.Unmarshal(raw, &payload); err != nil {
		return EdgeBatch{}, fmt.Errorf("wire: decoding edge batch: %w", err)
	}

	return payload, nil
}
