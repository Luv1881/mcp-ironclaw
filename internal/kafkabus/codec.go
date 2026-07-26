package kafkabus

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/ironclaw/mcp-ironclaw/internal/domain"
)

type commandPayload struct {
	Kind     uint8  `json:"kind"`
	UserID   string `json:"user_id"`
	DeviceID string `json:"device_id"`
	Actor    string `json:"actor"`
	IssuedAt int64  `json:"issued_at_unix_nanos"`
}

func EncodeCommand(command domain.Command) ([]byte, error) {
	encoded, err := json.Marshal(commandPayload{
		Kind:     uint8(command.Kind),
		UserID:   command.UserID,
		DeviceID: command.DeviceID,
		Actor:    command.Actor,
		IssuedAt: command.IssuedAt.UnixNano(),
	})
	if err != nil {
		return nil, fmt.Errorf("kafkabus: encoding command: %w", err)
	}
	return encoded, nil
}

func DecodeCommand(raw []byte) (domain.Command, error) {
	var payload commandPayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		return domain.Command{}, fmt.Errorf("kafkabus: decoding command: %w", err)
	}

	return domain.Command{
		Kind:     domain.CommandKind(payload.Kind),
		UserID:   payload.UserID,
		DeviceID: payload.DeviceID,
		Actor:    payload.Actor,
		IssuedAt: time.Unix(0, payload.IssuedAt).UTC(),
	}, nil
}
