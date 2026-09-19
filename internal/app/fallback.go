package app

import (
	"context"
	"errors"

	"github.com/ironclaw/mcp-ironclaw/internal/domain"
)

type fallbackState struct {
	StateBackend
	archive domain.StateReader
}

func newFallbackState(primary StateBackend, archive domain.StateReader) StateBackend {
	return fallbackState{StateBackend: primary, archive: archive}
}

func (f fallbackState) DeviceState(ctx context.Context, userID, deviceID string) (domain.DeviceState, error) {
	state, err := f.StateBackend.DeviceState(ctx, userID, deviceID)
	if err == nil {
		return state, nil
	}
	if errors.Is(err, domain.ErrDeviceNotFound) {
		return domain.DeviceState{}, err
	}

	archived, archiveErr := f.archive.DeviceState(ctx, userID, deviceID)
	if archiveErr != nil {
		return domain.DeviceState{}, err
	}

	archived.Stale = true

	return archived, nil
}

func (f fallbackState) UserDevices(ctx context.Context, userID string) (domain.DeviceListing, error) {
	listing, err := f.StateBackend.UserDevices(ctx, userID)
	if err == nil {
		return listing, nil
	}

	archived, archiveErr := f.archive.UserDevices(ctx, userID)
	if archiveErr != nil {
		return domain.DeviceListing{}, err
	}

	archived.Stale = true

	return archived, nil
}
