package store_test

import (
	"testing"

	"github.com/ironclaw/mcp-ironclaw/internal/locktest"
	"github.com/ironclaw/mcp-ironclaw/internal/store"
)

func TestMemoryLockerSatisfiesTheContract(t *testing.T) {
	locktest.Run(t, locktest.Contract{
		New:        func(*testing.T) locktest.Guarded { return store.NewLocker(nil) },
		Held:       store.ErrLockHeld,
		Superseded: store.ErrLeaseSuperseded,
	})
}
