package redisstore_test

import (
	"testing"

	"github.com/ironclaw/mcp-ironclaw/internal/locktest"
	"github.com/ironclaw/mcp-ironclaw/internal/redisstore"
)

func TestRedisLockerSatisfiesTheContract(t *testing.T) {
	locktest.Run(t, locktest.Contract{
		New:        func(t *testing.T) locktest.Guarded { return newStore(t) },
		Held:       redisstore.ErrLockHeld,
		Superseded: redisstore.ErrLeaseSuperseded,
	})
}
