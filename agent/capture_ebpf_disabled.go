//go:build !(ebpf && linux)

package main

import (
	"errors"

	"github.com/ironclaw/mcp-ironclaw/internal/domain"
)

var ErrEBPFNotCompiled = errors.New("agent: this binary was built without the ebpf build tag; rebuild with: go build -tags ebpf ./agent")

func newEBPFSource(captureSettings) (domain.EventSource, error) {
	return nil, ErrEBPFNotCompiled
}
