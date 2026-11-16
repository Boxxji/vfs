package storetest

import (
	"testing"

	"github.com/hanzoai/vfs/pkg/backend"
)

// The memory store that most tests use meets the same contract as every other store.
func TestMemConforms(t *testing.T) {
	Run(t, func(t *testing.T) backend.Backend { return NewMem() })
}
