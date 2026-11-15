package vfs_test

import (
	"os"
	"testing"

	"github.com/hanzoai/vfs/internal/storetest"
)

func TestMain(m *testing.M) {
	code := m.Run()
	storetest.StopReal()
	os.Exit(code)
}
