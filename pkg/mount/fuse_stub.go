//go:build !fuse

// Package mount serves a *vfs.Volume to the kernel over FUSE. Without the `fuse`
// build tag this is a stub: Mount returns an error pointing at `make build-fuse`.
package mount

import (
	"context"
	"fmt"

	"github.com/hanzoai/vfs"
)

// Mount returns a build-tag error. Build with `-tags fuse` to enable.
func Mount(_ context.Context, _ *vfs.Volume, _ string) error {
	return fmt.Errorf("mount: this binary was built without FUSE support — use `make build-fuse` or `go build -tags fuse`")
}
