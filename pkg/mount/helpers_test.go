//go:build fuse && (linux || darwin)

package mount_test

import "github.com/hanzoai/vfs/internal/mounttest"

// The helpers every mount test uses live in internal/mounttest, which also serves
// the tests of packages that run something else on a mount.
var (
	needFUSE      = mounttest.NeedFUSE
	unmount       = mounttest.Unmount
	mountAt       = mounttest.MountAt
	mountURL      = mounttest.MountURL
	newMountpoint = mounttest.NewMountpoint
)

type mounted = mounttest.Mounted
