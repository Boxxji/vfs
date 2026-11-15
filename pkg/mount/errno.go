//go:build fuse && (linux || darwin)

package mount

import (
	"context"
	"errors"
	"io/fs"
	"syscall"

	"github.com/hanzoai/vfs"
)

// blockSize is the st_blksize the mount reports. It is only a hint to callers
// about a good I/O size: the Volume has no block size.
const blockSize = 4096

// errno maps a Volume error onto the errno the kernel hands the caller.
func errno(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, fs.ErrNotExist):
		return syscall.ENOENT
	case errors.Is(err, fs.ErrExist):
		return syscall.EEXIST
	case errors.Is(err, vfs.ErrNotDir):
		return syscall.ENOTDIR
	case errors.Is(err, vfs.ErrIsDir):
		return syscall.EISDIR
	case errors.Is(err, vfs.ErrNotEmpty):
		return syscall.ENOTEMPTY
	case errors.Is(err, fs.ErrInvalid):
		return syscall.EINVAL
	case errors.Is(err, vfs.ErrReadOnly), errors.Is(err, vfs.ErrStaleRound):
		return syscall.EROFS
	}
	return syscall.EIO
}

// finish commits what the mount wrote once the kernel has released it. A Volume
// that was only opened has nothing to commit.
func finish(vol *vfs.Volume, serveErr error) error {
	if vol.Stats().Round == 0 {
		return serveErr
	}
	if err := vol.Flush(context.Background()); err != nil && serveErr == nil {
		return err
	}
	return serveErr
}
