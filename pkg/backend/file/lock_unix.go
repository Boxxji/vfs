//go:build unix

package file

import (
	"os"
	"path/filepath"
	"syscall"
)

// lock takes the store-wide exclusive lock and returns its release. flock
// excludes other open file descriptions, so it orders writers across goroutines
// and across processes sharing the directory.
func (f *fileBackend) lock() (func(), error) {
	if err := os.MkdirAll(f.root, 0o755); err != nil {
		return nil, err
	}
	l, err := os.OpenFile(filepath.Join(f.root, ".lock"), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(l.Fd()), syscall.LOCK_EX); err != nil {
		_ = l.Close()
		return nil, err
	}
	return func() { _ = l.Close() }, nil // closing releases the flock
}
