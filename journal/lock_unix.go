//go:build unix

package journal

import (
	"errors"
	"fmt"
	"os"
	"syscall"
)

// lockDirectory takes an exclusive advisory lock on the directory, held for as
// long as the returned file is open. The kernel drops it when the process dies.
func lockDirectory(dir string) (*os.File, error) {
	f, err := os.Open(dir)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, fmt.Errorf("%w: %s", ErrLocked, dir)
		}
		return nil, err
	}
	return f, nil
}
