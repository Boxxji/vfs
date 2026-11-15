package journal

import (
	"os"
	"syscall"
)

// datasync is fdatasync(2): file data and the metadata needed to read it back,
// not timestamps. The journal overwrites allocated blocks, so there is nothing
// else to flush and the call costs one device flush.
func datasync(f *os.File) error {
	conn, err := f.SyscallConn()
	if err != nil {
		return err
	}
	var errno error
	if err := conn.Control(func(fd uintptr) {
		for {
			errno = syscall.Fdatasync(int(fd))
			if errno != syscall.EINTR {
				return
			}
		}
	}); err != nil {
		return err
	}
	if errno != nil {
		return &os.PathError{Op: "fdatasync", Path: f.Name(), Err: errno}
	}
	return nil
}
