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

// writeback is sync_file_range(2) with SYNC_FILE_RANGE_WRITE: it starts the I/O
// for the dirty pages of the range and returns without waiting for it.
func writeback(f *os.File, off, n int64) error {
	conn, err := f.SyscallConn()
	if err != nil {
		return err
	}
	var errno syscall.Errno
	if err := conn.Control(func(fd uintptr) {
		_, _, errno = syscall.Syscall6(syscall.SYS_SYNC_FILE_RANGE, fd, uintptr(off), uintptr(n), 2, 0, 0)
	}); err != nil {
		return err
	}
	if errno != 0 {
		return &os.PathError{Op: "sync_file_range", Path: f.Name(), Err: errno}
	}
	return nil
}
