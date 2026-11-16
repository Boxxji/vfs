//go:build fuse && (linux || darwin)

package mount

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"sync"
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
	case errors.Is(err, vfs.ErrTooBig):
		return syscall.EFBIG
	case errors.Is(err, vfs.ErrReadOnly), errors.Is(err, vfs.ErrStaleRound):
		return syscall.EROFS
	case errors.Is(err, context.Canceled):
		// Only the end of the mount cancels a context here (see below): nobody is
		// waiting for the answer.
		return syscall.EINTR
	case errors.Is(err, context.DeadlineExceeded):
		return syscall.ETIMEDOUT
	}
	// The kernel is told only "I/O error"; say why, where an operator will look.
	fmt.Fprintf(os.Stderr, "vfs mount: %v\n", err)
	return syscall.EIO
}

// What an interrupt does: nothing. When a signal reaches a caller that is waiting on the
// mount (Go's runtime sends one every few milliseconds to preempt), the kernel sends the
// server FUSE_INTERRUPT and the request's context ends. No request is answered EINTR,
// because what a caller does with that answer was measured, and it is not what C
// libraries are said to do:
//
//   - fsync(2) answered EINTR is an error to SQLite (its Go port and its C library both
//     report it as an I/O error): a C caller under SIGALRM saw 246,912 EINTRs over 5,000
//     fsyncs, and `go test` of a SQLite database on the mount failed 7 times of 7 on an
//     amd64 kernel. Neither glibc nor the kernel restarts it.
//   - stat(2) and readdir answered EINTR reached a C caller as an error 1,586 times of
//     60,000 without SA_RESTART and 2,884 times with it: the kernel does not restart a
//     FUSE request that was answered EINTR.
//   - read(2) answered EINTR reaches the caller as EIO when the kernel's page cache was
//     filling the pages (20 GiB of Go reads under signals failed at the first block).
//   - close(2) is not restarted at all (Linux has dropped the descriptor; Go's
//     os.File.Close returns the error as it is).
//   - a request that changes the Volume, answered EINTR after it took effect, makes a
//     restarted create(2) with O_EXCL fail with EEXIST on its own file.
//
// So every handler runs on a context its request cannot end (context.WithoutCancel),
// and the work of a read runs on the mount's own context. A caller is always answered
// with the result of its call.
//
// The cost is that a call waits for the store. There are no deadlines on the store yet,
// so a store that stops answering stops every call that needs it, and killing a caller
// does not free it: the kernel keeps a request that reached the daemon until the daemon
// answers it, so read, stat, readdir, create, mkdir, unlink, truncate, write and fsync
// stayed in state D after SIGKILL until the store answered (an interrupt honoured
// with EINTR is what freed lookups and fsync before), while the daemon holds
// every abandoned request: the FUSE library holds a message buffer of about 128 KiB for
// each until its handler returns, whatever the handler does (3,000 stalled lookups held
// 386 MB and 1,600 stalled reads 225 MB, and limiting what a read allocates itself did
// not change that). Only answering the abandoned requests frees them, which is what a
// deadline on the store does: it is the next change (docs/SPECIFICATION.md, section 11).

// session is what every node and handle of one mount shares: the Volume, the mount's
// own context, and the committer. It is built only by newSession, which refuses a
// nil context: the work of a read runs on base, and a store client panics on a nil
// parent the first time it is used.
type session struct {
	vol   *vfs.Volume
	base  context.Context
	group *committer
}

func newSession(base context.Context, vol *vfs.Volume) *session {
	if base == nil || vol == nil {
		panic("mount: a session needs a context and a Volume")
	}
	return &session{vol: vol, base: base, group: newCommitter(vol)}
}

// committer runs the commits of a mount. A commit settles every open file and
// replaces the Head, and every other caller queues behind it, so it is shared work: it
// runs on a context nothing cancels, in a goroutine of its own, never half-cancelled.
//
// Commits do not pile up behind a stalled store: at most two exist, the one running
// and one waiting to start. A caller that arrives while one is running joins the one
// waiting, which starts after the caller's writes were made and so covers them; a
// caller that arrives when none is running starts one. Five thousand callers of fsync
// against a stalled store therefore share two commits, not five thousand.
type committer struct {
	vol       *vfs.Volume
	mu        sync.Mutex
	run, next *flight
}

// flight is one commit and the callers waiting on it.
type flight struct {
	done chan struct{}
	err  error
}

func newCommitter(vol *vfs.Volume) *committer { return &committer{vol: vol} }

// commit makes everything written before the call durable, and returns the result of
// the commit that did.
func (c *committer) commit() error {
	f := c.join()
	<-f.done
	return f.err
}

// join returns a flight that starts after now.
func (c *committer) join() *flight {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.run == nil {
		c.run = &flight{done: make(chan struct{})}
		go c.fly(c.run)
		return c.run
	}
	if c.next == nil {
		c.next = &flight{done: make(chan struct{})}
	}
	return c.next
}

// fly runs f, then starts the flight that was waiting, if any.
func (c *committer) fly(f *flight) {
	f.err = c.vol.Flush(context.Background())
	close(f.done)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.run, c.next = c.next, nil
	if c.run != nil {
		go c.fly(c.run)
	}
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

// unsupported is what the mount answers for an operation it does not implement:
// an honest "not supported", never a success that changed nothing.
var unsupported error = syscall.ENOSYS
