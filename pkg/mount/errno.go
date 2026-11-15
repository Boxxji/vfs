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
		// The kernel interrupted the request (a signal reached the caller while it
		// waited): not a fault. The caller's call is restarted, or it sees EINTR.
		return syscall.EINTR
	case errors.Is(err, context.DeadlineExceeded):
		return syscall.ETIMEDOUT
	}
	// The kernel is told only "I/O error"; say why, where an operator will look.
	fmt.Fprintf(os.Stderr, "vfs mount: %v\n", err)
	return syscall.EIO
}

// What an interrupt does. When a signal reaches a caller that is waiting on the mount
// (Go's runtime sends one every few milliseconds to preempt), the kernel sends the
// server FUSE_INTERRUPT and the request's context ends. What that may cost depends on
// what the caller does with the answer EINTR, which was measured, not assumed:
//
//   - fsync(2) and the lookups (stat, readdir) are restarted by the callers that matter
//     (libc, and Go's os package), so they honour the interrupt: the caller stops
//     waiting and is answered EINTR.
//   - read(2) answered EINTR reaches the caller as EIO when the kernel's page cache
//     was filling the pages (20 GiB of Go reads under signals: 2 of 2 workers failed
//     at the first block), so a read waits for its data, whatever signals arrive.
//   - close(2) is not restarted at all (Linux has dropped the descriptor; Go's
//     os.File.Close returns the error as it is), so a close waits for its commit.
//   - a request that changes the Volume (write, create, mkdir, unlink, truncate) runs
//     to the end: answered EINTR after it took effect, a restarted create(2) with
//     O_EXCL would fail with EEXIST on its own file.
//
// In every case the work is not tied to the request's context.

// own runs work on base, a context no single request owns, and waits for it only as
// long as the request lives. When the request ends first (an interrupt the call
// honours) the wait ends with the request's error, which errno turns into EINTR, and
// the work goes on by itself: it finishes or fails on its own terms, whoever queued
// behind it still gets its result, and the restarted call finds it done. The work
// must not touch memory the request owns. A request that must not be answered EINTR
// passes context.WithoutCancel(ctx) as req and waits to the end.
func own[T any](req, base context.Context, work func(context.Context) (T, error)) (T, error) {
	var zero T
	if err := req.Err(); err != nil {
		return zero, err
	}
	type result struct {
		v   T
		err error
	}
	done := make(chan result, 1)
	go func() {
		v, err := work(base)
		done <- result{v, err}
	}()
	select {
	case r := <-done:
		return r.v, r.err
	case <-req.Done():
		return zero, req.Err()
	}
}

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
// runs on a context nothing cancels, in a goroutine of its own, never half-cancelled
// and never left with the Head's outcome unknown because one caller was interrupted.
// A caller waits for it as long as its request lives.
//
// Commits do not pile up behind a stalled store: at most two exist, the one running
// and one waiting to start. A caller that arrives while one is running joins the one
// waiting, which starts after the caller's writes were made and so covers them; a
// caller that arrives when none is running starts one. Five thousand interrupted
// fsyncs retried against a stalled store therefore park two goroutines, not five
// thousand.
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

// commit makes everything written before the call durable, or ends with the
// request: with its error if the request ended first (EINTR), the commit going on
// by itself.
func (c *committer) commit(req context.Context) error {
	if err := req.Err(); err != nil {
		return err
	}
	f := c.join()
	select {
	case <-f.done:
		return f.err
	case <-req.Done():
		return req.Err()
	}
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
