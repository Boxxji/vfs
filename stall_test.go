package vfs_test

import (
	"context"
	"errors"
	"runtime"
	"testing"
	"time"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/internal/storetest"
	"github.com/hanzoai/vfs/pkg/backend"
)

// stall blocks a mutating call on key until ctx is done, as a store that
// accepts the request and never answers.
type stall struct {
	backend.Backend
	on      func(key string) bool
	entered chan struct{}
}

func (s *stall) block(ctx context.Context, key string) error {
	if s.on(key) {
		select {
		case s.entered <- struct{}{}:
		default:
		}
		<-ctx.Done()
		return ctx.Err()
	}
	return nil
}
func (s *stall) Put(ctx context.Context, k string, d []byte) error {
	if err := s.block(ctx, k); err != nil {
		return err
	}
	return s.Backend.Put(ctx, k, d)
}
func (s *stall) PutIfVersion(ctx context.Context, k string, d []byte, v backend.Version) (backend.Version, error) {
	if err := s.block(ctx, k); err != nil {
		return "", err
	}
	return s.Backend.PutIfVersion(ctx, k, d, v)
}

// TestStalledCommitHonoursItsDeadline: a commit blocked on the store holds the
// Volume lock, so another caller's Stat waits behind it and does not honour its own
// deadline: that is measured here, and is the limit of this change (a lock held
// across the store call, no per-call deadlines, no retries of its own). What does
// hold: the commit returns when its own context ends, nothing it had not committed
// is lost, the Volume is usable again, and the next Flush commits.
func TestStalledCommitHonoursItsDeadline(t *testing.T) {
	mem := storetest.NewMem()
	st := &stall{Backend: mem, on: func(string) bool { return false }, entered: make(chan struct{}, 1)}
	c := vfsAge(t)
	if err := vfs.Create(ctx, cfg(mem, c, 0)); err != nil {
		t.Fatal(err)
	}
	v, err := vfs.Attach(ctx, cfg(st, c, 1))
	if err != nil {
		t.Fatal(err)
	}
	in := write(t, v, vfs.RootInode, "f", []byte("hello"))
	st.on = func(k string) bool { return k == "head" }
	commitCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	done := make(chan error, 1)
	start := time.Now()
	go func() { done <- v.Flush(commitCtx) }()
	<-st.entered

	// Another caller asks for the attributes of a file while the commit is stalled.
	res := make(chan error, 1)
	go func() { _, err := v.Stat(ctx, in.ID); res <- err }()
	select {
	case err := <-res:
		t.Logf("Stat returned after %v behind the stalled commit: %v", time.Since(start).Round(time.Millisecond), err)
	case <-time.After(10 * time.Second):
		t.Fatal("Stat is still blocked long after the commit's deadline")
	}
	if err := <-done; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("the stalled Flush = %v, want its context's deadline", err)
	}
	st.on = func(string) bool { return false }
	flush(t, v)
	mustEqual(t, "after the stall", readAll(t, reopen(t, mem, c), in.ID), []byte("hello"))
}

// TestNoGoroutineLeak: opening, writing, flushing, failing and closing Volumes
// leaves no goroutine behind.
func TestNoGoroutineLeak(t *testing.T) {
	before := runtime.NumGoroutine()
	for i := range 20 {
		mem := storetest.NewMem()
		c := vfsAge(t)
		if err := vfs.Create(ctx, cfg(mem, c, 0)); err != nil {
			t.Fatal(err)
		}
		v, _ := vfs.Attach(ctx, cfg(mem, c, 1))
		write(t, v, vfs.RootInode, "f", content(uint64(i), 300<<10))
		flush(t, v)
		ro := reopen(t, mem, c)
		_, _ = ro.ReadDir(ctx, vfs.RootInode)
		_ = ro.Close()
		_ = v.Close()
	}
	time.Sleep(200 * time.Millisecond)
	runtime.GC()
	if after := runtime.NumGoroutine(); after > before+2 {
		buf := make([]byte, 1<<16)
		n := runtime.Stack(buf, true)
		t.Fatalf("goroutines %d -> %d\n%s", before, after, buf[:n])
	}
}
