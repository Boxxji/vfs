//go:build fuse && linux

package mount

import (
	"context"
	"testing"
	"time"

	"github.com/hanzoai/vfs"
)

// TestFsyncJoinsAFlightThatCoversIt: a caller of fsync whose write completed after a
// commit began must not be answered by that commit. Today a write waits for the Volume lock
// that a Flush holds, so the write below cannot complete while the commit is stalled and this
// test reports "write blocked" (the property holds by the lock). The day Flush gives the lock
// up (SPEC 11, "the next change"), join() handing back the running flight acknowledges a
// write that flight did not take.
func TestFsyncJoinsAFlightThatCoversIt(t *testing.T) {
	g, v, c, mem := testVolume(t)
	bg := context.Background()
	mk := func(name string) *vfs.File {
		in, err := v.Create(bg, vfs.RootInode, name, 0o644)
		if err != nil {
			t.Fatal(err)
		}
		f, err := v.Open(bg, in.ID)
		if err != nil {
			t.Fatal(err)
		}
		return f
	}
	f1, f2 := mk("a"), mk("b")
	defer f1.Close()
	defer f2.Close()
	defer g.free()
	if _, err := f1.WriteAt(bg, make([]byte, 100_000), 0); err != nil {
		t.Fatal(err)
	}
	cm := newCommitter(v)
	g.armed.Store(true)
	a := make(chan error, 1)
	go func() { a <- cm.commit() }()
	select {
	case <-g.reached:
	case <-time.After(20 * time.Second):
		t.Fatal("the commit never reached the Head")
	}
	wrote := make(chan error, 1)
	go func() { _, err := f2.WriteAt(bg, make([]byte, 77_000), 0); wrote <- err }()
	select {
	case err := <-wrote:
		t.Logf("a write COMPLETED while a commit is stalled at the Head (err=%v): the Volume lock is not held across the commit", err)
	case <-time.After(2 * time.Second):
		t.Log("write blocked behind the commit: the lock covers the property today")
		g.free()
		<-wrote
		<-a
		_ = c
		_ = mem
		return
	}
	b := make(chan error, 1)
	go func() { b <- cm.commit() }()
	time.Sleep(200 * time.Millisecond)
	g.free()
	<-a
	<-b
	ro, err := vfs.Open(bg, vfs.Config{Backend: mem, Cipher: c})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := ro.Resolve(bg, "/b"); err != nil || got.Size != 77_000 {
		t.Fatalf("fsync of /b returned but its write is not in the store: %+v %v", got, err)
	}
}
