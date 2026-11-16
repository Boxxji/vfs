//go:build fuse && linux

package mount

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/luxfi/age"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/internal/storetest"
)

// SPEC 11.2: SIGTERM ends a mount with a stalled store. stop gives up on the commit after commitGrace
// and Mount calls finish; the Volume's lock is held by the stalled commit and a flush, or a Stats, would
// queue behind it until the store answers. finish therefore ends after finishGrace and says that the last
// commit did not finish, so the process exits non-zero and does not wait for the store.
func TestFinishDoesNotWaitForAStalledCommitForever(t *testing.T) {
	k, _ := age.GenerateHybridIdentity()
	c := vfs.Hybrid(k)
	gate := make(chan struct{})
	h := &storetest.Hook{Backend: storetest.NewMem()}
	bg := context.Background()
	if err := vfs.Create(bg, vfs.Config{Backend: h, Cipher: c}); err != nil {
		t.Fatal(err)
	}
	v, err := vfs.Attach(bg, vfs.Config{Backend: h, Cipher: c, Round: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := v.Create(bg, vfs.RootInode, "x", 0o644); err != nil {
		t.Fatal(err)
	}
	h.Before = func(op, key string) error { <-gate; return nil } // the store has stopped answering
	go func() { _ = v.Flush(bg) }()                              // the commit stop() started and gave up on
	time.Sleep(300 * time.Millisecond)
	done := make(chan error, 1)
	go func() { done <- finish(v, nil) }()
	select {
	case err := <-done:
		if err == nil {
			t.Error("finish said nothing of a commit that never finished: the process would exit 0 with writes lost")
		}
		if err2 := finishWith(v, errors.New("serve failed")); err2 == nil || err2.Error() != "serve failed" {
			t.Errorf("finish after a serve error = %v, want that error first", err2)
		}
	case <-time.After(5 * time.Second):
		close(gate)
		t.Fatal("finish is still waiting 5 s after the commit stalled: Mount does not return on SIGTERM while the store is stopped")
	}
	close(gate)
}

// finishWith is finish, called while the commit is still stalled.
func finishWith(v *vfs.Volume, serveErr error) error { return finish(v, serveErr) }
