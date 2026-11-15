package vfs_test

import (
	"errors"
	"sync/atomic"
	"testing"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/internal/storetest"
)

// TestAttachAfterALostClaimNeedsAHigherRound: the claim lands and its reply is lost as
// a plain transport error. Attach returns that error, and a caller that tries again
// must take a higher Round: at the same Round it is told ErrStaleRound, since the
// Round is by then recorded. (Within one Attach call a claim that landed is
// recognised; across calls nothing identifies the caller, so the documented rule is
// a higher Round.)
func TestAttachAfterALostClaimNeedsAHigherRound(t *testing.T) {
	mem := storetest.NewMem()
	var once atomic.Bool
	hook := &storetest.Hook{Backend: mem, After: func(op, key string) error {
		if key == "head" && op == "putver" && once.CompareAndSwap(false, true) {
			return errors.New("connection reset by peer")
		}
		return nil
	}}
	c := vfsAge(t)
	if err := vfs.Create(ctx, cfg(hook, c, 0)); err != nil {
		t.Fatal(err)
	}
	_, err := vfs.Attach(ctx, cfg(hook, c, 5))
	if err == nil {
		t.Skip("the claim was retried inside Attach")
	}
	if _, err := vfs.Attach(ctx, cfg(hook, c, 5)); !errors.Is(err, vfs.ErrStaleRound) {
		t.Errorf("a retry at the Round of a claim that landed = %v, want ErrStaleRound", err)
	}
	v, err := vfs.Attach(ctx, cfg(hook, c, 6))
	if err != nil {
		t.Fatalf("round 6 after a lost claim: %v", err)
	}
	if _, err := v.Create(ctx, vfs.RootInode, "f", 0o644); err != nil {
		t.Fatal(err)
	}
	if err := v.Flush(ctx); err != nil {
		t.Fatal(err)
	}
}
