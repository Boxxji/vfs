package vfs_test

import "testing"

// HIP-1342 section 8: a clean Flush makes no store request; a read-only handle's close must
// not publish another handle's writes. On a mount every close(2) is a Flush.
func TestCleanFlushMakesNoStoreRequest(t *testing.T) {
	be, _, v := newMem(t)
	write(t, v, 1, "a", []byte("x"))
	flush(t, v)
	be.Reset()
	for i := 0; i < 5; i++ {
		flush(t, v)
	}
	if n := be.Total(); n != 0 {
		t.Errorf("5 clean Flushes made %d store writes (%v)", n, be.Puts())
	}
}
