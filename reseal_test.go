package vfs_test

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/luxfi/age"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/internal/storetest"
	"github.com/hanzoai/vfs/pkg/backend"
)

func resealCopy(t testing.TB, src, dst *storetest.Mem, skip func(string) bool) {
	t.Helper()
	for k, b := range src.Dump() {
		if skip != nil && skip(k) {
			continue
		}
		if err := dst.Put(ctx, k, b); err != nil {
			t.Fatal(err)
		}
	}
}

func resealPopulate(t testing.TB) (*storetest.Mem, vfs.Cipher, *vfs.Volume, map[string][]byte) {
	be, c, v := newMem(t)
	want := map[string][]byte{}
	for i, n := range []int{0, 5, 16 << 10, 16<<10 + 1, 700_000, 5 << 20} {
		name := fmt.Sprintf("f%d", n)
		data := content(uint64(i+1), n)
		write(t, v, vfs.RootInode, name, data)
		want[name] = data
	}
	flush(t, v)
	return be, c, v, want
}

func resealCheck(t testing.TB, v *vfs.Volume, want map[string][]byte) {
	t.Helper()
	for name, data := range want {
		in, err := v.Resolve(ctx, "/"+name)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		mustEqual(t, name, readAll(t, v, in.ID), data)
	}
}

// (a) a Reseal from the wrong place changes nothing; (b) a good one moves the Volume
// byte for byte; (c) the copy that was never resealed stays unopenable; (d) the old
// place keeps working: the move was a copy.
func TestResealWrongFromChangesNothing(t *testing.T) {
	be, c, _, want := resealPopulate(t)
	dst := storetest.NewMem()
	resealCopy(t, be, dst, nil)
	before := dst.Dump()["head"]
	for _, from := range []string{"x", dst.String() + "/", be.String() + "x", "mem://0x0"} {
		err := vfs.Reseal(ctx, cfg(dst, c, 50), from)
		if err == nil {
			t.Fatalf("Reseal from %q succeeded", from)
		}
		if !bytes.Equal(dst.Dump()["head"], before) {
			t.Fatalf("a failed Reseal (from %q) changed the Head: %v", from, err)
		}
	}
	if _, err := vfs.Open(ctx, cfg(dst, c, 0)); err == nil {
		t.Fatal("the copy opened without Reseal")
	}
	if err := vfs.Reseal(ctx, cfg(dst, c, 50), be.String()); err != nil {
		t.Fatal(err)
	}
	resealCheck(t, reopen(t, dst, c), want)
	// the source place is untouched and still a Volume: a copy, not a move
	resealCheck(t, reopen(t, be, c), want)
}

// A Volume copied head first is not resealed until its objects have come: Reseal refuses
// the partial copy and changes nothing, and resealed after the rest arrives the Volume is
// whole.
func TestPartialCopyFailsClosed(t *testing.T) {
	be, c, _, want := resealPopulate(t)
	dst := storetest.NewMem()
	resealCopy(t, be, dst, func(k string) bool { return k != "head" })
	headBefore := dst.Dump()["head"]
	if err := vfs.Reseal(ctx, cfg(dst, c, 9), be.String()); err == nil {
		t.Fatal("a copy that is only a Head was resealed")
	}
	if !bytes.Equal(dst.Dump()["head"], headBefore) {
		t.Fatal("a refused Reseal changed the Head")
	}
	resealCopy(t, be, dst, func(k string) bool { return k == "head" })
	if err := vfs.Reseal(ctx, cfg(dst, c, 10), be.String()); err != nil {
		t.Fatal(err)
	}
	w, err := vfs.Attach(ctx, cfg(dst, c, 11))
	if err != nil {
		t.Fatal(err)
	}
	resealCheck(t, w, want)
}

// Reseal at the live place fences the writer there without telling it: its next commit
// is told it was fenced, nothing it had acknowledged is lost, and nothing it had not
// is claimed to be.
func TestResealOfALiveVolume(t *testing.T) {
	be, c, w, want := resealPopulate(t)
	write(t, w, vfs.RootInode, "unflushed", []byte("lost with the fence"))
	if err := vfs.Reseal(ctx, cfg(be, c, 20), be.String()); err != nil {
		t.Fatal(err)
	}
	err := w.Flush(ctx)
	if !errors.Is(err, vfs.ErrStaleRound) {
		t.Fatalf("the fenced writer's flush = %v, want ErrStaleRound", err)
	}
	r := reopen(t, be, c)
	resealCheck(t, r, want)
	if _, err := r.Resolve(ctx, "/unflushed"); err == nil {
		t.Fatal("an unflushed file survived a fence")
	}
}

// Reseal racing a writer that commits in a loop: every commit the writer was told landed
// is in the final tree; the Head ends sealed for the place; the writer ends fenced.
func TestResealRacesAWriter(t *testing.T) {
	for trial := 0; trial < 25; trial++ {
		be, c, w, _ := resealPopulate(t)
		var acked []string
		var mu sync.Mutex
		var stop atomic.Bool
		var wg sync.WaitGroup
		wg.Add(1)
		var last error
		go func() {
			defer wg.Done()
			for i := 0; !stop.Load(); i++ {
				name := fmt.Sprintf("w%d", i)
				if _, err := w.Create(ctx, vfs.RootInode, name, 0o644); err != nil {
					last = err
					return
				}
				if err := w.Flush(ctx); err != nil {
					last = err
					return
				}
				mu.Lock()
				acked = append(acked, name)
				mu.Unlock()
			}
		}()
		time.Sleep(time.Duration(trial%5+1) * 2 * time.Millisecond)
		// A writer that commits without pause can win the race for the Head eight times:
		// Reseal says so (backend.ErrConflict) and is retried at a higher Round.
		var rerr error
		for round := uint64(30); round < 130; round++ {
			if rerr = vfs.Reseal(ctx, cfg(be, c, round), be.String()); rerr == nil || !errors.Is(rerr, backend.ErrConflict) {
				break
			}
		}
		if rerr != nil {
			t.Fatalf("trial %d: %v", trial, rerr)
		}
		time.Sleep(5 * time.Millisecond)
		stop.Store(true)
		wg.Wait()
		if !errors.Is(last, vfs.ErrStaleRound) && last != nil {
			t.Errorf("trial %d: the writer ended with %v, want ErrStaleRound or a clean stop", trial, last)
		}
		r := reopen(t, be, c)
		mu.Lock()
		for _, name := range acked {
			if _, err := r.Resolve(ctx, "/"+name); err != nil {
				t.Errorf("trial %d: acknowledged commit %s lost: %v", trial, name, err)
			}
		}
		n := len(acked)
		mu.Unlock()
		if trial == 0 {
			t.Logf("trial 0: %d acknowledged commits before the fence, writer ended: %v", n, last)
		}
	}
}

// A crash at the one write of a Reseal: the Head is either the old one or the new one,
// whole, and a second Reseal at a higher Round finishes the move.
func TestResealCrashAtTheWrite(t *testing.T) {
	for _, after := range []bool{false, true} {
		be, c, _, want := resealPopulate(t)
		dst := storetest.NewMem()
		resealCopy(t, be, dst, nil)
		h := &storetest.Hook{Backend: dst}
		boom := errors.New("crash")
		fire := func(op, key string) error {
			if op == "putver" && key == "head" {
				return boom
			}
			return nil
		}
		if after {
			h.After = fire
		} else {
			h.Before = fire
		}
		err := vfs.Reseal(ctx, cfg(h, c, 40), be.String())
		if err == nil {
			t.Fatalf("after=%v: Reseal did not report the failure", after)
		}
		h.After, h.Before = nil, nil
		// whichever state the store is in, a retry at a higher round from the old place
		// succeeds: a Head already sealed to the new place is recognised, not refused
		if err := vfs.Reseal(ctx, cfg(h, c, 41), be.String()); err != nil {
			t.Fatalf("after=%v: the retry from the old place: %v", after, err)
		}
		resealCheck(t, reopen(t, dst, c), want)
	}
}

// The old place is not remembered: Reseal with none opens the Head with the key alone,
// and still refuses what it can tell is wrong.
func TestResealWithoutThePlace(t *testing.T) {
	be, c, _, want := resealPopulate(t)
	dst := storetest.NewMem()
	resealCopy(t, be, dst, nil)
	if err := vfs.Reseal(ctx, cfg(dst, c, 50), ""); err != nil {
		t.Fatal(err)
	}
	resealCheck(t, reopen(t, dst, c), want)

	// a copy that is missing the root shard of its Head is refused, and nothing changes
	half := storetest.NewMem()
	resealCopy(t, be, half, func(k string) bool { return strings.HasPrefix(k, "meta/") })
	before := half.Dump()["head"]
	if err := vfs.Reseal(ctx, cfg(half, c, 50), ""); err == nil {
		t.Error("a copy without its shards was resealed")
	}
	if !bytes.Equal(half.Dump()["head"], before) {
		t.Error("a refused Reseal changed the Head")
	}

	// a Head of another Volume, sealed with the same key, copied over the head: its root
	// shard is not here
	other, c2, _, _ := resealPopulate(t)
	_ = c2
	mixed := storetest.NewMem()
	resealCopy(t, be, mixed, nil)
	if err := mixed.Put(ctx, "head", other.Dump()["head"]); err != nil {
		t.Fatal(err)
	}
	if err := vfs.Reseal(ctx, cfg(mixed, c, 50), ""); err == nil {
		t.Error("a Head of another Volume was resealed over this one's objects")
	}

	// a key that did not seal it opens nothing
	k, _ := age.GenerateHybridIdentity()
	if err := vfs.Reseal(ctx, cfg(dst, vfs.Hybrid(k), 60), ""); err == nil {
		t.Error("Reseal with a foreign key succeeded")
	}
}

// A Reseal can be repeated: at a higher Round it finds the Head already sealed to the
// place and claims the Volume, whatever it is told the old place was.
func TestResealIsRepeatable(t *testing.T) {
	be, c, _, want := resealPopulate(t)
	dst := storetest.NewMem()
	resealCopy(t, be, dst, nil)
	for i, from := range []string{be.String(), be.String(), dst.String(), "", "mem://unrelated"} {
		round := uint64(50 + i)
		if err := vfs.Reseal(ctx, cfg(dst, c, round), from); err != nil {
			t.Fatalf("round %d from %q: %v", round, from, err)
		}
		resealCheck(t, reopen(t, dst, c), want)
	}
	if err := vfs.Reseal(ctx, cfg(dst, c, 54), be.String()); !errors.Is(err, vfs.ErrStaleRound) {
		t.Errorf("a repeat at a spent Round = %v, want ErrStaleRound", err)
	}
}

// A key that is not the Volume's: nothing changes.
func TestResealWrongKeyChangesNothing(t *testing.T) {
	be, _, _, _ := resealPopulate(t)
	dst := storetest.NewMem()
	resealCopy(t, be, dst, nil)
	before := dst.Dump()["head"]
	k, _ := age.GenerateHybridIdentity()
	for _, from := range []string{"", be.String()} {
		if err := vfs.Reseal(ctx, cfg(dst, vfs.Hybrid(k), 10), from); err == nil {
			t.Fatalf("Reseal with another key succeeded (from %q)", from)
		}
		if !bytes.Equal(dst.Dump()["head"], before) {
			t.Fatalf("a refused Reseal changed the Head (from %q)", from)
		}
	}
}

// The copy is checked whichever way the Head was opened. Without --from a copy that lacks
// its shards was refused and with it was resealed and left a Head that opens to nothing.
func TestResealChecksTheCopy(t *testing.T) {
	be, c, _, want := resealPopulate(t)
	for _, from := range []string{"", be.String()} {
		dst := storetest.NewMem()
		resealCopy(t, be, dst, func(k string) bool { return strings.HasPrefix(k, "meta/") })
		before := dst.Dump()["head"]
		if err := vfs.Reseal(ctx, cfg(dst, c, 10), from); err == nil {
			t.Errorf("from %q: a copy with no shard at all was resealed", from)
		}
		if !bytes.Equal(dst.Dump()["head"], before) {
			t.Errorf("from %q: a refused Reseal changed the Head", from)
		}
	}
	// A shard the Head reaches is found by the full check, and what it lets through is
	// readable: a shard it does not reach is an older commit's, and the copy is whole
	// without it.
	var caught int
	for k := range be.Dump() {
		if !strings.HasPrefix(k, "meta/") {
			continue
		}
		dst := storetest.NewMem()
		resealCopy(t, be, dst, func(o string) bool { return o == k })
		full := cfg(dst, c, 11)
		full.Verify = true
		if err := vfs.Reseal(ctx, full, be.String()); err != nil {
			caught++
			continue
		}
		resealCheck(t, reopen(t, dst, c), want)
	}
	if caught == 0 {
		t.Error("the full check found no missing shard of a Volume that has a tree to reach")
	}
	// A pack that is missing, or shorter than a chunk in it, is refused by Verify.
	for _, how := range []string{"missing", "short"} {
		dst := storetest.NewMem()
		resealCopy(t, be, dst, nil)
		for k, b := range dst.Dump() {
			if strings.HasPrefix(k, "pack/") {
				if how == "missing" {
					_ = dst.Delete(ctx, k)
				} else if err := dst.Put(ctx, k, b[:len(b)/2]); err != nil {
					t.Fatal(err)
				}
			}
		}
		if err := vfs.Reseal(ctx, cfg(dst, c, 10), be.String()); err != nil {
			t.Errorf("a copy with a %s pack is refused without Verify: %v (chunks are checked when a file is read)", how, err)
		}
		dst2 := storetest.NewMem()
		resealCopy(t, be, dst2, nil)
		for k, b := range dst2.Dump() {
			if strings.HasPrefix(k, "pack/") {
				if how == "missing" {
					_ = dst2.Delete(ctx, k)
				} else if err := dst2.Put(ctx, k, b[:len(b)/2]); err != nil {
					t.Fatal(err)
				}
			}
		}
		full := cfg(dst2, c, 10)
		full.Verify = true
		if err := vfs.Reseal(ctx, full, be.String()); err == nil {
			t.Errorf("Verify resealed a copy with a %s pack", how)
		}
	}
	// A whole copy passes Verify and reads back.
	dst := storetest.NewMem()
	resealCopy(t, be, dst, nil)
	full := cfg(dst, c, 10)
	full.Verify = true
	if err := vfs.Reseal(ctx, full, ""); err != nil {
		t.Fatal(err)
	}
	resealCheck(t, reopen(t, dst, c), want)
}
