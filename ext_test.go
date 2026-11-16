package vfs_test

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"strings"
	"sync"
	"testing"

	"github.com/zeebo/blake3"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/pkg/backend"
)

func extObject(name string, key []byte) string {
	sum := blake3.Sum256(key)
	return fmt.Sprintf("ext/%s/%x", name, sum[:])
}

// SPEC 3.1: extension objects work the same on every store: created, replaced whole, read
// back, refused when absent, with names and sizes bounded.
func TestExtensionObjectsOnEveryStore(t *testing.T) {
	for _, st := range stores(t) {
		t.Run(st.name, func(t *testing.T) {
			c := vfsAge(t)
			if err := vfs.Create(ctx, cfg(st.client(t), c, 0)); err != nil {
				t.Fatal(err)
			}
			v, err := vfs.Attach(ctx, cfg(st.client(t), c, 1))
			if err != nil {
				t.Fatal(err)
			}
			keys := [][]byte{nil, []byte("workflow-1"), []byte("../../etc/passwd"), []byte("a b%2e\x00\xff"), []byte("é"), bytes.Repeat([]byte("k"), 1<<20)}
			for i, key := range keys {
				if _, err := v.GetExt(ctx, "trace", key); !errors.Is(err, fs.ErrNotExist) {
					t.Fatalf("key %d absent: %v", i, err)
				}
				first := []byte(fmt.Sprintf("first %d", i))
				if err := v.PutExt(ctx, "trace", key, first); err != nil {
					t.Fatalf("key %d: %v", i, err)
				}
				if got, err := v.GetExt(ctx, "trace", key); err != nil || !bytes.Equal(got, first) {
					t.Fatalf("key %d reads %q, %v", i, got, err)
				}
				second := bytes.Repeat([]byte{byte(i)}, 100_000)
				if err := v.PutExt(ctx, "trace", key, second); err != nil {
					t.Fatalf("replace %d: %v", i, err)
				}
				if got, err := v.GetExt(ctx, "trace", key); err != nil || !bytes.Equal(got, second) {
					t.Fatalf("key %d after a replacement reads %d bytes, %v", i, len(got), err)
				}
				// the object is where the text says, and nowhere else
				if _, _, err := st.client(t).Get(ctx, extObject("trace", key), backend.Unbounded); err != nil {
					t.Fatalf("key %d is not at %s: %v", i, extObject("trace", key), err)
				}
			}
			// another extension, and an empty object, are other objects
			if err := v.PutExt(ctx, "other-1", keys[1], nil); err != nil {
				t.Fatal(err)
			}
			if got, err := v.GetExt(ctx, "other-1", keys[1]); err != nil || len(got) != 0 {
				t.Fatalf("an empty object reads %q, %v", got, err)
			}
			if got, _ := v.GetExt(ctx, "trace", keys[1]); len(got) == 0 {
				t.Fatal("two extensions share a key")
			}
			// names
			for _, name := range []string{"", "Trace", "tr_ace", "a.b", "a/b", strings.Repeat("n", 33)} {
				if err := v.PutExt(ctx, name, keys[1], []byte("x")); !errors.Is(err, fs.ErrInvalid) {
					t.Errorf("PutExt name %q = %v, want ErrInvalid", name, err)
				}
				if _, err := v.GetExt(ctx, name, keys[1]); !errors.Is(err, fs.ErrInvalid) {
					t.Errorf("GetExt name %q = %v, want ErrInvalid", name, err)
				}
			}
			if err := v.PutExt(ctx, strings.Repeat("n", 32), keys[1], []byte("x")); err != nil {
				t.Errorf("a name of 32 bytes: %v", err)
			}
			if err := v.PutExt(ctx, "big", keys[1], make([]byte, 32<<20+1)); !errors.Is(err, backend.ErrTooLarge) {
				t.Errorf("an object over 32 MiB = %v, want ErrTooLarge", err)
			}
			if err := v.PutExt(ctx, "big", keys[1], make([]byte, 32<<20)); err != nil {
				t.Errorf("an object of 32 MiB: %v", err)
			}
			if got, err := v.GetExt(ctx, "big", keys[1]); err != nil || len(got) != 32<<20 {
				t.Errorf("an object of 32 MiB reads %d bytes, %v", len(got), err)
			}
		})
	}
}

// A Volume that was only opened reads extension objects and cannot write them, and a writer
// that was fenced out still can: the Round orders the tree, not these.
func TestExtensionObjectsAndTheFence(t *testing.T) {
	be, c, a := newMem(t)
	if err := a.PutExt(ctx, "hint", []byte("k"), []byte("from a")); err != nil {
		t.Fatal(err)
	}
	ro := reopen(t, be, c)
	if got, err := ro.GetExt(ctx, "hint", []byte("k")); err != nil || string(got) != "from a" {
		t.Fatalf("a read-only Volume reads %q, %v", got, err)
	}
	if err := ro.PutExt(ctx, "hint", []byte("k"), []byte("x")); !errors.Is(err, vfs.ErrReadOnly) {
		t.Fatalf("a read-only Volume wrote: %v", err)
	}
	b, err := vfs.Attach(ctx, cfg(be, c, 2))
	if err != nil {
		t.Fatal(err)
	}
	write(t, a, vfs.RootInode, "late", []byte("x"))
	if err := a.Flush(ctx); !errors.Is(err, vfs.ErrStaleRound) {
		t.Fatalf("the deposed writer's flush = %v", err)
	}
	if err := a.PutExt(ctx, "hint", []byte("k"), []byte("from the fenced a")); err != nil {
		t.Fatalf("a fenced-out writer cannot write an extension object: %v", err)
	}
	if got, err := b.GetExt(ctx, "hint", []byte("k")); err != nil || string(got) != "from the fenced a" {
		t.Fatalf("the new owner reads %q, %v", got, err)
	}
}

// Writers that replace one object at once leave one whole object: the value of one of them.
func TestExtensionObjectsRaced(t *testing.T) {
	for _, st := range stores(t) {
		t.Run(st.name, func(t *testing.T) {
			c := vfsAge(t)
			if err := vfs.Create(ctx, cfg(st.client(t), c, 0)); err != nil {
				t.Fatal(err)
			}
			v, err := vfs.Attach(ctx, cfg(st.client(t), c, 1))
			if err != nil {
				t.Fatal(err)
			}
			var wg sync.WaitGroup
			errs := make(chan error, 6)
			for i := range 6 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for j := range 5 {
						if err := v.PutExt(ctx, "race", []byte("k"), bytes.Repeat([]byte{byte(i)}, 1000+j)); err != nil && !errors.Is(err, backend.ErrConflict) {
							errs <- err
						}
					}
				}()
			}
			wg.Wait()
			close(errs)
			for err := range errs {
				t.Fatal(err)
			}
			got, err := v.GetExt(ctx, "race", []byte("k"))
			if err != nil || len(got) < 1000 || len(got) > 1004 || !bytes.Equal(got, bytes.Repeat(got[:1], len(got))) {
				t.Fatalf("after racing writers the object is %d bytes, %v", len(got), err)
			}
		})
	}
}

// SPEC 3.1: a key the Volume does not know is ignored by every reader and left as it is by
// every writer: what a Volume does (commit, read, reseal) never deletes or changes it.
func TestStrayKeysAreIgnoredAndKept(t *testing.T) {
	for _, st := range stores(t) {
		t.Run(st.name, func(t *testing.T) {
			c := vfsAge(t)
			be := st.client(t)
			if err := vfs.Create(ctx, cfg(be, c, 0)); err != nil {
				t.Fatal(err)
			}
			stray := map[string]string{
				"ext/unknown-ext/" + strings.Repeat("0", 64): "a later extension",
				"ext/y/ab":     "stray",
				"ext/y/a/c":    "stray, deeper",
				"zz/stray":     "another top level",
				"meta/00/free": "an object under meta/ of no shape this build knows",
				"pack/ff/zz":   "an object under pack/ of no shape this build knows",
			}
			for k, v := range stray {
				if err := be.Put(ctx, k, []byte(v)); err != nil {
					t.Fatal(err)
				}
			}
			v, err := vfs.Attach(ctx, cfg(be, c, 1))
			if err != nil {
				t.Fatalf("a Volume with stray keys does not attach: %v", err)
			}
			write(t, v, vfs.RootInode, "f", bytes.Repeat([]byte("data"), 100_000))
			flush(t, v)
			write(t, v, vfs.RootInode, "g", []byte("small"))
			flush(t, v)
			if err := v.PutExt(ctx, "kept", []byte("k"), []byte("mine")); err != nil {
				t.Fatal(err)
			}
			r := reopen(t, be, c)
			in, err := r.Resolve(ctx, "/f")
			if err != nil || in.Size != 400_000 {
				t.Fatalf("a Volume with stray keys reads %+v, %v", in, err)
			}
			if err := vfs.Reseal(ctx, cfg(be, c, 9), be.String()); err != nil {
				t.Fatalf("Reseal with stray keys: %v", err)
			}
			for k, want := range stray {
				if got, _, err := be.Get(ctx, k, backend.Unbounded); err != nil || string(got) != want {
					t.Errorf("stray key %s is %q, %v after the Volume worked", k, got, err)
				}
			}
		})
	}
}
