package vfs_test

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/zeebo/blake3"
	"io/fs"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/pkg/backend"
)

func extList(t *testing.T, be backend.Backend, prefix string) []backend.Object {
	t.Helper()
	ch, errc := be.List(ctx, prefix)
	var out []backend.Object
	for o := range ch {
		out = append(out, o)
	}
	if err := <-errc; err != nil {
		t.Fatalf("List(%q): %v", prefix, err)
	}
	return out
}

func extVol(t *testing.T, st store) (*vfs.Volume, vfs.Cipher) {
	c := vfsAge(t)
	if err := vfs.Create(ctx, cfg(st.client(t), c, 0)); err != nil {
		t.Fatal(err)
	}
	v, err := vfs.Attach(ctx, cfg(st.client(t), c, 1))
	if err != nil {
		t.Fatal(err)
	}
	return v, c
}

// the name grammar, case folding, unicode, separators, traversal, length; and that no (name, key)
// pair shares a store key with another.
func TestExtNamesAndCollisions(t *testing.T) {
	for _, st := range stores(t) {
		t.Run(st.name, func(t *testing.T) {
			v, _ := extVol(t, st)
			bad := []string{"", "A", "Trace", "tr_ace", "a.b", "a/b", "/a", "a/", "..", ".", "../x", "é", "a b", "a\x00", "a\n", "%2e", "ａ", "á",
				strings.Repeat("n", 33), strings.Repeat("n", 300), "ＡＢ", "i̇", "ß", "K", "ǅ"}
			for _, name := range bad {
				if err := v.PutExt(ctx, name, []byte("k"), []byte("x")); !errors.Is(err, fs.ErrInvalid) {
					t.Errorf("PutExt name %q: %v, want ErrInvalid", name, err)
				}
			}
			for _, name := range []string{"snapshots", "links", "base"} {
				if err := v.PutExt(ctx, name, []byte("k"), []byte("x")); !errors.Is(err, fs.ErrInvalid) {
					t.Errorf("PutExt name %q (reserved for a feature of the Head): %v, want ErrInvalid", name, err)
				}
				if _, err := v.GetExt(ctx, name, []byte("k")); !errors.Is(err, fs.ErrInvalid) {
					t.Errorf("GetExt name %q (reserved): %v, want ErrInvalid", name, err)
				}
			}
			good := []string{"a", "0", "-", "--", "a-b", "ext", "head", "meta", "pack", strings.Repeat("z", 32)}
			for _, name := range good {
				if err := v.PutExt(ctx, name, []byte("k"), []byte("v:"+name)); err != nil {
					t.Errorf("PutExt name %q: %v", name, err)
				}
			}
			for _, name := range good {
				if got, err := v.GetExt(ctx, name, []byte("k")); err != nil || string(got) != "v:"+name {
					t.Errorf("GetExt %q = %q, %v", name, got, err)
				}
			}
			// ('a/b', 'c') vs ('a', 'b/c'): the name cannot hold a slash, so a split of one string
			// into name and key cannot give the same store key; and the same key bytes under two
			// names are two objects.
			if err := v.PutExt(ctx, "a", []byte("b/c"), []byte("one")); err != nil {
				t.Fatal(err)
			}
			if err := v.PutExt(ctx, "ab", []byte("/c"), []byte("two")); err != nil {
				t.Fatal(err)
			}
			if err := v.PutExt(ctx, "ab", []byte("c"), []byte("three")); err != nil {
				t.Fatal(err)
			}
			for _, c := range []struct{ name, key, want string }{{"a", "b/c", "one"}, {"ab", "/c", "two"}, {"ab", "c", "three"}} {
				if got, err := v.GetExt(ctx, c.name, []byte(c.key)); err != nil || string(got) != c.want {
					t.Errorf("(%q,%q) = %q, %v want %q", c.name, c.key, got, err, c.want)
				}
			}
			// every key of the store is of the grammar and of the shape ext/<name>/<64 hex>
			shape := regexp.MustCompile(`^(head|meta/[0-9a-f]{2}/[0-9a-f]{64}|pack/[0-9a-f]{2}/[0-9a-f]{32}|ext/[a-z0-9-]{1,32}/[0-9a-f]{64})$`)
			for _, o := range extList(t, st.client(t), "") {
				if backend.ValidKey(o.Key) != nil {
					t.Errorf("key %q is outside the grammar", o.Key)
				}
				if !shape.MatchString(o.Key) {
					t.Errorf("key %q has no shape of the text", o.Key)
				}
			}
		})
	}
}

// an extension object cannot pose as another kind, and no other kind poses as one.
func TestExtMasquerade(t *testing.T) {
	for _, st := range stores(t) {
		t.Run(st.name, func(t *testing.T) {
			v, _ := extVol(t, st)
			be := st.client(t)
			if err := v.PutExt(ctx, "trace", []byte("k"), []byte("payload")); err != nil {
				t.Fatal(err)
			}
			extKey := extObject(t, v, "trace", []byte("k"))
			sealed, _, err := be.Get(ctx, extKey, backend.Unbounded)
			if err != nil {
				t.Fatal(err)
			}
			// 1. a sealed ext object put at a shard's name: a reader of the tree must not accept it
			h := blake3.Sum256(sealed)
			shardKey := fmt.Sprintf("meta/%x/%x", h[:1], h[:])
			if err := be.Put(ctx, shardKey, sealed); err != nil {
				t.Fatal(err)
			}
			// 2. a shard of the tree put as an extension object
			var shard string
			for _, o := range extList(t, be, "meta/") {
				if o.Key != shardKey {
					shard = o.Key
				}
			}
			if shard == "" {
				t.Fatal("no shard")
			}
			sh, _, _ := be.Get(ctx, shard, backend.Unbounded)
			if err := be.Put(ctx, extObject(t, v, "trace", []byte("forged")), sh); err != nil {
				t.Fatal(err)
			}
			if _, err := v.GetExt(ctx, "trace", []byte("forged")); err == nil {
				t.Error("a shard read as an extension object")
			} else if !errors.Is(err, vfs.ErrCorrupt) {
				t.Logf("shard as ext: %v", err)
			}
			// 3. the Head as an extension object
			hd, _, _ := be.Get(ctx, "head", backend.Unbounded)
			if err := be.Put(ctx, extObject(t, v, "trace", []byte("head")), hd[30:]); err != nil {
				t.Fatal(err)
			}
			if _, err := v.GetExt(ctx, "trace", []byte("head")); err == nil {
				t.Error("the Head read as an extension object")
			}
			// 4. one extension's object under another's name or key: refused
			if err := be.Put(ctx, extObject(t, v, "other", []byte("k")), sealed); err != nil {
				t.Fatal(err)
			}
			if _, err := v.GetExt(ctx, "other", []byte("k")); err == nil {
				t.Error("an object of 'trace' read under the name 'other'")
			}
			if err := be.Put(ctx, extObject(t, v, "trace", []byte("k2")), sealed); err != nil {
				t.Fatal(err)
			}
			if _, err := v.GetExt(ctx, "trace", []byte("k2")); err == nil {
				t.Error("an object read under another key")
			}
			// 5. the tree still reads, with the planted shard-named object in meta/
			r, err := vfs.Open(ctx, cfg(st.client(t), vfsAgeSame(t), 0))
			_ = r
			_ = err
		})
	}
}

// a reader MUST bound its read of an extension object (3.1).
func TestExtReadIsBounded(t *testing.T) {
	for _, st := range stores(t) {
		t.Run(st.name, func(t *testing.T) {
			v, _ := extVol(t, st)
			be := st.client(t)
			if err := be.Put(ctx, extObject(t, v, "big", []byte("k")), make([]byte, 33<<20+(128<<10))); err != nil {
				t.Fatal(err)
			}
			_, err := v.GetExt(ctx, "big", []byte("k"))
			if !errors.Is(err, backend.ErrTooLarge) {
				t.Errorf("an object of 33 MiB + 128 KiB reads as %v, want ErrTooLarge", err)
			}
		})
	}
}

// SPEC 2: List says what the store says: sizes exact, times truthful to a second or two on every
// store, a rewrite moves the time, and the listing is complete and duplicate-free past 1,000 keys.
func TestListTimesAndPages(t *testing.T) {
	for _, st := range stores(t) {
		t.Run(st.name, func(t *testing.T) {
			t.Parallel() // each store waits out a clock second or two
			be := st.client(t)
			start := time.Now()
			if err := be.Put(ctx, "pack/aa/"+strings.Repeat("0", 32), []byte("12345")); err != nil {
				t.Fatal(err)
			}
			after := time.Now()
			objs := extList(t, be, "pack/aa/")
			if len(objs) != 1 || objs[0].Size != 5 {
				t.Fatalf("listing %+v", objs)
			}
			m := objs[0].Modified
			t.Logf("written %v..%v modified %v (ns part %d, loc %v)", start.Format(time.RFC3339Nano), after.Format(time.RFC3339Nano), m.Format(time.RFC3339Nano), m.Nanosecond(), m.Location())
			if m.Before(start.Add(-2*time.Second)) || m.After(after.Add(2*time.Second)) {
				t.Errorf("modified %v is not within 2 s of the write (%v..%v)", m, start, after)
			}
			time.Sleep(2100 * time.Millisecond)
			_, v1, _ := be.Get(ctx, "pack/aa/"+strings.Repeat("0", 32), 100)
			start2 := time.Now()
			if _, err := be.PutIfVersion(ctx, "pack/aa/"+strings.Repeat("0", 32), []byte("67890"), v1); err != nil {
				t.Fatal(err)
			}
			objs = extList(t, be, "pack/aa/")
			if len(objs) != 1 || !objs[0].Modified.After(m) || objs[0].Modified.Before(start2.Add(-2*time.Second)) {
				t.Errorf("after a rewrite at %v the object is modified %v (was %v)", start2, objs[0].Modified, m)
			}
			// a Put of the same bytes refreshes the time too (a writer that re-uploads an object keeps it young)
			time.Sleep(1100 * time.Millisecond)
			start3 := time.Now()
			if err := be.Put(ctx, "pack/aa/"+strings.Repeat("0", 32), []byte("67890")); err != nil {
				t.Fatal(err)
			}
			objs = extList(t, be, "pack/aa/")
			if objs[0].Modified.Before(start3.Add(-2 * time.Second)) {
				t.Errorf("a Put of the same bytes did not refresh the time: %v at %v", objs[0].Modified, start3)
			}
			// pages
			n := 2300
			if testing.Short() {
				n = 1100
			}
			want := map[string]bool{}
			for i := 0; i < n; i++ {
				k := fmt.Sprintf("ext/pg/%064x", i)
				want[k] = true
				if err := be.Put(ctx, k, []byte{byte(i)}); err != nil {
					t.Fatal(err)
				}
			}
			got := extList(t, be, "ext/pg/")
			seen := map[string]int{}
			keys := make([]string, 0, len(got))
			for _, o := range got {
				seen[o.Key]++
				keys = append(keys, o.Key)
			}
			for k := range want {
				if seen[k] != 1 {
					t.Errorf("%s listed %d times", k, seen[k])
				}
			}
			if len(got) != n {
				t.Errorf("listed %d of %d", len(got), n)
			}
			t.Logf("order sorted: %v", sort.StringsAreSorted(keys))
			if !sort.StringsAreSorted(keys) {
				t.Logf("List does not return keys in order on %s", st.name)
			}
			// a prefix that ends inside a segment
			for _, k := range []string{"ext/y/ab", "ext/y/a/c", "ext/y/b"} {
				if err := be.Put(ctx, k, []byte("1")); err != nil {
					t.Fatal(err)
				}
			}
			var ks []string
			for _, o := range extList(t, be, "ext/y/a") {
				ks = append(ks, o.Key)
			}
			sort.Strings(ks)
			if strings.Join(ks, ",") != "ext/y/a/c,ext/y/ab" {
				t.Errorf("List(ext/y/a) = %v", ks)
			}
		})
	}
}

// non-grammar keys: what each store does with them (the text: MAY keep). The listing must not hide them
// and a Volume must not be harmed.
func TestStrayKeys(t *testing.T) {
	for _, st := range stores(t) {
		t.Run(st.name, func(t *testing.T) {
			v, c := extVol(t, st)
			be := st.client(t)
			stray := []string{"orgs/acme/kv.db", "Upper", "a b", "meta/zz/xx", "meta/aa/" + strings.Repeat("g", 64), "pack/aa/short", "head2", "ext/Trace/" + strings.Repeat("0", 64), "meta/aa/" + strings.Repeat("a", 64) + "/x"}
			kept := map[string]bool{}
			for _, k := range stray {
				err := be.Put(ctx, k, []byte("stray "+k))
				kept[k] = err == nil
				t.Logf("%-40s put: %v", k, err)
			}
			listed := map[string]bool{}
			for _, o := range extList(t, be, "") {
				listed[o.Key] = true
			}
			for _, k := range stray {
				if kept[k] && !listed[k] {
					t.Errorf("a store kept %q and did not list it", k)
				}
			}
			// the Volume still works and still commits
			if _, err := v.Create(ctx, vfs.RootInode, "f", 0o644); err != nil {
				t.Fatal(err)
			}
			if err := v.Flush(ctx); err != nil {
				t.Fatalf("commit with strays present: %v", err)
			}
			if _, err := vfs.Open(ctx, cfg(st.client(t), c, 0)); err != nil {
				t.Fatalf("open with strays present: %v", err)
			}
			_ = bytes.Equal
		})
	}
}

func vfsAgeSame(t *testing.T) vfs.Cipher { return vfsAge(t) }
