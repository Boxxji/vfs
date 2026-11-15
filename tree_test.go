package vfs

import (
	"bytes"
	"context"
	"fmt"
	"github.com/hanzoai/vfs/pkg/backend"
	"math/rand/v2"
	"sort"
	"testing"

	"github.com/luxfi/age"
	"github.com/zeebo/blake3"

	"github.com/hanzoai/vfs/internal/storetest"
)

// plainCipher seals deterministically, so equal trees have equal bytes and so
// equal refs: the plaintext XOR a 32 byte pad derived from its own hash, and a 16
// byte tag. It is a test double for the Cipher seam, strong enough to pass a
// Volume's check and fast enough for a million records, not a cipher to use.
type plainCipher struct{}

func (plainCipher) tag(ct []byte) []byte {
	h, _ := blake3.NewKeyed(bytes.Repeat([]byte{9}, 32))
	h.Write(ct)
	return h.Sum(nil)[:16]
}

func (c plainCipher) Seal(p []byte) ([]byte, string, error) {
	nonce := blake3.Sum256(p)
	pad := blake3.Sum256(nonce[:16])
	out := make([]byte, 16+len(p), 16+len(p)+16)
	copy(out, nonce[:16])
	for i := range p {
		out[16+i] = p[i] ^ pad[i%32]
	}
	return append(out, c.tag(out[16:])...), "t", nil
}

func (c plainCipher) Open(s []byte, id string) ([]byte, error) {
	if id != "t" || len(s) < 32 || !bytes.Equal(c.tag(s[16:len(s)-16]), s[len(s)-16:]) {
		return nil, ErrCorrupt
	}
	// The pad is the hash of the plaintext, which is what is being recovered: it
	// is stored as the first 16 bytes and the second 16 are derived from them.
	pad := blake3.Sum256(s[:16])
	p := make([]byte, len(s)-32)
	for i := range p {
		p[i] = s[16+i] ^ pad[i%32]
	}
	return p, nil
}

// smallTree returns a tree that cuts nodes of about 300 bytes, so a few thousand
// records stand five or six levels tall.
func smallTree(be *storetest.Mem) *tree {
	t := newTree(&shards{be: be, c: plainCipher{}, aad: []byte("test"), cache: newLRU[ref, *node](1 << 30)})
	t.target, t.max = 300, 3000
	return t
}

type model map[string]string

func (m model) sorted() []entry {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	es := make([]entry, len(keys))
	for i, k := range keys {
		es[i] = entry{[]byte(k), []byte(m[k])}
	}
	return es
}

func dump(t *testing.T, tr *tree, root ref, prefix []byte) []entry {
	t.Helper()
	var out []entry
	if err := tr.scan(context.Background(), root, prefix, func(k, v []byte) bool {
		out = append(out, entry{bytes.Clone(k), bytes.Clone(v)})
		return true
	}); err != nil {
		t.Fatal(err)
	}
	return out
}

func same(a, b []entry) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !bytes.Equal(a[i].key, b[i].key) || !bytes.Equal(a[i].val, b[i].val) {
			return false
		}
	}
	return true
}

// TestTreeMatchesModel applies random batches of puts and deletes and checks,
// after each, that the tree holds exactly the model's records, and that the root is
// the root of a tree built from scratch from those records. The second check is
// the one that matters: it says the incremental path leaves no trace of the
// history that led to the tree.
func TestTreeMatchesModel(t *testing.T) {
	ctx := context.Background()
	for seed := uint64(1); seed <= uint64(sized(3, 6)); seed++ {
		be := storetest.NewMem()
		tr := smallTree(be)
		rng := rand.New(rand.NewPCG(seed, 99))
		m := model{}
		var root ref
		for batch := range 40 {
			pending := map[string]mut{}
			// Batch sizes swing from a single record to a bulk load; keys cluster,
			// so inserts land before the first key, after the last, and in runs.
			n := 1 + rng.IntN(1+batch*batch*3)
			if batch%9 == 8 {
				n = 600
			}
			for range n {
				k := fmt.Sprintf("k%05d", rng.IntN(1500))
				if rng.IntN(100) < 35 {
					pending[k] = mut{key: []byte(k), del: true}
				} else {
					pending[k] = mut{key: []byte(k), val: []byte(fmt.Sprintf("v%d-%d", seed, rng.IntN(1000)))}
				}
			}
			if batch%13 == 12 { // empty it out, then refill
				for k := range m {
					pending[k] = mut{key: []byte(k), del: true}
				}
			}
			muts := make([]mut, 0, len(pending))
			for k, mu := range pending {
				muts = append(muts, mu)
				if mu.del {
					delete(m, k)
				} else {
					m[k] = string(mu.val)
				}
			}
			sort.Slice(muts, func(i, j int) bool { return bytes.Compare(muts[i].key, muts[j].key) < 0 })

			var err error
			if root, err = tr.apply(ctx, root, muts); err != nil {
				t.Fatalf("seed %d batch %d: %v", seed, batch, err)
			}
			if got := dump(t, tr, root, nil); !same(got, m.sorted()) {
				t.Fatalf("seed %d batch %d: tree holds %d records, model %d, or they differ", seed, batch, len(got), len(m))
			}
			want, err := tr.build(ctx, 0, m.sorted())
			if err != nil {
				t.Fatal(err)
			}
			if root != want {
				t.Fatalf("seed %d batch %d: incremental root %x differs from the root built from scratch %x", seed, batch, root[:6], want[:6])
			}
		}
	}
}

// TestTreeGetAndScan checks point reads and prefix scans, including a miss
// between two keys and a prefix that matches nothing.
func TestTreeGetAndScan(t *testing.T) {
	ctx := context.Background()
	tr := smallTree(storetest.NewMem())
	m := model{}
	for i := range 3000 {
		m[fmt.Sprintf("d%02d/f%04d", i%7, i)] = fmt.Sprint(i)
	}
	root, err := tr.build(ctx, 0, m.sorted())
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range m {
		got, ok, err := tr.get(ctx, root, []byte(k))
		if err != nil || !ok || string(got) != v {
			t.Fatalf("get %s = %q %v %v, want %q", k, got, ok, err, v)
		}
	}
	for _, k := range []string{"d00/f0000a", "a", "zzz", "d03"} {
		if _, ok, _ := tr.get(ctx, root, []byte(k)); ok {
			t.Fatalf("get of absent %q found something", k)
		}
	}
	var want []entry
	for _, e := range m.sorted() {
		if bytes.HasPrefix(e.key, []byte("d03/")) {
			want = append(want, e)
		}
	}
	if got := dump(t, tr, root, []byte("d03/")); !same(got, want) || len(want) == 0 {
		t.Fatalf("scan d03/ returned %d records, want %d", len(got), len(want))
	}
	if got := dump(t, tr, root, []byte("none/")); len(got) != 0 {
		t.Fatalf("scan of an absent prefix returned %d records", len(got))
	}
	var n int
	_ = tr.scan(ctx, root, nil, func(_, _ []byte) bool { n++; return n < 10 })
	if n != 10 {
		t.Fatalf("scan did not stop when asked: %d calls", n)
	}
}

// TestTreeWritesOnlyWhatChanged: changing one record of a large tree writes one
// node per level and reads no others; the old tree is untouched.
func TestTreeWritesOnlyWhatChanged(t *testing.T) {
	ctx := context.Background()
	be := storetest.NewMem()
	tr := smallTree(be)
	m := model{}
	for i := range 20000 {
		m[fmt.Sprintf("k%06d", i)] = "value"
	}
	root, err := tr.build(ctx, 0, m.sorted())
	if err != nil {
		t.Fatal(err)
	}
	top, _ := tr.sh.load(ctx, root)
	nodes := be.Count()
	be.Reset()
	// An insertion, a deletion and a rewrite, each alone: each shifts everything
	// after it by a record, which a tree cut by position would rewrite to the end.
	for name, m := range map[string]mut{
		"insert":  {key: []byte("k010000a"), val: []byte("new record")},
		"delete":  {key: []byte("k010000"), del: true},
		"rewrite": {key: []byte("k010000"), val: []byte("a longer value than before")},
	} {
		be.Reset()
		r2, err := tr.apply(ctx, root, []mut{m})
		if err != nil {
			t.Fatal(err)
		}
		wrote := be.Total()
		t.Logf("tree of %d nodes, %d levels: %s wrote %d nodes", nodes, top.level+1, name, wrote)
		if wrote > top.level+3 {
			t.Fatalf("%s changed %d nodes in a tree %d levels tall", name, wrote, top.level+1)
		}
		if v, _, _ := tr.get(ctx, root, []byte("k010000")); string(v) != "value" {
			t.Fatal("the old tree was modified")
		}
		if _, ok, _ := tr.get(ctx, r2, m.key); ok == m.del {
			t.Fatalf("%s: the new tree does not reflect the change", name)
		}
	}
}

// TestTreeDetectsSwappedShard: a shard whose bytes do not hash to its ref is
// refused, so a backend cannot substitute one valid shard for another.
func TestTreeDetectsSwappedShard(t *testing.T) {
	ctx := context.Background()
	be := storetest.NewMem()
	tr := smallTree(be)
	m := model{}
	for i := range 2000 {
		m[fmt.Sprintf("k%05d", i)] = "v"
	}
	root, _ := tr.build(ctx, 0, m.sorted())
	keys := be.Keys()
	a, b := keys[0], keys[1]
	da, _, _ := be.Get(ctx, a, backend.Unbounded)
	_ = be.Put(ctx, b, da) // b's name now holds a's (valid) bytes
	tr.sh.cache = newLRU[ref, *node](1 << 30)
	err := tr.scan(ctx, root, nil, func(_, _ []byte) bool { return true })
	if err == nil {
		t.Fatal("scan succeeded over a swapped shard")
	}
}

func ageGenerate() (*age.HybridIdentity, error) { return age.GenerateHybridIdentity() }
