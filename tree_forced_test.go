package vfs

import (
	"bytes"
	"context"
	"fmt"
	"math/rand/v2"
	"sort"
	"strings"
	"testing"

	"github.com/hanzoai/vfs/internal/storetest"
)

// TestTreeForcedCuts: the shipped tree test uses 5-byte values, so a node
// never reaches the forced-cut size. Real records are up to 16 KiB (inline file
// bytes) against a 512 KiB forced cut. This drives the tree with values of every
// size up to the forced-cut limit, small and large nodes, and checks (1) the
// records are exact after every batch (data correctness) and (2) whether the
// incremental root equals the from-scratch root (the 'pure function of its
// records' claim).
func TestTreeForcedCuts(t *testing.T) {
	ctx := context.Background()
	type params struct {
		target uint64
		max    int
		vmax   int
	}
	for _, p := range []params{{300, 3000, 2500}, {64, 256, 200}, {2000, 4000, 3900}, {1 << 12, 1 << 14, 1 << 13}} {
		var lastStore *storetest.Mem
		rootMismatch, batches := 0, 0
		for seed := uint64(1); seed <= uint64(sized(2, 10)); seed++ {
			be := storetest.NewMem()
			lastStore = be
			tr := smallTree(be)
			tr.target, tr.max = p.target, p.max
			rng := rand.New(rand.NewPCG(seed, 7))
			m := model{}
			var root ref
			for batch := range 30 {
				pending := map[string]mut{}
				n := 1 + rng.IntN(1+batch*batch*2)
				for range n {
					k := fmt.Sprintf("k%05d", rng.IntN(900))
					if rng.IntN(100) < 30 {
						pending[k] = mut{key: []byte(k), del: true}
					} else {
						pending[k] = mut{key: []byte(k), val: bytes.Repeat([]byte{byte(rng.IntN(250))}, 1+rng.IntN(p.vmax))}
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
					t.Fatalf("%v seed %d batch %d: %v", p, seed, batch, err)
				}
				if got := dump(t, tr, root, nil); !same(got, m.sorted()) {
					t.Fatalf("%v seed %d batch %d: tree records differ from the model (%d vs %d)", p, seed, batch, len(got), len(m))
				}
				for k, v := range m {
					g, ok, err := tr.get(ctx, root, []byte(k))
					if err != nil || !ok || string(g) != v {
						t.Fatalf("%v seed %d batch %d: get %s wrong", p, seed, batch, k)
					}
				}
				want, err := tr.build(ctx, 0, m.sorted())
				if err != nil {
					t.Fatal(err)
				}
				batches++
				if root != want {
					rootMismatch++
				}
			}
		}
		// No node is larger than a forced cut allows: the limit plus the one entry that
		// crossed it.
		for k, frame := range lastStore.Dump() {
			if !strings.HasPrefix(k, "meta/") {
				continue
			}
			plain, err := unseal(plainCipher{}, bind("test"), frame)
			if err != nil {
				t.Fatal(err)
			}
			if limit := p.max + p.vmax + 64 + 16; len(plain) > limit {
				t.Errorf("%+v: a node of %d bytes, past the forced cut %d", p, len(plain), limit)
			}
		}
		t.Logf("%+v: %d batches, incremental root != scratch root in %d", p, batches, rootMismatch)
		if rootMismatch > 0 {
			t.Errorf("%+v: the incremental tree differs from the tree built from the same records in %d of %d batches", p, rootMismatch, batches)
		}
	}
}
