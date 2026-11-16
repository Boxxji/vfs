package vfs_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/internal/storetest"
)

// TestEveryObjectIsHybridPQ: every stored object (head, shards, packs) must
// open with an age header whose only recipient stanza is the hybrid
// X25519 + ML-KEM-768 one: no classical-only stanza anywhere.
func TestEveryObjectIsHybridPQ(t *testing.T) {
	mem := storetest.NewMem()
	c := vfsAge(t)
	if err := vfs.Create(ctx, cfg(mem, c, 0)); err != nil {
		t.Fatal(err)
	}
	v, _ := vfs.Attach(ctx, cfg(mem, c, 1))
	write(t, v, vfs.RootInode, "a", content(1, 400<<10))
	write(t, v, vfs.RootInode, "b", []byte("inline"))
	flush(t, v)
	n := 0
	for k, b := range mem.Dump() {
		i := bytes.Index(b, []byte("age-encryption.org/v1\n"))
		if i < 0 || i > 300 {
			t.Fatalf("%s: no age header near the start (head carries a fence header and key id)", k)
		}
		hdr := b[i:]
		stanzas := 0
		for _, line := range strings.Split(string(hdr[:min(len(hdr), 4000)]), "\n") {
			if strings.HasPrefix(line, "-> ") {
				stanzas++
				if !strings.HasPrefix(line, "-> mlkem768x25519") {
					t.Fatalf("%s: stanza %q is not hybrid", k, strings.Fields(line)[1])
				}
			}
			if strings.HasPrefix(line, "--- ") {
				break
			}
		}
		if stanzas != 1 {
			t.Fatalf("%s: %d recipient stanzas, want exactly 1 hybrid", k, stanzas)
		}
		n++
	}
	t.Logf("%d objects, each with one mlkem768x25519 stanza", n)
}
