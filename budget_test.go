package vfs

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"slices"
	"testing"

	"github.com/hanzoai/vfs/pkg/backend"
)

// TestObjectBudget150GiB checks the cost of a 100,000 file, 150 GiB tree without
// writing 150 GiB. The metadata is exact: every record the Volume would write
// (inodes, entries, inline bytes, chunk references, the chunk table) is built
// into a real tree on a counting store. The data is modeled: the chunker's mean
// chunk size is measured here on random bytes, and the packs follow from the byte
// count and the per-chunk sealing overhead measured on a real sealed chunk. The
// total must stay at or under the ~40,000 objects a 4 MiB-per-object layout
// would cost.
func TestObjectBudget150GiB(t *testing.T) {
	if testing.Short() {
		t.Skip("scale test")
	}
	// Measured here: the chunker's mean chunk size.
	sample := randBytes(11, 48<<20)
	ns := chunks(sample)
	meanChunk := float64(len(sample)) / float64(len(ns))
	// Measured with the age cipher in TestAgeFrameOverhead: bytes a sealed chunk
	// adds to its plaintext, at a typical 600 KB chunk.
	const ageOverhead = 1760 + 600_000/4096*1

	type class struct {
		files int
		size  int
	}
	rest := func(classes []class, n int) []class {
		var used float64
		for _, k := range classes {
			used += float64(k.files) * float64(k.size)
		}
		return append(classes, class{n, int((150<<30 - used) / float64(n))})
	}
	for _, dist := range []struct {
		name    string
		classes []class
	}{
		// Source-like: mostly small files, a few huge ones.
		{"mixed", rest([]class{{70000, 4 << 10}, {25000, 100 << 10}, {4900, 2 << 20}}, 100)},
		// The worst shape for the object count: every small file as large as an
		// inline file can be, so the most bytes go into metadata shards.
		{"all-inline", rest([]class{{99000, small}}, 1000)},
	} {
		t.Run(dist.name, func(t *testing.T) {
			be := &discard{}
			tr := newTree(&shards{be: be, c: plainCipher{}, aad: bind("test"), cache: newLRU[ref, *node](64 << 20)}, [32]byte{1})
			var ino, nChunks, files int
			var bytesTotal float64
			var pending []entry
			zeros := make([]byte, small)
			put := func(k, v []byte) { pending = append(pending, entry{k, v}) }
			nextPack := 0
			for _, k := range dist.classes {
				for range k.files {
					ino++
					files++
					bytesTotal += float64(k.size)
					put(keyInode(InodeID(ino)), encodeInode(Inode{Mode: 0o644, Size: uint64(k.size)}))
					put(keyEntry(InodeID(1+ino%1000), fmt.Sprintf("file%06d", ino)), encodeEntry(InodeID(ino), 0))
					if k.size <= small {
						put(keyInline(InodeID(ino)), zeros[:k.size])
						continue
					}
					n := max(1, int(math.Ceil(float64(k.size)/meanChunk)))
					for j := range n {
						id := sumChunk(fmt.Appendf(nil, "chunk %d %d", ino, j))
						nChunks++
						off := uint64(j) * uint64(meanChunk)
						size := min(uint64(meanChunk), uint64(k.size)-off)
						put(keyExtent(InodeID(ino), off), encodeExtent(extent{off: off, n: uint32(size), id: id}))
						put(keyChunk(id), encodeWhere(where{pack: packID{byte(nextPack), byte(nextPack >> 8)}, off: off, n: size + ageOverhead}))
						nextPack = (nextPack + 1) % 10000
					}
				}
			}
			sortEntries(pending)
			if _, err := tr.build(context.Background(), 0, pending); err != nil {
				t.Fatal(err)
			}
			sealed := bytesTotal + float64(nChunks)*ageOverhead
			packs := int(math.Ceil(sealed / packTarget))
			total := be.puts + packs + 1
			t.Logf("%d files, %.1f GiB: %d chunks (mean %.0f KiB), %d records -> %d metadata shards (%d MiB), %d packs, 1 head = %d objects",
				files, bytesTotal/(1<<30), nChunks, meanChunk/1024, len(pending), be.puts, be.bytes>>20, packs, total)
			if files != 100000 {
				t.Fatalf("model has %d files", files)
			}
			if total > 40000 {
				t.Fatalf("%d objects, over the 40,000 budget", total)
			}
		})
	}
}

// discard counts the shards written to it and keeps none.
type discard struct {
	backend.Backend
	puts  int
	bytes int64
}

func (d *discard) Put(_ context.Context, _ string, b []byte) error {
	d.puts++
	d.bytes += int64(len(b))
	return nil
}

func sortEntries(es []entry) {
	slices.SortFunc(es, func(a, b entry) int { return bytes.Compare(a.key, b.key) })
}

// TestAgeFrameOverhead pins the sealing overhead the budget model uses: what the
// default cipher adds to a chunk is a fixed header for the hybrid recipient plus a
// 16 byte tag per 64 KiB.
func TestAgeFrameOverhead(t *testing.T) {
	k, err := ageGenerate()
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range []int{minChunk, 1 << 20, maxChunk} {
		frame, err := seal(Hybrid(k), bind("test"), make([]byte, n))
		if err != nil {
			t.Fatal(err)
		}
		over := len(frame) - n              // the frame less the chunk: the context is sealed with it
		want := 1724 + 16*((n+65535)/65536) // the header, the context (32) and the tag (32), and the age tag per 64 KiB
		t.Logf("a %d byte chunk seals to %d bytes: +%d (model %d)", n, len(frame), over, want)
		if over < want-40 || over > want+40 {
			t.Fatalf("overhead %d bytes, model says %d: update ageOverhead in TestObjectBudget150GiB", over, want)
		}
	}
}
