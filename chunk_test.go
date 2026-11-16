package vfs

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"testing"
)

// testGear is the chunking table of a Volume whose seed is zero.
var testGear = newGear([32]byte{})

func randBytes(seed uint64, n int) []byte {
	r := rand.New(rand.NewPCG(seed, seed^0x5eed))
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(r.Uint32())
	}
	return b
}

// chunks cuts data the way the Volume does and returns the chunk lengths.
func chunks(data []byte) []int {
	var out []int
	for len(data) > 0 {
		w := data[:min(len(data), maxChunk)]
		n := cut(testGear, w)
		out = append(out, n)
		data = data[n:]
	}
	return out
}

func ids(data []byte) []chunkID {
	var out []chunkID
	for _, n := range chunks(data) {
		out = append(out, sumChunk(data[:n]))
		data = data[n:]
	}
	return out
}

// TestCutBounds: every chunk but the last is between minChunk and maxChunk, and
// the mean sits where normalized chunking puts it.
func TestCutBounds(t *testing.T) {
	data := randBytes(1, sized(24<<20, 96<<20))
	ns := chunks(data)
	total := 0
	for i, n := range ns {
		total += n
		if n > maxChunk {
			t.Fatalf("chunk %d is %d bytes, over maxChunk", i, n)
		}
		if n < minChunk && i != len(ns)-1 {
			t.Fatalf("chunk %d is %d bytes, under minChunk and not last", i, n)
		}
	}
	if total != len(data) {
		t.Fatalf("chunks sum to %d, want %d", total, len(data))
	}
	mean := total / len(ns)
	t.Logf("%d chunks over %d MiB of random data, mean %d KiB", len(ns), total>>20, mean>>10)
	if mean < 256<<10 || mean > 1<<20 {
		t.Fatalf("mean chunk %d KiB is outside 256 KiB to 1 MiB", mean>>10)
	}
}

// TestCutOfShortInput: a file shorter than the minimum is one chunk; an empty
// file is none.
func TestCutOfShortInput(t *testing.T) {
	for _, n := range []int{1, minChunk - 1, minChunk} {
		if got := cut(testGear, randBytes(2, n)); got != n {
			t.Fatalf("cut of %d bytes = %d, want one chunk", n, got)
		}
	}
}

// TestCutDependsOnlyOnItsChunk: the cut of a window is unchanged by whatever
// follows it. This is what lets an edit leave later chunks alone.
func TestCutDependsOnlyOnItsChunk(t *testing.T) {
	data := randBytes(3, 8<<20)
	for off := 0; off < len(data)-maxChunk; {
		n := cut(testGear, data[off:off+maxChunk])
		if n < maxChunk {
			if got := cut(testGear, data[off:off+n+1000]); got != n {
				t.Fatalf("cut changed from %d to %d when 1000 bytes followed", n, got)
			}
		}
		off += n
	}
}

// diff returns how many of a's chunk ids are absent from b.
func diff(a, b []chunkID) int {
	have := map[chunkID]bool{}
	for _, id := range b {
		have[id] = true
	}
	n := 0
	for _, id := range a {
		if !have[id] {
			n++
		}
	}
	return n
}

// TestCutStableUnderEdit: a small edit changes only the chunks it touches. An
// overwrite, an insertion and a deletion in the middle of 24 MiB each change at
// most a few chunks of about a hundred; fixed-size blocks would change every block
// after an insertion.
func TestCutStableUnderEdit(t *testing.T) {
	data := randBytes(4, 24<<20)
	base := ids(data)
	at := 11 << 20

	over := bytes.Clone(data)
	for i := range 100 {
		over[at+i] ^= 0xff
	}
	ins := slicesInsert(data, at, randBytes(5, 100))
	del := append(bytes.Clone(data[:at]), data[at+100:]...)

	for name, edited := range map[string][]byte{"overwrite": over, "insert": ins, "delete": del} {
		changed := diff(ids(edited), base)
		t.Logf("%s of 100 bytes in %d chunks: %d chunks changed", name, len(base), changed)
		if changed > 4 {
			t.Fatalf("%s changed %d chunks, want at most 4", name, changed)
		}
	}
}

func slicesInsert(data []byte, at int, p []byte) []byte {
	out := make([]byte, 0, len(data)+len(p))
	out = append(out, data[:at]...)
	out = append(out, p...)
	return append(out, data[at:]...)
}

// TestGearTableIsFixed pins the derivation: it is part of the on-disk layout, and a change
// would re-cut every file.
func TestGearTableIsFixed(t *testing.T) {
	var b []byte
	for _, g := range testGear {
		b = binary.LittleEndian.AppendUint64(b, g)
	}
	sum := sumChunk(b)
	if got := fmt.Sprintf("%x", sum[:8]); got != "3cb6406096c0f78d" {
		t.Fatalf("gear table digest %s: the chunker changed, and with it every chunk id", got)
	}
}

// TestGearDiffersPerVolume: two seeds give two tables, so the same file is cut at
// different places in two Volumes and chunk lengths do not identify a known file.
func TestGearDiffersPerVolume(t *testing.T) {
	data := randBytes(21, 8<<20)
	other := newGear([32]byte{1})
	a, b := 0, 0
	same := 0
	for off := 0; off < len(data)-maxChunk; {
		n, m := cut(testGear, data[off:off+maxChunk]), cut(other, data[off:off+maxChunk])
		if n == m {
			same++
		}
		a, b = a+1, b+1
		off += n
	}
	if same == a {
		t.Fatalf("two seeds cut %d chunks at the same places", a)
	}
}

// sized returns the full size of a test's data, or the smaller one under -short,
// which is what the race detector run uses.
func sized(short, full int) int {
	if testing.Short() {
		return short
	}
	return full
}
