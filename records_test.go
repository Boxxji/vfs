package vfs

import (
	"context"
	"fmt"
	"testing"

	"github.com/hanzoai/vfs/internal/storetest"
)

func internalVolume(t *testing.T, conf Config) (*storetest.Mem, *Volume) {
	t.Helper()
	be := storetest.NewMem()
	conf.Backend, conf.Cipher = be, plainCipher{}
	if err := Create(context.Background(), conf); err != nil {
		t.Fatal(err)
	}
	conf.Round = 1
	v, err := Attach(context.Background(), conf)
	if err != nil {
		t.Fatal(err)
	}
	return be, v
}

// records counts the data records of a file: its inline bytes (0 or 1) and its
// chunk references.
func records(t *testing.T, v *Volume, ino InodeID) (inline, extents int) {
	t.Helper()
	v.mu.RLock()
	defer v.mu.RUnlock()
	recs, err := v.scan(context.Background(), key(tData, uint64(ino)))
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range recs {
		if len(r.key) == prefixLen {
			inline++
		} else {
			extents++
		}
	}
	return inline, extents
}

func put(t *testing.T, v *Volume, name string, data []byte) InodeID {
	t.Helper()
	ctx := context.Background()
	in, err := v.Create(ctx, RootInode, name, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	f, err := v.Open(ctx, in.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.WriteAt(ctx, data, 0); err != nil {
		t.Fatal(err)
	}
	return in.ID
}

// TestRecordsFollowTheFile: a file's data records are exactly its current form.
// Shrinking, truncating to nothing, growing back into chunks and removing the file
// leave no record of the form it had.
func TestRecordsFollowTheFile(t *testing.T) {
	ctx := context.Background()
	_, v := internalVolume(t, Config{})
	flush := func() {
		t.Helper()
		if err := v.Flush(ctx); err != nil {
			t.Fatal(err)
		}
	}
	ino := put(t, v, "f", randBytes(1, 3<<20))
	flush()
	if in, ex := records(t, v, ino); in != 0 || ex < 3 {
		t.Fatalf("a 3 MiB file has %d inline and %d chunk records", in, ex)
	}
	if err := v.Truncate(ctx, ino, 1000); err != nil { // chunked -> inline
		t.Fatal(err)
	}
	flush()
	if in, ex := records(t, v, ino); in != 1 || ex != 0 {
		t.Fatalf("a 1000 byte file has %d inline and %d chunk records, want 1 and 0", in, ex)
	}
	f, err := v.Open(ctx, ino) // inline -> chunked, by writing several chunks' worth
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt(ctx, randBytes(2, 3<<20), 0); err != nil {
		t.Fatal(err)
	}
	f.Close()
	flush()
	if in, ex := records(t, v, ino); in != 0 || ex < 3 {
		t.Fatalf("after writing 3 MiB the file has %d inline and %d chunk records", in, ex)
	}
	if err := v.Truncate(ctx, ino, 1<<20+7); err != nil { // chunked -> a shorter chunked
		t.Fatal(err)
	}
	flush()
	covered(t, v, ino, 1<<20+7)
	if err := v.Truncate(ctx, ino, 0); err != nil {
		t.Fatal(err)
	}
	flush()
	if in, ex := records(t, v, ino); in+ex != 0 {
		t.Fatalf("an empty file has %d inline and %d chunk records", in, ex)
	}
	if err := v.Truncate(ctx, ino, 1<<20); err != nil {
		t.Fatal(err)
	}
	flush()
	if err := v.Remove(ctx, RootInode, "f"); err != nil {
		t.Fatal(err)
	}
	flush()
	if in, ex := records(t, v, ino); in+ex != 0 {
		t.Fatalf("a removed file left %d inline and %d chunk records", in, ex)
	}
}

// TestSpillThenFlush: with room for only a few pending changes the Volume writes
// shards as it goes, and a Flush that finds nothing pending still commits them.
func TestSpillThenFlush(t *testing.T) {
	ctx := context.Background()
	be, v := internalVolume(t, Config{Overlay: 40})
	for i := range 300 {
		if _, err := v.Create(ctx, RootInode, fmt.Sprintf("f%03d", i), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if v.over.n >= 40 {
		t.Fatalf("%d changes pending, the limit is 40", v.over.n)
	}
	if got := be.Puts()["meta"]; got == 0 {
		t.Fatal("nothing was spilled to shards")
	}
	if err := v.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	ro, err := Open(ctx, Config{Backend: be, Cipher: plainCipher{}})
	if err != nil {
		t.Fatal(err)
	}
	ents, err := ro.ReadDir(ctx, RootInode)
	if err != nil || len(ents) != 300 {
		t.Fatalf("a fresh Open sees %d of 300 files, %v", len(ents), err)
	}
	// A second Flush with nothing at all pending is the idle case, and writes nothing.
	be.Reset()
	if err := v.Flush(ctx); err != nil || be.Total() != 0 {
		t.Fatalf("idle Flush: %v, %d objects", err, be.Total())
	}
}

// TestSpilledChangesAreInvisibleUntilCommit: shards written by a spill are not
// part of the Volume until a Flush: a read-only view of the store sees the last
// commit only.
func TestSpilledChangesAreInvisibleUntilCommit(t *testing.T) {
	ctx := context.Background()
	be, v := internalVolume(t, Config{Overlay: 10})
	for i := range 100 {
		if _, err := v.Create(ctx, RootInode, fmt.Sprintf("f%03d", i), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	ro, err := Open(ctx, Config{Backend: be, Cipher: plainCipher{}})
	if err != nil {
		t.Fatal(err)
	}
	if ents, _ := ro.ReadDir(ctx, RootInode); len(ents) != 0 {
		t.Fatalf("an uncommitted spill is visible: %d files", len(ents))
	}
}

// covered checks the file's chunk references are contiguous from 0 and end at size:
// nothing missing, nothing left over from a longer file.
func covered(t *testing.T, v *Volume, ino InodeID, size uint64) {
	t.Helper()
	v.mu.RLock()
	defer v.mu.RUnlock()
	recs, err := v.scan(context.Background(), key(tData, uint64(ino)))
	if err != nil {
		t.Fatal(err)
	}
	var end uint64
	for _, r := range recs {
		e, err := decodeExtent(offsetOf(r.key), r.val)
		if err != nil {
			t.Fatal(err)
		}
		if e.off != end {
			t.Fatalf("chunk at %d, want %d: the list has a gap or a stale chunk", e.off, end)
		}
		end = e.end()
	}
	if end != size {
		t.Fatalf("chunks end at %d, the file is %d bytes", end, size)
	}
}

// TestRefusesOtherFormat: a Head of another format version is an error, not a
// guess.
func TestRefusesOtherFormat(t *testing.T) {
	for _, version := range []uint64{1, 3} {
		var w writer
		w.uvarint(version)
		w.uvarint(0)
		w.raw(make([]byte, 32))
		w.uvarint(2)
		if _, err := unmarshalHead(w.b); err == nil {
			t.Fatalf("a Head of format %d was accepted", version)
		}
	}
	h := Head{Seq: 9, Next: 77}
	h.Root[3] = 5
	got, err := unmarshalHead(h.marshal())
	if err != nil || got != h {
		t.Fatalf("Head round trip: %+v, %v", got, err)
	}
}

// extentLens returns the lengths of a file's chunks as stored.
func extentLens(t *testing.T, v *Volume, ino InodeID) []int {
	t.Helper()
	v.mu.RLock()
	defer v.mu.RUnlock()
	recs, err := v.scan(context.Background(), key(tData, uint64(ino)))
	if err != nil {
		t.Fatal(err)
	}
	var out []int
	for _, r := range recs {
		e, err := decodeExtent(offsetOf(r.key), r.val)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, int(e.n))
	}
	return out
}

// TestChunkListIsTheWholeFileCut: whatever sequence of writes, appends and
// truncations built a file, its stored chunk list is exactly what cutting the
// file's final bytes from scratch gives. That is what makes a chunk list a function
// of content, so an edit's neighbours dedupe against the file's past, and what
// recutting only around the edit must preserve.
func TestChunkListIsTheWholeFileCut(t *testing.T) {
	ctx := context.Background()
	_, v := internalVolume(t, Config{})
	size := sized(3<<20, 5<<20)
	model := randBytes(7, size)
	ino := put(t, v, "f", model)
	f, err := v.Open(ctx, ino)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	check := func(what string) {
		t.Helper()
		if err := v.Flush(ctx); err != nil {
			t.Fatal(err)
		}
		got, want := extentLens(t, v, ino), chunks(model)
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("after %s the stored chunks are %v, a cut of the file gives %v", what, got, want)
		}
	}
	check("the first write")

	write := func(off int, b []byte) {
		t.Helper()
		if _, err := f.WriteAt(ctx, b, int64(off)); err != nil {
			t.Fatal(err)
		}
		if off+len(b) > len(model) {
			model = append(model, make([]byte, off+len(b)-len(model))...)
		}
		copy(model[off:], b)
	}
	write(size/2, randBytes(8, 100))
	check("a 100 byte overwrite")
	write(len(model), randBytes(9, 300_000))
	check("an append")
	write(len(model), randBytes(10, 17))
	check("a tiny append")
	write(len(model)+500_000, randBytes(11, 1000)) // past the end: a hole of zeros
	check("a write past the end")
	write(10, randBytes(12, 2<<20))
	check("a large overwrite near the start")
	if err := v.Truncate(ctx, ino, uint64(len(model)-123_456)); err != nil {
		t.Fatal(err)
	}
	model = model[:len(model)-123_456]
	check("a truncation")
	write(len(model)-5, randBytes(13, 900_000))
	check("a write over the end")
	if err := v.Truncate(ctx, ino, uint64(len(model)+700_000)); err != nil {
		t.Fatal(err)
	}
	model = append(model, make([]byte, 700_000)...)
	check("an extension")
	for i := range 6 { // several edits between commits
		write(i*400_000+1234, randBytes(uint64(20+i), 50_000))
	}
	check("six scattered writes")
}
