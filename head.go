package vfs

import (
	"context"
	"fmt"
)

// headKey is the one mutable object of a Volume. Everything else is written
// once under a name that is a hash or a random id, and never changes.
const headKey = "head"

// Head names the committed state of a Volume: the root of its metadata tree, the
// next inode number, and the Volume's identity. It is small and is replaced by
// one conditional write, the commit: the tree it names is exactly what the Volume
// was at that instant, so a crash before the write leaves the previous Head, and
// so the previous tree, intact. The Round that owns the Volume rides in the fence
// header around the sealed Head, where the store's conditional write can enforce it.
type Head struct {
	// Seq numbers commit attempts, strictly increasing. It names a state: no two
	// Heads a Volume writes share one. It can skip, since an attempt whose reply
	// was lost may or may not have landed, and the next attempt takes the next
	// number either way.
	Seq  uint64
	Root ref    // root of the metadata tree
	Next uint64 // next inode number to assign

	// ID names the Volume. Every shard and chunk is sealed to it, so an object
	// copied in from another Volume does not open. Seed draws the Volume's
	// chunking table, which is why chunk lengths are not comparable across Volumes.
	ID   volumeID
	Seed [32]byte
}

type volumeID [16]byte

func (h Head) marshal() []byte {
	var w writer
	w.uvarint(h.Seq)
	w.raw(h.Root[:])
	w.uvarint(h.Next)
	w.raw(h.ID[:])
	w.raw(h.Seed[:])
	return w.b
}

func unmarshalHead(b []byte) (Head, error) {
	r := reader{b: b}
	h := Head{Seq: r.uvarint()}
	copy(h.Root[:], r.raw(32))
	h.Next = r.uvarint()
	copy(h.ID[:], r.raw(16))
	copy(h.Seed[:], r.raw(32))
	return h, r.done()
}

// What each kind of sealed object is sealed to. A Head is sealed to the place it
// lives (the backend's name), so a Volume copied to another place does not open
// there until it is re-sealed; shards and chunks are sealed to the Volume, so they
// move with it, and a chunk is sealed to its own id as well.
func headAAD(place string) []byte { return []byte("vfs head " + place) }

func shardAAD(id volumeID) []byte { return append([]byte("vfs shard "), id[:]...) }

func chunkAAD(id volumeID, c chunkID) []byte {
	return append(append([]byte("vfs chunk "), id[:]...), c[:]...)
}

// readHead opens a fenced Head payload.
func (v *Volume) readHead(payload []byte) (Head, error) {
	plain, err := unseal(v.c, headAAD(v.be.String()), payload)
	if err != nil {
		return Head{}, fmt.Errorf("vfs: open head: %w", err)
	}
	return unmarshalHead(plain)
}

// writeHead seals h and replaces the Head at the Volume's round.
func (v *Volume) writeHead(ctx context.Context, h Head) error {
	sealed, err := seal(v.c, headAAD(v.be.String()), h.marshal())
	if err != nil {
		return err
	}
	return v.fence.Put(ctx, headKey, sealed, v.round)
}
