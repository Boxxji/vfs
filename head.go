package vfs

import (
	"context"
	"fmt"
)

// headKey is the one mutable object of a Volume. Everything else is written
// once under a name that is a hash or a random id, and never changes.
const headKey = "head"

// formatVersion is written into every Head; a reader refuses any other.
const formatVersion = 2

// Head names the committed state of a Volume: the root of its metadata tree and
// the next inode number. It is small and is replaced by one conditional write,
// the commit: the tree it names is exactly what the Volume was at that instant,
// so a crash before the write leaves the previous Head, and so the previous
// tree, intact. The Round that owns the Volume rides in the fence header around
// the sealed Head, where the store's conditional write can enforce it.
type Head struct {
	Seq  uint64 // commits since Create
	Root ref    // root of the metadata tree
	Next uint64 // next inode number to assign
}

func (h Head) marshal() []byte {
	var w writer
	w.uvarint(formatVersion)
	w.uvarint(h.Seq)
	w.raw(h.Root[:])
	w.uvarint(h.Next)
	return w.b
}

func unmarshalHead(b []byte) (Head, error) {
	r := reader{b: b}
	if v := r.uvarint(); r.err == nil && v != formatVersion {
		return Head{}, fmt.Errorf("vfs: volume format %d, this library reads %d", v, formatVersion)
	}
	h := Head{Seq: r.uvarint()}
	copy(h.Root[:], r.raw(32))
	h.Next = r.uvarint()
	return h, r.done()
}

// readHead opens a fenced Head payload.
func (v *Volume) readHead(payload []byte) (Head, error) {
	plain, err := unseal(v.c, payload)
	if err != nil {
		return Head{}, fmt.Errorf("vfs: open head: %w", err)
	}
	return unmarshalHead(plain)
}

// writeHead seals h and replaces the Head at the Volume's round.
func (v *Volume) writeHead(ctx context.Context, h Head) error {
	sealed, err := seal(v.c, h.marshal())
	if err != nil {
		return err
	}
	return v.fence.Put(ctx, headKey, sealed, v.round)
}
