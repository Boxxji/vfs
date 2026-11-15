package vfs

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/hanzoai/vfs/pkg/backend"
)

// packTarget is the size at which the open pack is uploaded. A pack is sealed
// chunks laid end to end; where each lies is in the chunk table, so a pack has no
// header and needs no index of its own. Packs are what keep the object count
// proportional to bytes divided by 16 MiB rather than to chunks. They stay under
// the single-request limit of the S3 backend so each is one PUT.
const packTarget = 16 << 20

// packID names a pack: 128 random bits, since a pack's records are written
// before its bytes are final. Integrity does not rest on the name: every chunk
// is checked against its own hash when read.
type packID [16]byte

func packKey(id packID) string {
	h := hex.EncodeToString(id[:])
	return "pack/" + h[:2] + "/" + h
}

// pack is the one open pack: chunks sealed since the last upload. Records
// pointing into it are already pending, and reads are served from buf until the
// upload, so an upload that fails loses nothing.
type pack struct {
	id  packID
	buf []byte
}

func (p *pack) empty() bool { return len(p.buf) == 0 }

// add appends a sealed frame and returns where it lies.
func (p *pack) add(frame []byte) (where, error) {
	if p.empty() {
		if _, err := rand.Read(p.id[:]); err != nil {
			return where{}, err
		}
	}
	w := where{pack: p.id, off: uint64(len(p.buf)), n: uint64(len(frame))}
	p.buf = append(p.buf, frame...)
	return w, nil
}

// upload writes the open pack, if any, and starts a new one.
func (p *pack) upload(ctx context.Context, be backend.Backend) error {
	if p.empty() {
		return nil
	}
	if err := be.Put(ctx, packKey(p.id), p.buf); err != nil {
		return fmt.Errorf("vfs: write pack: %w", err)
	}
	*p = pack{}
	return nil
}

// putChunk stores plain as a chunk and returns its id. If the Volume already has
// a chunk with these bytes nothing is written: that is dedupe. v.mu is held.
func (v *Volume) putChunk(ctx context.Context, plain []byte) (chunkID, error) {
	id := sumChunk(plain)
	_, ok, err := v.get(ctx, keyChunk(id))
	if err != nil || ok {
		return id, err
	}
	frame, err := seal(v.c, plain)
	if err != nil {
		return id, err
	}
	w, err := v.pack.add(frame)
	if err != nil {
		return id, err
	}
	v.over.put(keyChunk(id), encodeWhere(w))
	v.chunks.add(id, append([]byte(nil), plain...), int64(len(plain)))
	if len(v.pack.buf) >= packTarget {
		return id, v.pack.upload(ctx, v.be)
	}
	return id, nil
}

// getChunk returns the plaintext of a chunk, checked against its id. v.mu is held.
func (v *Volume) getChunk(ctx context.Context, id chunkID) ([]byte, error) {
	if p, ok := v.chunks.get(id); ok {
		return p, nil
	}
	rec, ok, err := v.get(ctx, keyChunk(id))
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("%w: chunk %x is in no table", ErrCorrupt, id[:6])
	}
	w, err := decodeWhere(rec)
	if err != nil {
		return nil, err
	}
	var frame []byte
	if w.pack == v.pack.id && w.off+w.n <= uint64(len(v.pack.buf)) {
		frame = v.pack.buf[w.off : w.off+w.n]
	} else if frame, err = v.be.GetRange(ctx, packKey(w.pack), int64(w.off), int64(w.n)); err != nil {
		if errors.Is(err, backend.ErrNotFound) {
			return nil, fmt.Errorf("%w: pack %x is missing", ErrCorrupt, w.pack[:6])
		}
		return nil, fmt.Errorf("vfs: read chunk %x: %w", id[:6], err)
	}
	plain, err := unseal(v.c, frame)
	if err != nil {
		return nil, fmt.Errorf("vfs: open chunk %x: %w", id[:6], err)
	}
	if sumChunk(plain) != id {
		return nil, fmt.Errorf("%w: chunk %x does not hash to its id", ErrCorrupt, id[:6])
	}
	v.chunks.add(id, plain, int64(len(plain)))
	return plain, nil
}
