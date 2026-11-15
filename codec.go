package vfs

import (
	"encoding/binary"
	"errors"
)

// ErrCorrupt is returned when an object fails to authenticate, hash to its name,
// or decode: it was damaged, swapped or forged. A Volume never guesses past it.
var ErrCorrupt = errors.New("vfs: object is corrupt")

// writer appends the varint-framed encoding every record and node uses.
type writer struct{ b []byte }

func (w *writer) uvarint(x uint64) { w.b = binary.AppendUvarint(w.b, x) }
func (w *writer) varint(x int64)   { w.b = binary.AppendVarint(w.b, x) }
func (w *writer) raw(p []byte)     { w.b = append(w.b, p...) }
func (w *writer) bytes(p []byte)   { w.uvarint(uint64(len(p))); w.raw(p) }

// reader decodes what writer wrote. The first malformed read latches err and
// every later read returns zero values, so a decoder checks once at the end.
type reader struct {
	b   []byte
	err error
}

func (r *reader) fail() { r.err = ErrCorrupt; r.b = nil }

func (r *reader) uvarint() uint64 {
	x, n := binary.Uvarint(r.b)
	if n <= 0 {
		r.fail()
		return 0
	}
	r.b = r.b[n:]
	return x
}

func (r *reader) varint() int64 {
	x, n := binary.Varint(r.b)
	if n <= 0 {
		r.fail()
		return 0
	}
	r.b = r.b[n:]
	return x
}

// raw returns the next n bytes without copying.
func (r *reader) raw(n uint64) []byte {
	if n > uint64(len(r.b)) {
		r.fail()
		return nil
	}
	p := r.b[:n:n]
	r.b = r.b[n:]
	return p
}

func (r *reader) bytes() []byte { return r.raw(r.uvarint()) }

// done reports the latched error, or ErrCorrupt if bytes remain unread.
func (r *reader) done() error {
	if r.err == nil && len(r.b) != 0 {
		r.fail()
	}
	return r.err
}
