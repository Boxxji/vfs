package vfs

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"slices"
	"sort"
	"time"
)

// inlineMax is the largest file stored whole in a metadata record. Larger files
// are chunked. A file's form is a function of its size alone, so a given
// content has exactly one representation.
const inlineMax = 16 << 10

// fileState is one file's content as the Volume holds it: the settled bytes
// (inline, or a list of chunks) plus the writes made since, which are chunked,
// sealed and uploaded together at the next settle. All handles to a file share
// one fileState, so every handle sees every write at once.
type fileState struct {
	ino  InodeID
	refs int  // open handles
	gone bool // removed; handles fail

	// The settled content: inline bytes, or chunks sorted by offset and
	// covering [0, base) without gaps.
	inline []byte
	ext    []extent
	base   uint64

	// Writes since. size is the file's length now. limit is how much of the
	// settled content still counts: truncating below it voids the rest, so a
	// later extension reads zeros, not the old bytes. Bytes in [limit, size)
	// that no write covers are zeros.
	size    uint64
	limit   uint64
	mtime   time.Time
	changed bool
	dirty   []span // sorted, disjoint, never touching
	dirtyN  int64
}

// span is a run of bytes written at an offset.
type span struct {
	off  uint64
	data []byte
}

func (s span) end() uint64 { return s.off + uint64(len(s.data)) }

// File is an open file. Its methods are safe for concurrent use.
type File struct {
	v *Volume
	f *fileState
}

// Open opens a regular file for reading and writing. Writes are visible to every
// handle at once and durable at the next Flush.
func (v *Volume) Open(ctx context.Context, ino InodeID) (*File, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	f, err := v.state(ctx, ino)
	if err != nil {
		return nil, err
	}
	f.refs++
	return &File{v: v, f: f}, nil
}

// state returns the file's state, loading it from the tree on first use.
func (v *Volume) state(ctx context.Context, ino InodeID) (*fileState, error) {
	if f := v.files[ino]; f != nil {
		return f, nil
	}
	in, err := v.stat(ctx, ino)
	if err != nil {
		return nil, err
	}
	if in.IsDir() {
		return nil, fmt.Errorf("%w: inode %d", ErrIsDir, ino)
	}
	recs, err := v.scan(ctx, key(tData, uint64(ino)))
	if err != nil {
		return nil, err
	}
	f := &fileState{ino: ino, base: in.Size, size: in.Size, limit: in.Size, mtime: in.Mtime}
	for _, r := range recs {
		if len(r.key) == prefixLen {
			f.inline = r.val
			continue
		}
		e, err := decodeExtent(offsetOf(r.key), r.val)
		if err != nil {
			return nil, err
		}
		f.ext = append(f.ext, e)
	}
	v.files[ino] = f
	return f, nil
}

// Stat returns the file's attributes, including writes not yet flushed.
func (h *File) Stat(ctx context.Context) (Inode, error) {
	h.v.mu.RLock()
	defer h.v.mu.RUnlock()
	if h.f.gone {
		return Inode{}, fs.ErrNotExist
	}
	return h.v.stat(ctx, h.f.ino)
}

// ReadAt reads up to len(p) bytes at off. Like io.ReaderAt it returns io.EOF
// with the bytes it has when the file ends inside the range.
func (h *File) ReadAt(ctx context.Context, p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, fmt.Errorf("%w: negative offset %d", fs.ErrInvalid, off)
	}
	h.v.mu.RLock()
	defer h.v.mu.RUnlock()
	if h.f.gone {
		return 0, fs.ErrNotExist
	}
	return h.v.read(ctx, h.f, p, uint64(off))
}

// read fills p from the file at off: settled content below limit, zeros above
// it, then the unsettled writes over both. v.mu is held.
func (v *Volume) read(ctx context.Context, f *fileState, p []byte, off uint64) (int, error) {
	if off >= f.size {
		return 0, io.EOF
	}
	n := min(uint64(len(p)), f.size-off)
	end := off + n
	if lim := min(f.limit, end); off < lim {
		if err := v.readBase(ctx, f, p[:lim-off], off); err != nil {
			return 0, err
		}
		clear(p[lim-off : n])
	} else {
		clear(p[:n])
	}
	for i := sort.Search(len(f.dirty), func(i int) bool { return f.dirty[i].end() > off }); i < len(f.dirty) && f.dirty[i].off < end; i++ {
		s := f.dirty[i]
		lo, hi := max(s.off, off), min(s.end(), end)
		copy(p[lo-off:hi-off], s.data[lo-s.off:hi-s.off])
	}
	if n < uint64(len(p)) {
		return int(n), io.EOF
	}
	return int(n), nil
}

// readBase copies settled bytes [off, off+len(p)) into p.
func (v *Volume) readBase(ctx context.Context, f *fileState, p []byte, off uint64) error {
	if f.ext == nil {
		copy(p, f.inline[off:])
		return nil
	}
	i := sort.Search(len(f.ext), func(i int) bool { return f.ext[i].end() > off })
	for len(p) > 0 {
		if i == len(f.ext) {
			return fmt.Errorf("%w: file %d has no chunk at %d", ErrCorrupt, f.ino, off)
		}
		e := f.ext[i]
		plain, err := v.getChunk(ctx, e.id)
		if err != nil {
			return err
		}
		n := copy(p, plain[off-e.off:])
		p, off, i = p[n:], off+uint64(n), i+1
	}
	return nil
}

// WriteAt writes p at off, extending the file if needed. The bytes are held in
// memory until a settle chunks and uploads them; they are durable after Flush.
func (h *File) WriteAt(ctx context.Context, p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, fmt.Errorf("%w: negative offset %d", fs.ErrInvalid, off)
	}
	if len(p) == 0 {
		return 0, nil
	}
	v := h.v
	v.mu.Lock()
	defer v.mu.Unlock()
	if err := v.writable(); err != nil {
		return 0, err
	}
	if h.f.gone {
		return 0, fs.ErrNotExist
	}
	f := h.f
	v.write(f, p, uint64(off))
	f.size = max(f.size, uint64(off)+uint64(len(p)))
	f.mtime, f.changed = time.Now(), true
	return len(p), v.relieve(ctx)
}

// write adds p at off to the file's unsettled writes, merging with the writes it
// touches so the list stays sorted, disjoint and gap-separated.
func (v *Volume) write(f *fileState, p []byte, off uint64) {
	end := off + uint64(len(p))
	d := f.dirty
	i := sort.Search(len(d), func(i int) bool { return d[i].end() >= off })
	j := i
	for j < len(d) && d[j].off <= end {
		j++
	}
	var merged span
	switch {
	case i == j:
		merged = span{off, append([]byte(nil), p...)}
	case j == i+1 && off >= d[i].off && end <= d[i].end():
		copy(d[i].data[off-d[i].off:], p) // inside one write: overwrite in place
		return
	case j == i+1 && off == d[i].end():
		merged = span{d[i].off, append(d[i].data, p...)} // append, amortized
	default:
		lo, hi := min(off, d[i].off), max(end, d[j-1].end())
		buf := make([]byte, hi-lo)
		for _, s := range d[i:j] {
			copy(buf[s.off-lo:], s.data)
		}
		copy(buf[off-lo:], p)
		merged = span{lo, buf}
	}
	grown := int64(len(merged.data))
	for _, s := range d[i:j] {
		grown -= int64(len(s.data))
	}
	f.dirty = slices.Replace(d, i, j, merged)
	f.dirtyN += grown
	v.dirty += grown
}

// Truncate sets the file's length. Shrinking discards the bytes past it for
// good; growing adds zeros.
func (h *File) Truncate(ctx context.Context, size uint64) error {
	h.v.mu.Lock()
	defer h.v.mu.Unlock()
	if err := h.v.writable(); err != nil {
		return err
	}
	if h.f.gone {
		return fs.ErrNotExist
	}
	h.v.truncate(h.f, size)
	return h.v.relieve(ctx)
}

// Truncate sets the length of the file with inode ino, as File.Truncate does.
func (v *Volume) Truncate(ctx context.Context, ino InodeID, size uint64) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if err := v.writable(); err != nil {
		return err
	}
	f, err := v.state(ctx, ino)
	if err != nil {
		return err
	}
	v.truncate(f, size)
	return v.relieve(ctx)
}

func (v *Volume) truncate(f *fileState, size uint64) {
	if size < f.size {
		f.limit = min(f.limit, size)
		var kept []span
		var n int64
		for _, s := range f.dirty {
			if s.off >= size {
				break
			}
			if s.end() > size {
				s.data = s.data[:size-s.off]
			}
			kept = append(kept, s)
			n += int64(len(s.data))
		}
		v.dirty += n - f.dirtyN
		f.dirty, f.dirtyN = kept, n
	}
	f.size, f.mtime, f.changed = size, time.Now(), true
}

// Close releases the handle. It does not Flush.
func (h *File) Close() error {
	h.v.mu.Lock()
	defer h.v.mu.Unlock()
	h.f.refs--
	if h.f.refs == 0 && !h.f.changed {
		delete(h.v.files, h.f.ino)
	}
	return nil
}

// settleAll settles every changed file, then forgets files nobody holds.
func (v *Volume) settleAll(ctx context.Context) error {
	for ino, f := range v.files {
		if f.changed {
			if err := v.settle(ctx, f); err != nil {
				return err
			}
		}
		if f.refs == 0 {
			delete(v.files, ino)
		}
	}
	return nil
}

// touched reports whether an unsettled write overlaps [a, b).
func (f *fileState) touched(a, b uint64) bool {
	i := sort.Search(len(f.dirty), func(i int) bool { return f.dirty[i].end() > a })
	return i < len(f.dirty) && f.dirty[i].off < b
}

// settle turns the file's unsettled writes into settled content: it re-cuts the
// file into chunks, uploads the new ones, and records the file's new shape as
// pending metadata. If it fails nothing about the file has changed, and the
// writes are still held.
//
// Only the part of the file the writes touch is re-cut. Cutting is local (a cut
// depends on the bytes since the previous cut), so a settled chunk that is
// unchanged and begins where the new cutting stands is exactly a chunk the new
// cutting would produce again: it is kept, and cutting resumes after it. The
// result is the chunk list a cut of the whole file would give, found by reading
// and sealing only the chunks around the edit. The chunk that ended the old file
// is kept only if the file still ends there, since it was cut by the end of the
// file, not by its bytes.
func (v *Volume) settle(ctx context.Context, f *fileState) error {
	var inline []byte
	var out []extent

	if f.size <= inlineMax {
		inline = make([]byte, f.size)
		if _, err := v.read(ctx, f, inline, 0); err != nil && err != io.EOF {
			return err
		}
	} else {
		var err error
		if out, err = v.recut(ctx, f); err != nil {
			return err
		}
	}

	in, err := v.stat(ctx, f.ino)
	if err != nil {
		return err
	}

	// Diff against the settled records and queue only what differs.
	if f.size <= inlineMax {
		if f.size > 0 {
			v.over.put(keyInline(f.ino), inline)
		} else {
			v.over.del(keyInline(f.ino))
		}
		for _, e := range f.ext {
			v.over.del(keyExtent(f.ino, e.off))
		}
	} else {
		v.over.del(keyInline(f.ino))
		old := make(map[uint64]extent, len(f.ext))
		for _, e := range f.ext {
			old[e.off] = e
		}
		for _, e := range out {
			if o, ok := old[e.off]; !ok || o != e {
				v.over.put(keyExtent(f.ino, e.off), encodeExtent(e))
			}
			delete(old, e.off)
		}
		for off := range old {
			v.over.del(keyExtent(f.ino, off))
		}
	}
	in.Size, in.Mtime, in.Ctime = f.size, f.mtime, f.mtime
	v.over.put(keyInode(f.ino), encodeInode(in))

	f.inline, f.ext, f.base, f.limit = inline, out, f.size, f.size
	v.dirty -= f.dirtyN
	f.dirty, f.dirtyN, f.changed = nil, 0, false
	if f.size <= inlineMax {
		f.ext = nil
	} else {
		f.inline = nil
	}
	return nil
}

// recut returns the chunk list of the file's content now, uploading new chunks.
func (v *Volume) recut(ctx context.Context, f *fileState) ([]extent, error) {
	clean := func(e extent) bool {
		return e.end() <= min(f.size, f.limit) && !f.touched(e.off, e.end()) && (e.end() < f.base || e.end() == f.size)
	}
	var out []extent
	window := make([]byte, min(f.size, maxChunk))
	var pos uint64
	old := f.ext
	for pos < f.size {
		for len(old) > 0 && old[0].off < pos {
			old = old[1:]
		}
		if len(old) > 0 && old[0].off == pos && clean(old[0]) {
			out = append(out, old[0])
			pos = old[0].end()
			continue
		}
		w := window[:min(uint64(len(window)), f.size-pos)]
		if _, err := v.read(ctx, f, w, pos); err != nil && err != io.EOF {
			return nil, err
		}
		piece := w[:cut(w)]
		id, err := v.putChunk(ctx, piece)
		if err != nil {
			return nil, err
		}
		out = append(out, extent{off: pos, n: uint32(len(piece)), id: id})
		pos += uint64(len(piece))
	}
	return out, nil
}
