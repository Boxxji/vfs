package vfs

import (
	"encoding/binary"
	"io/fs"
	"time"
)

// All metadata is records in one ordered tree, keyed by bytes:
//
//	[tInode][ino]                  inode attributes
//	[tEntry][dir ino][name]        directory entry: child ino and type
//	[tData][ino]                   a small file's bytes, inline
//	[tData][ino][offset]           a large file's chunk reference, by file offset
//	[tChunk][chunk id]             where a chunk lives: pack, offset, length
//
// Numbers in keys are big-endian so byte order is numeric order. Records of one
// directory, or of one file, are contiguous, and the inodes of consecutive
// numbers are dense in one table, so a listing or a stat sweep reads few shards.
const (
	tInode byte = 1
	tEntry byte = 2
	tData  byte = 3
	tChunk byte = 4

	// prefixLen is the length of the [table][ino] prefix every scan is by.
	prefixLen = 9
)

// InodeID names a file or directory in a Volume. IDs are assigned in increasing
// order and never reused within a Volume.
type InodeID uint64

// RootInode is the root directory of every Volume.
const RootInode InodeID = 1

// Inode is the attributes of a file or directory.
type Inode struct {
	ID    InodeID
	Mode  fs.FileMode // type and permission bits
	Size  uint64      // bytes, exact
	Mtime time.Time
	Atime time.Time
	Ctime time.Time
}

// IsDir reports whether the inode is a directory.
func (i Inode) IsDir() bool { return i.Mode.IsDir() }

// DirEntry is one name in a directory. Mode carries the type bits only; Stat
// the child for permissions and size.
type DirEntry struct {
	Name string
	ID   InodeID
	Mode fs.FileMode
}

func key(table byte, ino uint64, suffix ...byte) []byte {
	k := make([]byte, prefixLen, prefixLen+len(suffix))
	k[0] = table
	binary.BigEndian.PutUint64(k[1:], ino)
	return append(k, suffix...)
}

func keyInode(ino InodeID) []byte              { return key(tInode, uint64(ino)) }
func keyEntry(dir InodeID, name string) []byte { return key(tEntry, uint64(dir), []byte(name)...) }
func keyInline(ino InodeID) []byte             { return key(tData, uint64(ino)) }

func keyExtent(ino InodeID, off uint64) []byte {
	return key(tData, uint64(ino), binary.BigEndian.AppendUint64(nil, off)...)
}

func keyChunk(id chunkID) []byte { return append([]byte{tChunk}, id[:]...) }

// offsetOf reads the file offset out of an extent key.
func offsetOf(k []byte) uint64 { return binary.BigEndian.Uint64(k[prefixLen:]) }

func encodeInode(in Inode) []byte {
	var w writer
	w.uvarint(uint64(in.Mode))
	w.uvarint(in.Size)
	w.varint(in.Mtime.UnixNano())
	w.varint(in.Atime.UnixNano())
	w.varint(in.Ctime.UnixNano())
	return w.b
}

func decodeInode(id InodeID, b []byte) (Inode, error) {
	r := reader{b: b}
	in := Inode{ID: id, Mode: fs.FileMode(r.uvarint()), Size: r.uvarint()}
	in.Mtime = time.Unix(0, r.varint())
	in.Atime = time.Unix(0, r.varint())
	in.Ctime = time.Unix(0, r.varint())
	return in, r.done()
}

func encodeEntry(child InodeID, mode fs.FileMode) []byte {
	var w writer
	w.uvarint(uint64(child))
	w.uvarint(uint64(mode & fs.ModeType))
	return w.b
}

func decodeEntry(b []byte) (InodeID, fs.FileMode, error) {
	r := reader{b: b}
	id, mode := InodeID(r.uvarint()), fs.FileMode(r.uvarint())
	return id, mode, r.done()
}

// extent is a chunk of a large file: n bytes at file offset off.
type extent struct {
	off uint64
	n   uint32
	id  chunkID
}

func (e extent) end() uint64 { return e.off + uint64(e.n) }

func encodeExtent(e extent) []byte {
	var w writer
	w.raw(e.id[:])
	w.uvarint(uint64(e.n))
	return w.b
}

func decodeExtent(off uint64, b []byte) (extent, error) {
	r := reader{b: b}
	e := extent{off: off}
	copy(e.id[:], r.raw(32))
	e.n = uint32(r.uvarint())
	return e, r.done()
}

// where is a chunk's place: a frame of length n at offset off in a pack.
type where struct {
	pack packID
	off  uint64
	n    uint64
}

func encodeWhere(p where) []byte {
	var w writer
	w.raw(p.pack[:])
	w.uvarint(p.off)
	w.uvarint(p.n)
	return w.b
}

func decodeWhere(b []byte) (where, error) {
	r := reader{b: b}
	var p where
	copy(p.pack[:], r.raw(uint64(len(p.pack))))
	p.off, p.n = r.uvarint(), r.uvarint()
	return p, r.done()
}
