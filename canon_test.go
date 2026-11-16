package vfs

import (
	"bytes"
	"encoding/binary"
	"testing"
)

// SPEC 1: the Head's integers are in their shortest form. Go's binary.Uvarint accepts the others, so
// without the check two byte strings would give one Head.
func TestHeadNonMinimalUvarint(t *testing.T) {
	h := Head{Seq: 3, Next: 9, Journal: 5}
	good := h.marshal()
	// Seq=3 as 0x83 0x00
	alt := append([]byte{0x83, 0x00}, good[1:]...)
	g1, e1 := unmarshalHead(good)
	g2, e2 := unmarshalHead(alt)
	if e2 == nil {
		t.Errorf("a non-minimal Seq is accepted")
	}
	if e1 != nil {
		t.Fatal(e1)
	}
	if e2 == nil && g1 == g2 && !bytes.Equal(good, alt) {
		t.Errorf("two byte strings (%d and %d bytes) are one Head: a non-minimal uvarint is accepted", len(good), len(alt))
	}
	// Needs=0 as 0x80 0x00 then tail
	alt2 := append(append([]byte(nil), good[:len(good)-1]...), 0x80, 0x00)
	g3, e3 := unmarshalHead(alt2)
	if e3 == nil && g3.Tail == "" {
		t.Errorf("Needs 0 as two bytes is accepted")
	}
	if e3 == nil {
		t.Errorf("a non-minimal Needs is accepted")
	}
}

// SPEC 1: an integer is in its shortest form, and a value wider than its field is refused,
// everywhere a reader reads one: the Head, the inode, the entry, the extent, the node.
func TestIntegersAreInTheirShortestFormAndFitTheirField(t *testing.T) {
	// the reader itself
	for name, b := range map[string][]byte{
		"0 in two bytes":     {0x80, 0x00},
		"1 in two bytes":     {0x81, 0x00},
		"300 in three bytes": {0xac, 0x82, 0x00},
		"empty":              nil,
		"unfinished":         {0x80},
		"eleven bytes":       {0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01},
		"overflow":           {0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x02},
	} {
		r := reader{b: b}
		r.uvarint()
		if r.err == nil {
			t.Errorf("uvarint %s was accepted", name)
		}
		r = reader{b: b}
		r.varint()
		if r.err == nil && name != "0 in two bytes" {
			// a zigzag of 0x80 0x00 is 0 in two bytes too
			t.Errorf("varint %s was accepted", name)
		}
	}
	for _, x := range []uint64{0, 1, 127, 128, 300, 1 << 32, 1<<64 - 1} {
		r := reader{b: binary.AppendUvarint(nil, x)}
		if got := r.uvarint(); got != x || r.done() != nil {
			t.Errorf("uvarint %d does not read back: %d %v", x, got, r.err)
		}
	}
	for _, x := range []int64{0, 1, -1, 63, -64, 64, 1 << 40, -1 << 40, 1<<63 - 1, -1 << 63} {
		r := reader{b: binary.AppendVarint(nil, x)}
		if got := r.varint(); got != x || r.done() != nil {
			t.Errorf("varint %d does not read back: %d %v", x, got, r.err)
		}
	}
	// a value wider than its field
	r := reader{b: binary.AppendUvarint(nil, 1<<32)}
	if r.uvarint32(); r.err == nil {
		t.Error("2^32 was read into 32 bits")
	}
	r = reader{b: binary.AppendUvarint(nil, 1<<32-1)}
	if got := r.uvarint32(); got != 1<<32-1 || r.err != nil {
		t.Errorf("2^32-1 does not read: %d %v", got, r.err)
	}
	// the extent length, the mode of an entry and of an inode, the node level
	id := make([]byte, 32)
	if _, err := decodeExtent(0, append(append([]byte(nil), id...), binary.AppendUvarint(nil, 1<<32)...)); err == nil {
		t.Error("an extent of 2^32 bytes was read as 0")
	}
	if e, err := decodeExtent(0, append(append([]byte(nil), id...), binary.AppendUvarint(nil, 1<<32-1)...)); err != nil || e.n != 1<<32-1 {
		t.Errorf("an extent of 2^32-1 bytes: %+v %v", e, err)
	}
	if _, _, err := decodeEntry(append(binary.AppendUvarint(nil, 5), binary.AppendUvarint(nil, 1<<32)...)); err == nil {
		t.Error("an entry whose mode is 2^32 was read")
	}
	in := Inode{ID: 2, Mode: 0o644}
	good := encodeInode(in)
	var w writer
	w.uvarint(fieldMode)
	w.bytes(binary.AppendUvarint(nil, 1<<32|0o644))
	rest := good[len(good)-len(good):]
	_ = rest
	bad := append(w.b, skipField(t, good, fieldMode)...)
	if _, err := decodeInode(2, bad); err == nil {
		t.Error("an inode whose mode is 2^32+0644 was read as 0644")
	}
	node := (&node{level: 0, keys: [][]byte{{1}}, vals: [][]byte{{2}}}).encode()
	if _, err := decodeNode(node); err != nil {
		t.Fatal(err)
	}
	var nw writer
	nw.uvarint(256)
	nw.uvarint(1)
	nw.bytes([]byte{1})
	nw.bytes(make([]byte, 32))
	if _, err := decodeNode(nw.b); err == nil {
		t.Error("a node of level 256 was read")
	}
	nw = writer{}
	nw.uvarint(0x80 | 0)
	nw.raw([]byte{0x00}) // level 0 in two bytes
	nw.uvarint(1)
	nw.bytes([]byte{1})
	nw.bytes([]byte{2})
	if _, err := decodeNode(nw.b); err == nil {
		t.Error("a node whose level is in two bytes was read")
	}
}

// skipField returns an inode record without its field of the tag.
func skipField(t *testing.T, rec []byte, tag uint64) []byte {
	t.Helper()
	r := reader{b: rec}
	var out []byte
	for len(r.b) > 0 {
		start := r.b
		got := r.uvarint()
		r.bytes()
		if r.err != nil {
			t.Fatal(r.err)
		}
		if got != tag {
			out = append(out, start[:len(start)-len(r.b)]...)
		}
	}
	return out
}
