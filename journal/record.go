package journal

import (
	"encoding/binary"
	"hash/crc32"
)

// On-disk layout.
//
// A segment file starts with a header, then holds records back to back until
// the first byte that does not parse as the next record of this segment:
//
//	header  checksum u32 | first u64 | life u64
//	record  checksum u32 | length u32 | sequence u64 | acknowledged i64 | payload checksum u32 | payload
//
// The header checksum covers first and life. The record checksum covers every
// record header field after itself and is seeded from the segment's life, so a
// record left in the file by an earlier use of the same file never validates
// in a later one. The payload checksum is computed before the journal takes
// its lock; the record checksum then binds it to the sequence and the time.
// All integers are little endian. A zero header is a free segment.
const (
	headerSize   = 20
	recordHeader = 28
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

func seedOf(life uint64) uint32 { return uint32(life) ^ uint32(life>>32) }

func appendHeader(dst []byte, first, life uint64) []byte {
	var h [headerSize]byte
	binary.LittleEndian.PutUint64(h[4:], first)
	binary.LittleEndian.PutUint64(h[12:], life)
	binary.LittleEndian.PutUint32(h[0:], crc32.Checksum(h[4:], castagnoli))
	return append(dst, h[:]...)
}

// parseHeader reports the first sequence and the life of a segment, or false
// when b is not a header (free, torn or foreign).
func parseHeader(b []byte) (first, life uint64, ok bool) {
	if len(b) < headerSize {
		return 0, 0, false
	}
	if crc32.Checksum(b[4:headerSize], castagnoli) != binary.LittleEndian.Uint32(b) {
		return 0, 0, false
	}
	first = binary.LittleEndian.Uint64(b[4:])
	return first, binary.LittleEndian.Uint64(b[12:]), first != 0
}

// appendRecord encodes one record onto dst, which must have room for it.
func appendRecord(dst []byte, seed uint32, seq uint64, ack int64, payload []byte, payloadSum uint32) []byte {
	n := len(dst)
	dst = dst[:n+recordHeader]
	h := dst[n:]
	binary.LittleEndian.PutUint32(h[4:], uint32(len(payload)))
	binary.LittleEndian.PutUint64(h[8:], seq)
	binary.LittleEndian.PutUint64(h[16:], uint64(ack))
	binary.LittleEndian.PutUint32(h[24:], payloadSum)
	binary.LittleEndian.PutUint32(h[0:], crc32.Update(seed, castagnoli, h[4:recordHeader]))
	return append(dst, payload...)
}

// record is one parsed record. payload aliases the buffer it was parsed from.
type record struct {
	seq     uint64
	ack     int64
	payload []byte
}

// length reads the payload length from a record header; b must hold at least
// recordHeader bytes.
func length(b []byte) int64 { return int64(binary.LittleEndian.Uint32(b[4:])) }

// parseRecord parses the record at the start of b, which must hold the whole
// record (recordHeader + length bytes). It returns false when the bytes are not
// a record of the segment whose seed is given: torn, stale or never written.
func parseRecord(b []byte, seed uint32) (record, bool) {
	if len(b) < recordHeader {
		return record{}, false
	}
	n := length(b)
	if n > int64(len(b)-recordHeader) {
		return record{}, false
	}
	if crc32.Update(seed, castagnoli, b[4:recordHeader]) != binary.LittleEndian.Uint32(b) {
		return record{}, false
	}
	payload := b[recordHeader : recordHeader+n]
	if crc32.Checksum(payload, castagnoli) != binary.LittleEndian.Uint32(b[24:]) {
		return record{}, false
	}
	seq := binary.LittleEndian.Uint64(b[8:])
	if seq == 0 {
		return record{}, false
	}
	return record{seq, int64(binary.LittleEndian.Uint64(b[16:])), payload}, true
}

// window reads a file lazily through one buffer, so a scan touches only the
// bytes it needs and a recovery stops reading at the end of the valid prefix.
type window struct {
	f    File
	buf  []byte
	base int64
}

const windowSize = 1 << 20

// view returns up to n bytes at off, fewer only at the end of the file.
func (w *window) view(off int64, n int64) ([]byte, error) {
	size := w.f.Size()
	if off >= size {
		return nil, nil
	}
	n = min(n, size-off)
	if off >= w.base && off+n <= w.base+int64(len(w.buf)) {
		return w.buf[off-w.base : off-w.base+n], nil
	}
	read := min(max(n, windowSize), size-off)
	if int64(cap(w.buf)) < read {
		w.buf = make([]byte, read)
	}
	w.buf = w.buf[:read]
	if _, err := w.f.ReadAt(w.buf, off); err != nil {
		w.buf = w.buf[:0]
		return nil, err
	}
	w.base = off
	return w.buf[:n], nil
}

// scan walks the records of a segment from the first one, calling fn with each
// record and the offset just past it, and stops at the first byte that is not
// the next record: the record whose sequence is expect, then expect+1, and so on.
// It returns the sequence after the last valid record and the offset where the
// valid records end.
func scan(f File, seed uint32, expect uint64, fn func(record, int64) error) (uint64, int64, error) {
	w := window{f: f}
	off := int64(headerSize)
	for {
		b, err := w.view(off, recordHeader)
		if err != nil {
			return expect, off, err
		}
		if len(b) < recordHeader {
			return expect, off, nil
		}
		n := length(b)
		if n > f.Size()-off-recordHeader {
			return expect, off, nil
		}
		b, err = w.view(off, recordHeader+n)
		if err != nil {
			return expect, off, err
		}
		r, ok := parseRecord(b, seed)
		if !ok || r.seq != expect {
			return expect, off, nil
		}
		off += recordHeader + n
		if err := fn(r, off); err != nil {
			return expect, off, err
		}
		expect++
	}
}
