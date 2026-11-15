package journal

import (
	"crypto/sha256"
	"encoding/binary"
	"hash/crc32"
)

// On-disk layout.
//
// A segment file starts with a header and two marks, then holds records back
// to back until the first byte that does not parse as the next record of this
// segment:
//
//	header  checksum u32 | first u64 | life u64 | lineage [16]byte
//	mark    checksum u32 | through u64 | floor u64                       (two slots)
//	record  checksum u32 | length u32 | sequence u64 | acknowledged i64 | payload checksum u32 | payload
//
// The header checksum covers first, life and lineage. The lineage is a digest
// of an identity the caller supplies (a volume and a fencing round): a journal
// opened under another identity is refused, not read. The record and mark
// checksums are seeded from the segment's life and the lineage, so a record
// left in the file by an earlier use of the same file, or by another lineage,
// never validates in a later one. The payload checksum is computed before the
// journal takes its lock; the record checksum then binds it to the sequence and
// the time.
//
// A mark says what the journal knew when it was written: through, every record
// up to this sequence had been made durable by a completed sync; floor, the
// store held every record up to this sequence, and the segments that held them
// may be gone. A mark is written after the sync it describes, so wherever one
// survives, everything it claims is on the media. The two slots are written
// alternately; a torn write damages one and the other still stands, and a
// recovery takes the highest through and the highest floor of the slots that
// validate. All integers are little endian. A zero header is a free segment.
const (
	headerSize   = 36
	markSize     = 20
	dataStart    = headerSize + 2*markSize // where the records begin
	recordHeader = 28
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// lineage is the digest of the identity a journal belongs to.
type lineage [16]byte

func lineageOf(identity []byte) lineage {
	sum := sha256.Sum256(identity)
	var l lineage
	copy(l[:], sum[:])
	return l
}

func seedOf(life uint64, l lineage) uint32 {
	var b [8 + len(lineage{})]byte
	binary.LittleEndian.PutUint64(b[:], life)
	copy(b[8:], l[:])
	return crc32.Checksum(b[:], castagnoli)
}

// appendPreamble encodes the header and the first mark of a segment.
func appendPreamble(dst []byte, first, life uint64, l lineage, through, floor uint64) []byte {
	var h [headerSize]byte
	binary.LittleEndian.PutUint64(h[4:], first)
	binary.LittleEndian.PutUint64(h[12:], life)
	copy(h[20:], l[:])
	binary.LittleEndian.PutUint32(h[0:], crc32.Checksum(h[4:], castagnoli))
	dst = append(dst, h[:]...)
	dst = appendMark(dst, seedOf(life, l), through, floor)
	return append(dst, make([]byte, markSize)...) // the other slot
}

// parseHeader reports the first sequence, the life and the lineage of a
// segment, or false when b is not a header (free, torn or foreign).
func parseHeader(b []byte) (first, life uint64, l lineage, ok bool) {
	if len(b) < headerSize {
		return 0, 0, l, false
	}
	if crc32.Checksum(b[4:headerSize], castagnoli) != binary.LittleEndian.Uint32(b) {
		return 0, 0, l, false
	}
	first = binary.LittleEndian.Uint64(b[4:])
	copy(l[:], b[20:])
	return first, binary.LittleEndian.Uint64(b[12:]), l, first != 0
}

func appendMark(dst []byte, seed uint32, through, floor uint64) []byte {
	var m [markSize]byte
	binary.LittleEndian.PutUint64(m[4:], through)
	binary.LittleEndian.PutUint64(m[12:], floor)
	binary.LittleEndian.PutUint32(m[0:], crc32.Update(seed, castagnoli, m[4:]))
	return append(dst, m[:]...)
}

// parseMarks reads the two mark slots of a segment whose preamble is b.
func parseMarks(b []byte, seed uint32) (through, floor uint64) {
	for i := 0; i < 2; i++ {
		m := b[headerSize+i*markSize : headerSize+(i+1)*markSize]
		if crc32.Update(seed, castagnoli, m[4:]) == binary.LittleEndian.Uint32(m) {
			through = max(through, binary.LittleEndian.Uint64(m[4:]))
			floor = max(floor, binary.LittleEndian.Uint64(m[12:]))
		}
	}
	return through, floor
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
	off := int64(dataStart)
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
