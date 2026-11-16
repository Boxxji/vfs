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
//	header  checksum u32 | magic [4]byte | first u64 | life u64 | lineage [16]byte
//	mark    checksum u32 | through u64 | floor u64                       (two slots)
//	record  checksum u32 | length u32 | sequence u64 | acknowledged i64 | payload checksum u32 | payload
//
// The magic is four fixed bytes. It carries no number: there is one layout, and
// a directory holding a segment that is not in it is refused, not read and not
// reused. The header checksum covers the magic, first, life and lineage. The
// lineage is a digest of an identity the caller supplies (a volume and a
// fencing round): a journal opened under another identity is refused, not read.
// The record and mark checksums are seeded from the segment's life and the
// lineage, so a record left in the file by an earlier use of the same file, or
// by another lineage, never validates in a later one. The payload checksum is
// computed before the journal takes its lock; the record checksum then binds it
// to the sequence and the time.
//
// A mark says what the journal knew when it was written: through, every record
// up to this sequence had been made durable by a completed sync; floor, the
// store held every record up to this sequence, and the segments that held them
// may be gone. A mark is written after the sync it describes, so wherever one
// survives, everything it claims is on the media. The two slots are written
// alternately; a torn write damages one and the other still stands, and a
// recovery takes the highest through and the highest floor of the slots that
// validate. All integers are little endian. A header whose magic is zero is a
// free segment, as is one whose magic is ours and whose checksum fails (a header
// torn by a crash); a header with any other magic is not ours.
//
// The records begin at byte 80, which is not a block boundary, and the marks
// share the first block with the first records. That is chosen: the journal
// writes through the page cache and flushes with fdatasync, and direct I/O
// would need aligned records and marks in blocks of their own.
const (
	headerSize   = 40
	markSize     = 20
	dataStart    = headerSize + 2*markSize // where the records begin
	recordHeader = 28
)

// magic opens every header, inside its checksum.
var magic = [4]byte{'J', 'R', 'N', 'L'}

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
	copy(h[4:], magic[:])
	binary.LittleEndian.PutUint64(h[8:], first)
	binary.LittleEndian.PutUint64(h[16:], life)
	copy(h[24:], l[:])
	binary.LittleEndian.PutUint32(h[0:], crc32.Checksum(h[4:], castagnoli))
	dst = append(dst, h[:]...)
	dst = appendMark(dst, seedOf(life, l), through, floor)
	return append(dst, make([]byte, markSize)...) // the other slot
}

// headerKind is what the first bytes of a segment file say it is.
type headerKind int

const (
	headerFree    headerKind = iota // zeroed, never written, or torn: a segment to reuse
	headerLive                      // ours, and whole
	headerForeign                   // not ours: some other layout, or not a segment at all
)

// kindOf classifies the first headerSize bytes of a file. A header torn by a
// crash is ours and free. A write is torn at any byte, so the magic can be what
// a prefix of a new header left over zeros, or what a prefix of zeros left over
// the magic: every byte of it is then zero or the magic's own at that place,
// and the checksum fails. A magic with any other byte in it is not ours.
func kindOf(b []byte) headerKind {
	switch m := b[4:8]; {
	case string(m) == string(magic[:]):
		if _, _, _, ok := parseHeader(b); ok {
			return headerLive
		}
		return headerFree
	case tornMagic(m):
		return headerFree
	}
	return headerForeign
}

// tornMagic reports whether every byte of m is zero or the magic's own byte at
// its place: zeros, or a part of the magic over zeros, or zeros over a part of it.
func tornMagic(m []byte) bool {
	for i, c := range m {
		if c != 0 && c != magic[i] {
			return false
		}
	}
	return true
}

func allZero(b []byte) bool {
	for _, c := range b {
		if c != 0 {
			return false
		}
	}
	return true
}

// parseHeader reports the first sequence, the life and the lineage of a
// segment, or false when its checksum fails. The caller has seen the magic.
func parseHeader(b []byte) (first, life uint64, l lineage, ok bool) {
	if len(b) < headerSize {
		return 0, 0, l, false
	}
	if crc32.Checksum(b[4:headerSize], castagnoli) != binary.LittleEndian.Uint32(b) {
		return 0, 0, l, false
	}
	first = binary.LittleEndian.Uint64(b[8:])
	copy(l[:], b[24:])
	return first, binary.LittleEndian.Uint64(b[16:]), l, first != 0
}

func appendMark(dst []byte, seed uint32, through, floor uint64) []byte {
	var m [markSize]byte
	binary.LittleEndian.PutUint64(m[4:], through)
	binary.LittleEndian.PutUint64(m[12:], floor)
	binary.LittleEndian.PutUint32(m[0:], crc32.Update(seed, castagnoli, m[4:]))
	return append(dst, m[:]...)
}

// parseMarks reads the two mark slots of a segment whose preamble is b: the
// highest through and floor of the slots that validate, and the slot that holds
// them, the newest. With none valid it is the second, so that the first mark
// written goes to the first.
func parseMarks(b []byte, seed uint32) (through, floor uint64, newest int) {
	newest = 1
	var found bool
	for i := 0; i < 2; i++ {
		m := b[headerSize+i*markSize : headerSize+(i+1)*markSize]
		if crc32.Update(seed, castagnoli, m[4:]) != binary.LittleEndian.Uint32(m) {
			continue
		}
		t, f := binary.LittleEndian.Uint64(m[4:]), binary.LittleEndian.Uint64(m[12:])
		if !found || t > through || (t == through && f > floor) {
			newest = i
		}
		found = true
		through, floor = max(through, t), max(floor, f)
	}
	return through, floor, newest
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

// headerOK reports whether the first recordHeader bytes of b are the header of
// a record of the segment whose seed is given. Until it holds, the length in the
// header is not to be believed.
func headerOK(b []byte, seed uint32) bool {
	return crc32.Update(seed, castagnoli, b[4:recordHeader]) == binary.LittleEndian.Uint32(b)
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
	if !headerOK(b, seed) {
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
		if len(b) < recordHeader || !headerOK(b, seed) {
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

// behind finds the valid records that follow a damaged one at the end of the
// log of a segment: the record at off is the one with sequence expect, whose
// header is whole, so that its length is to be believed, and whose payload does
// not match its checksum. The records after it are followed by their headers,
// past any whose payloads are damaged too, and it returns the range from expect
// to the last valid one, or the zero Range when none follows. A header that
// fails its checksum cannot be passed: what follows it is not looked for.
func behind(f File, seed uint32, expect uint64, off int64) (Range, error) {
	w := window{f: f}
	var last uint64
	for seq := expect; ; seq++ {
		b, err := w.view(off, recordHeader)
		if err != nil {
			return Range{}, err
		}
		if len(b) < recordHeader || !headerOK(b, seed) || binary.LittleEndian.Uint64(b[8:]) != seq {
			break
		}
		n := length(b)
		if n > f.Size()-off-recordHeader {
			break
		}
		p, err := w.view(off, recordHeader+n)
		if err != nil {
			return Range{}, err
		}
		if _, ok := parseRecord(p, seed); ok {
			last = seq // the record at expect is damaged, or the scan would not have stopped there
		}
		off += recordHeader + n
	}
	if last == 0 {
		return Range{}, nil
	}
	return Range{expect, last}, nil
}

// reach is the range of records a segment holds that a reader could find: the
// valid run from its first record, and the valid records behind a damaged payload
// after it. The zero Range when it holds none.
func reach(s *slot) (Range, error) {
	next, end, err := scan(s.file, s.seed, s.first, func(record, int64) error { return nil })
	if err != nil {
		return Range{}, err
	}
	b, err := behind(s.file, s.seed, next, end)
	if err != nil {
		return Range{}, err
	}
	switch {
	case b != Range{}:
		return Range{s.first, b.Last}, nil
	case next > s.first:
		return Range{s.first, next - 1}, nil
	}
	return Range{}, nil
}

// lastNamed follows the record headers of a segment without trusting any of
// them: each one's length leads to the next, whose sequence is the previous plus
// one. It returns the sequence the last of the chain names, or 0 for none. A
// header torn on its way in has zeros behind it, or the records of an older life,
// whose sequences stop short of the next one's; the chain ends where they do.
func lastNamed(f File) (uint64, error) {
	w := window{f: f}
	off := int64(dataStart)
	var last uint64
	for {
		b, err := w.view(off, recordHeader)
		if err != nil {
			return 0, err
		}
		if len(b) < recordHeader {
			return last, nil
		}
		seq, n := binary.LittleEndian.Uint64(b[8:]), length(b)
		if seq == 0 || (last != 0 && seq != last+1) || n > f.Size()-off-recordHeader {
			return last, nil
		}
		last, off = seq, off+recordHeader+n
	}
}
