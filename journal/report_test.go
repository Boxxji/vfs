package journal_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"testing"

	"github.com/hanzoai/vfs/journal"
	"github.com/hanzoai/vfs/journal/crash"
)

// threeSegments: records 1 to 3, 4 to 6, 7 to 9 in three segments, the marks lost.
func threeSegments(t *testing.T) *crash.Disk {
	d := crash.New()
	j := open(t, small(d, newClock(), 0))
	for i := 1; i <= 9; i++ {
		mustAppend(t, j, bigRecord(i))
	}
	mustSync(t, j, 9)
	closeWithin(t, j)
	forgetMarks(t, d)
	return d
}

// covered returns the sequences the ranges name.
func covered(rs []journal.Range) map[uint64]bool {
	m := map[uint64]bool{}
	for _, r := range rs {
		for s := r.First; s <= r.Last; s++ {
			m[s] = true
		}
	}
	return m
}

// Every valid record that Open wipes or leaves unread is listed, wherever the
// damage is: a segment whose first record is damaged is wiped whole, and the
// records in it that are intact are among those given up.
func TestDroppedListsEveryValidRecordGivenUp(t *testing.T) {
	off := func(seg string, n int, plus int64) func(t *testing.T, d *crash.Disk) {
		return func(t *testing.T, d *crash.Disk) { corrupt(t, d, seg, recordOffset(n, len(big))+plus) }
	}
	for _, c := range []struct {
		name   string
		damage func(t *testing.T, d *crash.Disk)
		valid  []uint64 // records that are intact and that recovery cannot reach
	}{
		{"payload of the first record of a middle segment", off("000001.seg", 0, 40), []uint64{5, 6, 7, 8, 9}},
		{"header of the first record of a middle segment", off("000001.seg", 0, 10), nil}, // not passable: nothing is looked for behind it
		{"payload of the middle record of a middle segment", off("000001.seg", 1, 40), []uint64{6, 7, 8, 9}},
		{"payload of the last record of a middle segment", off("000001.seg", 2, 40), []uint64{7, 8, 9}},
		{"payload of the first record of the last segment", off("000002.seg", 0, 40), []uint64{8, 9}},
		{"payload of the middle record of the last segment", off("000002.seg", 1, 40), []uint64{9}},
		{"payload of the last record of the last segment", off("000002.seg", 2, 40), nil},
	} {
		t.Run(c.name, func(t *testing.T) {
			d := threeSegments(t)
			c.damage(t, d)
			j, err := journal.Open("", small(d, newClock(), 0))
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			defer closeWithin(t, j)
			r := j.Recovery()
			got := covered(r.Dropped)
			for _, s := range c.valid {
				if !got[s] {
					t.Errorf("record %d is intact, unreachable, and was given up, but Dropped is %v (Last %d)", s, r.Dropped, r.Last)
				}
			}
			for _, rg := range r.Dropped {
				if rg.Last < rg.First {
					t.Errorf("Dropped holds an empty range %v", rg)
				}
			}
		})
	}
}

// Two damaged payloads in one segment: the records behind the second are listed.
func TestDroppedPastASecondDamage(t *testing.T) {
	d := crash.New()
	opt := journal.Options{Capacity: 4 * 65536, Segment: 65536, Disk: d, Clock: newClock()}
	j := open(t, opt)
	const size = 100
	for i := 1; i <= 12; i++ {
		mustAppend(t, j, bytes.Repeat([]byte{byte('a' + i)}, size))
	}
	mustSync(t, j, 12)
	closeWithin(t, j)
	forgetMarks(t, d)
	corrupt(t, d, "000000.seg", recordOffset(4, size)+40) // record 5
	corrupt(t, d, "000000.seg", recordOffset(8, size)+40) // record 9
	j = open(t, opt)
	defer closeWithin(t, j)
	r := j.Recovery()
	got := covered(r.Dropped)
	for s := uint64(6); s <= 12; s++ {
		if s != 9 && !got[s] {
			t.Errorf("record %d is intact, was given up and is not listed: Dropped %v, Last %d", s, r.Dropped, r.Last)
		}
	}
}

// Discard names a range; the records it removes are those and only those, also
// when some of what the log holds is behind a damaged payload.
func TestDiscardRefusesRecordsBehindDamage(t *testing.T) {
	d := crash.New()
	opt := journal.Options{Capacity: 4 * 65536, Segment: 65536, Disk: d, Clock: newClock()}
	j := open(t, opt)
	const size = 100
	for i := 1; i <= 10; i++ {
		mustAppend(t, j, bytes.Repeat([]byte{byte('a' + i)}, size))
	}
	mustSync(t, j, 10)
	closeWithin(t, j)
	forgetMarks(t, d)
	corrupt(t, d, "000000.seg", recordOffset(5, size)+40) // record 6
	before := d.Fingerprint()
	if err := journal.Discard("", opt, 1, 5); !errors.Is(err, journal.ErrRange) {
		t.Fatalf("Discard(1, 5) with intact records 7 to 10 behind the damage: %v, want ErrRange", err)
	}
	if d.Fingerprint() != before {
		t.Fatal("a refused Discard changed the disk")
	}
	if err := journal.Discard("", opt, 1, 10); err != nil {
		t.Fatalf("Discard(1, 10) of what the journal holds: %v", err)
	}
}

// A segment whose first record is damaged holds intact records that Discard,
// naming the records before it, would remove without a word.
func TestDiscardRefusesAnOrphanDamagedAtItsStart(t *testing.T) {
	d := threeSegments(t)
	corrupt(t, d, "000001.seg", recordOffset(0, len(big))+40)
	if err := journal.Discard("", small(d, newClock(), 0), 1, 3); !errors.Is(err, journal.ErrRange) {
		t.Fatalf("Discard(1, 3) with intact records 5 to 9 behind a damaged 4: %v, want ErrRange", err)
	}
}

// The same with two segments; the records 5 and 6 are all that is behind.
func TestDiscardRefusesAnOrphanDamagedAtItsStartAlone(t *testing.T) {
	d := crash.New()
	j := open(t, small(d, newClock(), 0))
	for i := 1; i <= 6; i++ {
		mustAppend(t, j, bigRecord(i))
	}
	mustSync(t, j, 6)
	closeWithin(t, j)
	forgetMarks(t, d)
	corrupt(t, d, "000001.seg", recordOffset(0, len(big))+40) // record 4
	if err := journal.Discard("", small(d, newClock(), 0), 1, 3); !errors.Is(err, journal.ErrRange) {
		t.Fatalf("Discard(1, 3) with intact records 5 and 6 behind a damaged 4: %v, want ErrRange", err)
	}
}

// An orphan that begins before the range named is not covered by it either.
func TestDiscardRefusesAnOrphanBeforeTheRange(t *testing.T) {
	d := threeSegments(t)
	corrupt(t, d, "000000.seg", recordOffset(0, len(big))+40) // record 1: the first segment is no part of the log
	if err := journal.Discard("", small(d, newClock(), 0), 4, 9); !errors.Is(err, journal.ErrRange) {
		t.Fatalf("Discard(4, 9) with intact records 2 and 3 before it: %v, want ErrRange", err)
	}
}

// Discard names the whole range: a wrong first with the right last, a wrong last
// with the right first, a range wider or narrower, are all refused, and nothing
// is changed.
func TestDiscardNamesTheWholeRange(t *testing.T) {
	for _, r := range [][2]uint64{{4, 9}, {2, 9}, {1, 8}, {1, 10}, {0, 9}, {2, 3}, {10, 12}} {
		d := threeSegments(t)
		before := d.Fingerprint()
		if err := journal.Discard("", small(d, newClock(), 0), r[0], r[1]); !errors.Is(err, journal.ErrRange) {
			t.Errorf("Discard(%d, %d) of a journal holding 1 to 9: %v, want ErrRange", r[0], r[1], err)
		}
		if d.Fingerprint() != before {
			t.Errorf("Discard(%d, %d) changed the disk", r[0], r[1])
		}
	}
	d := threeSegments(t)
	if err := journal.Discard("", small(d, newClock(), 0), 1, 9); err != nil {
		t.Fatal(err)
	}
}

// failReads is a Disk whose files fail a read at or after the start of the
// records, for the segments named.
type failReads struct {
	journal.Disk
	names map[string]bool
}

var errRead = errors.New("EIO on read")

func (d failReads) Open(name string) (journal.File, error) {
	f, err := d.Disk.Open(name)
	if err != nil || !d.names[name] {
		return f, err
	}
	return failFile{f}, nil
}

type failFile struct{ journal.File }

func (f failFile) ReadAt(p []byte, off int64) (int, error) {
	if off >= dataStart {
		return 0, errRead
	}
	return f.File.ReadAt(p, off)
}

// A segment that cannot be read is not a segment that holds nothing: Discard
// returns the error and removes nothing.
func TestDiscardReturnsTheReadErrorOfAnOrphan(t *testing.T) {
	d := threeSegments(t)
	f, _ := d.Open("000001.seg") // the middle header gone: the last segment follows a gap
	f.WriteAt(make([]byte, headerSize), 0)
	before := d.Fingerprint()
	err := journal.Discard("", small(failReads{d, map[string]bool{"000002.seg": true}}, newClock(), 0), 1, 3)
	if !errors.Is(err, errRead) {
		t.Fatalf("Discard: %v, want the read error", err)
	}
	if d.Fingerprint() != before {
		t.Fatal("Discard changed the disk after a read error")
	}
}

// Damage to the payload of the last record leaves nothing valid behind it:
// nothing is listed, in particular not an empty range.
func TestDroppedHoldsNoEmptyRange(t *testing.T) {
	d := threeSegments(t)
	corrupt(t, d, "000002.seg", recordOffset(2, len(big))+40) // record 9
	j := open(t, small(d, newClock(), 0))
	defer closeWithin(t, j)
	if r := j.Recovery(); r.Last != 8 || len(r.Dropped) != 0 {
		t.Fatalf("recovery %+v: want the log to end at 8 with nothing listed", r)
	}
}

// forgeRecord is a record of the segment, with a header that holds and a payload
// that does not match its checksum.
func forgeRecord(seed uint32, seq uint64, payload []byte, goodPayload bool) []byte {
	h := make([]byte, 28)
	binary.LittleEndian.PutUint32(h[4:], uint32(len(payload)))
	binary.LittleEndian.PutUint64(h[8:], seq)
	sum := crc32.Checksum(payload, crc32.MakeTable(crc32.Castagnoli))
	if !goodPayload {
		sum ^= 1
	}
	binary.LittleEndian.PutUint32(h[24:], sum)
	binary.LittleEndian.PutUint32(h, crc32.Update(seed, crc32.MakeTable(crc32.Castagnoli), h[4:]))
	return append(h, payload...)
}

// What is looked for behind a damaged payload is the record with the next
// sequence: a header that holds but names another is not the log's.
func TestBehindDamageOnlyTheNextSequenceCounts(t *testing.T) {
	d := crash.New()
	j := open(t, small(d, newClock(), 0))
	for i := 1; i <= 3; i++ {
		mustAppend(t, j, []byte("kept"))
	}
	mustSync(t, j, 3)
	closeWithin(t, j)
	forgetMarks(t, d)
	seed := seedOfSegment(t, d, "000000.seg", nil)
	at := int64(dataStart + 3*(28+4))
	f, _ := d.Open("000000.seg")
	wrong := forgeRecord(seed, 99, []byte("stale"), false) // a header that holds, naming 99, not 4
	next := forgeRecord(seed, 5, []byte("after"), true)    // and a valid record that would be 5
	f.WriteAt(wrong, at)
	f.WriteAt(next, at+int64(len(wrong)))
	j = open(t, small(d, newClock(), 0))
	defer closeWithin(t, j)
	if r := j.Recovery(); r.Last != 3 || len(r.Dropped) != 0 {
		t.Fatalf("recovery %+v: a header naming another sequence is not the log's", r)
	}
	// And the right one is: the same record 4, damaged, then a valid 5.
	d2 := crash.New()
	opt := small(d2, newClock(), 0)
	jj := open(t, opt)
	for i := 1; i <= 3; i++ {
		mustAppend(t, jj, []byte("kept"))
	}
	mustSync(t, jj, 3)
	closeWithin(t, jj)
	forgetMarks(t, d2)
	g, _ := d2.Open("000000.seg")
	bad4 := forgeRecord(seed2(t, d2), 4, []byte("torn"), false)
	g.WriteAt(bad4, at)
	g.WriteAt(forgeRecord(seed2(t, d2), 5, []byte("after"), true), at+int64(len(bad4)))
	jj = open(t, small(d2, newClock(), 0))
	defer closeWithin(t, jj)
	if r := jj.Recovery(); r.Last != 3 || fmt.Sprint(r.Dropped) != "[{4 5}]" {
		t.Fatalf("recovery %+v: want 4 and 5 listed", r)
	}
}

func seed2(t testing.TB, d *crash.Disk) uint32 { return seedOfSegment(t, d, "000000.seg", nil) }

// Production cadence: damage inside the proven region is corruption; damage in
// the lag is a torn tail whose valid records are listed.
func TestDamageInTheLagOfTheProof(t *testing.T) {
	defer journal.MarkEvery(16)()
	for _, victim := range []int{5, 20, 31, 38} {
		t.Run(fmt.Sprint(victim), func(t *testing.T) {
			d := crash.New()
			opt := journal.Options{Capacity: 4 * 65536, Segment: 65536, Disk: d, Clock: newClock()}
			j := open(t, opt)
			const size = 100
			for i := 1; i <= 40; i++ {
				seq := mustAppend(t, j, bytes.Repeat([]byte{byte('a' + i%26)}, size))
				mustSync(t, j, seq) // one round each
			}
			img := d.Image(d.Len(), crash.Variant{Kind: crash.Lost}) // power loss
			closeWithin(t, j)
			corrupt(t, img, "000000.seg", recordOffset(victim-1, size)+40)
			o := opt
			o.Disk = img
			_, _, through, _ := preamble(t, img, "000000.seg")
			jj, err := journal.Open("", o)
			if uint64(victim) <= through {
				if !errors.Is(err, journal.ErrCorrupt) {
					t.Fatalf("damage to record %d, within the proven %d: %v, want ErrCorrupt", victim, through, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("victim %d (mark says through %d): %v", victim, through, err)
			}
			defer closeWithin(t, jj)
			r := jj.Recovery()
			if r.Last >= uint64(victim) {
				t.Errorf("the log runs through %d past the damaged record %d", r.Last, victim)
			}
			got := covered(r.Dropped)
			for s := victim + 1; s <= 40; s++ {
				if !got[uint64(s)] {
					t.Errorf("record %d is intact and was given up, not listed", s)
					break
				}
			}
		})
	}
}
