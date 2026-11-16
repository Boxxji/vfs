package journal_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/hanzoai/vfs/journal"
	"github.com/hanzoai/vfs/journal/crash"
)

// seedOfSegment is the CRC-32C seed of a segment: its life then its lineage.
func seedOfSegment(t testing.TB, d *crash.Disk, name string, lineage []byte) uint32 {
	t.Helper()
	f, err := d.Open(name)
	if err != nil {
		t.Fatal(err)
	}
	h := make([]byte, headerSize)
	if _, err := f.ReadAt(h, 0); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(lineage)
	b := make([]byte, 24)
	copy(b, h[16:24])
	copy(b[8:], sum[:16])
	return crc32.Checksum(b, crc32.MakeTable(crc32.Castagnoli))
}

func putMark(t testing.TB, d *crash.Disk, name string, slot int, seed uint32, through, floor uint64, valid bool) {
	t.Helper()
	m := make([]byte, 20)
	binary.LittleEndian.PutUint64(m[4:], through)
	binary.LittleEndian.PutUint64(m[12:], floor)
	sum := crc32.Update(seed, crc32.MakeTable(crc32.Castagnoli), m[4:])
	if !valid {
		sum ^= 0xdeadbeef
	}
	binary.LittleEndian.PutUint32(m, sum)
	f, err := d.Open(name)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt(m, int64(headerSize+markSize*slot)); err != nil {
		t.Fatal(err)
	}
}

func cleanJournal(t testing.TB, n int) *crash.Disk {
	t.Helper()
	d := crash.New()
	j := open(t, small(d, newClock(), 0))
	for i := 1; i <= n; i++ {
		mustAppend(t, j, bytes.Repeat([]byte{byte('a' + i)}, 100))
	}
	mustSync(t, j, uint64(n))
	if err := closeBounded(t, j); err != nil {
		t.Fatal(err)
	}
	return d
}

// A torn mark write leaves bytes whose checksum does not hold. They
// must be ignored, not believed: a through of 2^60 would be read as "records
// that were made durable are gone" and brick the journal.
func TestMarkWithABadChecksumIsIgnored(t *testing.T) {
	d := cleanJournal(t, 10)
	seed := seedOfSegment(t, d, "000000.seg", nil)
	putMark(t, d, "000000.seg", 0, seed, 1<<60, 1<<60, false)
	putMark(t, d, "000000.seg", 1, seed, 1<<59, 1<<59, false)
	j, err := journal.Open("", small(d, newClock(), 0))
	if err != nil {
		t.Fatalf("Open believed a mark whose checksum does not hold: %v", err)
	}
	closeWithin(t, j)
}

// Of two valid marks the higher through and the higher floor
// count, whichever slot holds them. With the newer in slot 0 and an older one
// in slot 1, damage between the two throughs is corruption.
func TestTheHigherMarkCountsWhicheverSlotHoldsIt(t *testing.T) {
	d := cleanJournal(t, 10)
	seed := seedOfSegment(t, d, "000000.seg", nil)
	putMark(t, d, "000000.seg", 0, seed, 10, 0, true)    // the newer
	putMark(t, d, "000000.seg", 1, seed, 2, 0, true)     // an older one
	corrupt(t, d, "000000.seg", recordOffset(5, 100)+40) // record 6, between the two
	if _, err := journal.Open("", small(d, newClock(), 0)); !errors.Is(err, journal.ErrCorrupt) {
		t.Fatalf("damage below the highest valid mark: %v, want ErrCorrupt", err)
	}
	// and the floor: a store below the higher floor in slot 0 is refused
	d = cleanJournal(t, 10)
	seed = seedOfSegment(t, d, "000000.seg", nil)
	putMark(t, d, "000000.seg", 0, seed, 10, 4, true)
	putMark(t, d, "000000.seg", 1, seed, 10, 1, true)
	if j, err := journal.Open("", small(d, newClock(), 2)); !errors.Is(err, journal.ErrMismatch) {
		if err == nil {
			closeWithin(t, j)
		}
		t.Fatalf("store at 2 under a floor of 4 (slot 0) and 1 (slot 1): %v, want ErrMismatch", err)
	}
}

// The mark a segment starts with says what was true when it was
// made. A journal started on an empty directory under a store at 9 must refuse
// a stale Head at 2 even though it never reclaimed anything: the floor is in
// the first segment's own preamble.
func TestAFreshSegmentCarriesTheFloorAndTheProof(t *testing.T) {
	defer journal.MarkEvery(1000)() // production writes a mark every sixteenth round: until then the preamble is all there is
	d := crash.New()
	j := open(t, small(d, newClock(), 9))
	for i := 0; i < 3; i++ {
		mustAppend(t, j, []byte("new"))
	}
	mustSync(t, j, 12)
	// a crash right here: the first segment's preamble is all that says 9
	img := d.Image(d.Len(), crash.Variant{Kind: crash.Lost})
	for _, wrong := range []uint64{0, 2, 8} {
		if jj, err := journal.Open("", small(img, newClock(), wrong)); !errors.Is(err, journal.ErrMismatch) {
			if err == nil {
				t.Errorf("store at %d under a journal started at 9: Open succeeded, dropped %v", wrong, jj.Recovery().Dropped)
				jj.Close()
			} else {
				t.Errorf("store at %d under a journal started at 9: %v, want ErrMismatch", wrong, err)
			}
		}
	}
	// and the through of the preamble: records 10 to 12 were synced, so a log
	// that has lost its start and begins at 13 is corruption, not a torn tail.
	_, first, through, floor := preamble(t, img, "000000.seg")
	if first != 10 || floor != 9 {
		t.Errorf("first segment: first %d floor %d, want 10 and 9 (through %d)", first, floor, through)
	}
}

// Acknowledgement times never go backwards inside a run, even if the
// clock does: the deadline is measured from the oldest record, and a record
// that looks older than its predecessor would move it.
func TestAcknowledgementTimesDoNotGoBackwards(t *testing.T) {
	c := newClock()
	j := open(t, small(crash.New(), c, 0))
	defer closeWithin(t, j)
	mustAppend(t, j, []byte("a"))
	c.Advance(10 * time.Second)
	mustAppend(t, j, []byte("b"))
	c.now = c.now.Add(-time.Hour) // the wall clock is stepped back
	mustAppend(t, j, []byte("c"))
	var prev time.Time
	err := j.Replay(func(r journal.Record) error {
		if r.Time.Before(prev) {
			t.Errorf("record %d acknowledged at %v, before its predecessor at %v", r.Seq, r.Time, prev)
		}
		prev = r.Time
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// A refused Open must release the directory: the operator fixes the
// Head and mounts again in the same process.
func TestRefusedOpenReleasesTheDirectory(t *testing.T) {
	dir := filepath.Join(scratch(t), "j")
	opt := journal.Options{Capacity: 4 * 65536, Segment: 65536, Published: 5}
	j, err := journal.Open(dir, opt)
	if err != nil {
		t.Fatal(err)
	}
	seq, _ := j.Append(ctx, []byte("x"))
	j.Sync(ctx, seq)
	if err := closeBounded(t, j); err != nil {
		t.Fatal(err)
	}
	wrong := opt
	wrong.Published = 0
	if _, err := journal.Open(dir, wrong); !errors.Is(err, journal.ErrMismatch) {
		t.Fatalf("Open with a stale Head: %v, want ErrMismatch", err)
	}
	j, err = journal.Open(dir, opt)
	if err != nil {
		t.Fatalf("Open after a refused Open (right Head): %v", err)
	}
	closeWithin(t, j)
}

type countedDisk struct {
	journal.Disk
	closed *int
}

func (c countedDisk) Close() error { *c.closed++; return nil }

// The journal owns the Disk from the call, also when the options are
// refused, so a lock the Disk holds is not leaked.
func TestRefusedOptionsCloseTheDisk(t *testing.T) {
	var closed int
	_, err := journal.Open("", journal.Options{Capacity: 1, Segment: 4096, Disk: countedDisk{crash.New(), &closed}})
	if err == nil || closed != 1 {
		t.Fatalf("Open with a capacity of one byte: err %v, Disk closed %d times, want an error and 1", err, closed)
	}
}

// A cancelled Due is forgotten: a publisher that loops on Due with a
// context it cancels each round must not grow the journal.
func TestCancelledDueIsForgotten(t *testing.T) {
	j := open(t, journal.Options{Capacity: 1 << 20, Segment: 64 << 10, Disk: crash.New()})
	defer closeWithin(t, j)
	mustAppend(t, j, []byte("x"))
	var m0, m1 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m0)
	for i := 0; i < 20000; i++ {
		c, cancel := ctxWithCancel()
		j.Due(c, time.Hour)
		cancel()
	}
	time.Sleep(300 * time.Millisecond)
	runtime.GC()
	runtime.ReadMemStats(&m1)
	if grown := int64(m1.HeapAlloc) - int64(m0.HeapAlloc); grown > 4<<20 {
		t.Fatalf("heap grew by %d MiB over 20,000 cancelled Due calls", grown>>20)
	}
}

// The mark a segment starts with proves the records before it were
// durable (the journal had synced them when it made the segment). With the
// production cadence that preamble is the only proof for the first rounds:
// damage to the previous segment is then corruption, not a torn tail whose
// consequence is to wipe this segment, which holds synced records.
func TestPreambleThroughProvesTheEarlierSegment(t *testing.T) {
	defer journal.MarkEvery(1000)()
	d := crash.New()
	j := open(t, small(d, newClock(), 0))
	for i := 1; i <= 3; i++ {
		mustAppend(t, j, bigRecord(i))
	}
	mustSync(t, j, 3)
	for i := 4; i <= 6; i++ {
		mustAppend(t, j, bigRecord(i))
	}
	mustSync(t, j, 6)
	img := d.Image(d.Len(), crash.Variant{Kind: crash.Lost}) // power loss: no Close, no final mark
	corrupt(t, img, "000000.seg", recordOffset(1, len(big))+500)
	if jj, err := journal.Open("", small(img, newClock(), 0)); !errors.Is(err, journal.ErrCorrupt) {
		if err == nil {
			t.Errorf("synced records 4 to 6 were wiped (dropped %v) after damage to record 2", jj.Recovery().Dropped)
			jj.Close()
		} else {
			t.Errorf("Open: %v, want ErrCorrupt", err)
		}
	}
}

// Close writes the final mark, and the mark must be on the media when
// Close returns: the docs say a clean Close leaves no window in which damage
// to the last records is taken for a torn tail.
func TestTheFinalMarkIsDurableWhenCloseReturns(t *testing.T) {
	d := crash.New()
	j := open(t, small(d, newClock(), 0))
	for i := 1; i <= 3; i++ {
		mustAppend(t, j, bigRecord(i))
	}
	mustSync(t, j, 3)
	if err := closeBounded(t, j); err != nil {
		t.Fatal(err)
	}
	img := d.Image(d.Len(), crash.Variant{Kind: crash.Lost}) // power loss the instant Close returns
	live, first, through, _ := preamble(t, img, "000000.seg")
	if !live || first != 1 || through != 3 {
		t.Fatalf("after Close the media says through %d (live %v, first %d), want 3", through, live, first)
	}
}
