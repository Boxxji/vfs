package journal_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hanzoai/vfs/journal"
	"github.com/hanzoai/vfs/journal/crash"
)

var rbCastagnoli = crc32.MakeTable(crc32.Castagnoli)

// preamble reads the header and the two marks of a segment: whether the header
// is live, and the highest through and floor of the marks that validate under
// the segment's seed (life, lineage), as a reader of the format computes them.
func preamble(t testing.TB, d *crash.Disk, name string) (live bool, first, through, floor uint64) {
	t.Helper()
	f, err := d.Open(name)
	if err != nil {
		t.Fatal(err)
	}
	b := make([]byte, dataStart)
	if _, err := f.ReadAt(b, 0); err != nil {
		t.Fatal(err)
	}
	if crc32.Checksum(b[4:headerSize], rbCastagnoli) != binary.LittleEndian.Uint32(b) {
		return false, 0, 0, 0
	}
	first = binary.LittleEndian.Uint64(b[8:])
	seed := make([]byte, 8+16)
	copy(seed, b[16:24])
	copy(seed[8:], b[24:40])
	s := crc32.Checksum(seed, rbCastagnoli)
	for i := 0; i < 2; i++ {
		m := b[headerSize+markSize*i : headerSize+markSize*(i+1)]
		if crc32.Update(s, rbCastagnoli, m[4:]) == binary.LittleEndian.Uint32(m) {
			through = max(through, binary.LittleEndian.Uint64(m[4:]))
			floor = max(floor, binary.LittleEndian.Uint64(m[12:]))
		}
	}
	return true, first, through, floor
}

// A mark that failed to be written is written again by the next Reclaim, and no
// header is wiped before the floor is on the media. Were the mark taken for
// written when its write failed, the next Reclaim would sync, wipe the older
// headers, and leave a log that begins after the store with nothing to say why:
// a stale Head would meet no ErrMismatch and Open would wipe records 7 to 9,
// synced and acknowledged.
func TestFailedFloorWriteIsNotRetried(t *testing.T) {
	for _, fail := range []bool{false, true} {
		d := crash.New()
		j := open(t, small(d, newClock(), 0))
		for i := 1; i <= 9; i++ {
			mustAppend(t, j, bigRecord(i))
		}
		mustSync(t, j, 9)
		publish(t, j, 6)

		var failed atomic.Bool
		if fail {
			d.Intercept(func(e crash.Event) error {
				if e.Kind == "write" && e.Len == markSize && (e.Off == headerSize || e.Off == headerSize+markSize) && !failed.Swap(true) {
					return errors.New("EIO")
				}
				return nil
			})
		}
		err1 := j.Reclaim()
		d.Intercept(nil)
		if fail && (err1 == nil || !failed.Load()) {
			t.Fatalf("the injected mark-write error did not reach Reclaim: %v", err1)
		}
		if !fail && err1 != nil {
			t.Fatal(err1)
		}
		if err := j.Reclaim(); err != nil { // the publisher's next round
			t.Fatalf("second Reclaim: %v", err)
		}
		// Power loss now: only what a completed sync covered.
		img := d.Image(d.Len(), crash.Variant{Kind: crash.Lost})

		live, first, _, floor := preamble(t, img, "000002.seg")
		if !live || first != 7 || floor != 6 {
			t.Errorf("fail=%v: the anchor (records 7 to 9) says floor %d (live %v, first %d), want 6: the floor was never made durable but the older headers were wiped", fail, floor, live, first)
		}
		// The same effect seen from Open: a stale Head (0) must be refused.
		if jj, err := journal.Open("", small(img, newClock(), 0)); !errors.Is(err, journal.ErrMismatch) {
			if err == nil {
				t.Errorf("fail=%v: Open with a stale Head succeeded; Dropped=%v First..Last=%d..%d", fail, jj.Recovery().Dropped, jj.Recovery().First, jj.Recovery().Last)
				jj.Close()
			} else {
				t.Errorf("fail=%v: Open with a stale Head: %v, want ErrMismatch", fail, err)
			}
		}
	}
}

// Discard removes exactly the range named and nothing else: not the intact
// segments that follow a hole, which are records it was not told about.
func TestDiscardRefusesWhatItWasNotToldAbout(t *testing.T) {
	d := crash.New()
	j := open(t, small(d, newClock(), 0))
	for i := 1; i <= 9; i++ {
		mustAppend(t, j, bigRecord(i))
	}
	mustSync(t, j, 9)
	closeWithin(t, j)
	// Lose the header of the middle segment: 1 to 3 are the log, 7 to 9 follow a hole.
	f, _ := d.Open("000001.seg")
	f.WriteAt(make([]byte, headerSize), 0)
	f.Sync()

	err := journal.Discard("", small(d, newClock(), 0), 1, 3)
	if !errors.Is(err, journal.ErrRange) {
		t.Fatalf("Discard(1, 3) with records 7 to 9 beyond a hole: %v, want ErrRange", err)
	}
	if live, first, _, _ := preamble(t, d, "000002.seg"); !live || first != 7 {
		t.Fatalf("Discard refused and still destroyed records 7 to 9")
	}
	if live, first, _, _ := preamble(t, d, "000000.seg"); !live || first != 1 {
		t.Fatalf("Discard refused and still destroyed records 1 to 3")
	}
}

// A copy of a segment within the range named is part of it, and goes with it.
func TestDiscardTakesTheCopyOfASegmentWithTheRange(t *testing.T) {
	d := crash.New()
	j := open(t, small(d, newClock(), 0))
	for i := 1; i <= 3; i++ {
		mustAppend(t, j, bigRecord(i))
	}
	mustSync(t, j, 3)
	closeWithin(t, j)
	src, _ := d.Open("000000.seg")
	data := make([]byte, src.Size())
	src.ReadAt(data, 0)
	twin, _ := d.Create("000042.seg", src.Size())
	twin.WriteAt(data, 0)
	twin.Sync()

	if err := journal.Discard("", small(d, newClock(), 0), 1, 3); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"000000.seg", "000042.seg"} {
		if live, _, _, _ := preamble(t, d, name); live {
			t.Errorf("%s still holds records after Discard(1, 3)", name)
		}
	}
}

// Open reports what it gave up. Damage to a payload ends the log in its
// segment, and the valid records behind it, whose length the intact header
// vouches for, are listed in Dropped.
func TestReportListsValidRecordsBehindInSegmentDamage(t *testing.T) {
	d := crash.New()
	j := open(t, small(d, newClock(), 0))
	const size = 100
	for i := 1; i <= 10; i++ {
		mustAppend(t, j, bytes.Repeat([]byte{byte('a' + i)}, size))
	}
	mustSync(t, j, 10)
	closeWithin(t, j)
	corrupt(t, d, "000000.seg", recordOffset(5, size)+40) // record 6
	forgetMarks(t, d)                                     // the proof lags: nothing vouches for 6

	j = open(t, small(d, newClock(), 0))
	defer closeWithin(t, j)
	r := j.Recovery()
	if r.Last != 5 || !reflect.DeepEqual(r.Dropped, []journal.Range{{First: 6, Last: 10}}) {
		t.Fatalf("recovery %+v: want 1 through 5 kept and 6 through 10 dropped", r)
	}
}

// Damage to a record's header cannot be looked past, since its length is not to
// be believed: the log ends there, and the records behind it are not listed.
func TestReportCannotListWhatLiesBehindADamagedHeader(t *testing.T) {
	d := crash.New()
	j := open(t, small(d, newClock(), 0))
	const size = 100
	for i := 1; i <= 10; i++ {
		mustAppend(t, j, bytes.Repeat([]byte{byte('a' + i)}, size))
	}
	mustSync(t, j, 10)
	closeWithin(t, j)
	corrupt(t, d, "000000.seg", recordOffset(5, size)+10) // inside the header of record 6
	forgetMarks(t, d)

	j = open(t, small(d, newClock(), 0))
	defer closeWithin(t, j)
	if r := j.Recovery(); r.Last != 5 || len(r.Dropped) != 0 {
		t.Fatalf("recovery %+v: want the log to end at 5 with nothing listed", r)
	}
}

// A segment of any other layout is not free. Open refuses the directory and
// touches nothing: a downgrade, or two builds sharing a directory, destroys
// nothing. The layout has a magic and no number; a layout that is not this one
// is unreadable here, and is for its owner to remove.
func TestUnknownFormatIsRefusedAndKept(t *testing.T) {
	for _, size := range []int64{8192, 4096, 10} { // another size, the size of a segment of this journal, and less than a preamble
		d := crash.New()
		other := bytes.Repeat([]byte("OTHR"), int(size)/4+1)[:size]
		f, err := d.Create("000000.seg", size)
		if err != nil {
			t.Fatal(err)
		}
		f.WriteAt(other, 0)
		f.Sync()
		j, err := journal.Open("", small(d, newClock(), 0))
		if !errors.Is(err, journal.ErrFormat) {
			if err == nil {
				closeWithin(t, j)
			}
			t.Fatalf("size %d: Open of a directory with a segment of another layout: %v, want ErrFormat", size, err)
		}
		if err := journal.Discard("", small(d, newClock(), 0), 1, 1); !errors.Is(err, journal.ErrFormat) {
			t.Fatalf("size %d: Discard of the same directory: %v, want ErrFormat", size, err)
		}
		names, _ := d.List()
		if len(names) != 1 || names[0] != "000000.seg" {
			t.Fatalf("size %d: files after the refusal: %v", size, names)
		}
		got := make([]byte, size)
		g, _ := d.Open("000000.seg")
		g.ReadAt(got, 0)
		if !bytes.Equal(got, other) {
			t.Fatalf("size %d: the foreign segment was changed", size)
		}
	}
}

// A header torn by a crash is ours and free, whatever bytes of the write
// reached the media: it is recycled, not refused.
func TestTornHeadersAreFreeNotForeign(t *testing.T) {
	d := crash.New()
	j := open(t, small(d, newClock(), 0))
	for i := 1; i <= 3; i++ {
		mustAppend(t, j, bigRecord(i))
	}
	mustSync(t, j, 3)
	closeWithin(t, j)
	// A wipe torn after its first byte; a header with its checksum damaged; a
	// header whose magic was zeroed by a wipe that went further.
	for name, damage := range map[string]func(f []byte){
		"000000.seg": func(h []byte) { h[0] ^= 0xff },
		"000001.seg": func(h []byte) { clear(h[:headerSize-1]) },
	} {
		f, err := d.Open(name)
		if err != nil {
			continue
		}
		h := make([]byte, headerSize)
		f.ReadAt(h, 0)
		damage(h)
		f.WriteAt(h, 0)
	}
	if j, err := journal.Open("", small(d, newClock(), 0)); err != nil {
		t.Fatalf("Open with torn headers: %v", err)
	} else {
		closeWithin(t, j)
	}
}

// What holds across a restart is a bound on the time left, not a promise that
// the time left never grows. A record is counted as acknowledged no later than
// the restart, so a clock found behind where it was gives it the whole bound
// again, and a clock ahead of it gives less than it had.
func TestRestartWithTheClockBehindStartsTheBoundAgain(t *testing.T) {
	c1 := newClock()
	d := crash.New()
	j := open(t, small(d, c1, 0))
	seq := mustAppend(t, j, []byte("x"))
	mustSync(t, j, seq)
	c1.Advance(4900 * time.Millisecond) // 4.9 s of the 5 s have passed: 0.1 s left
	closeWithin(t, j)

	c2 := newClock() // a restart with the clock an hour behind what it was
	c2.now = c1.Now().Add(-time.Hour)
	j = open(t, small(d, c2, 0))
	defer closeWithin(t, j)
	ch := j.Due(ctx, 5*time.Second)
	c2.Advance(4900 * time.Millisecond)
	select {
	case <-ch:
		t.Fatal("the record came due before the whole bound had passed since the restart")
	default:
	}
	c2.Advance(100 * time.Millisecond)
	select {
	case <-ch:
	default:
		t.Fatal("the record did not come due a whole bound after the restart")
	}
	if s := j.State(); s.Age != 5*time.Second {
		t.Fatalf("Age %v after the restart, want the 5 s that have passed since it", s.Age)
	}
}

// A journal directory restored from an older backup opens under the same
// lineage and a newer Head and replays an older timeline's records; everything
// acknowledged after the backup is gone and nothing says so. The journal cannot
// know, so a restored directory is never to be used (see the document).
func TestRestoredBackupIsInvisible(t *testing.T) {
	d := crash.New()
	j := open(t, small(d, newClock(), 0))
	for i := 1; i <= 5; i++ {
		mustAppend(t, j, []byte{byte('A'), byte(i)})
	}
	mustSync(t, j, 5)
	closeWithin(t, j)
	backup := d.Image(d.Len(), crash.Variant{Kind: crash.Lost}) // the operator's backup

	j = open(t, small(d, newClock(), 0))
	for i := 6; i <= 9; i++ {
		mustAppend(t, j, []byte{byte('B'), byte(i)})
	}
	mustSync(t, j, 9)
	closeWithin(t, j) // 6 to 9 acknowledged, unpublished; the disk dies; the backup is restored

	j, err := journal.Open("", small(backup, newClock(), 0))
	if err != nil {
		t.Fatal(err)
	}
	defer closeWithin(t, j)
	if got := replayed(t, j); !reflect.DeepEqual(seqs(got), []uint64{1, 2, 3, 4, 5}) {
		t.Fatalf("restored backup replayed %v", seqs(got))
	}
	if r := j.Recovery(); len(r.Dropped) != 0 {
		t.Fatalf("a restored backup is not detectable, yet Open reports %+v", r)
	}
}
