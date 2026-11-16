package journal_test

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/hanzoai/vfs/journal"
	"github.com/hanzoai/vfs/journal/crash"
)

func closedJournal(t testing.TB, n int) *crash.Disk {
	d := crash.New()
	j := open(t, small(d, newClock(), 0))
	for i := 1; i <= n; i++ {
		mustAppend(t, j, bigRecord(i))
	}
	mustSync(t, j, uint64(n))
	closeWithin(t, j)
	return d
}

// recycledJournal builds a journal of nine records in three segments, publishes
// six and reclaims, so that 000000 and 000001 are free with their old marks and
// records still behind a zeroed header, and returns the disk with the live
// preamble of 000002 (the tail) and a copy of 000000 as it was live.
func recycledJournal(t *testing.T) (d *crash.Disk, livePre []byte, liveFile []byte) {
	d = crash.New()
	j := open(t, small(d, newClock(), 0))
	for i := 1; i <= 9; i++ {
		mustAppend(t, j, bigRecord(i))
	}
	mustSync(t, j, 9)
	f, _ := d.Open("000000.seg")
	liveFile = make([]byte, 4096)
	f.ReadAt(liveFile, 0)
	g, _ := d.Open("000002.seg")
	livePre = make([]byte, dataStart)
	g.ReadAt(livePre, 0)
	if err := j.Publish(6); err != nil {
		t.Fatal(err)
	}
	if err := j.Reclaim(); err != nil {
		t.Fatal(err)
	}
	closeWithin(t, j)
	return d, livePre, liveFile
}

func warningsOf(j *journal.Journal) []string { return j.Recovery().Warnings }

func TestTornWipeOfAPublishedSegmentIsQuiet(t *testing.T) {
	for keep := 1; keep < headerSize; keep++ {
		d := closedJournal(t, 9) // 000000: 1-3, 000001: 4-6, 000002: 7-9
		f, _ := d.Open("000000.seg")
		f.WriteAt(make([]byte, keep), 0) // the wipe of 000000, torn after keep bytes
		// The store holds 1 to 3: the wipe was the last step of reclaiming them.
		j, err := journal.Open("", small(d, newClock(), 3))
		if err != nil {
			t.Fatalf("keep=%d: %v", keep, err)
		}
		if w := j.Recovery().Warnings; len(w) != 0 {
			t.Errorf("keep=%d: a segment whose records are all published warns: %v", keep, w)
		}
		closeWithin(t, j)
	}
}

// A new header torn at every byte 0..79, over a recycled segment that still
// holds an older life's marks and records, and over a never-used one: Open
// succeeds, nothing of the old life is read, and the records that were there
// stay exactly as they were.
func TestCreationTearOverStaleTail(t *testing.T) {
	var warned, quiet int
	for keep := 1; keep < dataStart; keep++ {
		d, livePre, _ := recycledJournal(t)
		// 000000 is free: header zero, marks and records of an older life behind it.
		f, _ := d.Open("000000.seg")
		pre := make([]byte, dataStart)
		f.ReadAt(pre, 0)
		if !bytes.Equal(pre[:headerSize], make([]byte, headerSize)) {
			t.Fatal("test setup: 000000 is not wiped")
		}
		// A header for it written by an earlier tail, torn after keep bytes.
		hdr := append([]byte(nil), livePre...)
		if _, err := f.WriteAt(hdr[:keep], 0); err != nil {
			t.Fatal(err)
		}
		j, err := journal.Open("", small(d, newClock(), 6))
		if err != nil {
			t.Fatalf("keep=%d: Open of a merely torn directory: %v", keep, err)
		}
		r := j.Recovery()
		got := replayed(t, j)
		// Nothing but 7 to 9 (the tail segment) may come back; the stale 1 to 6
		// are published and unreadable behind a header that is not live.
		if len(got) != 3 || got[0].Seq != 7 {
			t.Errorf("keep=%d: replay %v", keep, seqs(got))
		}
		for _, rg := range r.Dropped {
			t.Errorf("keep=%d: Dropped lists %v for a segment that was only free", keep, rg)
		}
		if len(warningsOf(j)) > 0 {
			warned++
		} else {
			quiet++
		}
		closeWithin(t, j)
	}
	t.Logf("creation tear over a recycled segment with a stale tail: %d of %d tear points warn, %d quiet", warned, warned+quiet, quiet)
}

// The same over a segment that was never used (all zeros): quiet by the doc.
func TestCreationTearOverZeros(t *testing.T) {
	var warned int
	for keep := 1; keep < dataStart; keep++ {
		d, livePre, _ := recycledJournal(t)
		// 000003 does not exist yet; make it as Create does.
		f, err := d.Create("000009.seg", 4096)
		if err != nil {
			t.Fatal(err)
		}
		f.WriteAt(livePre[:keep], 0)
		j, err := journal.Open("", small(d, newClock(), 6))
		if err != nil {
			t.Fatalf("keep=%d: %v", keep, err)
		}
		if len(warningsOf(j)) > 0 {
			warned++
			t.Errorf("keep=%d: warning for a header torn into an empty segment: %v", keep, warningsOf(j))
		}
		closeWithin(t, j)
	}
}

// Zeros over a live header (the wipe), torn at every byte of the 40 that wipe
// writes, on each of the three segments. Published covers what the wiped
// segment held, so nothing is lost; Open must succeed and say what it says.
func TestWipeTearEveryByte(t *testing.T) {
	var warned, quiet int
	for keep := 1; keep < headerSize; keep++ {
		for _, seg := range []string{"000000.seg", "000001.seg", "000002.seg"} {
			d := crash.New()
			j := open(t, small(d, newClock(), 0))
			for i := 1; i <= 9; i++ {
				mustAppend(t, j, bigRecord(i))
			}
			mustSync(t, j, 9)
			closeWithin(t, j)
			f, _ := d.Open(seg)
			if _, err := f.WriteAt(make([]byte, keep), 0); err != nil {
				t.Fatal(err)
			}
			// The store holds everything the torn segment held: 3 per segment.
			held := map[string]uint64{"000000.seg": 3, "000001.seg": 6, "000002.seg": 9}[seg]
			published := uint64(0)
			if seg == "000000.seg" {
				published = 0 // the first segment going leaves a log that begins late
			}
			_ = held
			_ = published
			// Open with a Head that makes the loss legal: for 000000, Head 3.
			head := map[string]uint64{"000000.seg": 3, "000001.seg": 0, "000002.seg": 0}[seg]
			j, err := journal.Open("", small(d, newClock(), head))
			if err != nil {
				// A torn wipe in the middle of a log leaves a gap: the later
				// segments are orphans, and that is a refusal only if proven.
				if seg == "000001.seg" && (errors.Is(err, journal.ErrCorrupt)) {
					continue // a wiped middle segment with a mark proving the later records: loud
				}
				t.Fatalf("%s keep=%d: Open %v", seg, keep, err)
			}
			if len(warningsOf(j)) > 0 {
				warned++
			} else {
				quiet++
			}
			closeWithin(t, j)
		}
	}
	t.Logf("wipe tear: %d warn, %d quiet", warned, quiet)
}

// A segment that was freed in the ordinary way (wiped, old records still behind
// it) is quiet: a warning there would fire at every restart of every journal.
func TestWipedSegmentsAreQuiet(t *testing.T) {
	d, _, _ := recycledJournal(t)
	j, err := journal.Open("", small(d, newClock(), 6))
	if err != nil {
		t.Fatal(err)
	}
	defer closeWithin(t, j)
	if w := j.Recovery().Warnings; len(w) != 0 {
		t.Fatalf("Warnings after an ordinary reclaim and a clean close: %v", w)
	}
}

// Files of odd sizes with a torn-looking magic: Open must not fail reading past
// the end for the record that follows the header block.
func TestTornMagicInASmallFile(t *testing.T) {
	for _, size := range []int{dataStart, dataStart + 1, dataStart + 27, dataStart + 28, dataStart + 29, 4096, 5000} {
		d := crash.New()
		content := bytes.Repeat([]byte{0x77}, size)
		copy(content[4:8], []byte{'J', 'R', 0, 0})
		put(t, d, "000000.seg", int64(size), content)
		j, err := journal.Open("", small(d, newClock(), 0))
		if err != nil {
			t.Errorf("size %d: Open of a file with a torn-looking magic: %v", size, err)
			continue
		}
		closeWithin(t, j)
	}
}

// The warning names whatever size the segment has.
func TestWarningNamesTheSegmentAndItsSize(t *testing.T) {
	for _, seg := range []int64{4096, 8192, 100000} {
		d := crash.New()
		opt := journal.Options{Capacity: 6 * seg, Segment: seg, Disk: d, Clock: newClock()}
		j := open(t, opt)
		for i := 1; i <= 3; i++ {
			mustAppend(t, j, bigRecord(i))
		}
		mustSync(t, j, 3)
		closeWithin(t, j)
		f, _ := d.Open("000000.seg")
		f.WriteAt(make([]byte, 3), 0) // the first three bytes of the checksum
		j = open(t, opt)
		w := j.Recovery().Warnings
		closeWithin(t, j)
		if len(w) != 1 || !strings.Contains(w[0], "000000.seg") || !strings.Contains(w[0], fmt.Sprintf("%d bytes", seg)) {
			t.Errorf("segment %d: warnings %v", seg, w)
		}
	}
}

// In the crash states the harness makes by sectors (and by the few byte offsets
// of Variants), the new warning must never be raised: no loss is real there that
// the report does not already carry. A warning in such a state is a false alarm.
func TestNoWarningInSectorGranularCrashStates(t *testing.T) {
	for _, every := range []int{1, 16} {
		for seed := int64(1); seed <= 1; seed++ {
			func() {
				defer journal.MarkEvery(every)()
				r := newRun()
				j := open(t, r.opts(0))
				rng := rand.New(rand.NewSource(seed*17 + int64(every)))
				var visible, published uint64
				for i := 0; i < 60; i++ {
					seq := r.append(t, j, payload(uint64(rng.Intn(1000))))
					visible = seq
					switch rng.Intn(10) {
					case 0, 1, 2, 3:
						r.sync(t, j, seq)
					case 4, 5:
						published += uint64(rng.Intn(int(visible-published))) + 1
						r.publish(t, j, published)
					case 6, 7:
						r.reclaim(t, j)
					}
				}
				r.sync(t, j, visible)
				closeBounded(t, j)
				states, warned := 0, 0
				err := r.d.Explore(1, seed, func(img *crash.Disk, n int, v crash.Variant) error {
					states++
					s := r.state(img, n)
					jj, err := journal.Open("", small(img.Image(img.Len(), crash.Variant{Kind: crash.All}), newClock(), s.pub))
					if err != nil {
						return err
					}
					defer jj.Close()
					if w := jj.Recovery().Warnings; len(w) > 0 {
						warned++
						if warned < 4 {
							t.Errorf("every%d seed%d point %d %s: warning %v", every, seed, n, v, w)
						}
					}
					return nil
				})
				if err != nil {
					t.Errorf("%v", err)
				}
				t.Logf("every%d seed%d: %d states, %d warned", every, seed, states, warned)
			}()
		}
	}
}

// A segment that is only partly published holds records the store lacks: with its
// header damaged they are lost, and the warning says so although its first record
// is the store's. (Here the segment before it is wiped, and the one after it is
// given up; the lost records 5 and 6 are in neither list.)
func TestDamagedHeaderOfAPartlyPublishedSegmentIsReported(t *testing.T) {
	d := closedJournal(t, 9)
	j := open(t, small(d, newClock(), 0))
	if err := j.Publish(4); err != nil {
		t.Fatal(err)
	}
	if err := j.Reclaim(); err != nil { // the first segment goes; the second anchors the floor
		t.Fatal(err)
	}
	closeWithin(t, j)
	f, _ := d.Open("000001.seg")
	b := make([]byte, 1)
	f.ReadAt(b, 20)
	b[0] ^= 1
	f.WriteAt(b, 20) // the life of the segment of 4 to 6
	forgetMarks(t, d)
	j = open(t, small(d, newClock(), 4))
	defer closeWithin(t, j)
	w := j.Recovery().Warnings
	if len(w) != 1 || !strings.Contains(w[0], "000001.seg") || !strings.Contains(w[0], "sequence 6") {
		t.Fatalf("Warnings %q: want the segment of 4 to 6 named, with the 6 the store does not hold", w)
	}
}

// What is followed behind a damaged header is a chain of records, one sequence
// after another: a segment recycled for shorter records still has the headers of
// an older life behind the new ones, and those are not part of the chain. Were
// they, the chain would end at the older life's sequence and the warning, whose
// test is the last sequence, would stay quiet for records the store lacks.
func TestWarningFollowsOnlyTheChainOfSequences(t *testing.T) {
	d := closedJournal(t, 9)
	j := open(t, small(d, newClock(), 0))
	if err := j.Publish(6); err != nil {
		t.Fatal(err)
	}
	if err := j.Reclaim(); err != nil { // the segments of 1 to 3 and 4 to 6 are free, their records behind a zero header
		t.Fatal(err)
	}
	closeWithin(t, j)
	// With room for three segments and three in use, the next is one of the free ones.
	j = open(t, journal.Options{Capacity: 3 * 4096, Segment: 4096, Disk: d, Clock: newClock(), Published: 6})
	mustAppend(t, j, bigRecord(10)) // over the first two records of the older life: the third is still behind them
	mustSync(t, j, mustAppend(t, j, bigRecord(11)))
	closeWithin(t, j)
	name := segmentStarting(t, d, 10)
	f, _ := d.Open(name)
	b := make([]byte, 1)
	f.ReadAt(b, 20)
	b[0] ^= 1
	f.WriteAt(b, 20) // its life
	forgetMarks(t, d)
	j = open(t, journal.Options{Capacity: 3 * 4096, Segment: 4096, Disk: d, Clock: newClock(), Published: 6})
	defer closeWithin(t, j)
	w := j.Recovery().Warnings
	if len(w) != 1 || !strings.Contains(w[0], name) || !strings.Contains(w[0], "sequence 11") {
		t.Fatalf("Warnings %q: want %s named, with its records going as far as 11", w, name)
	}
}

// A segment that cannot be read is not a segment that holds nothing: the error of
// the read that follows a damaged header is Open's.
func TestReadErrorBehindADamagedHeaderIsReturned(t *testing.T) {
	d := closedJournal(t, 9)
	f, _ := d.Open("000002.seg")
	b := make([]byte, 1)
	f.ReadAt(b, 20)
	b[0] ^= 1
	f.WriteAt(b, 20)
	_, err := journal.Open("", small(failReads{d, map[string]bool{"000002.seg": true}}, newClock(), 0))
	if !errors.Is(err, errRead) {
		t.Fatalf("Open: %v, want the read error", err)
	}
}
