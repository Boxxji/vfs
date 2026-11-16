package journal_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"testing"

	"github.com/hanzoai/vfs/journal"
	"github.com/hanzoai/vfs/journal/crash"
)

// The layout the tests reach into is the one the document specifies: a 40-byte
// header, two 20-byte marks, then records of a 28-byte header and the payload.
const (
	headerAt   = 0
	markAt     = 40
	markLen    = 20
	recordsAt  = 80
	recordHead = 28
)

// segmentStarting is the segment file whose header says it begins at first.
func segmentStarting(t testing.TB, d *crash.Disk, first uint64) string {
	t.Helper()
	names, err := d.List()
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range names {
		if binary.LittleEndian.Uint64(peek(t, d, name, 8, 8)) == first {
			return name
		}
	}
	t.Fatalf("no segment begins at %d", first)
	return ""
}

func peek(t testing.TB, d *crash.Disk, name string, off, n int64) []byte {
	t.Helper()
	f, err := d.Open(name)
	if err != nil {
		t.Fatal(err)
	}
	b := make([]byte, n)
	if _, err := f.ReadAt(b, off); err != nil {
		t.Fatal(err)
	}
	return b
}

func poke(t testing.TB, d *crash.Disk, name string, off int64, b []byte) {
	t.Helper()
	f, err := d.Open(name)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt(b, off); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
}

// flip changes one byte of a file.
func flip(t testing.TB, d *crash.Disk, name string, off int64) {
	t.Helper()
	b := peek(t, d, name, off, 1)
	poke(t, d, name, off, []byte{b[0] ^ 0xff})
}

// through is the sequence a mark slot of a segment claims, 0 if the slot is empty.
func through(t testing.TB, d *crash.Disk, name string, slot int64) uint64 {
	t.Helper()
	return binary.LittleEndian.Uint64(peek(t, d, name, markAt+slot*markLen+4, 8))
}

// The two mark slots are written alternately so that a mark torn in flight
// leaves the one before it standing. Tear the newer and put damage under what
// the older proves: the journal must still know those records were durable.
func TestATornMarkLeavesTheOtherSlot(t *testing.T) {
	defer journal.MarkEvery(1)()
	d := crash.New()
	j := open(t, small(d, newClock(), 0))
	for i := 1; i <= 3; i++ {
		mustSync(t, j, mustAppend(t, j, []byte{byte('0' + i)}))
	}
	closeWithin(t, j)

	name := segmentStarting(t, d, 1)
	a, b := through(t, d, name, 0), through(t, d, name, 1)
	if a == 0 || b == 0 || a == b {
		t.Fatalf("setup: the slots claim %d and %d", a, b)
	}
	newer := int64(0)
	if b > a {
		newer = 1
	}
	flip(t, d, name, markAt+newer*markLen) // a checksum byte: the slot no longer validates
	flip(t, d, name, recordsAt+recordHead) // the first payload

	if _, err := journal.Open("", small(d, newClock(), 0)); !errors.Is(err, journal.ErrCorrupt) {
		t.Fatalf("damage under a surviving mark, torn newer slot: %v", err)
	}
}

// A log with nothing readable in it, after a mark said records were durable, is
// corruption, not an empty log.
func TestAnEmptyLogAfterAMarkIsCorruption(t *testing.T) {
	d := crash.New()
	j := open(t, small(d, newClock(), 0))
	for i := 1; i <= 3; i++ {
		mustSync(t, j, mustAppend(t, j, []byte{byte('0' + i)}))
	}
	closeWithin(t, j)
	flip(t, d, segmentStarting(t, d, 1), recordsAt+recordHead)
	if _, err := journal.Open("", small(d, newClock(), 0)); !errors.Is(err, journal.ErrCorrupt) {
		t.Fatalf("every record unreadable under a mark: %v", err)
	}
}

// The first segments of a log can be lost with the tail in a power cut, and then
// nothing in them was acknowledged. When a finished sync is proven past the
// store they were acknowledged, and a log that begins later is corruption.
func TestALogThatBeginsLateAfterAcknowledgedRecordsIsCorruption(t *testing.T) {
	defer journal.MarkEvery(1)()
	d := crash.New()
	j := open(t, small(d, newClock(), 0))
	var last uint64
	for i := 0; i < 8; i++ {
		last = mustAppend(t, j, bytes.Repeat([]byte{byte(i)}, 1000))
	}
	mustSync(t, j, last)
	closeWithin(t, j)
	poke(t, d, segmentStarting(t, d, 1), headerAt, make([]byte, 40))
	if _, err := journal.Open("", small(d, newClock(), 0)); !errors.Is(err, journal.ErrCorrupt) {
		t.Fatalf("the start of the log gone, records through %d acknowledged: %v", last, err)
	}
}

// Reclaim makes the floor durable before it wipes anything, so a store older
// than what was reclaimed is never read as a log that happens to start late.
// Every crash state of the workload: Open with an empty store either refuses or
// returns every acknowledged record.
func TestAStaleHeadAfterReclaimIsNeverReadAsAnEmptyLog(t *testing.T) {
	defer journal.MarkEvery(1000)() // no mark but the ones a segment starts with
	r := newRun()
	j := open(t, r.opts(0))
	var last uint64
	for i := 0; i < 7; i++ {
		last = r.append(t, j, bytes.Repeat([]byte{byte(i)}, 1000))
	}
	r.sync(t, j, last)
	r.publish(t, j, 3)
	r.reclaim(t, j)
	if err := closeBounded(t, j); err != nil {
		t.Fatal(err)
	}
	refused := 0
	err := r.d.Explore(1, 11, func(img *crash.Disk, n int, v crash.Variant) error {
		if r.state(img, n).ack < last {
			return nil
		}
		j2, err := journal.Open("", r.opts2(img, 0))
		if err != nil {
			refused++
			return nil
		}
		defer j2.Close()
		if got := replayed(t, j2); uint64(len(got)) != last {
			return fmt.Errorf("an empty store read %d of the %d acknowledged records", len(got), last)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("%v\n%s", err, r.d.Log(min(r.d.Len(), 40)))
	}
	if refused == 0 {
		t.Fatal("no crash state after the Reclaim refused an empty store")
	}
}
