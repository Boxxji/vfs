package journal_test

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hanzoai/vfs/journal"
	"github.com/hanzoai/vfs/journal/crash"
)

var big = bytes.Repeat([]byte("B"), 1000) // three records to a 4 KiB segment

func bigRecord(i int) []byte { return append([]byte{byte('a' + i%26)}, big[1:]...) }

// The segment layout, as the tests need it: a header of 40 bytes, two marks of
// 20, then the records.
const (
	headerSize = 40
	markSize   = 20
	dataStart  = headerSize + 2*markSize
)

// recordOffset is where record n (0-based) starts in the first segment when
// every payload has the same size.
func recordOffset(n, size int) int64 { return int64(dataStart + n*(28+size)) }

// forgetMarks zeroes the marks of every segment: what a power loss leaves when
// it takes the marks that were still volatile. Damage then has no proof of a
// finished sync past it, and recovery treats it as a torn tail.
func forgetMarks(t testing.TB, d *crash.Disk) {
	t.Helper()
	names, _ := d.List()
	for _, name := range names {
		f, err := d.Open(name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.WriteAt(make([]byte, 2*markSize), headerSize); err != nil {
			t.Fatal(err)
		}
	}
}

func corrupt(t testing.TB, d *crash.Disk, name string, off int64) {
	t.Helper()
	f, err := d.Open(name)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte{0xFF, 0xFE}, off); err != nil {
		t.Fatal(err)
	}
}

// A damaged record at the end of what a sync proved is a torn tail: Replay
// returns the records before it and nothing after it, and new records continue
// from the damaged one's number without bringing the old ones back. Open says
// what it kept, and the valid records behind the damage that it left unread.
func TestTornTail(t *testing.T) {
	d := crash.New()
	j := open(t, small(d, newClock(), 0))
	const size = 100
	for i := 1; i <= 10; i++ {
		mustAppend(t, j, bytes.Repeat([]byte{byte('a' + i)}, size))
	}
	mustSync(t, j, 10)
	closeWithin(t, j)

	corrupt(t, d, "000000.seg", recordOffset(5, size)+40) // record 6, inside its payload
	forgetMarks(t, d)                                     // no later sync finished: nothing proves record 6 was durable

	j = open(t, small(d, newClock(), 0))
	if r := j.Recovery(); r.First != 1 || r.Last != 5 || r.Proven != 0 || !reflect.DeepEqual(r.Dropped, []journal.Range{{First: 6, Last: 10}}) {
		t.Fatalf("recovery %+v: want 1 through 5 kept and 6 through 10 dropped", r)
	}
	got := replayed(t, j)
	if want := []uint64{1, 2, 3, 4, 5}; !reflect.DeepEqual(seqs(got), want) {
		t.Fatalf("replay after damage %v, want %v", seqs(got), want)
	}
	if seq := mustAppend(t, j, []byte("new six")); seq != 6 {
		t.Fatalf("numbering after damage: %d, want 6", seq)
	}
	mustSync(t, j, 6)
	closeWithin(t, j)

	j = open(t, small(d, newClock(), 0))
	defer closeWithin(t, j)
	got = replayed(t, j)
	if len(got) != 6 || got[5] != (entry{6, "new six"}) {
		t.Fatalf("replay after the new record %v: the old record 6 or 7 came back", seqs(got))
	}
}

// Damage before the last mark is not a torn tail. A mark is written after the
// sync it describes, so when one survives, every record it claims was on the
// media: a record that is not there now is lost to the media, not to a crash.
// Open refuses, and wipes nothing.
func TestDamageBeforeTheMarkIsCorruption(t *testing.T) {
	d := crash.New()
	j := open(t, small(d, newClock(), 0))
	const size = 100
	for i := 1; i <= 10; i++ {
		mustAppend(t, j, bytes.Repeat([]byte{byte('a' + i)}, size))
	}
	mustSync(t, j, 10)
	closeWithin(t, j) // writes the mark that proves 10

	corrupt(t, d, "000000.seg", recordOffset(5, size)+40)
	for try := 0; try < 2; try++ { // the refusal changes nothing, so it is the same every time
		if _, err := journal.Open("", small(d, newClock(), 0)); !errors.Is(err, journal.ErrCorrupt) {
			t.Fatalf("Open %d: %v, want ErrCorrupt", try, err)
		}
	}
}

// A hole in an early segment makes every later segment unreachable, even
// though their records are intact: they follow a gap, and with no proof that a
// sync finished past it they were never reported durable. They are discarded,
// and Open says so; the numbers they carry can be reused. With the proof, the
// same hole is corruption.
func TestHoleDiscardsLaterSegments(t *testing.T) {
	d := crash.New()
	j := open(t, small(d, newClock(), 0))
	for i := 1; i <= 9; i++ {
		mustAppend(t, j, bigRecord(i))
	}
	mustSync(t, j, 9)
	closeWithin(t, j)

	corrupt(t, d, "000001.seg", recordOffset(1, len(big))+500) // record 5, the middle of segment two
	if _, err := journal.Open("", small(d, newClock(), 0)); !errors.Is(err, journal.ErrCorrupt) {
		t.Fatalf("a hole before the mark: %v", err)
	}
	forgetMarks(t, d)

	j = open(t, small(d, newClock(), 0))
	if r := j.Recovery(); r.Last != 4 || !reflect.DeepEqual(r.Dropped, []journal.Range{{First: 5, Last: 6}, {First: 7, Last: 9}}) {
		t.Fatalf("recovery %+v: want records through 4 kept, 5 to 6 (behind the damage) and 7 to 9 dropped", r)
	}
	got := replayed(t, j)
	if want := []uint64{1, 2, 3, 4}; !reflect.DeepEqual(seqs(got), want) {
		t.Fatalf("replay %v, want %v", seqs(got), want)
	}
	for i := 5; i <= 8; i++ {
		mustAppend(t, j, []byte{byte('0' + i)})
	}
	mustSync(t, j, 8)
	closeWithin(t, j)

	j = open(t, small(d, newClock(), 0))
	defer closeWithin(t, j)
	got = replayed(t, j)
	if len(got) != 8 {
		t.Fatalf("replay %v, want 1 to 8", seqs(got))
	}
	for i, e := range got[4:] {
		if want := string([]byte{byte('0' + 5 + i)}); e.Data != want {
			t.Fatalf("record %d is %q, want %q", e.Seq, e.Data, want)
		}
	}
}

// The store holds records the journal's disk does not have: the disk lost
// synced records, or the Head belongs to something else. Open refuses and
// changes nothing. A directory with no journal in it is another matter: a new
// disk for a volume that already has history. Numbering continues after the
// store, and the new records survive a restart.
func TestStoreAheadOfTheLog(t *testing.T) {
	d := crash.New()
	j := open(t, small(d, newClock(), 0))
	for i := 1; i <= 5; i++ {
		mustAppend(t, j, []byte{byte('0' + i)})
	}
	mustSync(t, j, 5)
	closeWithin(t, j)

	if _, err := journal.Open("", small(d, newClock(), 9)); !errors.Is(err, journal.ErrMismatch) {
		t.Fatalf("Open with the store at 9 and a log ending at 5: %v", err)
	}
	j = open(t, small(d, newClock(), 0))
	if got := replayed(t, j); len(got) != 5 {
		t.Fatalf("the refusal cost records: %v", seqs(got))
	}
	closeWithin(t, j)

	fresh := crash.New()
	j = open(t, small(fresh, newClock(), 9))
	if m := j.Marks(); m != (journal.Marks{Visible: 9, Durable: 9, Published: 9}) {
		t.Fatalf("marks %+v", m)
	}
	if got := replayed(t, j); len(got) != 0 {
		t.Fatalf("replay %v: records the store holds", seqs(got))
	}
	if seq := mustAppend(t, j, []byte("ten")); seq != 10 {
		t.Fatalf("seq %d, want 10", seq)
	}
	mustSync(t, j, 10)
	closeWithin(t, j)

	j = open(t, small(fresh, newClock(), 9))
	defer closeWithin(t, j)
	if got := replayed(t, j); !reflect.DeepEqual(got, []entry{{10, "ten"}}) {
		t.Fatalf("replay %v", got)
	}
}

// The Head is the truth, and a Head that does not fit the journal is refused,
// never acted on. Records 7 to 9 are synced and unpublished; the Volume mounts
// with a zero Head (a path that failed to read it), then with a stale one, and
// the records are still there for the mount with the right one.
func TestWrongPublishedIsRefusedAndHarmless(t *testing.T) {
	d := crash.New()
	j := open(t, small(d, newClock(), 0))
	for i := 1; i <= 9; i++ {
		mustAppend(t, j, bigRecord(i))
	}
	mustSync(t, j, 9)
	publish(t, j, 6)
	if err := j.Reclaim(); err != nil { // the segments of 1 to 6 are gone
		t.Fatal(err)
	}
	closeWithin(t, j)

	for _, wrong := range []uint64{0, 2, 5} {
		if _, err := journal.Open("", small(d, newClock(), wrong)); !errors.Is(err, journal.ErrMismatch) {
			t.Fatalf("Open with the store at %d: %v, want ErrMismatch", wrong, err)
		}
	}
	j = open(t, small(d, newClock(), 6))
	if got, want := seqs(replayed(t, j)), []uint64{7, 8, 9}; !reflect.DeepEqual(got, want) {
		t.Fatalf("records 7 to 9 after the wrong mounts and the right one: %v", got)
	}
	closeWithin(t, j)

	// Giving the records up is a separate, named act, and says what it gives up.
	if err := journal.Discard("", small(d, newClock(), 0), 7, 8); !errors.Is(err, journal.ErrRange) {
		t.Fatalf("Discard of a range the journal does not hold: %v", err)
	}
	j = open(t, small(d, newClock(), 6))
	if got := replayed(t, j); len(got) != 3 {
		t.Fatalf("a refused Discard cost records: %v", seqs(got))
	}
	closeWithin(t, j)
	if err := journal.Discard("", small(d, newClock(), 0), 7, 9); err != nil {
		t.Fatal(err)
	}
	j = open(t, small(d, newClock(), 6))
	defer closeWithin(t, j)
	if got := replayed(t, j); len(got) != 0 {
		t.Fatalf("Discard left %v", seqs(got))
	}
	if seq := mustAppend(t, j, []byte("x")); seq != 7 {
		t.Fatalf("numbering after Discard: %d, want 7", seq)
	}
}

// Sequence numbers mean something only inside one history. The old owner's
// records 9 and 10, never published, must not be replayed over a newer Head at
// 8: the journal carries the identity it was written under, Open refuses a
// directory of another, and Discard is the way to give it up.
func TestAnotherLineageIsRefused(t *testing.T) {
	d := crash.New()
	opt := small(d, newClock(), 0)
	opt.Lineage = []byte("volume 7, round 1")
	j := open(t, opt)
	var last uint64
	for i := 1; i <= 10; i++ {
		last = mustAppend(t, j, []byte{byte('A'), byte(i)})
	}
	mustSync(t, j, last)
	closeWithin(t, j)

	newer := small(d, newClock(), 8)
	newer.Lineage = []byte("volume 7, round 2")
	if _, err := journal.Open("", newer); !errors.Is(err, journal.ErrLineage) {
		t.Fatalf("Open under another lineage: %v", err)
	}
	if _, err := journal.Open("", small(d, newClock(), 8)); !errors.Is(err, journal.ErrLineage) {
		t.Fatalf("Open with no lineage at all: %v", err)
	}
	same := opt
	same.Published = 0
	j = open(t, same)
	if got := replayed(t, j); len(got) != 10 {
		t.Fatalf("the refusals cost records: %v", seqs(got))
	}
	closeWithin(t, j)

	if err := journal.Discard("", newer, 1, 10); err != nil {
		t.Fatal(err)
	}
	j = open(t, newer)
	defer closeWithin(t, j)
	if got := replayed(t, j); len(got) != 0 {
		t.Fatalf("the old lineage's records %v are in the new one", seqs(got))
	}
	if seq := mustAppend(t, j, []byte("B")); seq != 9 {
		t.Fatalf("numbering after Discard: %d, want 9", seq)
	}
}

// Records of this run are visible to Replay before any Sync.
func TestReplaySeesUnsyncedRecords(t *testing.T) {
	j := open(t, small(crash.New(), newClock(), 0))
	defer closeWithin(t, j)
	for i := 1; i <= 3; i++ {
		mustAppend(t, j, []byte{byte('0' + i)})
	}
	if got := replayed(t, j); len(got) != 3 {
		t.Fatalf("replay %v", seqs(got))
	}
}

// Replay yields only what comes after the published watermark, and an error
// from the callback stops it.
func TestReplayAfterPublished(t *testing.T) {
	j := open(t, small(crash.New(), newClock(), 0))
	defer closeWithin(t, j)
	for i := 1; i <= 6; i++ {
		mustAppend(t, j, []byte{byte('0' + i)})
	}
	publish(t, j, 4)
	got := replayed(t, j)
	if want := []uint64{5, 6}; !reflect.DeepEqual(seqs(got), want) {
		t.Fatalf("replay %v, want %v", seqs(got), want)
	}
	stop := errors.New("stop")
	n := 0
	if err := j.Replay(func(journal.Record) error { n++; return stop }); err != stop || n != 1 {
		t.Fatalf("Replay returned %v after %d records", err, n)
	}
}

// Replay holds Reclaim back: a segment is not recycled under a reader.
func TestReplayBlocksReclaim(t *testing.T) {
	d := crash.New()
	wiped := make(chan struct{}, 8)
	d.Intercept(func(e crash.Event) error {
		if e.Kind == "write" && e.Off == 0 && e.Len == headerSize { // the header of a segment being freed
			wiped <- struct{}{}
		}
		return nil
	})
	j := open(t, small(d, newClock(), 0))
	defer closeWithin(t, j)
	for i := 1; i <= 12; i++ {
		mustAppend(t, j, bigRecord(i))
	}
	mustSync(t, j, 12)
	publish(t, j, 3)

	inside := make(chan struct{})
	release := make(chan struct{})
	replayDone := make(chan []uint64)
	go func() {
		var got []uint64
		j.Replay(func(r journal.Record) error {
			if len(got) == 0 {
				close(inside)
				<-release
			}
			got = append(got, r.Seq)
			return nil
		})
		replayDone <- got
	}()
	<-inside
	publish(t, j, 9)
	reclaimed := make(chan error)
	go func() { reclaimed <- j.Reclaim() }()
	select {
	case <-wiped:
		t.Fatal("Reclaim recycled a segment while Replay was reading")
	case <-time.After(50 * time.Millisecond): // a wait for something that must not happen
	}
	close(release)
	if got := <-replayDone; len(got) != 9 || got[0] != 4 || got[8] != 12 {
		t.Fatalf("replay %v, want 4 to 12", got)
	}
	if err := <-reclaimed; err != nil {
		t.Fatal(err)
	}
	select {
	case <-wiped:
	default:
		t.Fatal("Reclaim never ran")
	}
}

// Close makes everything appended durable.
func TestCloseIsDurable(t *testing.T) {
	d := crash.New()
	j := open(t, small(d, newClock(), 0))
	for i := 1; i <= 5; i++ {
		mustAppend(t, j, []byte{byte('0' + i)})
	}
	if err := closeBounded(t, j); err != nil {
		t.Fatal(err)
	}
	img := d.Image(d.Len(), crash.Variant{Kind: crash.Lost})
	j = open(t, small(img, newClock(), 0))
	defer closeWithin(t, j)
	if got := replayed(t, j); len(got) != 5 {
		t.Fatalf("after Close and a crash that keeps only synced data: %v", seqs(got))
	}
}

// What a process crash leaves is everything it wrote, and replay is the same
// however often it runs.
func TestProcessCrashKeepsSyncedRecords(t *testing.T) {
	d := crash.New()
	j := open(t, small(d, newClock(), 0))
	for i := 1; i <= 20; i++ {
		seq := mustAppend(t, j, payload(uint64(i)))
		if i == 14 {
			mustSync(t, j, seq)
		}
	}
	// The process dies here: no Close, nothing more is written.
	for _, v := range []crash.Variant{{Kind: crash.Lost}, {Kind: crash.All}} {
		img := d.Image(d.Len(), v)
		r := open(t, small(img, newClock(), 0))
		got := replayed(t, r)
		if len(got) < 14 {
			t.Fatalf("%v: %d records recovered, 14 were synced", v, len(got))
		}
		for _, e := range got {
			if e.Data != string(payload(e.Seq)) {
				t.Fatalf("%v: record %d damaged", v, e.Seq)
			}
		}
		if again := replayed(t, r); !reflect.DeepEqual(again, got) {
			t.Fatalf("%v: second replay differs", v)
		}
		r.Close()
	}
	closeWithin(t, j)
}

func TestOpenOptions(t *testing.T) {
	for name, opt := range map[string]journal.Options{
		"no capacity":      {Segment: 4096},
		"one segment":      {Capacity: 4096, Segment: 4096},
		"tiny segment":     {Capacity: 100, Segment: 10},
		"capacity too low": {Capacity: 5000, Segment: 4096},
	} {
		opt.Disk = crash.New()
		if j, err := journal.Open("", opt); err == nil {
			closeWithin(t, j)
			t.Errorf("%s: Open accepted %+v", name, opt)
		}
	}
}

func TestPayloadLimits(t *testing.T) {
	j := open(t, small(crash.New(), newClock(), 0))
	defer closeWithin(t, j)
	if _, err := j.Append(ctx, make([]byte, 4096)); !errors.Is(err, journal.ErrTooLarge) {
		t.Fatalf("payload of a segment: %v", err)
	}
	largest := make([]byte, 4096-dataStart-28)
	for i := range largest {
		largest[i] = byte(i)
	}
	seq := mustAppend(t, j, largest)
	empty := mustAppend(t, j, nil)
	mustSync(t, j, empty)
	got := replayed(t, j)
	if len(got) != 2 || got[0].Seq != seq || got[0].Data != string(largest) || got[1].Data != "" {
		t.Fatalf("largest and empty payloads came back as %d records", len(got))
	}
	c, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := j.Append(c, []byte("x")); !errors.Is(err, context.Canceled) {
		t.Fatalf("Append with a cancelled context: %v", err)
	}
}

// Files that are not segments are left alone, and a segment file of another
// size is still read but not reused.
func TestForeignFiles(t *testing.T) {
	d := crash.New()
	if _, err := d.Create("README", 10); err != nil {
		t.Fatal(err)
	}
	j := open(t, small(d, newClock(), 0))
	mustSync(t, j, mustAppend(t, j, []byte("kept")))
	closeWithin(t, j)
	names, _ := d.List()
	found := false
	for _, n := range names {
		found = found || n == "README"
	}
	if !found {
		t.Fatal("a file that is not a segment was removed")
	}

	other := small(d, newClock(), 0)
	other.Segment, other.Capacity = 8192, 6*8192
	j = open(t, other)
	defer closeWithin(t, j)
	if got := replayed(t, j); !reflect.DeepEqual(got, []entry{{1, "kept"}}) {
		t.Fatalf("records of a segment of another size: %v", got)
	}
}

func TestStatsCountWhatHappened(t *testing.T) {
	j := open(t, small(crash.New(), newClock(), 0))
	defer closeWithin(t, j)
	a := mustAppend(t, j, []byte("abc"))
	mustSync(t, j, a)
	s := j.Stats()
	if s.Records != 1 || s.Bytes != 28+3 || s.Waited != 1 || s.Rounds < 1 {
		t.Fatalf("stats %+v", s)
	}
	var released uint64
	for _, c := range s.Groups {
		released += c
	}
	if released != 1 || s.Groups[1] != 1 {
		t.Fatalf("a group of one waiter is Groups[1]: %v", s.Groups)
	}
}

// volatile copies a disk's files onto a new disk through writes, so they
// count as written but not yet synced.
func volatile(t testing.TB, from *crash.Disk) *crash.Disk {
	t.Helper()
	to := crash.New()
	names, _ := from.List()
	for _, name := range names {
		src, err := from.Open(name)
		if err != nil {
			t.Fatal(err)
		}
		data := make([]byte, src.Size())
		if _, err := src.ReadAt(data, 0); err != nil {
			t.Fatal(err)
		}
		dst, err := to.Create(name, src.Size())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := dst.WriteAt(data, 0); err != nil {
			t.Fatal(err)
		}
	}
	return to
}

// Open reports what it recovered as durable, so it makes it durable: the
// records may have been read from a page cache that a power loss will empty.
func TestOpenMakesRecoveredRecordsDurable(t *testing.T) {
	d := crash.New()
	j := open(t, small(d, newClock(), 0))
	for i := 1; i <= 5; i++ {
		mustAppend(t, j, []byte{byte('0' + i)})
	}
	closeWithin(t, j)
	v := volatile(t, d)

	j = open(t, small(v, newClock(), 0))
	if m := j.Marks(); m.Durable != 5 {
		t.Fatalf("marks %+v", m)
	}
	img := v.Image(v.Len(), crash.Variant{Kind: crash.Lost})
	closeWithin(t, j)
	r := open(t, small(img, newClock(), 0))
	defer r.Close()
	if got := replayed(t, r); len(got) != 5 {
		t.Fatalf("Open called %d records durable and a power loss kept %d", 5, len(got))
	}
}

// A segment file that held records before keeps them as bytes. When it is
// reused, its old records must not come back as the new log's: the new
// records here end exactly where an old record begins, and that old record has
// the very sequence number the log expects next. Only the segment's life,
// fresh on every use, tells it apart.
func TestStaleRecordsOfAnEarlierLifeAreNotRevived(t *testing.T) {
	d := crash.New()
	opt := small(d, newClock(), 0)
	opt.Capacity = 3 * 4096 // exactly the three files the first run fills
	j := open(t, opt)
	for i := 1; i <= 9; i++ {
		mustAppend(t, j, bigRecord(i))
	}
	mustSync(t, j, 9)
	closeWithin(t, j)

	corrupt(t, d, "000001.seg", recordOffset(1, len(big))+500) // record 5: nothing after it counts
	forgetMarks(t, d)

	j = open(t, opt) // reuses the file that held records 7 to 9
	var fresh []entry
	for i, n := range []int{300, 300, 344} { // 3*28 + 944 = 1028: where old record 8 begins
		p := bytes.Repeat([]byte{byte('x' + i)}, n)
		fresh = append(fresh, entry{uint64(5 + i), string(p)})
		mustAppend(t, j, p)
	}
	mustSync(t, j, 7)
	closeWithin(t, j)

	j = open(t, opt)
	defer closeWithin(t, j)
	got := replayed(t, j)
	if len(got) != 7 || !reflect.DeepEqual(got[4:], fresh) {
		t.Fatalf("replay is %v: old records 8 and 9 were revived", seqs(got))
	}
}

// Nothing is waiting once everything is published: no timer is left behind.
func TestDueLeavesNoTimerWhenDrained(t *testing.T) {
	j, clk := newDue(t)
	defer closeWithin(t, j)
	mustAppend(t, j, []byte("a"))
	j.Due(ctx, 5*time.Second)
	if clk.Pending() != 1 {
		t.Fatalf("%d timers", clk.Pending())
	}
	publish(t, j, 1)
	if clk.Pending() != 0 {
		t.Fatalf("%d timers pending with nothing unpublished", clk.Pending())
	}
}

// Capacity is a bound on disk use. A smaller capacity at the next Open removes
// the files that are free and no longer fit.
func TestOpenTrimsToCapacity(t *testing.T) {
	d := crash.New()
	opt := small(d, ticking{newClock(), 2e6}, 0)
	opt.Capacity = 8 * 4096
	j := open(t, opt)
	var last uint64
	for i := 1; i <= 15; i++ { // five segments
		last = mustAppend(t, j, bigRecord(i))
	}
	mustSync(t, j, last)
	publish(t, j, last)
	if err := j.Reclaim(); err != nil {
		t.Fatal(err)
	}
	closeWithin(t, j)
	before, _ := d.List()
	if len(before) < 5 {
		t.Fatalf("only %d files: %v", len(before), before)
	}

	opt.Capacity, opt.Published = 3*4096, last
	j = open(t, opt)
	defer closeWithin(t, j)
	if after, _ := d.List(); len(after) > 3 {
		t.Fatalf("%d files for a capacity of 3 segments: %v", len(after), after)
	}
}

// A record that is whole and from this segment, but not the next in sequence,
// ends the log: here record 2 has been copied over record 4.
func TestRecordOutOfSequenceEndsTheLog(t *testing.T) {
	d := crash.New()
	j := open(t, small(d, newClock(), 0))
	const size = 100
	for i := 1; i <= 6; i++ {
		mustAppend(t, j, bytes.Repeat([]byte{byte('a' + i)}, size))
	}
	mustSync(t, j, 6)
	closeWithin(t, j)

	f, err := d.Open("000000.seg")
	if err != nil {
		t.Fatal(err)
	}
	two := make([]byte, 28+size)
	if _, err := f.ReadAt(two, recordOffset(1, size)); err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt(two, recordOffset(3, size)); err != nil {
		t.Fatal(err)
	}
	forgetMarks(t, d)

	j = open(t, small(d, newClock(), 0))
	defer closeWithin(t, j)
	if got, want := seqs(replayed(t, j)), []uint64{1, 2, 3}; !reflect.DeepEqual(got, want) {
		t.Fatalf("replay %v, want %v", got, want)
	}
}

// Before a segment is wiped, the sequence the store holds is made durable in the
// oldest segment that stays, and that segment must have its header on the
// disk. Here the next segment has a record queued behind a writer that is held,
// so it has no header yet: Reclaim must leave the full one alone.
func TestReclaimWaitsForTheAnchor(t *testing.T) {
	d := crash.New()
	var parked atomic.Bool
	hold := make(chan struct{})
	wiped := make(chan struct{}, 8)
	d.Intercept(func(e crash.Event) error {
		if e.Kind != "write" {
			return nil
		}
		if e.Len == headerSize && e.Off == 0 { // a segment being freed
			wiped <- struct{}{}
		} else if parked.Load() {
			<-hold
		}
		return nil
	})
	j := open(t, small(d, newClock(), 0))
	defer closeWithin(t, j)
	releaseOnce := sync.OnceFunc(func() { close(hold) })
	defer releaseOnce()       // before the Close, on every way out
	for i := 1; i <= 3; i++ { // the first segment, full
		mustAppend(t, j, bigRecord(i))
	}
	publish(t, j, 3)
	parked.Store(true)
	mustAppend(t, j, bigRecord(4)) // starts the next segment; its first write is held
	if err := j.Reclaim(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-wiped:
		t.Fatal("Reclaim freed a segment whose successor has no header on the disk")
	default:
	}
	parked.Store(false)
	releaseOnce()
	if got := seqs(replayed(t, j)); !reflect.DeepEqual(got, []uint64{4}) { // returns once the record is written
		t.Fatalf("replay %v", got)
	}
	if err := j.Reclaim(); err != nil { // now it may
		t.Fatal(err)
	}
	select {
	case <-wiped:
	default:
		t.Fatal("Reclaim never freed the segment")
	}
}
