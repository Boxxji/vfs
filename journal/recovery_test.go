package journal_test

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/hanzoai/vfs/journal"
	"github.com/hanzoai/vfs/journal/crash"
)

var big = bytes.Repeat([]byte("B"), 1000) // three records to a 4 KiB segment

func bigRecord(i int) []byte { return append([]byte{byte('a' + i%26)}, big[1:]...) }

// recordOffset is where record n (0-based) starts in the first segment when
// every payload has the same size.
func recordOffset(n, size int) int64 { return int64(20 + n*(28+size)) }

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

// A damaged record ends the log there: Replay returns the records before it
// and nothing after it, and new records continue from the damaged one's
// number without bringing the old ones back.
func TestTornTail(t *testing.T) {
	d := crash.New()
	j := open(t, small(d, newClock(), 0))
	const size = 100
	for i := 1; i <= 10; i++ {
		mustAppend(t, j, bytes.Repeat([]byte{byte('a' + i)}, size))
	}
	mustSync(t, j, 10)
	j.Close()

	corrupt(t, d, "000000.seg", recordOffset(5, size)+40) // record 6, inside its payload

	j = open(t, small(d, newClock(), 0))
	got := replayed(t, j)
	if want := []uint64{1, 2, 3, 4, 5}; !reflect.DeepEqual(seqs(got), want) {
		t.Fatalf("replay after damage %v, want %v", seqs(got), want)
	}
	if seq := mustAppend(t, j, []byte("new six")); seq != 6 {
		t.Fatalf("numbering after damage: %d, want 6", seq)
	}
	mustSync(t, j, 6)
	j.Close()

	j = open(t, small(d, newClock(), 0))
	defer j.Close()
	got = replayed(t, j)
	if len(got) != 6 || got[5] != (entry{6, "new six"}) {
		t.Fatalf("replay after the new record %v: the old record 6 or 7 came back", seqs(got))
	}
}

// A hole in an early segment makes every later segment unreachable, even
// though their records are intact: they follow a gap, and a gap means those
// records were never reported durable. They are discarded for good, so the
// numbers they carry can be reused.
func TestHoleDiscardsLaterSegments(t *testing.T) {
	d := crash.New()
	j := open(t, small(d, newClock(), 0))
	for i := 1; i <= 9; i++ {
		mustAppend(t, j, bigRecord(i))
	}
	mustSync(t, j, 9)
	j.Close()

	corrupt(t, d, "000001.seg", recordOffset(1, len(big))+500) // record 5, the middle of segment two

	j = open(t, small(d, newClock(), 0))
	got := replayed(t, j)
	if want := []uint64{1, 2, 3, 4}; !reflect.DeepEqual(seqs(got), want) {
		t.Fatalf("replay %v, want %v", seqs(got), want)
	}
	for i := 5; i <= 8; i++ {
		mustAppend(t, j, []byte{byte('0' + i)})
	}
	mustSync(t, j, 8)
	j.Close()

	j = open(t, small(d, newClock(), 0))
	defer j.Close()
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

// The store can be ahead of the disk: the publisher published records that
// were never synced and the machine lost them. Numbering continues after what
// the store holds, and the new records survive a restart.
func TestPublishedAheadOfDisk(t *testing.T) {
	d := crash.New()
	j := open(t, small(d, newClock(), 0))
	for i := 1; i <= 5; i++ {
		mustAppend(t, j, []byte{byte('0' + i)})
	}
	mustSync(t, j, 5)
	j.Close()

	j = open(t, small(d, newClock(), 9))
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
	j.Close()

	j = open(t, small(d, newClock(), 9))
	defer j.Close()
	if got := replayed(t, j); !reflect.DeepEqual(got, []entry{{10, "ten"}}) {
		t.Fatalf("replay %v", got)
	}
	if m := j.Marks(); m.Published != 9 || m.Visible != 10 {
		t.Fatalf("marks %+v", m)
	}
}

// The store is the truth. A log that begins after the store's state is missing
// the records in between, and what follows a gap is not a prefix: it is
// discarded, and numbering continues from the store.
func TestStoreBehindTheJournal(t *testing.T) {
	d := crash.New()
	j := open(t, small(d, newClock(), 0))
	for i := 1; i <= 9; i++ {
		mustAppend(t, j, bigRecord(i))
	}
	mustSync(t, j, 9)
	if err := j.Publish(6); err != nil {
		t.Fatal(err)
	}
	if err := j.Reclaim(); err != nil { // the first two segments are gone
		t.Fatal(err)
	}
	j.Close()

	j = open(t, small(d, newClock(), 2)) // the store holds only 2
	if m := j.Marks(); m != (journal.Marks{Visible: 2, Durable: 2, Published: 2}) {
		t.Fatalf("marks %+v", m)
	}
	if got := replayed(t, j); len(got) != 0 {
		t.Fatalf("replay %v: records after a gap", seqs(got))
	}
	if seq := mustAppend(t, j, []byte("three")); seq != 3 {
		t.Fatalf("seq %d, want 3", seq)
	}
	mustSync(t, j, 3)
	j.Close()

	j = open(t, small(d, newClock(), 2))
	defer j.Close()
	if got := replayed(t, j); !reflect.DeepEqual(got, []entry{{3, "three"}}) {
		t.Fatalf("replay %v: the discarded records came back", got)
	}
}

// Records of this run are visible to Replay before any Sync.
func TestReplaySeesUnsyncedRecords(t *testing.T) {
	j := open(t, small(crash.New(), newClock(), 0))
	defer j.Close()
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
	defer j.Close()
	for i := 1; i <= 6; i++ {
		mustAppend(t, j, []byte{byte('0' + i)})
	}
	if err := j.Publish(4); err != nil {
		t.Fatal(err)
	}
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
		if e.Kind == "write" && e.Off == 0 && e.Len == 20 { // the header of a segment being freed
			wiped <- struct{}{}
		}
		return nil
	})
	j := open(t, small(d, newClock(), 0))
	defer j.Close()
	for i := 1; i <= 12; i++ {
		mustAppend(t, j, bigRecord(i))
	}
	mustSync(t, j, 12)
	if err := j.Publish(3); err != nil {
		t.Fatal(err)
	}

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
	if err := j.Publish(9); err != nil {
		t.Fatal(err)
	}
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
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	img := d.Image(d.Len(), crash.Variant{Kind: crash.Lost})
	j = open(t, small(img, newClock(), 0))
	defer j.Close()
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
	j.Close()
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
			j.Close()
			t.Errorf("%s: Open accepted %+v", name, opt)
		}
	}
}

func TestPayloadLimits(t *testing.T) {
	j := open(t, small(crash.New(), newClock(), 0))
	defer j.Close()
	if _, err := j.Append(ctx, make([]byte, 4096)); !errors.Is(err, journal.ErrTooLarge) {
		t.Fatalf("payload of a segment: %v", err)
	}
	largest := make([]byte, 4096-20-28)
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
	j.Close()
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
	defer j.Close()
	if got := replayed(t, j); !reflect.DeepEqual(got, []entry{{1, "kept"}}) {
		t.Fatalf("records of a segment of another size: %v", got)
	}
}

func TestStatsCountWhatHappened(t *testing.T) {
	j := open(t, small(crash.New(), newClock(), 0))
	defer j.Close()
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
	j.Close()
	v := volatile(t, d)

	j = open(t, small(v, newClock(), 0))
	if m := j.Marks(); m.Durable != 5 {
		t.Fatalf("marks %+v", m)
	}
	img := v.Image(v.Len(), crash.Variant{Kind: crash.Lost})
	j.Close()
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
	j.Close()

	corrupt(t, d, "000001.seg", recordOffset(1, len(big))+500) // record 5: nothing after it counts

	j = open(t, opt) // reuses the file that held records 7 to 9
	var fresh []entry
	for i, n := range []int{300, 300, 344} { // 3*28 + 944 = 1028: where old record 8 begins
		p := bytes.Repeat([]byte{byte('x' + i)}, n)
		fresh = append(fresh, entry{uint64(5 + i), string(p)})
		mustAppend(t, j, p)
	}
	mustSync(t, j, 7)
	j.Close()

	j = open(t, opt)
	defer j.Close()
	got := replayed(t, j)
	if len(got) != 7 || !reflect.DeepEqual(got[4:], fresh) {
		t.Fatalf("replay is %v: old records 8 and 9 were revived", seqs(got))
	}
}

// Nothing is waiting once everything is published: no timer is left behind.
func TestDueLeavesNoTimerWhenDrained(t *testing.T) {
	j, clk := newDue(t)
	defer j.Close()
	mustAppend(t, j, []byte("a"))
	j.Due(5 * time.Second)
	if clk.Pending() != 1 {
		t.Fatalf("%d timers", clk.Pending())
	}
	if err := j.Publish(1); err != nil {
		t.Fatal(err)
	}
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
	if err := j.Publish(last); err != nil {
		t.Fatal(err)
	}
	if err := j.Reclaim(); err != nil {
		t.Fatal(err)
	}
	j.Close()
	before, _ := d.List()
	if len(before) < 5 {
		t.Fatalf("only %d files: %v", len(before), before)
	}

	opt.Capacity, opt.Published = 3*4096, last
	j = open(t, opt)
	defer j.Close()
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
	j.Close()

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

	j = open(t, small(d, newClock(), 0))
	defer j.Close()
	if got, want := seqs(replayed(t, j)), []uint64{1, 2, 3}; !reflect.DeepEqual(got, want) {
		t.Fatalf("replay %v, want %v", got, want)
	}
}

// The publisher may publish records the writer has not written yet, since it
// reads memory, not the files. Reclaim must not free a segment whose records
// are still queued: the late write would land in a segment that is free.
func TestReclaimWaitsForTheWriter(t *testing.T) {
	d := crash.New()
	hold := make(chan struct{})
	wiped := make(chan struct{}, 8)
	d.Intercept(func(e crash.Event) error {
		if e.Kind == "write" {
			if e.Len == 20 && e.Off == 0 { // a segment being freed
				wiped <- struct{}{}
			} else {
				<-hold
			}
		}
		return nil
	})
	j := open(t, small(d, newClock(), 0))
	defer j.Close()
	for i := 1; i <= 4; i++ { // three fill the first segment, the fourth starts the next
		mustAppend(t, j, bigRecord(i))
	}
	if err := j.Publish(4); err != nil {
		t.Fatal(err)
	}
	if err := j.Reclaim(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-wiped:
		t.Fatal("Reclaim freed a segment whose records had not been written")
	default:
	}
	close(hold)
	if got := replayed(t, j); len(got) != 0 {
		t.Fatalf("replay %v: all published", seqs(got))
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
