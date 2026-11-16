package journal_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hanzoai/vfs/journal"
	"github.com/hanzoai/vfs/journal/crash"
)

// Concurrent Sync calls share one fsync. A sync is held in the file layer
// while 32 more writers arrive; they all wait for the next one.
func TestSyncGroupsWaiters(t *testing.T) {
	d := crash.New()
	var syncs atomic.Int32
	entered := make(chan struct{})
	release := make(chan struct{})
	d.Intercept(func(e crash.Event) error {
		if e.Kind == "sync" && syncs.Add(1) == 1 {
			close(entered)
			<-release
		}
		return nil
	})
	j := open(t, small(d, newClock(), 0))
	defer j.Close()

	var wg sync.WaitGroup
	first := mustAppend(t, j, []byte("first"))
	wg.Add(1)
	go func() { defer wg.Done(); mustSync(t, j, first) }()
	<-entered
	const writers = 32
	for i := 0; i < writers; i++ {
		seq := mustAppend(t, j, payload(uint64(i)))
		wg.Add(1)
		go func() { defer wg.Done(); mustSync(t, j, seq) }()
	}
	waitFor(t, "all writers waiting", func() bool { return j.Stats().Waited == writers+1 })
	close(release)
	wg.Wait()

	// Rounds, not file syncs: a round that spans two segment files syncs both.
	// The writer may not have written every record when the first waiter's turn
	// comes, so a round or two more than the least is the grouping at work; one
	// per call is what a journal without it does.
	if n := j.Stats().Rounds; n > 6 {
		t.Fatalf("%d sync rounds for %d Sync calls (%d file syncs)", n, writers+1, syncs.Load())
	}
	var big uint64
	for i, c := range j.Stats().Groups {
		if i >= 5 {
			big += c
		}
	}
	if big == 0 {
		t.Fatalf("no group of 16 or more waiters: %v", j.Stats().Groups)
	}
}

// A Sync call is never satisfied by a sync that began before the record was
// appended. Sync one is held; a second record is appended and waited for
// while it is held; when the first sync completes the second call must stay
// blocked until a sync that started after the second record was written.
func TestSyncGenerationOrder(t *testing.T) {
	d := crash.New()
	p := park(d)
	j := open(t, small(d, newClock(), 0))
	defer j.Close()

	a := mustAppend(t, j, []byte("a"))
	doneA := make(chan struct{})
	go func() { mustSync(t, j, a); close(doneA) }()
	<-p.entered // the first sync is in flight and covers only a

	b := mustAppend(t, j, []byte("b"))
	doneB := make(chan struct{})
	go func() { mustSync(t, j, b); close(doneB) }()
	waitFor(t, "second waiter", func() bool { return j.Stats().Waited == 2 })

	p.release <- struct{}{} // first sync ends
	<-doneA
	<-p.entered // a second sync begins, because b is not covered
	select {
	case <-doneB:
		t.Fatal("Sync(b) returned on a sync that began before b was written")
	default:
	}
	if m := j.Marks(); m.Durable != a {
		t.Fatalf("durable %d while only %d is covered", m.Durable, a)
	}
	p.release <- struct{}{}
	<-doneB
	if m := j.Marks(); m.Durable != b {
		t.Fatalf("durable %d, want %d", m.Durable, b)
	}
	close(p.release)
}

// The store never gets ahead of the disk: Publish refuses a record no sync has
// covered. That is what lets Open treat the store's sequence as the truth, and
// what keeps a Sync from being satisfied by a publication that did not wait
// for the disk.
func TestPublishRefusesWhatIsNotDurable(t *testing.T) {
	d := crash.New()
	p := park(d)
	j := open(t, small(d, newClock(), 0))
	a := mustAppend(t, j, []byte("a"))
	done := make(chan error)
	go func() { done <- j.Sync(ctx, a) }()
	<-p.entered // the disk is stuck in the sync that covers a
	if err := j.Publish(a); !errors.Is(err, journal.ErrAhead) {
		t.Fatalf("Publish of a record no sync has covered: %v", err)
	}
	if m := j.Marks(); m.Published != 0 || m.Durable != 0 {
		t.Fatalf("marks %+v after a refused Publish", m)
	}
	close(p.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := j.Publish(a); err != nil {
		t.Fatalf("Publish after the sync: %v", err)
	}
	if m := j.Marks(); m != (journal.Marks{Visible: a, Durable: a, Published: a}) {
		t.Fatalf("marks %+v", m)
	}
	j.Close()
}

func TestSyncArguments(t *testing.T) {
	j := open(t, small(crash.New(), newClock(), 0))
	defer j.Close()
	if err := j.Sync(ctx, 0); err != nil {
		t.Fatalf("Sync(0): %v", err)
	}
	if err := j.Sync(ctx, 1); !errors.Is(err, journal.ErrAhead) {
		t.Fatalf("Sync of a record not appended: %v", err)
	}
	seq := mustAppend(t, j, []byte("x"))
	if err := j.Sync(ctx, seq+1); !errors.Is(err, journal.ErrAhead) {
		t.Fatalf("Sync past the last record: %v", err)
	}
	if err := j.Publish(seq + 1); !errors.Is(err, journal.ErrAhead) {
		t.Fatalf("Publish past the last record: %v", err)
	}
}

// A caller that gives up leaves the journal as it was.
func TestSyncCancel(t *testing.T) {
	d := crash.New()
	p := park(d)
	j := open(t, small(d, newClock(), 0))
	a := mustAppend(t, j, []byte("a"))
	c, cancel := context.WithCancel(ctx)
	done := make(chan error)
	go func() { done <- j.Sync(c, a) }()
	<-p.entered
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled Sync returned %v", err)
	}
	close(p.release)
	mustSync(t, j, a)
	j.Close()
}

// After a failed write or sync the file is in an unknown state: nothing is
// retried, every waiter and every later Append gets the error, and what was
// already durable stays acknowledged.
func TestFailureIsFinal(t *testing.T) {
	for _, kind := range []string{"sync", "write"} {
		t.Run(kind, func(t *testing.T) {
			d := crash.New()
			var fail atomic.Bool
			boom := errors.New("input/output error")
			d.Intercept(func(e crash.Event) error {
				if fail.Load() && e.Kind == kind {
					return boom
				}
				return nil
			})
			j := open(t, small(d, newClock(), 0))
			a := mustAppend(t, j, []byte("a"))
			mustSync(t, j, a)

			fail.Store(true)
			b := mustAppend(t, j, []byte("b"))
			if err := j.Sync(ctx, b); !errors.Is(err, boom) {
				t.Fatalf("Sync after a failed %s: %v", kind, err)
			}
			if err := j.Sync(ctx, a); err != nil {
				t.Fatalf("Sync of a record that was already durable: %v", err)
			}
			if _, err := j.Append(ctx, []byte("c")); !errors.Is(err, boom) {
				t.Fatalf("Append after failure: %v", err)
			}
			s := j.State()
			if !errors.Is(s.Err, boom) || !s.Degraded {
				t.Fatalf("state %+v", s)
			}
			if m := j.Marks(); m.Durable != a {
				t.Fatalf("durable %d, want %d", m.Durable, a)
			}
			if err := j.Close(); !errors.Is(err, boom) {
				t.Fatalf("Close: %v", err)
			}
		})
	}
}

func TestClosedJournal(t *testing.T) {
	j := open(t, small(crash.New(), newClock(), 0))
	a := mustAppend(t, j, []byte("a"))
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	if err := j.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if _, err := j.Append(ctx, []byte("b")); !errors.Is(err, journal.ErrClosed) {
		t.Fatalf("Append after Close: %v", err)
	}
	if err := j.Sync(ctx, a); err != nil {
		t.Fatalf("Close made a durable, Sync says %v", err)
	}
	if err := j.Replay(func(journal.Record) error { return nil }); !errors.Is(err, journal.ErrClosed) {
		t.Fatalf("Replay after Close: %v", err)
	}
}

// Memory is bounded: when the writer cannot keep up, Append waits for it
// instead of queueing without limit, and a small Append is not stuck behind
// more than a few megabytes of someone else's bulk.
func TestAppendWaitsForTheWriter(t *testing.T) {
	d := crash.New()
	hold := make(chan struct{})
	var held atomic.Bool
	d.Intercept(func(e crash.Event) error {
		if e.Kind == "write" && held.CompareAndSwap(false, true) {
			<-hold
		}
		return nil
	})
	j, err := journal.Open("", journal.Options{Capacity: 256 << 20, Segment: 16 << 20, Disk: d, Clock: newClock()})
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	chunk := make([]byte, 1<<20)
	var appended atomic.Int32
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		for i := 0; i < 40; i++ {
			if _, err := j.Append(ctx, chunk); err != nil {
				t.Error(err)
				return
			}
			appended.Add(1)
		}
	}()
	// The writer is stuck in its first write. Append must stop well short of 40 MiB.
	waitFor(t, "the first write to be held", held.Load)
	waitFor(t, "Append to fill the queue", func() bool { return appended.Load() > 12 })
	select {
	case <-finished:
		t.Fatalf("Append queued all %d MiB behind a stuck writer", appended.Load())
	case <-time.After(200 * time.Millisecond): // a wait for something that must not happen
	}
	if n := appended.Load(); n > 20 {
		t.Fatalf("%d MiB queued behind a stuck writer", n)
	}
	close(hold)
	<-finished
}

// Data is synced in the background once enough has been written, with nobody
// waiting, so the kernel is never left holding a huge dirty tail and the
// durable watermark moves.
func TestBackgroundSync(t *testing.T) {
	j := open(t, journal.Options{Capacity: 64 << 20, Segment: 16 << 20, Disk: crash.New(), Clock: newClock()})
	defer j.Close()
	chunk := make([]byte, 1<<20)
	for i := 0; i < 12; i++ {
		mustAppend(t, j, chunk)
	}
	waitFor(t, "a sync nobody asked for", func() bool { return j.Marks().Durable >= 8 })
}

// A caller that gives up takes its wait with it: nobody is waiting, so no sync
// is started on its behalf.
func TestCancelledSyncStartsNoSync(t *testing.T) {
	d := crash.New()
	var syncs atomic.Int32
	hold := make(chan struct{})
	var held atomic.Bool
	d.Intercept(func(e crash.Event) error {
		switch e.Kind {
		case "write":
			if held.CompareAndSwap(false, true) {
				<-hold
			}
		case "sync":
			syncs.Add(1)
		}
		return nil
	})
	j := open(t, small(d, newClock(), 0))
	defer j.Close()
	a := mustAppend(t, j, []byte("a"))
	waitFor(t, "the write to be held", held.Load)
	c, cancel := context.WithCancel(ctx)
	done := make(chan error)
	go func() { done <- j.Sync(c, a) }()
	waitFor(t, "the waiter", func() bool { return j.Stats().Waited == 1 })
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Sync returned %v", err)
	}
	close(hold)
	if got := replayed(t, j); len(got) != 1 { // returns once the record is written
		t.Fatalf("replay %v", got)
	}
	if n := syncs.Load(); n != 0 {
		t.Fatalf("%d syncs for a call that was cancelled", n)
	}
}

// Data written but not synced is bounded, so a bulk writer cannot make the next
// fsync wait behind gigabytes. With a sync held in the file layer the writer
// stops after a few megabytes instead of writing everything it was given.
func TestWriterStopsAheadOfASlowSync(t *testing.T) {
	d := crash.New()
	var written atomic.Int64
	entered := make(chan struct{})
	release := make(chan struct{})
	var held atomic.Bool
	d.Intercept(func(e crash.Event) error {
		switch e.Kind {
		case "write":
			written.Add(int64(e.Len))
		case "sync":
			if held.CompareAndSwap(false, true) {
				close(entered)
				<-release
			}
		}
		return nil
	})
	j, err := journal.Open("", journal.Options{Capacity: 256 << 20, Segment: 16 << 20, Disk: d, Clock: newClock()})
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	chunk := make([]byte, 1<<20)
	go func() {
		for i := 0; i < 60; i++ {
			if _, err := j.Append(ctx, chunk); err != nil {
				return
			}
		}
	}()
	<-entered // enough was written that a sync began, and it will not end
	waitFor(t, "the writer to get ahead", func() bool { return written.Load() >= 12<<20 })
	time.Sleep(200 * time.Millisecond) // a wait for something that must not happen
	if n := written.Load(); n > 28<<20 {
		t.Fatalf("%d MiB written ahead of a sync that has not finished", n>>20)
	}
	close(release)
}

// Bulk data is handed to the system for writeback as it is written, so a
// later Sync finds it already on its way. Small writes are not: a hint costs a
// system call.
func TestWritebackIsRequestedForBulkData(t *testing.T) {
	d := crash.New()
	var hints, hinted atomic.Int64
	d.Intercept(func(e crash.Event) error {
		if e.Kind == "writeback" {
			hints.Add(1)
			hinted.Add(int64(e.Len))
		}
		return nil
	})
	j, err := journal.Open("", journal.Options{Capacity: 64 << 20, Segment: 16 << 20, Disk: d, Clock: newClock()})
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	for i := 0; i < 100; i++ {
		mustAppend(t, j, []byte("small"))
	}
	if got := replayed(t, j); len(got) != 100 { // everything is written
		t.Fatalf("%d records", len(got))
	}
	if n := hints.Load(); n != 0 {
		t.Fatalf("%d hints for 3 KiB of small writes", n)
	}
	chunk := make([]byte, 1<<20)
	for i := 0; i < 6; i++ {
		mustAppend(t, j, chunk)
	}
	if got := replayed(t, j); len(got) != 106 {
		t.Fatalf("%d records", len(got))
	}
	if n := hinted.Load(); n < 5<<20 || n > 7<<20 {
		t.Fatalf("%d bytes hinted for 6 MiB written", n)
	}
}

// A recycled segment is hinted from its start again, not from where its
// previous use ended.
func TestWritebackStartsOverInARecycledSegment(t *testing.T) {
	d := crash.New()
	var hinted atomic.Int64
	d.Intercept(func(e crash.Event) error {
		if e.Kind == "writeback" {
			hinted.Add(int64(e.Len))
		}
		return nil
	})
	j, err := journal.Open("", journal.Options{Capacity: 16 << 20, Segment: 4 << 20, Disk: d, Clock: newClock()})
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	chunk := make([]byte, 1<<20)
	var last uint64
	for i := 0; i < 9; i++ { // three segments
		last = mustAppend(t, j, chunk)
	}
	mustSync(t, j, last)
	publish(t, j, last)
	if err := j.Reclaim(); err != nil {
		t.Fatal(err)
	}
	hinted.Store(0)
	for i := 0; i < 9; i++ { // the same files again
		last = mustAppend(t, j, chunk)
	}
	mustSync(t, j, last)
	if n := hinted.Load(); n < 6<<20 {
		t.Fatalf("%d MiB hinted while 9 MiB went into recycled segments", n>>20)
	}
}

// A mark is a second block to flush, which on some disks doubles the cost of a
// lone writer's fsync, so marks are spaced out: one every few sync rounds, and
// at Close. With a mark every fourth round, eight rounds write two.
func TestMarksAreSpacedOutByRounds(t *testing.T) {
	defer journal.MarkEvery(4)()
	d := crash.New()
	var marks atomic.Int64
	d.Intercept(func(e crash.Event) error {
		if e.Kind == "write" && e.Off >= headerSize && e.Off < dataStart && e.Len == markSize {
			marks.Add(1)
		}
		return nil
	})
	j := open(t, small(d, newClock(), 0))
	for i := 0; i < 8; i++ {
		mustSync(t, j, mustAppend(t, j, []byte("x")))
	}
	if n := marks.Load(); n != 2 {
		t.Fatalf("%d marks written in 8 sync rounds, want 2", n)
	}
	j.Close()
	if n := marks.Load(); n != 3 {
		t.Fatalf("%d marks after Close, want the final one too", n)
	}
}
