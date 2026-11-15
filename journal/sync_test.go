package journal_test

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

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

	if n := syncs.Load(); n > 3 {
		t.Fatalf("%d fsyncs for %d Sync calls", n, writers+1)
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

// Publication satisfies Sync: the store holds the record, so waiting for the
// local disk is pointless.
func TestPublishReleasesSync(t *testing.T) {
	d := crash.New()
	p := park(d)
	j := open(t, small(d, newClock(), 0))
	a := mustAppend(t, j, []byte("a"))
	done := make(chan error)
	go func() { done <- j.Sync(ctx, a) }()
	<-p.entered // the disk is stuck
	if err := j.Publish(a); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatalf("Sync after Publish: %v", err)
	}
	if m := j.Marks(); m.Durable != a || m.Published != a || m.Visible != a {
		t.Fatalf("marks %+v", m)
	}
	// A call for a published record never touches the disk.
	if err := j.Sync(ctx, a); err != nil {
		t.Fatal(err)
	}
	close(p.release)
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
// instead of queueing without limit.
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
		for i := 0; i < 100; i++ {
			if _, err := j.Append(ctx, chunk); err != nil {
				t.Error(err)
				return
			}
			appended.Add(1)
		}
	}()
	// The writer is stuck in its first write. Append must stop well short of 100 MiB.
	waitFor(t, "the first write to be held", held.Load)
	stable := 0
	waitFor(t, "Append to stop", func() bool {
		n := appended.Load()
		runtime.Gosched()
		if n > 60 && n == appended.Load() {
			stable++
		} else {
			stable = 0
		}
		return stable == 5000
	})
	if n := appended.Load(); n > 70 {
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
