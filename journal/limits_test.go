package journal_test

import (
	"bytes"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/hanzoai/vfs/journal"
	"github.com/hanzoai/vfs/journal/crash"
)

// The exact limit of a payload is the segment less its preamble (40 + 40) and the record header (28).
func TestPayloadLimitIsExact(t *testing.T) {
	j := open(t, small(crash.New(), newClock(), 0))
	defer closeWithin(t, j)
	if _, err := j.Append(ctx, make([]byte, 4096-dataStart-28)); err != nil {
		t.Fatalf("the largest payload: %v", err)
	}
	if _, err := j.Append(ctx, make([]byte, 4096-dataStart-28+1)); !errors.Is(err, journal.ErrTooLarge) {
		t.Fatalf("one byte more: %v, want ErrTooLarge", err)
	}
}

// A segment file shorter than its preamble (a Create that the file system
// kept half of) is not a segment: Open drops it and goes on.
func TestShortSegmentFileIsDropped(t *testing.T) {
	d := crash.New()
	if _, err := d.Create("000009.seg", 10); err != nil {
		t.Fatal(err)
	}
	j := open(t, small(d, newClock(), 0))
	defer closeWithin(t, j)
	mustSync(t, j, mustAppend(t, j, []byte("x")))
}

// A record larger than the 16 MiB queue cap is admitted and does not deadlock.
func TestRecordLargerThanTheQueueCap(t *testing.T) {
	d := crash.New()
	j := open(t, journal.Options{Capacity: 256 << 20, Segment: 64 << 20, Disk: d, Clock: newClock()})
	defer closeWithin(t, j)
	big := bytes.Repeat([]byte{7}, 40<<20)
	done := make(chan struct{})
	go func() {
		defer close(done)
		a := mustAppend(t, j, big)
		b := mustAppend(t, j, []byte("after"))
		mustSync(t, j, b)
		mustSync(t, j, a)
	}()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("deadlock with a record larger than the queue cap")
	}
}

func TestZeroLengthRecord(t *testing.T) {
	d := crash.New()
	j := open(t, small(d, newClock(), 0))
	a := mustAppend(t, j, nil)
	b := mustAppend(t, j, []byte{})
	mustSync(t, j, b)
	closeWithin(t, j)
	j = open(t, small(d, newClock(), 0))
	defer closeWithin(t, j)
	got := replayed(t, j)
	if len(got) != 2 || got[0].Seq != a || got[1].Seq != b || got[0].Data != "" || got[1].Data != "" {
		t.Fatalf("replay of empty records: %v", got)
	}
}

// Close with Sync waiters whose sync is held in the file layer: both return,
// the waiters see success (Close makes everything durable), nothing hangs.
func TestCloseWithWaiters(t *testing.T) {
	d := crash.New()
	p := park(d)
	j := open(t, small(d, newClock(), 0))
	a := mustAppend(t, j, []byte("a"))
	var wg sync.WaitGroup
	errs := make([]error, 4)
	for i := range errs {
		wg.Add(1)
		go func(i int) { defer wg.Done(); errs[i] = j.Sync(ctx, a) }(i)
	}
	<-p.entered // the first sync is in the file layer
	closed := make(chan error, 1)
	go func() { closed <- j.Close() }()
	time.Sleep(20 * time.Millisecond)
	// let every sync finish
	go func() {
		for {
			select {
			case p.release <- struct{}{}:
			case <-time.After(300 * time.Millisecond):
				return
			}
		}
	}()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Close hung with waiters")
	}
	wg.Wait()
	// A Sync that starts after Close took the lock may say ErrClosed (safe: the
	// caller learns nothing false); it must never say success for a record that
	// is not durable, and Close must have made the record durable.
	for i, err := range errs {
		if err != nil && !errors.Is(err, journal.ErrClosed) {
			t.Fatalf("waiter %d: %v", i, err)
		}
	}
	if err := j.Sync(ctx, a); err != nil {
		t.Fatalf("Close did not make record %d durable: %v", a, err)
	}
}

// Replay MUST fail rather than skip a record that should be present
// (spec requirement 6): bit rot after Open.
func TestReplayDetectsRuntimeCorruption(t *testing.T) {
	d := crash.New()
	j := open(t, small(d, newClock(), 0))
	defer closeWithin(t, j)
	const size = 100
	for i := 1; i <= 6; i++ {
		mustAppend(t, j, bytes.Repeat([]byte{byte('a' + i)}, size))
	}
	mustSync(t, j, 6)
	corrupt(t, d, "000000.seg", recordOffset(2, size)+40) // bit rot in record 3 after Open
	err := j.Replay(func(journal.Record) error { return nil })
	if !errors.Is(err, journal.ErrCorrupt) {
		t.Fatalf("Replay over a damaged record 3 returned %v, want ErrCorrupt", err)
	}
}

// The length of a record is 32 bits: a segment larger than 4 GiB could hold a
// record whose length does not fit, so it is refused before anything is made.
func TestSegmentLargerThan4GiBIsRefused(t *testing.T) {
	if j, err := journal.Open("", journal.Options{Capacity: 20 << 30, Segment: 5 << 30, Disk: crash.New()}); err == nil {
		closeWithin(t, j)
		t.Fatal("Open accepted a segment of 5 GiB")
	}
}
