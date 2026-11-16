package journal_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/hanzoai/vfs/journal"
	"github.com/hanzoai/vfs/journal/crash"
)

// stuckClock hands out timers that have already fired: AfterFunc keeps the
// callback and Stop reports false, which is what a real time.Timer reports
// when its function is already running (here: parked on the journal's mutex).
type stuckClock struct {
	*clock
	mu sync.Mutex
	fs []func()
}

type firedTimer struct{}

func (firedTimer) Stop() bool { return false }

func (c *stuckClock) AfterFunc(d time.Duration, f func()) journal.Timer {
	c.mu.Lock()
	c.fs = append(c.fs, f)
	c.mu.Unlock()
	return firedTimer{}
}

// A Due timer whose callback is already running when Close takes the mutex must
// not close the channel a second time when it gets the mutex: that would be a
// panic in a timer goroutine nobody can recover.
func TestDueTimerRunningDuringCloseDoesNotDoubleClose(t *testing.T) {
	clk := &stuckClock{clock: newClock()}
	j := open(t, journal.Options{Capacity: 1 << 20, Segment: 64 << 10, Disk: crash.New(), Clock: clk})
	mustAppend(t, j, []byte("x"))
	due := j.Due(ctx, 5*time.Second)
	clk.Advance(5 * time.Second) // the deadline passes; the callback is "running"
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	if !fired(due) {
		t.Fatal("Close left Due open")
	}
	clk.mu.Lock()
	fs := clk.fs
	clk.mu.Unlock()
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("timer callback after Close panicked: %v", r)
		}
	}()
	for _, f := range fs {
		f() // what the runtime does when the callback gets the mutex after Close
	}
}

// The same race with the wall clock and real timers: a deadline that falls due
// while Close holds the mutex. A hit used to kill the test binary.
func TestDueCloseRaceWithTheWallClock(t *testing.T) {
	t.Parallel() // real time, not CPU: it runs beside the others once the serial tests are done
	end := time.Now().Add(1500 * time.Millisecond)
	for i := 0; time.Now().Before(end); i++ {
		j, err := journal.Open("", journal.Options{Capacity: 1 << 20, Segment: 64 << 10, Disk: crash.New()})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := j.Append(ctx, []byte("x")); err != nil {
			t.Fatal(err)
		}
		_ = j.Due(ctx, 200*time.Microsecond)
		time.Sleep(time.Duration(150+i%100) * time.Microsecond)
		j.Close()
	}
}

// A restarted node must not give itself a longer wait than the bound. The
// acknowledgement time lives in the log as wall-clock nanoseconds. If the wall
// clock is behind at restart (RTC reset, a VM resumed from a snapshot, NTP not
// yet stepped), the record looks acknowledged in the future: it counts as
// acknowledged at the restart, so the bound runs from there at the latest.
func TestRestartWithTheClockBehindKeepsTheBound(t *testing.T) {
	d := crash.New()
	clk1 := newClock()
	j := open(t, journal.Options{Capacity: 1 << 20, Segment: 64 << 10, Disk: d, Clock: clk1})
	mustSync(t, j, mustAppend(t, j, []byte("old")))
	j.Close()

	clk2 := newClock()
	clk2.Advance(-time.Hour) // the clock after the restart reads an hour earlier
	j = open(t, journal.Options{Capacity: 1 << 20, Segment: 64 << 10, Disk: d, Clock: clk2})
	defer j.Close()
	if s := j.State(); s.Age != 0 || s.Backlog == 0 {
		t.Fatalf("state %+v: the record is unpublished and, as far as the clock says, just acknowledged", s)
	}
	due := j.Due(ctx, 5*time.Second)
	clk2.Advance(5*time.Second - time.Millisecond)
	if fired(due) {
		t.Fatal("Due fired early")
	}
	clk2.Advance(time.Millisecond)
	if !fired(due) {
		t.Fatalf("Due did not fire 5 s after the restart for a record acknowledged before it; state %+v", j.State())
	}
}

// And a restart never makes a record younger than it was: with the clock right,
// the age carries across.
func TestRestartKeepsTheAgeOfARecord(t *testing.T) {
	d := crash.New()
	clk := newClock()
	j := open(t, journal.Options{Capacity: 1 << 20, Segment: 64 << 10, Disk: d, Clock: clk})
	mustSync(t, j, mustAppend(t, j, []byte("old")))
	clk.Advance(4 * time.Second)
	j.Close()
	j = open(t, journal.Options{Capacity: 1 << 20, Segment: 64 << 10, Disk: d, Clock: clk})
	defer j.Close()
	if s := j.State(); s.Age != 4*time.Second {
		t.Fatalf("age %v after a restart, want 4 s", s.Age)
	}
	due := j.Due(ctx, 5*time.Second)
	clk.Advance(time.Second)
	if !fired(due) {
		t.Fatal("Due did not count the time before the restart")
	}
}

var errBoom = errors.New("input/output error")

// Due closes when the journal fails: the publisher learns of it at once, not
// when the deadline passes.
func TestDueIsClosedWhenTheJournalFails(t *testing.T) {
	d := crash.New()
	boom := false
	var mu sync.Mutex
	d.Intercept(func(e crash.Event) error {
		mu.Lock()
		defer mu.Unlock()
		if boom && e.Kind == "sync" {
			return errBoom
		}
		return nil
	})
	clk := newClock()
	j := open(t, journal.Options{Capacity: 1 << 20, Segment: 64 << 10, Disk: d, Clock: clk})
	defer j.Close()
	seq := mustAppend(t, j, []byte("x"))
	due := j.Due(ctx, time.Hour)
	mu.Lock()
	boom = true
	mu.Unlock()
	if err := j.Sync(ctx, seq); err == nil {
		t.Fatal("sync succeeded")
	}
	if !fired(due) {
		t.Fatal("Due stayed open after the journal failed")
	}
}

// A publisher waits on Due beside other triggers (dirty bytes, an explicit
// Flush, shutdown) and abandons it each time another trigger wins. An abandoned
// Due must release its timer and its place in the list: otherwise memory and
// the work of every Publish grow with the trigger rate.
func TestAbandonedDueIsReleased(t *testing.T) {
	clk := newClock()
	j := open(t, journal.Options{Capacity: 1 << 20, Segment: 64 << 10, Disk: crash.New(), Clock: clk})
	defer j.Close()
	mustAppend(t, j, []byte("x"))
	for i := 0; i < 1000; i++ {
		c, cancel := context.WithCancel(ctx)
		j.Due(c, 5*time.Second) // another trigger won
		cancel()
	}
	waitFor(t, "the abandoned timers to be released", func() bool { return clk.Pending() == 0 })
	// And a Due that is kept still works.
	due := j.Due(ctx, 5*time.Second)
	clk.Advance(5 * time.Second)
	if !fired(due) {
		t.Fatal("a Due after many abandoned ones did not fire")
	}
}

// A cancelled Due never closes its channel, and one made with a context that has
// already ended holds nothing.
func TestCancelledDueNeverFires(t *testing.T) {
	clk := newClock()
	j := open(t, journal.Options{Capacity: 1 << 20, Segment: 64 << 10, Disk: crash.New(), Clock: clk})
	defer j.Close()
	mustAppend(t, j, []byte("x"))
	c, cancel := context.WithCancel(ctx)
	due := j.Due(c, time.Second)
	cancel()
	dead := j.Due(c, time.Second)
	waitFor(t, "the timers to be released", func() bool { return clk.Pending() == 0 })
	clk.Advance(time.Hour)
	if fired(due) || fired(dead) {
		t.Fatal("a cancelled Due fired")
	}
}

// A publisher whose publication keeps failing sees the same overdue record each
// time. Due fires for it no more often than Retry, so its loop cannot spin.
func TestDueDoesNotSpinOnAnOverdueRecord(t *testing.T) {
	clk := newClock()
	j := open(t, journal.Options{Capacity: 1 << 20, Segment: 64 << 10, Retry: 250 * time.Millisecond, Disk: crash.New(), Clock: clk})
	defer j.Close()
	mustAppend(t, j, []byte("x"))
	clk.Advance(time.Hour) // the record is far overdue
	fires := 0
	for step := 0; step < 100; step++ { // a second of publisher failures, 10 ms apart
		due := j.Due(ctx, time.Second)
		if fired(due) {
			fires++
		}
		clk.Advance(10 * time.Millisecond)
	}
	if fires > 5 {
		t.Fatalf("Due fired %d times in a second with a Retry of 250 ms", fires)
	}
}
