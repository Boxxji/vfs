package journal_test

import (
	"testing"
	"time"

	"github.com/hanzoai/vfs/journal"
	"github.com/hanzoai/vfs/journal/crash"
)

func fired(c <-chan struct{}) bool {
	select {
	case <-c:
		return true
	default:
		return false
	}
}

func newDue(t testing.TB) (*journal.Journal, *clock) {
	clk := newClock()
	return open(t, small(crash.New(), clk, 0)), clk
}

// Due fires when the oldest unpublished record has been acknowledged for the
// given age, to the millisecond, and not before.
func TestDueFiresAtTheDeadline(t *testing.T) {
	j, clk := newDue(t)
	defer closeWithin(t, j)
	mustAppend(t, j, []byte("a"))
	due := j.Due(ctx, 5*time.Second)
	clk.Advance(5*time.Second - time.Millisecond)
	if fired(due) {
		t.Fatal("fired 1 ms early")
	}
	clk.Advance(time.Millisecond)
	if !fired(due) {
		t.Fatal("did not fire at the deadline")
	}
}

// Later writes never move the deadline of an older record.
func TestDueIsNotResetByLaterWrites(t *testing.T) {
	j, clk := newDue(t)
	defer closeWithin(t, j)
	mustAppend(t, j, []byte("old"))
	due := j.Due(ctx, 5*time.Second)
	for i := 0; i < 4; i++ {
		clk.Advance(time.Second)
		mustAppend(t, j, []byte("newer"))
		if fired(due) {
			t.Fatalf("fired at %d s", i+1)
		}
	}
	clk.Advance(time.Second)
	if !fired(due) {
		t.Fatal("a stream of writes held the deadline back")
	}
}

// Publishing retires the oldest record; the deadline moves to the next one.
func TestDueMovesWhenOldestIsPublished(t *testing.T) {
	j, clk := newDue(t)
	defer closeWithin(t, j)
	mustAppend(t, j, []byte("one"))
	clk.Advance(3 * time.Second)
	mustAppend(t, j, []byte("two"))
	due := j.Due(ctx, 5*time.Second)
	clk.Advance(time.Second)
	publish(t, j, 1)
	clk.Advance(time.Second) // 5 s after one, 2 s after two
	if fired(due) {
		t.Fatal("fired for a record that was published")
	}
	clk.Advance(2*time.Second + 999*time.Millisecond)
	if fired(due) {
		t.Fatal("fired before record two is 5 s old")
	}
	clk.Advance(time.Millisecond)
	if !fired(due) {
		t.Fatal("did not fire for record two")
	}
}

// With nothing unpublished there is nothing to wait for and no timer; the
// deadline starts with the next write.
func TestDueWaitsForTheNextWrite(t *testing.T) {
	j, clk := newDue(t)
	defer closeWithin(t, j)
	due := j.Due(ctx, 5*time.Second)
	clk.Advance(time.Hour)
	if fired(due) || clk.Pending() != 0 {
		t.Fatalf("idle journal fired=%v with %d timers", fired(due), clk.Pending())
	}
	mustAppend(t, j, []byte("a"))
	if clk.Pending() != 1 {
		t.Fatalf("%d timers after the first write", clk.Pending())
	}
	clk.Advance(5 * time.Second)
	if !fired(due) {
		t.Fatal("did not fire 5 s after the write")
	}
	// An overdue record fires again, but not at once: a publisher whose
	// publication fails would otherwise spin on it. Retry apart.
	due = j.Due(ctx, 5*time.Second)
	if fired(due) {
		t.Fatal("Due fired again within Retry of firing")
	}
	clk.Advance(100 * time.Millisecond)
	if !fired(due) {
		t.Fatal("an overdue record must fire Retry after the last firing")
	}
	// Publishing everything leaves nothing to fire for.
	publish(t, j, 1)
	due = j.Due(ctx, time.Second)
	clk.Advance(time.Hour)
	if fired(due) {
		t.Fatal("fired with nothing unpublished")
	}
}

// A deadline is a timer, not a loop: a thousand writes do not add timers.
func TestDueUsesOneTimer(t *testing.T) {
	clk := newClock()
	j := open(t, journal.Options{Capacity: 1 << 20, Segment: 64 << 10, Disk: crash.New(), Clock: clk})
	defer closeWithin(t, j)
	j.Due(ctx, 5*time.Second)
	for i := 0; i < 1000; i++ {
		mustAppend(t, j, []byte("x"))
		clk.Advance(time.Millisecond)
	}
	if n := clk.Pending(); n != 1 {
		t.Fatalf("%d timers pending", n)
	}
}

func TestDueEndsWithTheJournal(t *testing.T) {
	j, _ := newDue(t)
	due := j.Due(ctx, 5*time.Second)
	closeWithin(t, j)
	if !fired(due) {
		t.Fatal("Close left a Due channel open")
	}
	if !fired(j.Due(ctx, time.Second)) {
		t.Fatal("Due on a closed journal blocks")
	}
}

// The publication bound, end to end: a writer that never stops and a
// publisher that acts when Due says so. No record is unpublished for longer
// than the bound while the store is healthy; during an outage the age grows
// and State says the bound no longer holds; after it the backlog drains.
func TestBoundHoldsUnderContinuousWrites(t *testing.T) {
	const bound = 5 * time.Second
	clk := newClock()
	j := open(t, journal.Options{Capacity: 1 << 20, Segment: 64 << 10, Bound: bound, Disk: crash.New(), Clock: clk})
	defer closeWithin(t, j)

	var worst time.Duration
	step := 10 * time.Millisecond
	due := j.Due(ctx, bound)
	publish := func() {
		publish(t, j, j.Marks().Visible)
		if err := j.Reclaim(); err != nil {
			t.Fatal(err)
		}
		due = j.Due(ctx, bound)
	}
	for i := 0; i < 6000; i++ { // one minute
		mustAppend(t, j, payload(uint64(i)))
		clk.Advance(step)
		if age := j.State().Age; age > worst {
			worst = age
		}
		if fired(due) {
			publish()
		}
	}
	if worst > bound {
		t.Fatalf("a record stayed unpublished for %v with a healthy store", worst)
	}
	if worst < bound-step {
		t.Fatalf("worst age %v: the publisher acted long before it had to", worst)
	}
	if s := j.State(); s.Degraded {
		t.Fatalf("degraded with a healthy store: %+v", s)
	}

	// The store stops answering. The writer goes on; nothing is dropped.
	var before journal.State
	for i := 0; i < 2000; i++ { // twenty seconds
		mustAppend(t, j, payload(uint64(i)))
		clk.Advance(step)
		if i == 0 {
			before = j.State()
		}
	}
	s := j.State()
	if !s.Degraded || s.Age < 20*time.Second || s.Age > 21*time.Second {
		t.Fatalf("state %+v after 20 s of outage", s)
	}
	if s.Age <= before.Age || s.Backlog <= before.Backlog {
		t.Fatalf("outage not visible: %+v then %+v", before, s)
	}
	if !fired(due) {
		t.Fatal("Due did not fire during the outage")
	}

	// The store comes back and takes everything.
	publish()
	if s := j.State(); s.Degraded || s.Age != 0 || s.Backlog != 0 {
		t.Fatalf("state %+v after the backlog drained", s)
	}
}
