package journal_test

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/hanzoai/vfs/journal"
	"github.com/hanzoai/vfs/journal/crash"
)

// Backpressure: as the journal fills, Append slows; when it is full, Append
// fails with ErrFull; nothing is dropped; and once the publisher drains it,
// appends resume and the old records are replayed in order.
func TestBackpressure(t *testing.T) {
	const seg = 64 << 10
	clk := newClock()
	d := crash.New()
	j := open(t, journal.Options{Capacity: 32 * seg, Segment: seg, Disk: d, Clock: clk})
	defer j.Close()
	chunk := bytes.Repeat([]byte("c"), 20<<10) // three to a segment

	var seqs []uint64
	slowed, fullAt := -1, -1
	for i := 0; i < 200 && fullAt < 0; i++ {
		type result struct {
			seq uint64
			err error
		}
		done := make(chan result, 1)
		go func() {
			seq, err := j.Append(ctx, chunk)
			done <- result{seq, err}
		}()
		var r result
		select {
		case r = <-done:
		default:
			// Either it finished or it set a timer to wait out its delay.
			waitFor(t, "append to finish or slow down", func() bool { return len(done) == 1 || clk.Pending() > 0 })
			if clk.Pending() > 0 {
				if slowed < 0 {
					slowed = i
				}
				clk.Advance(time.Second)
			}
			r = <-done
		}
		switch {
		case errors.Is(r.err, journal.ErrFull):
			fullAt = i
		case r.err != nil:
			t.Fatalf("Append %d: %v", i, r.err)
		default:
			seqs = append(seqs, r.seq)
		}
	}
	if slowed < 0 {
		t.Fatal("Append never slowed as the journal filled")
	}
	if fullAt < 0 {
		t.Fatal("Append never returned ErrFull")
	}
	if slowed >= fullAt {
		t.Fatalf("slowed at %d, full at %d: no slowdown before the failure", slowed, fullAt)
	}
	s := j.State()
	if !s.Degraded || s.Backlog < int64(len(seqs)*len(chunk)) {
		t.Fatalf("state %+v with %d records of %d bytes unpublished", s, len(seqs), len(chunk))
	}
	// Refusing is stable: it neither drops nor changes anything.
	if _, err := j.Append(ctx, chunk); !errors.Is(err, journal.ErrFull) {
		t.Fatalf("Append while full: %v", err)
	}
	if m := j.Marks(); m.Visible != uint64(len(seqs)) {
		t.Fatalf("visible %d, appended %d", m.Visible, len(seqs))
	}
	got := replayed(t, j)
	if len(got) != len(seqs) {
		t.Fatalf("replay has %d records, %d were acknowledged", len(got), len(seqs))
	}

	// Recovery drains in order: publish half, reclaim, and the journal takes records again.
	half := uint64(len(seqs) / 2)
	if err := j.Publish(half); err != nil {
		t.Fatal(err)
	}
	if err := j.Reclaim(); err != nil {
		t.Fatal(err)
	}
	next, err := j.Append(ctx, []byte("after"))
	if err != nil {
		t.Fatalf("Append after draining: %v", err)
	}
	if next != uint64(len(seqs))+1 {
		t.Fatalf("sequence %d after draining, want %d", next, len(seqs)+1)
	}
	got = replayed(t, j)
	if got[0].Seq != half+1 || got[len(got)-1].Seq != next {
		t.Fatalf("replay after draining %d..%d", got[0].Seq, got[len(got)-1].Seq)
	}
	for i := 1; i < len(got); i++ {
		if got[i].Seq != got[i-1].Seq+1 {
			t.Fatalf("replay out of order at %d", i)
		}
	}
	if s := j.State(); s.Err != nil {
		t.Fatalf("a full journal is not a failed one: %v", s.Err)
	}
}

// A journal with room is not slowed.
func TestNoSlowdownWhenEmpty(t *testing.T) {
	clk := newClock()
	j := open(t, journal.Options{Capacity: 64 << 20, Segment: 16 << 20, Disk: crash.New(), Clock: clk})
	defer j.Close()
	chunk := bytes.Repeat([]byte("c"), 1<<20)
	for i := 0; i < 20; i++ {
		mustAppend(t, j, chunk) // blocks forever on the fake clock if it slows
	}
	if clk.Pending() != 0 {
		t.Fatal("a timer is pending though the journal is a third full")
	}
	if s := j.State(); s.Degraded {
		t.Fatalf("degraded at %+v", s)
	}
}

// An Append that is slowed gives up when its context ends.
func TestSlowedAppendCancels(t *testing.T) {
	const seg = 64 << 10
	clk := newClock()
	j := open(t, journal.Options{Capacity: 32 * seg, Segment: seg, Disk: crash.New(), Clock: clk})
	defer j.Close()
	chunk := bytes.Repeat([]byte("c"), 20<<10)
	for i := 0; i < 200; i++ {
		c, cancel := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { _, err := j.Append(c, chunk); done <- err }()
		waitFor(t, "append to finish or slow down", func() bool { return len(done) == 1 || clk.Pending() > 0 })
		if len(done) == 0 {
			cancel()
			if err := <-done; !errors.Is(err, context.Canceled) {
				t.Fatalf("slowed Append returned %v when cancelled", err)
			}
			if clk.Pending() != 0 {
				t.Fatal("the cancelled Append left its timer behind")
			}
			return
		}
		cancel()
		if err := <-done; err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	t.Fatal("Append never slowed")
}

// A publisher that is slow but alive keeps the journal out of the red: State
// shows the backlog and the age, and Due tells the publisher when to act.
func TestOutageIsReported(t *testing.T) {
	clk := newClock()
	j := open(t, journal.Options{Capacity: 64 << 20, Segment: 1 << 20, Bound: 5 * time.Second, Disk: crash.New(), Clock: clk})
	defer j.Close()
	if s := j.State(); s.Degraded || s.Age != 0 || s.Backlog != 0 {
		t.Fatalf("idle state %+v", s)
	}
	for i := 0; i < 10; i++ {
		mustAppend(t, j, []byte("0123456789"))
		clk.Advance(time.Second)
	}
	s := j.State()
	if !s.Degraded || s.Age != 10*time.Second || s.Backlog != 10*(28+10) {
		t.Fatalf("state %+v after 10 s without publication", s)
	}
	// Later writes do not reset the age of the oldest.
	mustAppend(t, j, []byte("late"))
	if s := j.State(); s.Age != 10*time.Second {
		t.Fatalf("age %v after another write", s.Age)
	}
	if err := j.Publish(10); err != nil {
		t.Fatal(err)
	}
	if s := j.State(); s.Degraded || s.Age != 0 || s.Backlog != 28+4 {
		t.Fatalf("state %+v after publishing the old records", s)
	}
}

func TestStateBacklogIsExact(t *testing.T) {
	j := open(t, small(crash.New(), ticking{newClock(), 2e6}, 0))
	defer j.Close()
	sizes := []int{0, 1, 77, 300, 5}
	var total int64
	for _, n := range sizes {
		mustAppend(t, j, make([]byte, n))
		total += int64(28 + n)
	}
	if s := j.State(); s.Backlog != total {
		t.Fatalf("backlog %d, want %d", s.Backlog, total)
	}
	if err := j.Publish(3); err != nil {
		t.Fatal(err)
	}
	if s := j.State(); s.Backlog != int64(28+300+28+5) {
		t.Fatalf("backlog %d after Publish(3)", s.Backlog)
	}
	if err := j.Publish(1); err != nil { // never goes back
		t.Fatal(err)
	}
	if m := j.Marks(); m.Published != 3 {
		t.Fatalf("published %d after a lower Publish", m.Published)
	}
}
