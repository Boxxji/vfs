package journal_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/hanzoai/vfs/journal"
	"github.com/hanzoai/vfs/journal/crash"
)

var ctx = context.Background()

// clock is a clock the test controls. Time moves only when the test moves it.
type clock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*timer
}

type timer struct {
	c    *clock
	at   time.Time
	f    func()
	done bool
}

func newClock() *clock { return &clock{now: time.Unix(1_700_000_000, 0)} }

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *clock) AfterFunc(d time.Duration, f func()) journal.Timer {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := &timer{c: c, at: c.now.Add(d), f: f}
	c.timers = append(c.timers, t)
	return t
}

func (t *timer) Stop() bool {
	t.c.mu.Lock()
	defer t.c.mu.Unlock()
	was := !t.done
	t.done = true
	return was
}

// Advance moves time forward and runs, in deadline order, every timer that
// falls due, each in the calling goroutine.
func (c *clock) Advance(d time.Duration) {
	c.mu.Lock()
	target := c.now.Add(d)
	c.mu.Unlock()
	for {
		c.mu.Lock()
		var next *timer
		for _, t := range c.timers {
			if !t.done && !t.at.After(target) && (next == nil || t.at.Before(next.at)) {
				next = t
			}
		}
		if next == nil {
			c.now = target
			c.mu.Unlock()
			return
		}
		next.done = true
		if next.at.After(c.now) {
			c.now = next.at
		}
		c.mu.Unlock()
		next.f()
	}
}

// Pending is how many timers have not fired or been stopped.
func (c *clock) Pending() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, t := range c.timers {
		if !t.done {
			n++
		}
	}
	return n
}

// ticking advances by step on every Now, so every acknowledgement gets a
// distinct time further apart than the journal's span quantum.
type ticking struct {
	*clock
	step time.Duration
}

func (t ticking) Now() time.Time {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	t.clock.now = t.clock.now.Add(t.step)
	return t.clock.now
}

// small is a journal that fills quickly: 4 KiB segments, six of them.
func small(disk journal.Disk, clk journal.Clock, published uint64) journal.Options {
	return journal.Options{Capacity: 6 * 4096, Segment: 4096, Disk: disk, Clock: clk, Published: published}
}

func open(t testing.TB, opt journal.Options) *journal.Journal {
	t.Helper()
	j, err := journal.Open("", opt)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return j
}

func mustAppend(t testing.TB, j *journal.Journal, p []byte) uint64 {
	t.Helper()
	seq, err := j.Append(ctx, p)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	return seq
}

func mustSync(t testing.TB, j *journal.Journal, seq uint64) {
	t.Helper()
	if err := j.Sync(ctx, seq); err != nil {
		t.Fatalf("Sync(%d): %v", seq, err)
	}
}

type entry struct {
	Seq  uint64
	Data string
}

func replay(j *journal.Journal) ([]entry, error) {
	var out []entry
	err := j.Replay(func(r journal.Record) error {
		out = append(out, entry{r.Seq, string(r.Data)})
		return nil
	})
	return out, err
}

func replayed(t testing.TB, j *journal.Journal) []entry {
	t.Helper()
	out, err := replay(j)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	return out
}

// payload is the bytes of the record with sequence seq in the property tests:
// a size that varies with seq and a content that names it.
func payload(seq uint64) []byte {
	n := int(seq*37%200) + 1
	return bytes.Repeat([]byte(fmt.Sprintf("%d.", seq)), n/2+1)[:n]
}

func seqs(es []entry) []uint64 {
	out := make([]uint64, len(es))
	for i, e := range es {
		out[i] = e.Seq
	}
	return out
}

func sorted(s []uint64) bool {
	return sort.SliceIsSorted(s, func(a, b int) bool { return s[a] < s[b] })
}

var _ = crash.Sector

// waitFor spins until cond holds. It watches state the journal exposes (a
// waiter registered, a timer set), so it ends as soon as the state is reached.
func waitFor(t testing.TB, what string, cond func() bool) {
	t.Helper()
	for i := 0; i < 50_000_000; i++ {
		if cond() {
			return
		}
		runtime.Gosched()
	}
	t.Fatalf("gave up waiting for %s", what)
}

// parked counts syncs and holds each one in the file layer until the test
// lets it finish.
type parked struct {
	entered chan struct{}
	release chan struct{}
}

func park(d *crash.Disk) *parked {
	p := &parked{make(chan struct{}, 64), make(chan struct{})}
	d.Intercept(func(e crash.Event) error {
		if e.Kind == "sync" {
			p.entered <- struct{}{}
			<-p.release
		}
		return nil
	})
	return p
}

// publish tells the journal the store holds seq, as a publisher does once the
// Head carries it. The record is made durable first: Publish refuses a record
// no local sync has covered.
func publish(t testing.TB, j *journal.Journal, seq uint64) {
	t.Helper()
	mustSync(t, j, seq)
	if err := j.Publish(seq); err != nil {
		t.Fatalf("Publish(%d): %v", seq, err)
	}
}

// scratch is a directory for a test that needs the operating system's files.
// It is made next to the tests, not in the temporary directory: that may be
// memory (tmpfs), where the journal refuses to run.
func scratch(t testing.TB) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	dir, err := os.MkdirTemp(wd, ".scratch-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}
