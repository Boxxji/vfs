package bufpool

import (
	"bytes"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"
	"weak"
)

// A fakeClock stands in for the clock and the deadline timer of a Budget, so that
// decay is tested without waiting.
type fakeClock struct {
	now    time.Time
	timers []*fakeTimer
}

type fakeTimer struct {
	at             time.Time
	f              func()
	stopped, fired bool
}

func (t *fakeTimer) Stop() bool {
	was := !t.stopped && !t.fired
	t.stopped = true
	return was
}

func (c *fakeClock) after(d time.Duration, f func()) timer {
	t := &fakeTimer{at: c.now.Add(d), f: f}
	c.timers = append(c.timers, t)
	return t
}

// armed is the number of timers that are neither stopped nor fired.
func (c *fakeClock) armed() (n int) {
	for _, t := range c.timers {
		if !t.stopped && !t.fired {
			n++
		}
	}
	return n
}

// advance moves the clock and fires the timers that fall due, earliest first.
func (c *fakeClock) advance(d time.Duration) {
	c.now = c.now.Add(d)
	for fired := 0; ; fired++ {
		if fired > 1000 {
			panic("timers re-arm at the same instant for ever")
		}
		var next *fakeTimer
		for _, t := range c.timers {
			if !t.stopped && !t.fired && !t.at.After(c.now) && (next == nil || t.at.Before(next.at)) {
				next = t
			}
		}
		if next == nil {
			return
		}
		next.fired = true
		next.f()
	}
}

func fakeBudget(limit int64, decay time.Duration) (*Budget, *fakeClock) {
	b := NewBudget(limit, decay)
	c := &fakeClock{now: time.Unix(1000, 0)}
	b.now = func() time.Time { return c.now }
	b.after = c.after
	return b, c
}

func allZero(b []byte) bool {
	for _, x := range b {
		if x != 0 {
			return false
		}
	}
	return true
}

func TestClasses(t *testing.T) {
	prev := 0
	for n := 1; n <= MaxSize; n += 1 + n/97 {
		idx, size := classOf(n)
		if size < n || size < MinSize {
			t.Fatalf("classOf(%d) = %d, too small", n, size)
		}
		if n > MinSize && size > n+n/4 {
			t.Fatalf("classOf(%d) = %d wastes more than a quarter", n, size)
		}
		if idx < prev {
			t.Fatalf("classOf(%d) index %d went back from %d", n, idx, prev)
		}
		if again, s2 := classOf(size); again != idx || s2 != size {
			t.Fatalf("class size %d is not its own class: %d/%d vs %d/%d", size, again, s2, idx, size)
		}
		prev = idx
	}
	if idx, size := classOf(MaxSize); idx != classes-1 || size != MaxSize {
		t.Fatalf("classOf(MaxSize) = %d, %d; want %d, %d", idx, size, classes-1, MaxSize)
	}
	for _, c := range []struct{ n, size int }{
		{1, 4096}, {4096, 4096}, {4097, 5120}, {5120, 5120}, {5121, 6144}, {7169, 8192}, {8192, 8192}, {8193, 10240},
		{1 << 20, 1 << 20}, {1<<20 + 1, 1<<20 + 256<<10},
	} {
		if _, size := classOf(c.n); size != c.size {
			t.Errorf("classOf(%d) size = %d, want %d", c.n, size, c.size)
		}
	}
}

func TestGetPut(t *testing.T) {
	p := NewBudget(1<<30, 0).Pool(false)
	b := p.Get(100000)
	if len(b) != 100000 || cap(b) < 100000 {
		t.Fatalf("Get(100000): len %d cap %d", len(b), cap(b))
	}
	if _, size := classOf(100000); cap(b) != size {
		t.Fatalf("cap %d, want class size %d", cap(b), size)
	}
	b[0] = 7
	p.Put(b)
	if got := p.Stats(); got.Idle != int64(cap(b)) || got.Hits != 0 {
		t.Fatalf("after Put: %+v", got)
	}
	// Another size in the same class gets the same storage back.
	c := p.Get(105000)
	if &c[0] != &b[0] {
		t.Fatal("a slice of the same class was not reused")
	}
	if len(c) != 105000 || cap(c) != cap(b) {
		t.Fatalf("reused slice has len %d cap %d, want 105000 and %d", len(c), cap(c), cap(b))
	}
	if got := p.Stats(); got.Idle != 0 || got.Hits != 1 || got.Gets != 2 {
		t.Fatalf("after reuse: %+v", got)
	}
	if p.Get(0) != nil {
		t.Fatal("Get(0) should return nil")
	}
	p.Put(nil)
}

// TestLargestClassIsKept: the 64 MiB class is a class like the others.
func TestLargestClassIsKept(t *testing.T) {
	p := NewBudget(1<<30, 0).Pool(false)
	p.Put(p.Get(MaxSize))
	if st := p.Stats(); st.Idle != MaxSize || st.Drops != 0 || st.Refused != 0 {
		t.Fatalf("the 64 MiB class was not kept: %+v", st)
	}
}

func TestOversize(t *testing.T) {
	p := NewBudget(1<<30, 0).Pool(false)
	b := p.Get(MaxSize + 1)
	if len(b) != MaxSize+1 {
		t.Fatalf("len %d", len(b))
	}
	p.Put(b)
	if got := p.Stats(); got.Idle != 0 || got.Refused != 1 || got.Gets != 1 {
		t.Fatalf("oversize: %+v", got)
	}
}

// TestLimit: the Budget holds at most its limit, to the byte, and a limit of 0
// keeps nothing.
func TestLimit(t *testing.T) {
	size := 1 << 20
	bud := NewBudget(int64(2*size), 0)
	p := bud.Pool(false)
	bufs := [][]byte{p.Get(size), p.Get(size), p.Get(size)}
	for _, b := range bufs {
		p.Put(b)
	}
	if got := p.Stats(); got.Idle != int64(2*size) || got.Drops != 1 {
		t.Fatalf("limit of two buffers: %+v", got)
	}
	if got := bud.Stats(); got.Idle != int64(2*size) || got.Evicted != 1 {
		t.Fatalf("budget: %+v", got)
	}
	if n := registered(bud); n != 2 {
		t.Fatalf("%d records for two tracked slices (the evicted one must go)", n)
	}
	// Taking one makes room for one.
	p.Get(size)
	p.Put(bufs[2])
	if got := p.Stats(); got.Idle != int64(2*size) {
		t.Fatalf("after making room: %+v", got)
	}
	q := NewBudget(0, 0).Pool(false)
	q.Put(q.Get(size))
	if got := q.Stats(); got.Idle != 0 || got.Drops != 1 {
		t.Fatalf("limit 0: %+v", got)
	}
}

// TestEvictsOldestAcrossPoolsAndClasses: when the Budget is full the slice that
// has been idle longest goes, whichever pool and class it is in.
func TestEvictsOldestAcrossPoolsAndClasses(t *testing.T) {
	bud := NewBudget(3<<20, 0)
	a, b := bud.Pool(false), bud.Pool(true)
	x := a.Get(1 << 20)
	y := b.Get(2 << 20)
	z := a.Get(1 << 20)
	a.Put(x) // oldest
	b.Put(y)
	if bud.Stats().Idle != 3<<20 {
		t.Fatalf("budget: %+v", bud.Stats())
	}
	a.Put(z) // 4 MiB would idle: x, the oldest, goes
	if got := a.Stats(); got.Idle != 1<<20 || got.Drops != 1 {
		t.Fatalf("pool a: %+v", got)
	}
	if got := b.Stats(); got.Idle != 2<<20 || got.Drops != 0 {
		t.Fatalf("pool b: %+v", got)
	}
	if &a.Get(1 << 20)[0] != &z[0] {
		t.Fatal("the surviving slice of class 1 MiB is not the later one")
	}
}

// TestClassLockout: a phase of one size no longer starves the sizes that follow.
func TestClassLockout(t *testing.T) {
	const limit = 64 << 20
	p := NewBudget(limit, 0).Pool(false)
	var held [][]byte
	for range 16 {
		held = append(held, p.Get(4<<20))
	}
	for _, b := range held {
		p.Put(b)
	}
	if got := p.Stats().Idle; got != limit {
		t.Fatalf("phase 1 left %d idle, want %d", got, limit)
	}
	before := p.Stats().Hits
	for range 1000 {
		p.Put(p.Get(1280 << 10))
	}
	if got := p.Stats().Hits - before; got != 999 {
		t.Fatalf("%d of 1000 Gets served after the workload changed class, want 999", got)
	}
	if got := p.Stats().Idle; got > limit {
		t.Fatalf("idle %d over the limit", got)
	}
}

// TestGlobalCap: eight pools hold no more between them than the Budget allows.
func TestGlobalCap(t *testing.T) {
	const limit = 64 << 20
	bud := NewBudget(limit, 0)
	var pools []*Pool
	for range 8 {
		p := bud.Pool(true)
		var bs [][]byte
		for range 16 {
			bs = append(bs, p.Get(4<<20))
		}
		for _, b := range bs {
			p.Put(b)
		}
		pools = append(pools, p)
		if got := bud.Stats().Idle; got > limit {
			t.Fatalf("idle %d over the limit %d", got, limit)
		}
	}
	var sum int64
	for _, p := range pools {
		sum += p.Stats().Idle
	}
	if got := bud.Stats().Idle; got != sum || got != limit {
		t.Fatalf("budget idle %d, pools hold %d, want %d", got, sum, limit)
	}
	if got := bud.Stats().Evicted; got != 8*16-16 {
		t.Fatalf("%d evicted, want %d", got, 8*16-16)
	}
}

// TestDecay: a slice idle for the decay time is let go; one timer runs while
// anything is idle and none otherwise.
func TestDecay(t *testing.T) {
	const decay = time.Minute
	bud, clock := fakeBudget(1<<30, decay)
	p := bud.Pool(false)
	if clock.armed() != 0 {
		t.Fatal("a timer runs with nothing idle")
	}
	a, b, c := p.Get(1<<20), p.Get(1<<20), p.Get(2<<20)
	p.Put(a)
	if clock.armed() != 1 {
		t.Fatalf("%d timers armed with one idle slice", clock.armed())
	}
	clock.advance(20 * time.Second)
	p.Put(b)
	p.Put(c)
	if clock.armed() != 1 {
		t.Fatalf("%d timers armed with three idle slices", clock.armed())
	}
	clock.advance(39 * time.Second) // 59 s: nothing yet
	if got := bud.Stats(); got.Idle != 4<<20 || got.Decayed != 0 {
		t.Fatalf("before the decay time: %+v", got)
	}
	clock.advance(time.Second) // 60 s: a goes
	if got := bud.Stats(); got.Idle != 3<<20 || got.Decayed != 1 {
		t.Fatalf("a should have decayed: %+v", got)
	}
	if clock.armed() != 1 {
		t.Fatalf("%d timers armed for the next slice, want 1", clock.armed())
	}
	clock.advance(20 * time.Second) // b and c, idle since 20 s, go
	if got := bud.Stats(); got.Idle != 0 || got.Decayed != 3 {
		t.Fatalf("all should have decayed: %+v", got)
	}
	if clock.armed() != 0 {
		t.Fatalf("%d timers armed with nothing idle", clock.armed())
	}
	if got := p.Stats(); got.Drops != 3 || got.Idle != 0 {
		t.Fatalf("pool: %+v", got)
	}
	if n := registered(bud); n != 0 {
		t.Fatalf("%d records kept for slices that were let go", n)
	}
	// A slice that was dropped is no longer the pool's to take back.
	p.Put(a)
	if got := p.Stats(); got.Refused != 1 || got.Idle != 0 {
		t.Fatalf("a decayed slice was taken back: %+v", got)
	}
}

// TestDecayStopsWhenTaken: taking the last idle slice stops the timer, and the
// stopped timer cannot drop what is idle later.
func TestDecayStopsWhenTaken(t *testing.T) {
	bud, clock := fakeBudget(1<<30, time.Minute)
	p := bud.Pool(false)
	p.Put(p.Get(1 << 20))
	if clock.armed() != 1 {
		t.Fatalf("%d armed", clock.armed())
	}
	p.Get(1 << 20)
	if clock.armed() != 0 {
		t.Fatalf("%d armed with nothing idle", clock.armed())
	}
	clock.advance(30 * time.Second)
	q := p.Get(1 << 20)
	p.Put(q) // idle again from 30 s
	if clock.armed() != 1 {
		t.Fatalf("%d armed", clock.armed())
	}
	// A timer that was stopped can still fire (Stop does not wait for a callback
	// that has started). It must neither drop what is not due nor arm a second timer.
	clock.timers[0].f()
	if clock.armed() != 1 {
		t.Fatalf("a stale timer firing left %d armed, want 1", clock.armed())
	}
	clock.advance(45 * time.Second) // 75 s: the first timer's deadline passed, this slice has idled 45 s
	if got := bud.Stats(); got.Idle != 1<<20 || got.Decayed != 0 {
		t.Fatalf("dropped too early: %+v", got)
	}
	clock.advance(15 * time.Second)
	if got := bud.Stats(); got.Idle != 0 || got.Decayed != 1 {
		t.Fatalf("not dropped at the decay time: %+v", got)
	}
}

// TestNoDecay: a decay of 0 never arms a timer.
func TestNoDecay(t *testing.T) {
	bud, clock := fakeBudget(1<<30, 0)
	p := bud.Pool(false)
	p.Put(p.Get(1 << 20))
	if clock.armed() != 0 {
		t.Fatal("a timer runs with no decay time")
	}
}

// TestRefusesWhatIsNotOurs: Put takes back a slice that Get gave out, whole, and
// nothing else, and does not touch the memory of what it refuses.
func TestRefusesWhatIsNotOurs(t *testing.T) {
	bud := NewBudget(1<<30, 0)
	p, q := bud.Pool(true), bud.Pool(true)

	// A view cut from a larger buffer, as a frame is cut from a pack.
	pack := p.Get(1 << 20)
	for i := range pack {
		pack[i] = 0xAB
	}
	p.Put(pack[4096 : 4096+1000])
	if i := bytes.IndexByte(pack, 0); i >= 0 {
		t.Fatalf("Put of a view cleared the parent: first zero at %d", i)
	}

	// A three-index view whose capacity is a class size.
	p.Put(pack[0:8192:8192])
	if i := bytes.IndexByte(pack, 0); i >= 0 {
		t.Fatalf("Put of a class-sized view cleared the parent: first zero at %d", i)
	}
	if x := p.Get(8192); &x[0] == &pack[0] {
		t.Fatal("a view of a live buffer was kept and handed out again")
	}

	// A slice the pool never produced, of a class size, and of other sizes.
	foreign := bytes.Repeat([]byte{7}, 8192)
	p.Put(foreign)
	p.Put(make([]byte, 100))
	p.Put(make([]byte, MaxSize+4096))
	if !bytes.Equal(foreign, bytes.Repeat([]byte{7}, 8192)) {
		t.Fatal("Put cleared a slice it does not own")
	}

	// A slice of another pool.
	other := q.Get(1 << 20)
	p.Put(other)

	if st := p.Stats(); st.Idle != 0 || st.Refused != 6 {
		t.Fatalf("pool: %+v", st)
	}
	if st := bud.Stats(); st.Idle != 0 {
		t.Fatalf("budget: %+v", st)
	}
	// Each of the legitimate owners can still put theirs back.
	p.Put(pack)
	q.Put(other)
	if p.Stats().Idle != 1<<20 || q.Stats().Idle != 1<<20 {
		t.Fatalf("owners could not put back: %+v %+v", p.Stats(), q.Stats())
	}
}

// TestCollectedSliceIsRefused: a record whose slice the collector has taken must
// not vouch for whatever is later allocated at the same address.
func TestCollectedSliceIsRefused(t *testing.T) {
	bud := NewBudget(1<<30, 0)
	p := bud.Pool(false)
	b := p.Get(1 << 20)
	bud.mu.Lock()
	bud.reg[start(b)].wp = weak.Pointer[byte]{} // as after a collection
	bud.mu.Unlock()
	p.Put(b)
	if st := p.Stats(); st.Idle != 0 || st.Refused != 1 {
		t.Fatalf("a stale record was honoured: %+v", st)
	}
}

// TestDoublePut: the second Put of a slice is refused, so one array is never on a
// free list twice and never handed to two owners.
func TestDoublePut(t *testing.T) {
	p := NewBudget(1<<30, 0).Pool(false)
	b := p.Get(1 << 20)
	p.Put(b)
	p.Put(b)
	if st := p.Stats(); st.Idle != 1<<20 || st.Refused != 1 {
		t.Fatalf("after a double Put: %+v", st)
	}
	x, y := p.Get(1<<20), p.Get(1<<20)
	if &x[0] == &y[0] {
		t.Fatal("two Gets returned the same memory after a double Put")
	}
	// Putting back what is out again, after a reuse, is fine and refused twice over.
	p.Put(x)
	p.Put(x)
	if st := p.Stats(); st.Refused != 2 {
		t.Fatalf("%+v", st)
	}
}

func TestZero(t *testing.T) {
	bud := NewBudget(1<<30, 0)
	p := bud.Pool(true)
	b := p.Get(70000)
	for i := range b[:cap(b)] {
		b[:cap(b)][i] = 0xAA
	}
	p.Put(b)
	c := p.Get(75000)
	if &c[0] != &b[0] {
		t.Fatal("not reused")
	}
	if !allZero(c[:cap(c)]) {
		t.Fatal("a zeroing pool handed out bytes that were not cleared")
	}
	// A slice let go at once is cleared too: it may be the last owner of plaintext.
	q := NewBudget(0, 0).Pool(true)
	d := q.Get(70000)
	d[:cap(d)][5] = 1
	q.Put(d)
	if !allZero(d[:cap(d)]) {
		t.Fatal("a dropped slice was not cleared")
	}
	// A pool that does not zero leaves the bytes alone.
	r := bud.Pool(false)
	e := r.Get(70000)
	e[3] = 9
	r.Put(e)
	if f := r.Get(70000); f[3] != 9 {
		t.Fatal("a non-zeroing pool cleared the slice")
	}
}

func TestClose(t *testing.T) {
	bud := NewBudget(1<<30, 0)
	p, q := bud.Pool(false), bud.Pool(false)
	out := p.Get(1 << 20)
	p.Put(p.Get(2 << 20))
	q.Put(q.Get(1 << 20))
	p.Close()
	if got := bud.Stats().Idle; got != 1<<20 {
		t.Fatalf("after Close the budget holds %d, want only q's 1 MiB", got)
	}
	p.Put(out)
	if st := p.Stats(); st.Idle != 0 || st.Refused != 1 {
		t.Fatalf("a closed pool took a slice back: %+v", st)
	}
	if b := p.Get(1 << 20); len(b) != 1<<20 {
		t.Fatal("a closed pool should still allocate")
	}
}

func TestBuffer(t *testing.T) {
	p := NewBudget(1<<30, 0).Pool(false)
	b := p.Buffer(1000)
	var want bytes.Buffer
	for i := 0; i < 300; i++ {
		chunk := bytes.Repeat([]byte{byte(i)}, 1+i*37)
		if n, err := b.Write(chunk); n != len(chunk) || err != nil {
			t.Fatalf("Write: %d, %v", n, err)
		}
		want.Write(chunk)
		if !bytes.Equal(b.Bytes(), want.Bytes()) || b.Len() != want.Len() {
			t.Fatalf("contents differ after %d writes", i+1)
		}
	}
	// The outgrown storage went back to the pool and is reused.
	if got := p.Stats(); got.Idle == 0 {
		t.Fatalf("growth did not return storage: %+v", got)
	}
	b.Reset()
	if b.Len() != 0 {
		t.Fatal("Reset did not empty")
	}
	before := cap(b.Bytes())
	b.Write([]byte("x"))
	if cap(b.Bytes()) != before {
		t.Fatal("Reset gave up its storage")
	}
	idle := p.Stats().Idle
	b.Release()
	if b.Len() != 0 {
		t.Fatal("Release did not empty")
	}
	if got := p.Stats().Idle; got <= idle {
		t.Fatalf("Release did not return the storage: idle %d before, %d after", idle, got)
	}
	b.Release() // twice is harmless
	if st := p.Stats(); st.Refused != 0 {
		t.Fatalf("a second Release was offered to Put: %+v", st)
	}
	b.Write([]byte("again"))
	if string(b.Bytes()) != "again" {
		t.Fatalf("write after Release: %q", b.Bytes())
	}
	var _ io.Writer = b
}

// TestBufferAmortized checks that growth is geometric: a quarter million
// one-byte writes take about ten slices from the pool, not a quarter million.
func TestBufferAmortized(t *testing.T) {
	p := NewBudget(1<<30, 0).Pool(false)
	b := p.Buffer(0)
	for range 1 << 18 {
		b.Write([]byte{1})
	}
	if b.Len() != 1<<18 {
		t.Fatalf("Len %d", b.Len())
	}
	if got := p.Stats().Gets; got > 16 {
		t.Fatalf("%d slices taken for 256 KiB", got)
	}
}

func TestBufferGrowPreserves(t *testing.T) {
	p := NewBudget(1<<30, 0).Pool(false)
	b := p.Buffer(0)
	b.Write([]byte("abc"))
	b.Grow(10 << 20)
	if string(b.Bytes()) != "abc" || cap(b.Bytes())-b.Len() < 10<<20 {
		t.Fatalf("Grow: %q cap %d", b.Bytes(), cap(b.Bytes()))
	}
}

// TestBufferViewDiesOnGrow pins down what Bytes promises, which is little: a view
// of the storage does not survive a growing write, because the old array goes back
// to the pool and, on a zeroing pool, is cleared.
func TestBufferViewDiesOnGrow(t *testing.T) {
	p := NewBudget(1<<30, 0).Pool(true)
	b := p.Buffer(8192)
	b.Write(bytes.Repeat([]byte{0x5A}, 6000))
	view := b.Bytes()[:4096]
	b.Write(make([]byte, 1<<20))
	if !allZero(view) {
		t.Fatal("the documented hazard changed: a view of the old storage still holds data after a growing write")
	}
}

// TestBufferDetach: bytes that must outlive the Buffer's next writes are
// detached, and then nothing the Buffer does reaches them.
func TestBufferDetach(t *testing.T) {
	p := NewBudget(1<<30, 0).Pool(true)
	b := p.Buffer(8192)
	b.Write(bytes.Repeat([]byte{0x5A}, 6000))
	span := b.Detach()
	if len(span) != 6000 || b.Len() != 0 {
		t.Fatalf("detached %d bytes, buffer holds %d", len(span), b.Len())
	}
	view := span[:4096]
	b.Write(make([]byte, 1<<20)) // grows, resets, releases: none may reach the span
	b.Reset()
	b.Release()
	b.Write([]byte("more"))
	if bytes.Count(view, []byte{0x5A}) != len(view) {
		t.Fatal("a detached span changed under the buffer's later use")
	}
	p.Put(span)
	if st := p.Stats(); st.Refused != 0 || st.Idle == 0 {
		t.Fatalf("the detached span could not be put back: %+v", st)
	}
}

// TestResetClearsPlaintext: Reset on a zeroing pool does not leave the last use's
// bytes in storage the Buffer keeps.
func TestResetClearsPlaintext(t *testing.T) {
	p := NewBudget(1<<30, 0).Pool(true)
	b := p.Buffer(8192)
	b.Write([]byte("PLAINTEXT-OF-THE-LAST-FLUSH"))
	view := b.Bytes()
	b.Reset()
	if bytes.Contains(view[:cap(view)], []byte("PLAINTEXT")) {
		t.Fatal("Reset left the plaintext in the buffer of a zeroing pool")
	}
	q := NewBudget(1<<30, 0).Pool(false)
	c := q.Buffer(8192)
	c.Write([]byte("CIPHERTEXT"))
	cv := c.Bytes()
	c.Reset()
	if !bytes.Contains(cv[:cap(cv)], []byte("CIPHERTEXT")) {
		t.Fatal("Reset cleared a pool that does not zero")
	}
}

// TestShared: a slice held by a cache and a reader goes back only when both have
// let go, and the reader's bytes are stable until then.
func TestShared(t *testing.T) {
	p := NewBudget(1<<30, 0).Pool(true)
	b := p.Get(1 << 20)
	copy(b, "chunk")
	s := p.Share(b)

	view, ok := s.Pin()
	if !ok || string(view[:5]) != "chunk" {
		t.Fatalf("Pin: %v", ok)
	}
	s.Release() // the cache evicts it
	s.Release() // twice is harmless
	if st := p.Stats(); st.Idle != 0 {
		t.Fatalf("the slice went back while pinned: %+v", st)
	}
	other := p.Get(1 << 20)
	if &other[0] == &view[0] {
		t.Fatal("a pinned slice was handed to another owner")
	}
	copy(other, "intruder")
	if string(view[:5]) != "chunk" {
		t.Fatalf("the reader's bytes changed: %q", view[:8])
	}
	s.Unpin()
	if st := p.Stats(); st.Idle != 1<<20 || st.Refused != 0 {
		t.Fatalf("the last unpin did not return the slice: %+v", st)
	}
	if _, ok := s.Pin(); ok {
		t.Fatal("Pin succeeded on a slice that has gone back")
	}
	if p.Get(1 << 20); !allZero(view) && &view[0] != &other[0] {
		t.Fatal("the returned slice was not cleared")
	}
}

// TestSharedConcurrent: readers pin and verify while the owner releases and the
// pool recycles; no reader ever sees another owner's bytes.
func TestSharedConcurrent(t *testing.T) {
	p := NewBudget(8<<20, 0).Pool(true)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 60; i++ {
				b := p.Get(64 << 10)
				for j := range b {
					b[j] = byte(g + 1)
				}
				s := p.Share(b)
				var readers sync.WaitGroup
				for r := 0; r < 3; r++ {
					readers.Add(1)
					go func() {
						defer readers.Done()
						if v, ok := s.Pin(); ok {
							for _, x := range v {
								if x != byte(g+1) {
									t.Errorf("reader saw %d, want %d", x, g+1)
									break
								}
							}
							s.Unpin()
						}
					}()
				}
				s.Release()
				readers.Wait()
			}
		}()
	}
	wg.Wait()
}

// registered is the number of slices the budget has a record of.
func registered(b *Budget) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.reg)
}

// TestRegistryIsSwept: the records of slices that were dropped without a Put, and
// that the collector has taken, do not stay for ever. Records are made stale by hand,
// as a collection would, and the next Gets sweep them.
func TestRegistryIsSwept(t *testing.T) {
	bud := NewBudget(1<<30, 0)
	p := bud.Pool(false)
	keep := p.Get(MinSize)
	bud.mu.Lock()
	for i := range 1000 {
		bud.reg[uintptr(i+1)] = &entry{key: uintptr(i + 1), size: MinSize, pool: p} // a zero weak pointer is a collected one
	}
	bud.mu.Unlock()
	for range 10 {
		p.Put(p.Get(MinSize))
	}
	if n := registered(bud); n > 10 {
		t.Fatalf("%d records remain after a sweep, want only the live ones", n)
	}
	p.Put(keep)
	if st := p.Stats(); st.Refused != 0 || st.Idle == 0 {
		t.Fatalf("a live slice lost its record in the sweep: %+v", st)
	}
}

// TestConcurrent has no two goroutines own one slice at a time: each stamps its
// slice with its own id, yields, and checks the stamp before returning it.
func TestConcurrent(t *testing.T) {
	bud := NewBudget(8<<20, 0)
	p := bud.Pool(true)
	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 150; i++ {
				n := 4096 + (g*7919+i*104729)%(256<<10)
				b := p.Get(n)
				if !allZero(b) {
					t.Errorf("goroutine %d: Get returned unclean bytes", g)
					return
				}
				for j := range b {
					b[j] = byte(g + 1)
				}
				for j := range b {
					if b[j] != byte(g+1) {
						t.Errorf("goroutine %d: slice shared with another owner", g)
						return
					}
				}
				if i%7 == 0 {
					bb := p.Buffer(n)
					bb.Write(b[:min(len(b), 100000)])
					bb.Release()
				}
				if i%11 == 0 {
					_ = p.Stats()
					_ = bud.Stats()
				}
				p.Put(b)
			}
		}()
	}
	wg.Wait()
	if got := bud.Stats(); got.Idle > 8<<20 || got.Idle < 0 {
		t.Fatalf("after the run: %+v", got)
	}
	if got := p.Stats(); got.Idle != bud.Stats().Idle || got.Refused != 0 {
		t.Fatalf("pool %+v, budget %+v", got, bud.Stats())
	}
}

// BenchmarkGetPut is the cost of taking a slice from a warm pool and giving it back.
func BenchmarkGetPut(b *testing.B) {
	for _, zero := range []bool{false, true} {
		for _, n := range []int{4 << 10, 128 << 10, 1 << 20, 4 << 20} {
			b.Run(fmt.Sprintf("zero=%v/%dKiB", zero, n>>10), func(b *testing.B) {
				p := NewBudget(1<<30, 0).Pool(zero)
				p.Put(p.Get(n))
				b.SetBytes(int64(n))
				for range b.N {
					x := p.Get(n)
					x[0] = 1
					p.Put(x)
				}
			})
		}
	}
}
