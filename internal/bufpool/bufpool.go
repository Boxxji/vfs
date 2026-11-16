// Package bufpool recycles the large byte slices of a write pipeline: the
// plaintext of a chunk, the sealed frame, the pack that is uploaded.
//
// A Budget caps the idle bytes of every Pool drawn from it, however many there
// are, and forgets a slice that has sat idle for its decay time. A Pool keeps
// idle slices in size classes a quarter of a power of two apart, so a slice is at
// most 25 percent larger than asked for. When the Budget is full the slice that
// has been idle longest goes, whatever its pool or class, so a workload that
// changes size is never locked out by the sizes it used before.
//
// A Pool takes back only what it gave out. Every slice from Get is recorded, and
// Put refuses a slice that is not exactly one of them, that is a view of one, or
// that is already back: it never clears or keeps memory it does not own. A Pool
// can clear every slice it takes back, which is what plaintext wants: the bytes
// of a file do not wait in a free list for the next caller.
//
// The pool cannot stop an owner from using a slice after it has put it back.
// Where a slice has several holders, a cache entry and the readers copying from
// it, hold it in a Shared, which returns it only when the last holder is done.
// The race detector does not see the reads and writes of the assembly hash
// kernels (see internal/blake3), so a use after Put can pass it unnoticed; the
// tests of this package check the sharing rules deterministically instead.
//
// A Pool does not count the slices that are out. Bounding the memory in flight is
// the job of the pipeline that owns the slices, by the number of workers and the
// size of what each holds.
package bufpool

import (
	"container/list"
	"math/bits"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
	"weak"
)

const (
	// MinSize is the smallest class; a smaller request is served from it.
	MinSize = 4 << 10
	// MaxSize is the largest class; a larger request is allocated, not pooled.
	MaxSize = 64 << 20

	// The classes of a doubling between 2^e and 2^(e+1) are 2^e plus one to
	// four quarters of 2^e; the first class is MinSize on its own.
	minShift = 12
	classes  = 1 + (26-minShift)*4
)

// classOf returns the index and size of the smallest class that holds n bytes,
// for 0 < n <= MaxSize.
func classOf(n int) (idx, size int) {
	if n <= MinSize {
		return 0, MinSize
	}
	e := bits.Len(uint(n-1)) - 1 // 2^e < n <= 2^(e+1)
	step := 1 << (e - 2)
	j := (n-1-1<<e)/step + 1 // 1 to 4
	return 1 + (e-minShift)*4 + j - 1, 1<<e + j*step
}

// A timer is what a Budget needs of a deadline timer.
type timer interface{ Stop() bool }

// Budget is the idle memory shared by the Pools made from it. It is safe for
// concurrent use.
type Budget struct {
	limit int64
	decay time.Duration

	// The clock and the deadline timer, which a test replaces.
	now   func() time.Time
	after func(d time.Duration, f func()) timer

	mu    sync.Mutex
	idle  int64
	lru   list.List // of *entry, the idle ones, front the most recently returned
	reg   map[uintptr]*entry
	swept int // size of reg after the last sweep
	tm    timer
	gen   uint64 // identifies the armed timer, so that a stopped one that fires anyway is ignored
	stats Totals
}

// Totals counts what a Budget holds and what it has let go.
type Totals struct {
	Idle    int64  // bytes held idle now, in all its pools
	Evicted uint64 // slices dropped because the limit was reached
	Decayed uint64 // slices dropped because they sat idle for the decay time
}

// NewBudget returns a Budget that holds at most limit idle bytes (a negative limit
// is 0: it keeps nothing) and drops a slice after decay of idleness; a decay of 0
// never drops. The decay is driven by
// one deadline timer that is armed only while there are idle bytes.
func NewBudget(limit int64, decay time.Duration) *Budget {
	return &Budget{
		limit: max(limit, 0),
		decay: decay,
		now:   time.Now,
		after: func(d time.Duration, f func()) timer { return time.AfterFunc(d, f) },
		reg:   map[uintptr]*entry{},
	}
}

// Stats returns the counters.
func (b *Budget) Stats() Totals {
	b.mu.Lock()
	defer b.mu.Unlock()
	s := b.stats
	s.Idle = b.idle
	return s
}

// entry records a slice the pool gave out: where it starts, how big its class is,
// whose it is, and, while it is idle, where it waits.
type entry struct {
	key  uintptr
	size int
	wp   weak.Pointer[byte]
	pool *Pool

	held bool   // back in the pool, or on its way: Put refuses it
	buf  []byte // the whole array, while idle
	at   time.Time
	lel  *list.Element // in the Budget's lru
	cel  *list.Element // in the pool's list of its class
}

func start(b []byte) uintptr { return uintptr(unsafe.Pointer(unsafe.SliceData(b))) }

// forget removes an idle entry from the lists and the registry. b.mu is held.
func (b *Budget) forget(e *entry) {
	b.lru.Remove(e.lel)
	e.pool.free[rank(e.size)].Remove(e.cel)
	e.pool.idle -= int64(e.size)
	b.idle -= int64(e.size)
	e.buf = nil
	delete(b.reg, e.key)
}

func rank(size int) int {
	idx, _ := classOf(size)
	return idx
}

// take removes an idle entry for reuse. b.mu is held.
func (b *Budget) take(e *entry) []byte {
	buf := e.buf
	b.lru.Remove(e.lel)
	e.pool.free[rank(e.size)].Remove(e.cel)
	e.pool.idle -= int64(e.size)
	b.idle -= int64(e.size)
	e.buf, e.held = nil, false
	if b.lru.Len() == 0 {
		b.disarm()
	}
	return buf
}

// evict drops idle slices, oldest first, until the limit holds. b.mu is held.
func (b *Budget) evict() {
	for b.idle > b.limit {
		e := b.lru.Back().Value.(*entry)
		e.pool.stats.Drops++
		b.stats.Evicted++
		b.forget(e)
	}
	if b.lru.Len() == 0 {
		b.disarm()
	}
}

// arm starts the timer for the oldest idle slice if none runs. b.mu is held.
func (b *Budget) arm() {
	if b.decay <= 0 || b.tm != nil || b.lru.Len() == 0 {
		return
	}
	e := b.lru.Back().Value.(*entry)
	d := max(0, b.decay-b.now().Sub(e.at))
	b.gen++
	gen := b.gen
	b.tm = b.after(d, func() { b.expire(gen) })
}

func (b *Budget) disarm() {
	if b.tm != nil {
		b.tm.Stop()
		b.tm = nil
		b.gen++
	}
}

// expire drops the slices idle for the decay time and arms the timer for the next.
func (b *Budget) expire(gen uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if gen != b.gen {
		return
	}
	b.tm = nil
	now := b.now()
	for b.lru.Len() > 0 {
		e := b.lru.Back().Value.(*entry)
		if now.Sub(e.at) < b.decay {
			break
		}
		e.pool.stats.Drops++
		b.stats.Decayed++
		b.forget(e)
	}
	b.arm()
}

// sweep forgets the records of slices the garbage collector has taken. b.mu is held.
func (b *Budget) sweep() {
	if len(b.reg) < 2*b.swept+64 {
		return
	}
	for k, e := range b.reg {
		if !e.held && e.wp.Value() == nil {
			delete(b.reg, k)
		}
	}
	b.swept = len(b.reg)
}

// Pool is a set of free lists drawn on a Budget. It is safe for concurrent use.
type Pool struct {
	b    *Budget
	zero bool

	// Guarded by b.mu.
	free   [classes]list.List // of *entry
	idle   int64
	stats  Stats
	closed bool
}

// Stats counts what a Pool has done and what it holds.
type Stats struct {
	Gets    uint64 // slices asked for
	Hits    uint64 // served from a free list
	Drops   uint64 // returned, then let go by the Budget
	Refused uint64 // offered to Put and not accepted: not one of ours, a view, or already back
	Idle    int64  // bytes held in free lists now
}

// Pool returns a Pool that draws on b. If zero is set, every slice is cleared
// when it is returned, so that a Get always sees zeros and no contents outlive
// their owner; leave it off for ciphertext.
func (b *Budget) Pool(zero bool) *Pool {
	p := &Pool{b: b, zero: zero}
	for i := range p.free {
		p.free[i].Init()
	}
	return p
}

// Get returns a slice of length n whose capacity is its class size, and which
// the caller owns until it puts it back. On a zeroing Pool its bytes are zero;
// otherwise they are whatever the last owner left. Get(0) returns nil, and a
// request above MaxSize is allocated and never pooled.
func (p *Pool) Get(n int) []byte {
	b := p.b
	if n <= 0 {
		return nil
	}
	if n > MaxSize {
		b.mu.Lock()
		p.stats.Gets++
		b.mu.Unlock()
		return make([]byte, n)
	}
	idx, size := classOf(n)
	b.mu.Lock()
	p.stats.Gets++
	if l := &p.free[idx]; l.Len() > 0 && !p.closed {
		buf := b.take(l.Front().Value.(*entry))
		p.stats.Hits++
		b.mu.Unlock()
		return buf[:n]
	}
	closed := p.closed
	b.mu.Unlock()
	buf := make([]byte, n, size)
	if closed {
		return buf
	}
	e := &entry{key: start(buf), size: size, wp: weak.Make(unsafe.SliceData(buf)), pool: p}
	b.mu.Lock()
	b.sweep()
	b.reg[e.key] = e
	b.mu.Unlock()
	return buf
}

// Put returns b, which must be a slice that Get gave out, whole: the same start
// and capacity, any length. The caller must not use it again. A slice that is not
// one of this Pool's, that is a view of one, or that has been put back already is
// refused and left alone, and counted.
func (p *Pool) Put(s []byte) {
	if cap(s) == 0 {
		return
	}
	b := p.b
	b.mu.Lock()
	e := b.reg[start(s)]
	if e == nil || e.pool != p || e.held || e.size != cap(s) || e.wp.Value() != unsafe.SliceData(s) || p.closed {
		p.stats.Refused++
		b.mu.Unlock()
		return
	}
	e.held = true
	b.mu.Unlock()

	buf := unsafe.Slice(unsafe.SliceData(s), e.size)
	if p.zero {
		clear(buf)
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if p.closed {
		delete(b.reg, e.key)
		return
	}
	e.buf, e.at = buf, b.now()
	e.lel = b.lru.PushFront(e)
	e.cel = p.free[rank(e.size)].PushFront(e)
	p.idle += int64(e.size)
	b.idle += int64(e.size)
	b.evict()
	b.arm()
}

// Close lets go of every idle slice of the Pool and stops it keeping any more.
// Slices still out are not tracked afterwards: putting them back is refused and
// the garbage collector has them.
func (p *Pool) Close() {
	b := p.b
	b.mu.Lock()
	defer b.mu.Unlock()
	p.closed = true
	for i := range p.free {
		for l := &p.free[i]; l.Len() > 0; {
			b.forget(l.Front().Value.(*entry))
		}
	}
	if b.lru.Len() == 0 {
		b.disarm()
	}
}

// Stats returns the counters.
func (p *Pool) Stats() Stats {
	p.b.mu.Lock()
	defer p.b.mu.Unlock()
	s := p.stats
	s.Idle = p.idle
	return s
}

// Buffer is a byte buffer that grows through a Pool: a growth step takes a larger
// slice, copies, and gives the old one back. It is an io.Writer. A Buffer is not
// safe for concurrent use.
type Buffer struct {
	pool *Pool
	b    []byte
}

// Buffer returns an empty Buffer with room for hint bytes.
func (p *Pool) Buffer(hint int) *Buffer {
	return &Buffer{pool: p, b: p.Get(hint)[:0]}
}

// Len is the number of bytes written.
func (b *Buffer) Len() int { return len(b.b) }

// Bytes returns the bytes written. They are a view of the Buffer's storage and
// die with it: at the next Write that grows the Buffer, at Reset and at Release,
// the storage is cleared or handed to another owner. A holder that needs the
// bytes longer takes them with Detach.
func (b *Buffer) Bytes() []byte { return b.b }

// Grow makes room for n more bytes.
func (b *Buffer) Grow(n int) {
	b.pool.Put(b.grow(n))
}

// grow makes room for n more bytes and returns the storage it left, which the
// caller gives back to the Pool when it no longer needs to read from it, or nil if
// the Buffer did not move.
func (b *Buffer) grow(n int) (old []byte) {
	if len(b.b)+n <= cap(b.b) {
		return nil
	}
	next := b.pool.Get(max(len(b.b)+n, cap(b.b)+cap(b.b)/2))[:len(b.b)]
	copy(next, b.b)
	old, b.b = b.b, next
	return old
}

// Write appends p. It never fails. p may be a view of the Buffer's own bytes: the
// old storage goes back to the Pool only after p has been copied.
func (b *Buffer) Write(p []byte) (int, error) {
	old := b.grow(len(p))
	b.b = append(b.b, p...)
	b.pool.Put(old)
	return len(p), nil
}

// Reset empties the Buffer and keeps its storage. On a zeroing Pool the bytes
// that were written are cleared.
func (b *Buffer) Reset() {
	if b.pool.zero {
		clear(b.b)
	}
	b.b = b.b[:0]
}

// Detach gives the written bytes and their storage to the caller, who owns the
// slice and puts it back to the Pool when done, or hands it to Share. The Buffer
// is empty afterwards and takes new storage if it is written to again, so the
// slice is not disturbed by later writes.
func (b *Buffer) Detach() []byte {
	s := b.b
	b.b = nil
	return s
}

// Release gives the storage back to the Pool. The Buffer is empty afterwards and
// takes new storage if it is written to again. Releasing twice is harmless.
func (b *Buffer) Release() {
	b.pool.Put(b.b)
	b.b = nil
}

// Shared is a slice of a Pool held by several parties at once: an owner, such as
// a cache entry, and any number of readers. It goes back to the Pool when the
// owner has released it and the last reader has unpinned it.
type Shared struct {
	pool  *Pool
	b     []byte
	refs  atomic.Int64 // the owner's reference and the pins
	owned atomic.Bool  // the owner has not released yet
}

// Share takes the slice s, which Get gave out, into a Shared held by its owner.
func (p *Pool) Share(s []byte) *Shared {
	sh := &Shared{pool: p, b: s}
	sh.refs.Store(1)
	sh.owned.Store(true)
	return sh
}

// Pin returns the bytes and holds them until Unpin. It fails if the slice has
// already gone back to the Pool.
func (s *Shared) Pin() ([]byte, bool) {
	for {
		r := s.refs.Load()
		if r <= 0 {
			return nil, false
		}
		if s.refs.CompareAndSwap(r, r+1) {
			return s.b, true
		}
	}
}

// Unpin ends a Pin.
func (s *Shared) Unpin() { s.drop() }

// Release ends the owner's hold, once; further calls do nothing.
func (s *Shared) Release() {
	if s.owned.CompareAndSwap(true, false) {
		s.drop()
	}
}

func (s *Shared) drop() {
	if s.refs.Add(-1) == 0 {
		s.pool.Put(s.b)
	}
}
