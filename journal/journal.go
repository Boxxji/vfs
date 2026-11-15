// Package journal is an append-only log on local disk that makes a write
// acknowledgement and an fsync run at the speed of the local NVMe, while a
// slower publisher (the object store) catches up behind it.
//
// Every record has a sequence number and moves through three watermarks:
//
//	visible    the record was appended; later reads see it. Append returns here.
//	durable    the record is on local disk and survives a process crash and a
//	           power loss. Sync waits for it.
//	published  the record is in the object store. The publisher reports it
//	           with Publish, and the journal then reclaims the space.
//
// Publication is allowed to be ahead of local durability (the publisher reads
// the in-memory state, not the journal), so the durable watermark is the larger
// of what the disk has made durable and what the store holds.
//
// The ideas, studied and implemented independently here: a commit pipeline in
// which sequence assignment is separate from the write and from a sync queue
// that merges concurrent waiters into one fsync (CockroachDB Pebble's WAL
// writer, BSD-3-Clause); a durable-sequence watermark callers wait on, bounded
// unflushed bytes and admission control when the remote falls behind (SlateDB,
// Apache-2.0); byte-granularity records, sync absorption and systematic crash
// testing (NVLog, FAST 2025). Segment recycling follows the log-file recycling
// that RocksDB and Pebble use for the same reason: fsync into blocks that are
// already written is much cheaper than fsync into freshly allocated ones.
//
// See docs/journal.md for the contract and the integration interface.
package journal

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math/bits"
	"sync"
	"time"
)

var (
	// ErrFull is returned by Append when the journal's capacity is used up by
	// records the publisher has not yet published. Nothing was appended.
	ErrFull = errors.New("journal: full")
	// ErrClosed is returned after Close.
	ErrClosed = errors.New("journal: closed")
	// ErrTooLarge is returned by Append for a payload that cannot fit in one segment.
	ErrTooLarge = errors.New("journal: payload larger than a segment")
	// ErrAhead is returned for a sequence that has not been appended.
	ErrAhead = errors.New("journal: sequence not appended")
	// ErrCorrupt is returned by Replay when a record that should be on disk is not.
	ErrCorrupt = errors.New("journal: corrupt record")
)

// Options configure Open.
type Options struct {
	// Capacity is the most disk the journal may use, in bytes. It is required:
	// set it from the node's NVMe budget. At least two segments.
	Capacity int64
	// Segment is the size of one segment file. Default 64 MiB.
	Segment int64
	// Bound is the longest an acknowledged record should stay unpublished.
	// State reports Degraded when the oldest one is older. Default 5 s.
	Bound time.Duration
	// Published is the highest sequence the store holds (the Head's journal
	// sequence; 0 for a store with none). It is the truth about what is
	// published. Records at or below it are not replayed. New sequences continue
	// after it even if the disk lost the tail. A log that ends at or before it
	// is discarded, and so is one that begins after Published+1: it is missing
	// the records in between and what follows a gap is not a prefix.
	Published uint64
	// Disk is the file layer. Nil means the operating system's files in dir.
	Disk Disk
	// Clock supplies time. Nil means the wall clock.
	Clock Clock
}

// Marks are the three watermarks. Published <= Durable <= Visible.
type Marks struct {
	Visible   uint64
	Durable   uint64
	Published uint64
}

// State is what a caller or an agent reads to learn whether the publication
// bound can be trusted.
type State struct {
	// Degraded is true when the bound cannot be guaranteed: the oldest
	// unpublished record is older than Options.Bound, the journal is slowing
	// appends because it is nearly full, or it has failed.
	Degraded bool
	// Backlog is the bytes of acknowledged records not yet published.
	Backlog int64
	// Age is how long the oldest unpublished record has been acknowledged. It
	// is never reset by later writes. Zero when nothing is unpublished.
	Age time.Duration
	// Err is the error that stopped the journal, if any.
	Err error
}

// Record is one journal record.
type Record struct {
	Seq  uint64
	Time time.Time // when Append acknowledged it
	Data []byte    // valid only during the Replay callback
}

// Stats count what the journal did, for metrics and benchmarks.
type Stats struct {
	Records uint64 // records appended
	Bytes   uint64 // bytes appended, record headers included
	Waited  uint64 // Sync calls that had to wait
	Rounds  uint64 // local syncs (one per group)
	// Groups[i] counts rounds that released n waiters with bits.Len(n) == i,
	// the last bucket taking every larger group.
	Groups [12]uint64
}

const (
	batchCap   = 4 << 20                 // bytes one write holds before the next record starts another
	maxPending = 64 << 20                // bytes queued for the file before Append waits for the writer
	syncBytes  = 8 << 20                 // unsynced bytes that start a sync nobody asked for
	quantum    = int64(time.Millisecond) // acknowledgement times closer than this share a span
	slowFrom   = 0.75                    // fill at which Append starts to slow
	slowRate   = 64 << 20                // bytes per second Append is held to at a full journal
)

// slot is one segment file and, while it is in the log, the segment it holds.
type slot struct {
	name  string
	id    uint64
	size  int64
	ready chan struct{} // closed once file and err are set
	file  File
	err   error

	first uint64 // sequence of the first record; 0 until one is placed
	last  uint64 // sequence of the last record placed
	end   int64  // offset past the last byte placed
	life  uint64
	seed  uint32
	dirty bool // written since it was last synced
}

type batch struct {
	slot *slot
	off  int64
	data []byte
	last uint64 // sequence of the last record in data
}

type waiter struct {
	seq  uint64
	done chan struct{}
	err  error
}

type span struct {
	first uint64 // first sequence in the span
	ack   int64  // acknowledgement time of that record
	cum   uint64 // bytes appended before it
}

type due struct {
	age   time.Duration
	ch    chan struct{}
	timer Timer
	gen   int
	fired bool
}

// Journal is safe for concurrent use.
type Journal struct {
	disk    Disk
	clock   Clock
	segment int64
	limit   int // most segment files
	bound   time.Duration

	rw sync.RWMutex // Replay holds it shared, Reclaim exclusive: a segment is not recycled while it is read
	mu sync.Mutex

	next      uint64 // next sequence to assign
	written   uint64 // highest sequence written to a file
	synced    uint64 // highest sequence a completed sync covered
	published uint64
	ack       int64 // last acknowledgement time given

	queue   []*batch // batches not yet written, in order
	cur     *batch   // last batch of queue, still open to Append
	pending int64    // bytes in queue
	room    chan struct{}

	segs   []*slot // log segments, oldest first; the last is the tail
	tail   *slot
	spare  *slot // next segment, ready or being created
	free   []*slot
	nextID uint64
	nslots int
	held   int64 // bytes in segs, published or not

	dirty    []*slot
	unsynced int64
	waiters  []*waiter
	wdone    bool // the writer has exited

	spans []span
	head  int // spans[head:] are live
	total uint64
	debt  time.Duration

	dues []*due

	err    error
	closed bool
	done   chan struct{} // closed on Close or failure
	kickw  chan struct{}
	kicks  chan struct{}
	wg     sync.WaitGroup
	stats  Stats
}

// Open opens the journal in dir, recovering what is on disk: it keeps the
// longest prefix of valid records, discards anything after the first torn or
// stale byte, and makes what it kept durable before it returns. New records go
// to a fresh segment, never behind a torn tail.
func Open(dir string, opt Options) (*Journal, error) {
	if opt.Segment == 0 {
		opt.Segment = 64 << 20
	}
	if opt.Bound == 0 {
		opt.Bound = 5 * time.Second
	}
	if opt.Segment <= headerSize+recordHeader {
		return nil, fmt.Errorf("journal: segment of %d bytes is too small", opt.Segment)
	}
	limit := opt.Capacity / opt.Segment
	if limit < 2 {
		return nil, fmt.Errorf("journal: capacity %d holds fewer than two segments of %d", opt.Capacity, opt.Segment)
	}
	if opt.Disk == nil {
		d, err := Files(dir)
		if err != nil {
			return nil, err
		}
		opt.Disk = d
	}
	if opt.Clock == nil {
		opt.Clock = wallClock{}
	}
	j := &Journal{
		disk:    opt.Disk,
		clock:   opt.Clock,
		segment: opt.Segment,
		limit:   int(limit),
		bound:   opt.Bound,
		room:    make(chan struct{}),
		done:    make(chan struct{}),
		kickw:   make(chan struct{}, 1),
		kicks:   make(chan struct{}, 1),
	}
	if err := j.recover(opt.Published); err != nil {
		j.wg.Wait()
		j.release()
		return nil, err
	}
	j.wg.Add(2)
	go j.writer()
	go j.syncer()
	return j, nil
}

func (j *Journal) kick(c chan struct{}) {
	select {
	case c <- struct{}{}:
	default:
	}
}

// fault reports why the journal cannot take work. Caller holds mu.
func (j *Journal) fault() error {
	if j.err != nil {
		return j.err
	}
	if j.closed {
		return ErrClosed
	}
	return nil
}

// failLocked stops the journal on an I/O error. After a failed write or sync
// the state of the file is unknown, so nothing is retried.
func (j *Journal) failLocked(err error) {
	if j.err != nil {
		return
	}
	j.err = fmt.Errorf("journal: %w", err)
	j.finish()
	for _, w := range j.waiters {
		w.err = j.err
		close(w.done)
	}
	j.waiters = nil
	j.kick(j.kickw)
	j.kick(j.kicks)
	j.broadcast()
}

func (j *Journal) finish() {
	select {
	case <-j.done:
	default:
		close(j.done)
	}
}

// broadcast wakes everything waiting on progress of the writer. Caller holds mu.
func (j *Journal) broadcast() {
	close(j.room)
	j.room = make(chan struct{})
}

func (j *Journal) durable() uint64 { return max(j.synced, j.published) }

// Append adds payload to the journal and returns its sequence number. The
// record is visible when Append returns; it is durable only after Sync. Near
// capacity Append slows down; at capacity it returns ErrFull.
func (j *Journal) Append(ctx context.Context, payload []byte) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	n := int64(recordHeader + len(payload))
	if n > j.segment-headerSize {
		return 0, ErrTooLarge
	}
	sum := crc32.Checksum(payload, castagnoli)

	j.mu.Lock()
	if err := j.fault(); err != nil {
		j.mu.Unlock()
		return 0, err
	}
	delay := j.slow(n)
	j.mu.Unlock()
	if delay > 0 {
		if err := j.sleep(ctx, delay); err != nil {
			return 0, err
		}
	}

	for {
		j.mu.Lock()
		if err := j.fault(); err != nil {
			j.mu.Unlock()
			return 0, err
		}
		if j.pending >= maxPending {
			wait := j.room
			j.mu.Unlock()
			select {
			case <-wait:
				continue
			case <-ctx.Done():
				return 0, ctx.Err()
			case <-j.done:
				continue
			}
		}
		seq, err := j.place(payload, sum, n)
		j.mu.Unlock()
		return seq, err
	}
}

// slow returns how long Append must wait before it may place n bytes. The wait
// grows with how full the journal is, so the writers' rate falls toward
// slowRate as the journal fills and settles where it matches the drain. The
// debt carries fractions of a millisecond between calls so small records are
// held to the same rate as large ones. Caller holds mu.
func (j *Journal) slow(n int64) time.Duration {
	fill := float64(j.held) / float64(int64(j.limit)*j.segment)
	if fill <= slowFrom {
		j.debt = 0
		return 0
	}
	t := min((fill-slowFrom)/(1-slowFrom), 1)
	j.debt += time.Duration(float64(n) / slowRate * t * float64(time.Second))
	if j.debt < time.Millisecond {
		return 0
	}
	d := j.debt
	j.debt = 0
	return d
}

func (j *Journal) sleep(ctx context.Context, d time.Duration) error {
	fired := make(chan struct{})
	t := j.clock.AfterFunc(d, func() { close(fired) })
	select {
	case <-fired:
		return nil
	case <-ctx.Done():
		t.Stop()
		return ctx.Err()
	case <-j.done:
		t.Stop()
		return nil
	}
}

// place assigns the next sequence to payload and queues its record. Caller holds mu.
func (j *Journal) place(payload []byte, sum uint32, n int64) (uint64, error) {
	if j.tail == nil || j.tail.end+n > j.tail.size {
		if err := j.rotate(); err != nil {
			return 0, err
		}
	}
	t := j.tail
	seq := j.next
	ack := max(j.clock.Now().UnixNano(), j.ack)
	j.ack = ack

	header := t.first == 0
	if header {
		t.first = seq
	}
	b := j.cur
	if b == nil || b.slot != t || int64(cap(b.data)-len(b.data)) < n {
		b = j.open(t, header, n)
	}
	b.data = appendRecord(b.data, t.seed, seq, ack, payload, sum)
	b.last = seq
	t.end += n
	t.last = seq
	j.held += n
	j.pending += n
	j.next++
	j.stats.Records++
	j.stats.Bytes += uint64(n)

	fresh := j.head == len(j.spans) // nothing was unpublished
	if fresh || ack-j.spans[len(j.spans)-1].ack >= quantum {
		j.spans = append(j.spans, span{seq, ack, j.total})
	}
	j.total += uint64(n)
	if fresh {
		j.rearm()
	}
	j.kick(j.kickw)
	return seq, nil
}

// open starts a batch for slot t. The first batch of a segment starts at its
// header. Caller holds mu.
func (j *Journal) open(t *slot, header bool, n int64) *batch {
	b := &batch{slot: t, off: t.end}
	size := int64(batchCap)
	if header {
		b.off = 0
		size += headerSize
	}
	if n > size {
		size = n + headerSize
	}
	b.data = getBuffer(int(size))
	if header {
		b.data = appendHeader(b.data, t.first, t.life)
		j.pending += headerSize
	}
	j.queue = append(j.queue, b)
	j.cur = b
	return b
}

var buffers = sync.Pool{New: func() any { return make([]byte, 0, batchCap+headerSize) }}

func getBuffer(size int) []byte {
	if size <= batchCap+headerSize {
		return buffers.Get().([]byte)[:0]
	}
	return make([]byte, 0, size)
}

func putBuffer(b []byte) {
	if cap(b) == batchCap+headerSize {
		buffers.Put(b[:0]) //nolint:staticcheck // a slice header per pooled buffer is cheaper than a boxed pointer here
	}
}

// rotate makes the spare segment the tail. Caller holds mu.
func (j *Journal) rotate() error {
	if j.spare == nil {
		j.refill()
	}
	s := j.spare
	if s == nil {
		return ErrFull
	}
	j.spare = nil
	var life [8]byte
	if _, err := rand.Read(life[:]); err != nil {
		return err
	}
	s.life = binary.LittleEndian.Uint64(life[:])
	s.seed = seedOf(s.life)
	s.first, s.last, s.end = 0, 0, headerSize
	j.tail = s
	j.segs = append(j.segs, s)
	j.held += headerSize
	j.refill()
	return nil
}

// refill makes sure a spare segment exists or is being created, if the
// capacity allows another file. Caller holds mu.
func (j *Journal) refill() {
	if j.spare != nil || j.err != nil || j.closed {
		return
	}
	if n := len(j.free); n > 0 {
		j.spare = j.free[n-1]
		j.free = j.free[:n-1]
		return
	}
	if j.nslots < j.limit {
		j.spare = j.create()
	}
}

// create starts a new segment file in the background. The writer waits for
// ready before it writes there, so a segment being created never delays Append.
func (j *Journal) create() *slot {
	s := &slot{id: j.nextID, size: j.segment, ready: make(chan struct{})}
	s.name = slotName(s.id)
	j.nextID++
	j.nslots++
	j.wg.Add(1)
	go func() {
		defer j.wg.Done()
		s.file, s.err = j.disk.Create(s.name, s.size)
		close(s.ready)
	}()
	return s
}

// Sync returns once the record with sequence seq, and every record before it,
// is durable. Concurrent calls share one fsync. A call is released only by an
// fsync that began after the record was written, never by an earlier one.
func (j *Journal) Sync(ctx context.Context, seq uint64) error {
	j.mu.Lock()
	if seq <= j.durable() {
		j.mu.Unlock()
		return nil
	}
	if err := j.fault(); err != nil {
		j.mu.Unlock()
		return err
	}
	if seq >= j.next {
		j.mu.Unlock()
		return ErrAhead
	}
	w := &waiter{seq: seq, done: make(chan struct{})}
	j.waiters = append(j.waiters, w)
	j.stats.Waited++
	j.mu.Unlock()
	j.kick(j.kickw)
	j.kick(j.kicks)

	select {
	case <-w.done:
		return w.err
	case <-ctx.Done():
		j.mu.Lock()
		for i, x := range j.waiters {
			if x == w {
				j.waiters = append(j.waiters[:i], j.waiters[i+1:]...)
				break
			}
		}
		j.mu.Unlock()
		select {
		case <-w.done:
			return w.err
		default:
			return ctx.Err()
		}
	}
}

// Marks returns the watermarks.
func (j *Journal) Marks() Marks {
	j.mu.Lock()
	defer j.mu.Unlock()
	return Marks{Visible: j.next - 1, Durable: j.durable(), Published: j.published}
}

// Publish records that the store holds every record up to seq. The call is
// monotonic: a lower seq than before is ignored. It releases Sync calls the
// store now satisfies and moves the Due deadline to the next record. The
// space is recycled by Reclaim.
func (j *Journal) Publish(seq uint64) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if seq >= j.next {
		return ErrAhead
	}
	if seq <= j.published {
		return nil
	}
	j.published = seq
	j.trim()
	j.rearm()
	j.releaseLocked()
	return nil
}

// trim drops spans of published records. Caller holds mu.
func (j *Journal) trim() {
	if j.published+1 >= j.next {
		j.head = len(j.spans)
	} else {
		for j.head+1 < len(j.spans) && j.spans[j.head+1].first <= j.published+1 {
			j.head++
		}
	}
	if j.head > 1024 && j.head > len(j.spans)/2 {
		j.spans = append(j.spans[:0], j.spans[j.head:]...)
		j.head = 0
	}
}

// oldest is the acknowledgement time of the oldest unpublished record, rounded
// down to the span it started. Caller holds mu.
func (j *Journal) oldest() (int64, bool) {
	if j.head == len(j.spans) {
		return 0, false
	}
	return j.spans[j.head].ack, true
}

// releaseLocked frees every waiter the durable watermark now covers and
// returns how many it freed. Caller holds mu.
func (j *Journal) releaseLocked() int {
	d := j.durable()
	kept := j.waiters[:0]
	freed := 0
	for _, w := range j.waiters {
		if w.seq <= d {
			close(w.done)
			freed++
		} else {
			kept = append(kept, w)
		}
	}
	clear(j.waiters[len(kept):])
	j.waiters = kept
	return freed
}

// State reports the backlog and whether the publication bound holds.
func (j *Journal) State() State {
	j.mu.Lock()
	defer j.mu.Unlock()
	s := State{Err: j.err}
	if ack, ok := j.oldest(); ok {
		s.Age = max(0, time.Duration(j.clock.Now().UnixNano()-ack))
		s.Backlog = int64(j.total - j.spans[j.head].cum)
	}
	fill := float64(j.held) / float64(int64(j.limit)*j.segment)
	s.Degraded = j.err != nil || s.Age > j.bound || fill > slowFrom
	return s
}

// Stats returns the counters.
func (j *Journal) Stats() Stats {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.stats
}

// Due returns a channel that is closed when the oldest unpublished record has
// been acknowledged for at least age. The age is the record's own: later writes
// never reset it, and when Publish retires the oldest record the deadline moves
// to the next one. If nothing is unpublished the deadline starts with the next
// Append. The channel fires once; call Due again after publishing. It is also
// closed when the journal closes or fails. It is driven by a timer set for the
// exact deadline, not by polling.
func (j *Journal) Due(age time.Duration) <-chan struct{} {
	d := &due{age: age, ch: make(chan struct{})}
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.err != nil || j.closed {
		close(d.ch)
		return d.ch
	}
	j.dues = append(j.dues, d)
	j.arm(d)
	return d.ch
}

// rearm moves every pending deadline to the oldest unpublished record. Caller holds mu.
func (j *Journal) rearm() {
	if len(j.dues) == 0 {
		return
	}
	for _, d := range append([]*due(nil), j.dues...) {
		j.arm(d)
	}
}

// arm sets d's timer for the oldest unpublished record's deadline, or fires d
// if it has passed. Caller holds mu.
func (j *Journal) arm(d *due) {
	if d.fired {
		return
	}
	d.gen++
	if d.timer != nil {
		d.timer.Stop()
		d.timer = nil
	}
	ack, ok := j.oldest()
	if !ok {
		return
	}
	wait := time.Duration(ack + int64(d.age) - j.clock.Now().UnixNano())
	if wait <= 0 {
		j.fire(d)
		return
	}
	gen := d.gen
	d.timer = j.clock.AfterFunc(wait, func() {
		j.mu.Lock()
		defer j.mu.Unlock()
		if d.gen == gen {
			d.timer = nil
			j.arm(d)
		}
	})
}

func (j *Journal) fire(d *due) {
	d.fired = true
	for i, x := range j.dues {
		if x == d {
			j.dues = append(j.dues[:i], j.dues[i+1:]...)
			break
		}
	}
	close(d.ch)
}

// Close makes everything appended durable, stops the journal and closes its
// files. Records stay on disk for the next Open.
func (j *Journal) Close() error {
	j.mu.Lock()
	if j.closed {
		j.mu.Unlock()
		return nil
	}
	j.closed = true
	j.kick(j.kickw)
	j.kick(j.kicks)
	j.mu.Unlock()
	j.wg.Wait()

	j.mu.Lock()
	err := j.err
	j.finish()
	for _, d := range j.dues {
		close(d.ch)
		if d.timer != nil {
			d.timer.Stop()
		}
	}
	j.dues = nil
	for _, w := range j.waiters {
		w.err = ErrClosed
		close(w.done)
	}
	j.waiters = nil
	j.broadcast()
	j.mu.Unlock()
	j.release()
	return err
}

// release closes every file. Called once the goroutines have stopped.
func (j *Journal) release() {
	all := append(append([]*slot(nil), j.segs...), j.free...)
	if j.spare != nil {
		all = append(all, j.spare)
	}
	for _, s := range all {
		if s.file != nil {
			s.file.Close()
		}
	}
}

// bucket is the histogram index for a group of n waiters.
func bucket(n int) int { return min(bits.Len(uint(n)), 11) }
