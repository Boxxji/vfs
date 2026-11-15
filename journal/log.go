package journal

import (
	"errors"
	"fmt"
	"sort"
	"time"
)

var ready = func() chan struct{} {
	c := make(chan struct{})
	close(c)
	return c
}()

var errStop = errors.New("stop")

// inventory opens every segment file of disk and reads what each says about
// itself. A file is live when its header parses; a live file of another lineage
// than want is an error (want nil accepts any: Discard reads whatever is there).
// Live files come back ordered by the sequence of their first record, then by
// name, so two files that claim the same first sequence are ordered the same way
// every time. The marks a live file carries are in through and floor.
func inventory(disk Disk, want *lineage) (all, live []*slot, err error) {
	names, err := disk.List()
	if err != nil {
		return nil, nil, err
	}
	for _, name := range names {
		id, ok := parseSlot(name)
		if !ok {
			continue
		}
		f, err := disk.Open(name)
		if err != nil {
			return all, nil, err
		}
		s := &slot{name: name, id: id, size: f.Size(), ready: ready, file: f}
		all = append(all, s)
		var pre [dataStart]byte
		if s.size < dataStart {
			continue
		}
		if _, err := f.ReadAt(pre[:], 0); err != nil {
			return all, nil, err
		}
		first, life, l, ok := parseHeader(pre[:])
		if !ok {
			continue
		}
		if want != nil && l != *want {
			return all, nil, fmt.Errorf("%w: %s", ErrLineage, name)
		}
		s.first, s.life, s.seed = first, life, seedOf(life, l)
		s.through, s.floor = parseMarks(pre[:], s.seed)
		live = append(live, s)
	}
	sort.Slice(live, func(a, b int) bool {
		if live[a].first != live[b].first {
			return live[a].first < live[b].first
		}
		return live[a].id < live[b].id
	})
	return all, live, nil
}

// chainOf keeps, of the live segments, the run in which each starts where the
// one before ended and holds at least one valid record. That run is the longest
// valid prefix of the log: after a torn write or a lost write there is a hole,
// and what follows it is not a prefix. fn sees every record kept. The rest are
// the orphans. last is the sequence of the last record kept, 0 for none.
func chainOf(live []*slot, fn func(record, int64) error) (chain, orphans []*slot, last uint64, err error) {
	for _, s := range live {
		if len(chain) > 0 && s.first != last+1 {
			orphans = append(orphans, s)
			continue
		}
		next, end, err := scan(s.file, s.seed, s.first, func(r record, off int64) error {
			if fn != nil {
				return fn(r, off)
			}
			return nil
		})
		if err != nil {
			return nil, nil, 0, err
		}
		if next == s.first {
			orphans = append(orphans, s)
			continue
		}
		s.last, s.end = next-1, end
		chain = append(chain, s)
		last = next - 1
	}
	return chain, orphans, last, nil
}

// rangeOf is the run of valid records a segment holds, read without changing anything.
func rangeOf(s *slot) (Range, error) {
	next, _, err := scan(s.file, s.seed, s.first, func(record, int64) error { return nil })
	return Range{s.first, next - 1}, err
}

// wipe makes a segment free: its header is zeroed and the zeroing made durable.
func wipe(s *slot) error {
	if _, err := s.file.WriteAt(make([]byte, headerSize), 0); err != nil {
		return err
	}
	return s.file.Sync()
}

// recover rebuilds the log from the segment files, checks it against what the
// caller says the store holds, and only then writes anything. It refuses, and
// writes nothing, when the directory belongs to another lineage, when the store
// is behind what the journal has already reclaimed or ahead of what it holds, or
// when records that a mark proves durable are missing. What it does write is
// the removal of segments that follow a gap with no proof of a finished sync
// past it: the torn tail of a power loss. Recovery reports them.
func (j *Journal) recover(published uint64) error {
	now := j.clock.Now()
	all, live, err := inventory(j.disk, &j.lineage)
	j.all = all
	if err != nil {
		return err
	}
	for _, s := range all {
		j.nextID = max(j.nextID, s.id+1)
	}
	j.nslots = len(all)
	var through, floor uint64
	for _, s := range live {
		through, floor = max(through, s.through), max(floor, s.floor)
	}
	chain, orphans, last, err := chainOf(live, func(r record, _ int64) error {
		if r.seq > published {
			j.account(r.seq, r.ack, int64(recordHeader+len(r.payload)), now)
		}
		return nil
	})
	if err != nil {
		return err
	}

	// The store is the truth about what is published.
	if published < floor {
		return fmt.Errorf("%w: the store holds %d, the journal reclaimed through %d", ErrMismatch, published, floor)
	}
	if len(chain) == 0 {
		if through > published {
			return fmt.Errorf("%w: records through %d were made durable and none is left", ErrCorrupt, through)
		}
	} else if first := chain[0].first; first-1 > published {
		// The log begins after the store. If a finished sync is proven past the
		// store the records in between are lost; if not, the start of the log was
		// lost with the tail in a power cut and nothing in it was acknowledged.
		if through > published {
			return fmt.Errorf("%w: the store holds %d, the log begins at %d, and records through %d were made durable", ErrCorrupt, published, first, through)
		}
		orphans = append(append([]*slot(nil), chain...), orphans...)
		chain, last = nil, 0
		j.spans, j.head, j.total = nil, 0, 0
	} else {
		if published > last {
			return fmt.Errorf("%w: the store holds %d, the log ends at %d", ErrMismatch, published, last)
		}
		if through > last && through > published {
			return fmt.Errorf("%w: records through %d were made durable, the log ends at %d", ErrCorrupt, through, last)
		}
	}

	j.report = Report{First: published + 1, Last: published, Proven: through}
	if disk, ok := j.disk.(interface{ Warnings() []string }); ok {
		j.report.Warnings = disk.Warnings()
	}
	if len(chain) > 0 {
		j.report.First, j.report.Last = max(chain[0].first, published+1), last
	}
	for _, s := range orphans {
		r, err := rangeOf(s)
		if err != nil {
			return err
		}
		if r.Last >= r.First {
			j.report.Dropped = append(j.report.Dropped, r)
		}
	}

	// From here on the journal writes. Segments that are not in the log lose
	// their header, durably, before any new record is written: otherwise a
	// stale segment could be mistaken for a continuation of the new records,
	// whose sequence numbers it may repeat.
	for _, s := range chain {
		if err := s.file.Sync(); err != nil {
			return err
		}
	}
	for _, s := range orphans {
		if err := wipe(s); err != nil {
			return err
		}
	}
	inChain := map[*slot]bool{}
	for _, s := range chain {
		inChain[s] = true
	}
	for _, s := range all {
		if inChain[s] {
			continue
		}
		s.first, s.last, s.end = 0, 0, 0
		if s.size != j.segment {
			j.nslots--
			if err := j.drop(s); err != nil {
				return err
			}
			continue
		}
		j.free = append(j.free, s)
	}
	for j.nslots > j.limit && len(j.free) > 0 {
		s := j.free[len(j.free)-1]
		j.free = j.free[:len(j.free)-1]
		j.nslots--
		if err := j.drop(s); err != nil {
			return err
		}
	}

	j.published = published
	j.next = max(last, published) + 1
	j.segs = chain
	for _, s := range chain {
		j.held += s.end
	}
	j.written, j.synced = j.next-1, j.next-1

	for {
		wait, err := j.rotate()
		if err != nil && !errors.Is(err, ErrFull) {
			return err
		}
		if wait != nil {
			<-wait
			continue
		}
		if err != nil && len(chain) == 0 {
			return err // not even one segment can be made
		}
		break
	}
	return nil
}

// drop closes and deletes a segment file that cannot be reused. It does I/O, so
// the caller must not hold mu.
func (j *Journal) drop(s *slot) error {
	err := s.file.Close()
	s.file = nil
	if rerr := j.disk.Remove(s.name); err == nil {
		err = rerr
	}
	return err
}

// account adds one recovered record to the spans. Spans cover the unpublished
// records; one span stands for every record acknowledged within a quantum of its
// first. A record whose time is ahead of now, because the clock is behind what
// it was when the record was written, counts as acknowledged now: a restart can
// never lengthen the time a record has left, and may shorten it.
func (j *Journal) account(seq uint64, ack int64, n int64, now time.Time) {
	t := time.Unix(0, ack)
	if t.After(now) {
		t = now
	}
	if j.head == len(j.spans) || t.Sub(j.spans[len(j.spans)-1].ack) >= quantum {
		j.spans = append(j.spans, span{seq, t, j.total})
	}
	j.total += uint64(n)
}

// Discard gives up the records of the journal in dir, which must be exactly
// first through last: it removes them and nothing else. It is the explicit
// answer to the cases in which Open refuses (another lineage, a store that does
// not fit the log), when the operator has decided the records are not wanted;
// Open never does it. It returns ErrRange, and changes nothing, if the journal
// holds anything but the range named. Lineage is not checked: the point is to
// remove the records of a lineage that Open refused.
func Discard(dir string, opt Options, first, last uint64) (err error) {
	disk := opt.Disk
	if disk == nil {
		if disk, err = Files(dir); err != nil {
			return err
		}
	}
	all, live, err := inventory(disk, nil)
	defer func() {
		for _, s := range all {
			if s.file != nil {
				s.file.Close()
			}
		}
		if cerr := disk.Close(); err == nil {
			err = cerr
		}
	}()
	if err != nil {
		return err
	}
	chain, _, end, err := chainOf(live, nil)
	if err != nil {
		return err
	}
	var held Range
	if len(chain) > 0 {
		held = Range{chain[0].first, end}
	}
	if held != (Range{first, last}) {
		return fmt.Errorf("%w: the journal holds %d through %d, not %d through %d", ErrRange, held.First, held.Last, first, last)
	}
	for _, s := range live {
		if err := wipe(s); err != nil {
			return err
		}
	}
	return nil
}

// Replay calls fn for every record after the published watermark, in order,
// then returns. It reads the journal and changes nothing, so calling it twice
// yields the same records; fn must tolerate seeing a record again after a crash
// if it applies effects. The Data of a record is valid only during fn. Replay
// first waits for appended records to reach the files, so it also sees records
// of this run. It blocks Reclaim while it runs.
func (j *Journal) Replay(fn func(Record) error) error {
	j.rw.RLock()
	defer j.rw.RUnlock()

	j.mu.Lock()
	for {
		if err := j.fault(); err != nil {
			j.mu.Unlock()
			return err
		}
		if j.written >= j.next-1 {
			break
		}
		j.kick(j.kickw)
		wait := j.room
		j.mu.Unlock()
		<-wait
		j.mu.Lock()
	}
	from, target := j.published, j.next-1
	type view struct {
		s           *slot
		first, last uint64
		seed        uint32
	}
	var views []view
	for _, s := range j.segs {
		if s.first != 0 && s.last > from {
			views = append(views, view{s, s.first, min(s.last, target), s.seed})
		}
	}
	j.mu.Unlock()

	for _, v := range views {
		next, _, err := scan(v.s.file, v.seed, v.first, func(r record, _ int64) error {
			if r.seq > v.last {
				return errStop
			}
			if r.seq <= from {
				return nil
			}
			return fn(Record{Seq: r.seq, Time: time.Unix(0, r.ack), Data: r.payload})
		})
		if err != nil && err != errStop {
			return err
		}
		if err == nil && next <= v.last {
			return ErrCorrupt
		}
	}
	return nil
}

// Reclaim recycles every segment whose records are all published, so the space
// can take new records. It does file I/O, so the publisher calls it after
// Publish, off the path of Append and Sync. The tail is never recycled.
//
// Before a header is wiped, the sequence the store holds is made durable in the
// oldest segment that stays. Without it a crash could leave a log that begins
// after the store with nothing to say why, and Open could not tell a store that
// is behind (a stale Head, to be refused) from a start of the log lost with the
// tail (to be dropped).
func (j *Journal) Reclaim() error {
	j.rw.Lock()
	defer j.rw.Unlock()

	j.mu.Lock()
	if err := j.fault(); err != nil {
		j.mu.Unlock()
		return err
	}
	n := 0
	for n+1 < len(j.segs) && j.segs[n].last <= min(j.published, j.written) {
		n++
	}
	for n > 0 && !(j.segs[n].first != 0 && j.segs[n].first <= j.written) {
		n-- // the segment that keeps the floor must have its header on the disk
	}
	if n == 0 {
		j.mu.Unlock()
		return nil
	}
	old := append([]*slot(nil), j.segs[:n]...)
	anchor := j.segs[n]
	j.mu.Unlock()

	if _, err := j.writeMark(anchor, 0, old[n-1].last); err != nil {
		return err
	}
	if err := anchor.file.Sync(); err != nil {
		return err
	}
	// Oldest first, each made durable before the next is touched: a crash may
	// then lose the newest wipes, never an older one while keeping a newer,
	// which would leave a gap in the log and discard everything after it.
	for _, s := range old {
		if err := wipe(s); err != nil {
			return err
		}
	}

	var drops []*slot
	j.mu.Lock()
	j.segs = append(j.segs[:0], j.segs[n:]...)
	for _, s := range old {
		j.held -= s.end
		s.first, s.last, s.end = 0, 0, 0
		if s.size != j.segment {
			j.nslots--
			drops = append(drops, s)
			continue
		}
		j.free = append(j.free, s)
	}
	j.refill()
	j.mu.Unlock()
	for _, s := range drops {
		if err := j.drop(s); err != nil {
			return err
		}
	}
	return nil
}
