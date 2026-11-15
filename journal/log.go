package journal

import (
	"errors"
	"sort"
	"time"
)

var ready = func() chan struct{} {
	c := make(chan struct{})
	close(c)
	return c
}()

var errStop = errors.New("stop")

// recover rebuilds the log from the segment files. A segment is live when its
// header parses. Live segments are ordered by their first sequence and kept
// while each starts where the one before ended and holds at least one valid
// record; everything else is discarded. The kept records are the longest valid
// prefix: after a torn write or a lost write there is a hole, nothing past it
// can be trusted, and nothing past it was ever reported durable.
func (j *Journal) recover(published uint64) error {
	names, err := j.disk.List()
	if err != nil {
		return err
	}
	var live, idle []*slot
	for _, name := range names {
		id, ok := parseSlot(name)
		if !ok {
			continue
		}
		f, err := j.disk.Open(name)
		if err != nil {
			return err
		}
		s := &slot{name: name, id: id, size: f.Size(), ready: ready, file: f}
		j.nextID = max(j.nextID, id+1)
		j.nslots++
		var header [headerSize]byte
		if s.size >= headerSize {
			if _, err := f.ReadAt(header[:], 0); err != nil {
				return err
			}
		}
		if first, life, ok := parseHeader(header[:]); ok {
			s.first, s.life, s.seed = first, life, seedOf(life)
			live = append(live, s)
		} else {
			idle = append(idle, s)
		}
	}
	sort.Slice(live, func(a, b int) bool { return live[a].first < live[b].first })

	var chain []*slot
	var last uint64
	for _, s := range live {
		if len(chain) > 0 && s.first != last+1 {
			idle = append(idle, s)
			continue
		}
		next, end, err := scan(s.file, s.seed, s.first, func(r record, _ int64) error {
			if r.seq > published {
				j.account(r.seq, r.ack, int64(recordHeader+len(r.payload)))
			}
			return nil
		})
		if err != nil {
			return err
		}
		if next == s.first {
			idle = append(idle, s)
			continue
		}
		s.last, s.end = next-1, end
		chain = append(chain, s)
		last = next - 1
	}
	// The store is the truth about what is published. A log that ends before
	// the store's state holds nothing new. A log that begins after it is
	// missing the records in between, and what follows a gap is not a prefix.
	if len(chain) > 0 && (published > last || chain[0].first-1 > published) {
		idle = append(idle, chain...)
		chain = nil
		j.spans, j.head, j.total = nil, 0, 0
	}

	j.published = published
	j.next = published + 1
	if len(chain) > 0 {
		j.next = last + 1
	}

	// Segments that are not in the log lose their header, durably, before any
	// new record is written: otherwise a stale segment could be mistaken for a
	// continuation of the new records, whose sequence numbers it may repeat.
	var wipe []*slot
	for _, s := range idle {
		if s.first != 0 {
			if _, err := s.file.WriteAt(make([]byte, headerSize), 0); err != nil {
				return err
			}
			wipe = append(wipe, s)
		}
	}
	for _, s := range chain {
		if err := s.file.Sync(); err != nil {
			return err
		}
	}
	for _, s := range wipe {
		if err := s.file.Sync(); err != nil {
			return err
		}
	}
	for _, s := range idle {
		s.first, s.last, s.end = 0, 0, 0
		if s.size != j.segment {
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
		if err := j.drop(s); err != nil {
			return err
		}
	}

	j.segs = chain
	for _, s := range chain {
		j.held += s.end
	}
	j.written, j.synced = j.next-1, j.next-1

	switch err := j.rotate(); err {
	case nil:
		<-j.tail.ready
		if j.tail.err != nil {
			return j.tail.err
		}
	case ErrFull:
	default:
		return err
	}
	return nil
}

// drop closes and deletes a segment file that cannot be reused.
func (j *Journal) drop(s *slot) error {
	j.nslots--
	if err := s.file.Close(); err != nil {
		return err
	}
	return j.disk.Remove(s.name)
}

// account adds one record to the spans. Spans cover the unpublished records;
// one span stands for every record acknowledged within a quantum of its first.
func (j *Journal) account(seq uint64, ack int64, n int64) {
	if j.head == len(j.spans) || ack-j.spans[len(j.spans)-1].ack >= quantum {
		j.spans = append(j.spans, span{seq, ack, j.total})
	}
	j.total += uint64(n)
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
	old := append([]*slot(nil), j.segs[:n]...)
	j.mu.Unlock()

	// Oldest first, each made durable before the next is touched: a crash may
	// then lose the newest wipes, never an older one while keeping a newer,
	// which would leave a gap in the log and discard everything after it.
	for _, s := range old {
		if _, err := s.file.WriteAt(make([]byte, headerSize), 0); err != nil {
			return err
		}
		if err := s.file.Sync(); err != nil {
			return err
		}
	}

	j.mu.Lock()
	defer j.mu.Unlock()
	j.segs = append(j.segs[:0], j.segs[n:]...)
	for _, s := range old {
		j.held -= s.end
		s.first, s.last, s.end = 0, 0, 0
		if s.size != j.segment {
			if err := j.drop(s); err != nil {
				return err
			}
			continue
		}
		j.free = append(j.free, s)
	}
	j.refill()
	return nil
}
