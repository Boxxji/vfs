package journal

// The pipeline has two goroutines. The writer takes the batches Append queued
// and writes them to the segment files; it never waits for a sync, so the next
// batch is written while the previous one is being synced. The syncer is the
// sync queue: it snapshots what has been written, syncs the files that hold
// it, and only then releases the Sync calls that snapshot covers. A call for
// sequence N is therefore released by a sync that began after N was written;
// a sync already in flight never satisfies it. Calls that arrive during a sync
// form the next group, so the group size adapts to the load with no timer.

func (j *Journal) writer() {
	defer j.wg.Done()
	defer func() {
		j.mu.Lock()
		j.wdone = true
		j.kick(j.kicks)
		j.mu.Unlock()
	}()
	for {
		j.mu.Lock()
		for len(j.queue) == 0 {
			if j.closed || j.err != nil {
				j.mu.Unlock()
				return
			}
			j.mu.Unlock()
			<-j.kickw
			j.mu.Lock()
		}
		if j.err != nil {
			j.mu.Unlock()
			return
		}
		batches := j.queue
		j.queue, j.cur = nil, nil
		j.mu.Unlock()

		for _, b := range batches {
			// Data written but not synced is what a sync has to flush and what
			// a Sync call can end up waiting behind. Bound it, so one bulk
			// writer cannot make the next fsync wait for gigabytes.
			j.mu.Lock()
			for j.behind >= exposed && j.err == nil {
				wait := j.room
				j.mu.Unlock()
				<-wait
				j.mu.Lock()
			}
			failed := j.err != nil
			j.mu.Unlock()
			if failed {
				return
			}
			<-b.slot.ready
			err := b.slot.err
			if err == nil {
				_, err = b.slot.file.WriteAt(b.data, b.off)
			}
			if err == nil {
				// Ask for the data to start going out while more is being
				// written. It is a hint: if it fails Sync still does the work.
				if b.off == 0 {
					b.slot.hint = 0
				}
				if end := b.off + int64(len(b.data)); end-b.slot.hint >= hintBytes {
					b.slot.file.Writeback(b.slot.hint, end-b.slot.hint)
					b.slot.hint = end
				}
			}
			j.mu.Lock()
			if err != nil {
				j.failLocked(err)
				j.mu.Unlock()
				return
			}
			j.written = b.last
			if !b.slot.dirty {
				b.slot.dirty = true
				j.dirty = append(j.dirty, b.slot)
			}
			j.unsynced += int64(len(b.data))
			j.behind += int64(len(b.data))
			j.pending -= int64(len(b.data))
			j.broadcast()
			if j.wantSync() {
				j.kick(j.kicks)
			}
			j.mu.Unlock()
			putBuffer(b.data)
		}
	}
}

// wantSync reports whether a sync is worth starting: a waiter's record is
// written, enough bytes are waiting that the kernel should not hold them, or
// the journal is closing. Caller holds mu.
func (j *Journal) wantSync() bool {
	if j.written <= j.synced {
		return false
	}
	if j.unsynced >= syncBytes || (j.closed && j.wdone) {
		return true
	}
	for _, w := range j.waiters {
		if w.seq <= j.written {
			return true
		}
	}
	return false
}

func (j *Journal) syncer() {
	defer j.wg.Done()
	for {
		j.mu.Lock()
		for {
			if j.err != nil || (j.closed && j.wdone && j.written == j.synced) {
				j.mu.Unlock()
				return
			}
			if j.wantSync() {
				break
			}
			j.mu.Unlock()
			<-j.kicks
			j.mu.Lock()
		}
		target := j.written
		files := j.dirty
		j.dirty = nil
		for _, s := range files {
			s.dirty = false
		}
		round := j.unsynced
		j.unsynced = 0
		j.mu.Unlock()

		var err error
		for _, s := range files {
			if err = s.file.Sync(); err != nil {
				break
			}
		}

		j.mu.Lock()
		if err != nil {
			j.failLocked(err)
			j.mu.Unlock()
			return
		}
		j.synced = max(j.synced, target)
		j.behind -= round
		j.broadcast()
		j.stats.Rounds++
		if freed := j.releaseLocked(); freed > 0 {
			j.stats.Groups[bucket(freed)]++
		}
		j.mu.Unlock()
	}
}
