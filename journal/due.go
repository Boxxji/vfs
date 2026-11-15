package journal

import (
	"context"
	"time"
)

type due struct {
	age   time.Duration
	ch    chan struct{}
	timer Timer
	stop  func() bool // releases the context hook
	gen   int
	done  bool // fired or abandoned: nothing more happens to it
}

// Due returns a channel that is closed when the oldest unpublished record has
// been acknowledged for at least age. The age is the record's own: later writes
// never reset it, and when Publish retires the oldest record the deadline moves
// to the next one. If nothing is unpublished the deadline starts with the next
// Append. It is driven by one timer set for the exact deadline, not by polling.
//
// The channel is closed once. It is also closed when the journal closes or
// fails, so a publisher learns of a failure at once. Two firings are at least
// Options.Retry apart, so a publisher whose publication keeps failing does not
// spin on an overdue record. When ctx ends first, the registration and its timer
// are released and the channel is never closed: a publisher that waits on Due
// beside other triggers passes a context it cancels when another trigger wins.
func (j *Journal) Due(ctx context.Context, age time.Duration) <-chan struct{} {
	d := &due{age: age, ch: make(chan struct{})}
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.err != nil || j.closed {
		d.done = true
		close(d.ch)
		return d.ch
	}
	if ctx.Err() != nil {
		d.done = true
		return d.ch
	}
	j.dues = append(j.dues, d)
	d.stop = context.AfterFunc(ctx, func() {
		j.mu.Lock()
		defer j.mu.Unlock()
		j.forget(d)
	})
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
	if d.done {
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
	now := j.clock.Now()
	wait := ack.Add(d.age).Sub(now)
	if !j.fired.IsZero() {
		wait = max(wait, j.fired.Add(j.retry).Sub(now))
	}
	if wait <= 0 {
		j.fire(d)
		return
	}
	gen := d.gen
	d.timer = j.clock.AfterFunc(wait, func() {
		j.mu.Lock()
		defer j.mu.Unlock()
		if d.gen == gen && !d.done {
			d.timer = nil
			j.arm(d)
		}
	})
}

// fire closes d's channel, once, and releases everything d holds. Caller holds mu.
func (j *Journal) fire(d *due) {
	if d.done {
		return
	}
	j.fired = j.clock.Now()
	j.forget(d)
	close(d.ch)
}

// forget releases d without closing its channel. Caller holds mu.
func (j *Journal) forget(d *due) {
	if d.done {
		return
	}
	d.done = true
	if d.timer != nil {
		d.timer.Stop()
		d.timer = nil
	}
	if d.stop != nil {
		d.stop()
	}
	for i, x := range j.dues {
		if x == d {
			j.dues = append(j.dues[:i], j.dues[i+1:]...)
			break
		}
	}
}
