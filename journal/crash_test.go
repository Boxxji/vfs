package journal_test

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"github.com/hanzoai/vfs/journal"
	"github.com/hanzoai/vfs/journal/crash"
)

// The crash tests run a workload once on a crash.Disk, which logs every write,
// sync, create and remove. They then rebuild the disk a crash would leave after
// EVERY point of that log, in every way the volatile writes can have reached
// the media (none, all, each prefix, torn writes inside a sector and across
// sectors, random scatters), recover each, and check the contract:
//
//   - Open succeeds and Replay returns a contiguous run of records after the
//     published watermark, each with the bytes that were appended;
//   - the run reaches every record whose Sync had returned, unless the store
//     holds it;
//   - a second Replay, and a second Open of the recovered disk, return the same;
//   - after recovery the journal takes new records, and a crash that keeps only
//     what those new records' Sync made durable returns the old run followed by
//     exactly the new ones: nothing left behind by the lost tail comes back.

type acked uint64  // a Sync for this sequence has returned
type stored uint64 // a Publish for this sequence has returned

type run struct {
	d        *crash.Disk
	mu       sync.Mutex
	data     map[uint64][]byte
	creates  int // segment files made
	activate int // segments started: a write at offset 0 carries a header
}

func newRun() *run {
	r := &run{d: crash.New(), data: map[uint64][]byte{}}
	r.d.Intercept(func(e crash.Event) error {
		r.mu.Lock()
		defer r.mu.Unlock()
		switch {
		case e.Kind == "create":
			r.creates++
		case e.Kind == "write" && e.Off == 0:
			r.activate++
		}
		return nil
	})
	return r
}

func (r *run) opts(published uint64) journal.Options {
	return small(r.d, ticking{newClock(), 3e6}, published)
}

// The workload helpers report with Error, not Fatal, so goroutines may use them.

func (r *run) append(t testing.TB, j *journal.Journal, p []byte) uint64 {
	var seq uint64
	var err error
	// A full journal is waiting for its publisher; here the writer drains it.
	for try := 0; try < 100; try++ {
		if seq, err = j.Append(ctx, p); !errors.Is(err, journal.ErrFull) {
			break
		}
		r.sync(t, j, j.Marks().Visible)
		r.publish(t, j, j.Marks().Visible)
		r.reclaim(t, j)
	}
	if err != nil {
		t.Errorf("Append: %v", err)
		return 0
	}
	r.mu.Lock()
	r.data[seq] = p
	r.mu.Unlock()
	return seq
}

func (r *run) sync(t testing.TB, j *journal.Journal, seq uint64) {
	c, cancel := within()
	defer cancel()
	if err := j.Sync(c, seq); err != nil {
		t.Errorf("Sync(%d): %v", seq, err)
		return
	}
	r.d.Mark(acked(seq))
}

// publish tells the journal the store holds seq, or as much of it as is
// durable: Publish refuses a record no sync has covered. The store holds it
// before the journal is told, so the mark goes into the log first.
func (r *run) publish(t testing.TB, j *journal.Journal, seq uint64) {
	seq = min(seq, j.Marks().Durable)
	if seq <= j.Marks().Published {
		return
	}
	r.d.Mark(stored(seq))
	if err := j.Publish(seq); err != nil {
		t.Errorf("Publish(%d): %v", seq, err)
	}
}

func (r *run) reclaim(t testing.TB, j *journal.Journal) {
	if err := j.Reclaim(); err != nil {
		t.Errorf("Reclaim: %v", err)
	}
}

// explore checks every crash state of the run.
func (r *run) explore(t *testing.T, scatter int) {
	t.Helper()
	states, distinct := 0, map[state]bool{}
	err := r.d.Explore(scatter, 1, func(img *crash.Disk, n int, v crash.Variant) error {
		states++
		if _, again := r.novel(distinct, img, n); again {
			return nil
		}
		return r.check(img, n)
	})
	if err != nil {
		t.Fatalf("%v\n%s", err, r.d.Log(min(r.d.Len(), 40)))
	}
	t.Logf("%d crash states over %d points, %d distinct", states, r.d.Len(), len(distinct))
}

// state is a crash case: the bytes on the media and the two things the Volume
// knew when it happened.
type state struct {
	disk     [sha256.Size]byte
	ack, pub uint64
}

func (r *run) state(img *crash.Disk, n int) state {
	s := state{disk: img.Fingerprint()}
	for _, note := range r.d.Marks(n) {
		switch x := note.(type) {
		case acked:
			s.ack = max(s.ack, uint64(x))
		case stored:
			s.pub = max(s.pub, uint64(x))
		}
	}
	return s
}

// novel reports the crash case and whether seen already held it. What recovery
// must give depends on the disk and on what the Volume knew at the crash; two
// points that agree on both are one case.
func (r *run) novel(seen map[state]bool, img *crash.Disk, n int) (s state, again bool) {
	s = r.state(img, n)
	again = seen[s]
	seen[s] = true
	return s, again
}

func (r *run) check(img *crash.Disk, n int) error {
	s := r.state(img, n)
	ack, pub := s.ack, s.pub
	// The Volume tells Open the Head's sequence, which is the store's truth.
	return r.recovered(img.Image(img.Len(), crash.Variant{Kind: crash.All}), pub, ack, pub)
}

func (r *run) recovered(img *crash.Disk, given, ack, pub uint64) error {
	j, err := journal.Open("", small(img, ticking{newClock(), 3e6}, given))
	if err != nil {
		return fmt.Errorf("Open: %w", err)
	}
	defer j.Close()

	m := j.Marks()
	if m.Published > pub {
		return fmt.Errorf("claims %d published but the store holds %d", m.Published, pub)
	}
	got, err := replay(j)
	if err != nil {
		return fmt.Errorf("Replay: %w", err)
	}
	for i, e := range got {
		if e.Seq != m.Published+1+uint64(i) {
			return fmt.Errorf("replay is not contiguous after %d: %v", m.Published, seqs(got))
		}
		r.mu.Lock()
		want := r.data[e.Seq]
		r.mu.Unlock()
		if !bytes.Equal([]byte(e.Data), want) {
			return fmt.Errorf("record %d came back as %q, appended as %q", e.Seq, e.Data, want)
		}
	}
	tail := m.Visible
	if tail != m.Published+uint64(len(got)) {
		return fmt.Errorf("visible %d but %d records after %d", tail, len(got), m.Published)
	}
	if ack > tail && ack > pub {
		return fmt.Errorf("Sync had returned for %d, only %d recovered, the store holds %d", ack, tail, pub)
	}
	if again, err := replay(j); err != nil || !equal(again, got) {
		return fmt.Errorf("second Replay differs: %v then %v (%v)", seqs(got), seqs(again), err)
	}

	// Recovery is idempotent: closing and opening again changes nothing.
	if err := j.Close(); err != nil {
		return fmt.Errorf("Close: %w", err)
	}
	j, err = journal.Open("", small(img, ticking{newClock(), 3e6}, given))
	if err != nil {
		return fmt.Errorf("second Open: %w", err)
	}
	if m2 := j.Marks(); m2 != m {
		return fmt.Errorf("second Open has marks %+v, first %+v", m2, m)
	}
	if again, err := replay(j); err != nil || !equal(again, got) {
		return fmt.Errorf("Replay after second Open differs: %v then %v (%v)", seqs(got), seqs(again), err)
	}

	// The journal keeps working, and what the lost tail left behind stays gone.
	// A recovered journal can be full of records nobody has published; the
	// publisher then drains it, which is what a full journal is waiting for.
	fresh := map[uint64][]byte{}
	var last uint64
	drained := given
	for i := 0; i < 4; i++ {
		p := append([]byte("again."), payload(uint64(i+1)*7)...)
		seq, err := j.Append(ctx, p)
		if errors.Is(err, journal.ErrFull) {
			drained = j.Marks().Visible
			if err := j.Publish(drained); err != nil {
				return fmt.Errorf("Publish after recovery: %w", err)
			}
			if err := j.Reclaim(); err != nil {
				return fmt.Errorf("Reclaim after recovery: %w", err)
			}
			seq, err = j.Append(ctx, p)
		}
		if err != nil {
			return fmt.Errorf("Append after recovery: %w", err)
		}
		fresh[seq] = p
		last = seq
	}
	if err := j.Sync(ctx, last); err != nil {
		return fmt.Errorf("Sync after recovery: %w", err)
	}
	survivor := img.Image(img.Len(), crash.Variant{Kind: crash.Lost})
	j2, err := journal.Open("", small(survivor, ticking{newClock(), 3e6}, drained))
	if err != nil {
		return fmt.Errorf("Open after the second crash: %w", err)
	}
	defer j2.Close()
	after, err := replay(j2)
	if err != nil {
		return fmt.Errorf("Replay after the second crash: %w", err)
	}
	var want []entry
	for _, e := range got {
		if e.Seq > drained {
			want = append(want, e)
		}
	}
	for seq := tail + 1; seq <= last; seq++ {
		want = append(want, entry{seq, string(fresh[seq])})
	}
	if !equal(after, want) {
		return fmt.Errorf("after the second crash replay is %v, want %v", seqs(after), seqs(want))
	}
	return nil
}

func equal(a, b []entry) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// Sequential: append, sync, publish and reclaim in an order that crosses
// every segment boundary and recycles every file several times.
func TestCrashSequential(t *testing.T) {
	for _, every := range []int{1, 3, 16, 1000} {
		t.Run(fmt.Sprintf("every%d", every), func(t *testing.T) {
			defer journal.MarkEvery(every)()
			r := newRun()
			j := open(t, r.opts(0))
			rng := rand.New(rand.NewSource(int64(7 + every)))
			var visible, published uint64
			for i := 0; i < 90; i++ {
				seq := r.append(t, j, payload(uint64(rng.Intn(1000))))
				visible = seq
				switch rng.Intn(10) {
				case 0, 1, 2, 3:
					r.sync(t, j, seq)
				case 4, 5:
					published += uint64(rng.Intn(int(visible-published))) + 1
					r.publish(t, j, published)
				case 6, 7:
					r.reclaim(t, j)
				}
			}
			r.sync(t, j, visible)
			if err := j.Close(); err != nil {
				t.Fatal(err)
			}
			if r.activate <= r.creates {
				t.Fatalf("%d segments started over %d files: nothing was recycled", r.activate, r.creates)
			}
			r.explore(t, 2)
		})
	}
}

// Concurrent: several writers appending and syncing while a publisher
// publishes and reclaims. The log is whatever order the disk saw.
func TestCrashConcurrent(t *testing.T) {
	defer journal.MarkEvery(3)()
	r := newRun()
	j := open(t, r.opts(0))
	var wg sync.WaitGroup
	for g := 0; g < 5; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(g)))
			for i := 0; i < 40; i++ {
				seq := r.append(t, j, payload(uint64(rng.Intn(1000))))
				if rng.Intn(3) == 0 {
					r.sync(t, j, seq)
				}
			}
		}(g)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 25; i++ {
			if m := j.Marks(); m.Visible > m.Published {
				r.publish(t, j, m.Published+(m.Visible-m.Published+1)/2)
				r.reclaim(t, j)
			}
		}
	}()
	wg.Wait()
	m := j.Marks()
	r.sync(t, j, m.Visible)
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	r.explore(t, 2)
}

// Sessions: close, reopen with the Head's sequence, and carry on, as the
// Volume does. The files of earlier sessions are still on the disk.
func TestCrashSessions(t *testing.T) {
	defer journal.MarkEvery(16)()
	r := newRun()
	var published uint64
	for session := 0; session < 3; session++ {
		j := open(t, r.opts(published))
		var seq uint64
		for i := 0; i < 60; i++ {
			seq = r.append(t, j, payload(uint64(i*31%700)))
			if i%5 == 0 {
				r.sync(t, j, seq)
			}
			if i%11 == 10 {
				r.publish(t, j, seq-3)
				published = j.Marks().Published // what the store holds, which the next session is told
				r.reclaim(t, j)
			}
		}
		r.sync(t, j, seq)
		if err := j.Close(); err != nil {
			t.Fatal(err)
		}
	}
	r.explore(t, 2)
}

// Reclaim wipes the headers of published segments, oldest first. Every crash
// point inside it, with the wipes persisted in any order, recovers to a log
// that is a prefix: a wipe of a newer segment must never persist without the
// wipe of an older one, or the synced records after the gap would be lost.
func TestCrashReclaim(t *testing.T) {
	for _, every := range []int{1, 3, 16, 1000} {
		t.Run(fmt.Sprintf("every%d", every), func(t *testing.T) {
			defer journal.MarkEvery(every)()
			r := newRun()
			j := open(t, r.opts(0))
			big := bytes.Repeat([]byte("x"), 1000) // three to a segment
			var last uint64
			for i := 0; i < 9; i++ {
				last = r.append(t, j, big)
			}
			r.sync(t, j, last)
			for i := 0; i < 3; i++ {
				r.append(t, j, big) // 10 to 12, not synced
			}
			r.publish(t, j, 6) // the first two segments; 7 to 9 are synced and unpublished
			r.reclaim(t, j)
			if err := j.Close(); err != nil {
				t.Fatal(err)
			}
			r.explore(t, 2)
		})
	}
}
