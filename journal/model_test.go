package journal_test

import (
	"errors"
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/hanzoai/vfs/journal"
	"github.com/hanzoai/vfs/journal/crash"
)

// The model is a plain map of what was appended and three integers. A random
// history of Append, Sync, Publish, Reclaim, Replay, restart and crash runs
// against the journal and the model, and after every step the two must agree
// on the watermarks, the backlog, the age of the oldest unpublished record and
// the records Replay returns.

type modelRecord struct {
	data []byte
	ack  time.Time
}

type model struct {
	recs                       map[uint64]modelRecord
	visible, published, synced uint64
}

func TestModel(t *testing.T) {
	for seed := int64(1); seed <= 12; seed++ {
		t.Run(fmt.Sprint(seed), func(t *testing.T) { runModel(t, seed, 1500) })
	}
}

func runModel(t *testing.T, seed int64, steps int) {
	rng := rand.New(rand.NewSource(seed))
	clk := newClock()
	d := crash.New()
	m := &model{recs: map[uint64]modelRecord{}}
	opts := func() journal.Options {
		return journal.Options{Capacity: 8 * 4096, Segment: 4096, Disk: d, Clock: clk, Published: m.published, Bound: time.Hour}
	}
	j := open(t, opts())
	defer func() { j.Close() }()
	var log []string
	fail := func(format string, args ...any) {
		t.Helper()
		start := max(0, len(log)-12)
		t.Fatalf("%s\nlast steps:\n%s", fmt.Sprintf(format, args...), joinLines(log[start:]))
	}

	// drain is what a publisher does for a full journal.
	drain := func() {
		if err := j.Sync(ctx, j.Marks().Visible); err != nil {
			fail("drain: Sync: %v", err)
		}
		m.synced = max(m.synced, j.Marks().Visible)
		if err := j.Publish(j.Marks().Visible); err != nil {
			fail("drain: Publish: %v", err)
		}
		m.published = m.visible
		if err := j.Reclaim(); err != nil {
			fail("drain: Reclaim: %v", err)
		}
	}

	for step := 0; step < steps; step++ {
		clk.Advance(2 * time.Millisecond)
		switch n := rng.Intn(100); {
		case n < 45:
			p := make([]byte, rng.Intn(1200))
			rng.Read(p)
			seq, err := j.Append(ctx, p)
			if errors.Is(err, journal.ErrFull) {
				log = append(log, "append: full, drain")
				drain()
				seq, err = j.Append(ctx, p)
			}
			if err != nil {
				fail("Append: %v", err)
			}
			if seq != m.visible+1 {
				fail("Append returned %d, want %d", seq, m.visible+1)
			}
			m.visible = seq
			m.recs[seq] = modelRecord{p, clk.Now()}
			log = append(log, fmt.Sprintf("append %d (%d bytes)", seq, len(p)))
		case n < 62:
			if m.visible == 0 {
				continue
			}
			seq := 1 + uint64(rng.Intn(int(m.visible)))
			if err := j.Sync(ctx, seq); err != nil {
				fail("Sync(%d): %v", seq, err)
			}
			if seq > m.published {
				m.synced = max(m.synced, seq)
			}
			log = append(log, fmt.Sprintf("sync %d", seq))
		case n < 75:
			seq := m.published + uint64(rng.Intn(int(m.visible-m.published)+1))
			if err := j.Publish(seq); err != nil {
				fail("Publish(%d): %v", seq, err)
			}
			m.published = max(m.published, seq)
			log = append(log, fmt.Sprintf("publish %d", seq))
		case n < 85:
			if err := j.Reclaim(); err != nil {
				fail("Reclaim: %v", err)
			}
			log = append(log, "reclaim")
		case n < 90:
			got, err := replay(j)
			if err != nil {
				fail("Replay: %v", err)
			}
			if msg := m.differs(got); msg != "" {
				fail("Replay: %s", msg)
			}
			log = append(log, "replay")
		case n < 95:
			if err := j.Close(); err != nil {
				fail("Close: %v", err)
			}
			j = open(t, opts())
			m.synced = m.visible
			log = append(log, "restart")
		default:
			vs := d.Variants(d.Len(), 2, seed*1000+int64(step))
			v := vs[rng.Intn(len(vs))]
			img := d.Image(d.Len(), v)
			j.Close()
			d = img
			j = open(t, opts())
			tail := j.Marks().Visible
			if tail < m.synced && m.synced > m.published {
				fail("after %v the journal holds %d, Sync had returned for %d (store: %d)", v, tail, m.synced, m.published)
			}
			if tail > m.visible {
				fail("after %v the journal holds %d, only %d were appended", v, tail, m.visible)
			}
			m.visible = max(tail, m.published)
			for seq := range m.recs {
				if seq > m.visible {
					delete(m.recs, seq)
				}
			}
			m.synced = m.visible
			log = append(log, fmt.Sprintf("crash (%v): %d records survive", v, tail))
		}

		marks := j.Marks()
		if marks.Visible != m.visible || marks.Published != m.published {
			fail("marks %+v, model visible %d published %d", marks, m.visible, m.published)
		}
		if marks.Durable < m.synced || marks.Durable > marks.Visible || marks.Published > marks.Durable {
			fail("marks %+v, model synced %d", marks, m.synced)
		}
		s := j.State()
		var backlog int64
		var age time.Duration
		if m.visible > m.published {
			for seq := m.published + 1; seq <= m.visible; seq++ {
				backlog += int64(28 + len(m.recs[seq].data))
			}
			age = clk.Now().Sub(m.recs[m.published+1].ack)
		}
		if s.Backlog != backlog || s.Age != age {
			fail("state backlog %d age %v, model %d %v", s.Backlog, s.Age, backlog, age)
		}
	}
	got, err := replay(j)
	if err != nil {
		fail("final Replay: %v", err)
	}
	if msg := m.differs(got); msg != "" {
		fail("final Replay: %s", msg)
	}
}

func (m *model) differs(got []entry) string {
	var want []entry
	for seq := m.published + 1; seq <= m.visible; seq++ {
		want = append(want, entry{seq, string(m.recs[seq].data)})
	}
	if !equal(got, want) {
		return fmt.Sprintf("returned %v, model has %v", seqs(got), seqs(want))
	}
	return ""
}

func joinLines(s []string) string {
	out := ""
	for _, l := range s {
		out += "  " + l + "\n"
	}
	return out
}

// Many goroutines append and sync while a publisher, driven by Due, publishes
// and reclaims, and a watcher checks that the watermarks only move forward and
// stay ordered. Run under -race.
func TestConcurrentWriters(t *testing.T) {
	for _, onDisk := range []bool{false, true} {
		name := "memory"
		if onDisk {
			name = "directory"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			opt := journal.Options{Capacity: 8 << 20, Segment: 256 << 10}
			if !onDisk {
				opt.Disk = crash.New()
			}
			j, err := journal.Open(dir, opt)
			if err != nil {
				t.Fatal(err)
			}
			const writers, each = 16, 150
			var (
				mu   sync.Mutex
				data = map[uint64][]byte{}
				wg   sync.WaitGroup
			)
			stop := make(chan struct{})

			var pub sync.WaitGroup
			pub.Add(2)
			go func() { // publisher, woken by Due
				defer pub.Done()
				for {
					select {
					case <-j.Due(time.Millisecond):
						m := j.Marks()
						if m.Visible > m.Published {
							if err := j.Publish(m.Published + (m.Visible-m.Published+1)/2); err != nil {
								t.Error(err)
								return
							}
							if err := j.Reclaim(); err != nil {
								t.Error(err)
								return
							}
						}
					case <-stop:
						return
					}
				}
			}()
			go func() { // watcher
				defer pub.Done()
				var last journal.Marks
				for {
					select {
					case <-stop:
						return
					default:
					}
					m := j.Marks()
					if m.Visible < last.Visible || m.Durable < last.Durable || m.Published < last.Published ||
						m.Published > m.Durable || m.Durable > m.Visible {
						t.Errorf("marks went wrong: %+v then %+v", last, m)
						return
					}
					last = m
					runtime.Gosched()
				}
			}()
			for w := 0; w < writers; w++ {
				wg.Add(1)
				go func(w int) {
					defer wg.Done()
					rng := rand.New(rand.NewSource(int64(w)))
					for i := 0; i < each; i++ {
						p := make([]byte, rng.Intn(2000))
						rng.Read(p)
						var seq uint64
						var err error
						for {
							if seq, err = j.Append(ctx, p); !errors.Is(err, journal.ErrFull) {
								break
							}
							runtime.Gosched() // the publisher is draining
						}
						if err != nil {
							t.Error(err)
							return
						}
						mu.Lock()
						data[seq] = p
						mu.Unlock()
						if err := j.Sync(ctx, seq); err != nil {
							t.Error(err)
							return
						}
						if m := j.Marks(); m.Durable < seq {
							t.Errorf("Sync(%d) returned with durable %d", seq, m.Durable)
							return
						}
					}
				}(w)
			}
			wg.Wait()
			close(stop)
			pub.Wait()
			if t.Failed() {
				return
			}

			total := uint64(writers * each)
			m := j.Marks()
			if m.Visible != total || uint64(len(data)) != total {
				t.Fatalf("marks %+v, %d records appended", m, len(data))
			}
			check := func(j *journal.Journal) {
				t.Helper()
				m := j.Marks()
				got, err := replay(j)
				if err != nil {
					t.Fatal(err)
				}
				if uint64(len(got)) != total-m.Published {
					t.Fatalf("%d records after %d, want %d", len(got), m.Published, total-m.Published)
				}
				for i, e := range got {
					if e.Seq != m.Published+1+uint64(i) || e.Data != string(data[e.Seq]) {
						t.Fatalf("record %d wrong", e.Seq)
					}
				}
			}
			check(j)
			if err := j.Close(); err != nil {
				t.Fatal(err)
			}
			if onDisk {
				opt.Published = m.Published // what the store holds
				j2, err := journal.Open(dir, opt)
				if err != nil {
					t.Fatal(err)
				}
				defer j2.Close()
				check(j2)
			}
		})
	}
}
