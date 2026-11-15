package journal_test

import (
	"bytes"
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hanzoai/vfs/journal"
	"github.com/hanzoai/vfs/journal/crash"
)

// S1. Everything at once on a file layer that delays writes and syncs by
// random amounts: appenders, syncers, a publisher that publishes ahead of the
// disk and reclaims, replayers, and a sampler of the watermarks. Checks:
// Visible >= Durable >= Published at every sample and each never decreases;
// every Sync that returned covers the sequence it was asked for (an observer
// that samples Marks right after sees Durable >= seq); Replay is contiguous
// after the Published it started from and every payload is the one appended;
// nothing deadlocks. Run with -race.
func TestStress(t *testing.T) {
	d := crash.New()
	rng := rand.New(rand.NewSource(3))
	var rmu sync.Mutex
	d.Intercept(func(e crash.Event) error {
		rmu.Lock()
		n := rng.Intn(100)
		rmu.Unlock()
		if n < 30 {
			time.Sleep(time.Duration(n) * 20 * time.Microsecond)
		}
		return nil
	})
	j := open(t, journal.Options{Capacity: 12 * 4096, Segment: 4096, Disk: d})
	var stop atomic.Bool
	var wg sync.WaitGroup
	var data sync.Map // seq -> payload
	fail := make(chan string, 16)
	report := func(f string, a ...any) {
		select {
		case fail <- fmt.Sprintf(f, a...):
		default:
		}
	}

	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(int64(g)))
			for !stop.Load() {
				p := payload(uint64(r.Intn(1000)))
				seq, err := j.Append(ctx, p)
				if err == journal.ErrFull {
					time.Sleep(50 * time.Microsecond)
					continue
				}
				if err != nil {
					report("Append: %v", err)
					return
				}
				data.Store(seq, p)
				if r.Intn(4) == 0 {
					if err := j.Sync(ctx, seq); err != nil {
						report("Sync(%d): %v", seq, err)
						return
					}
					if m := j.Marks(); m.Durable < seq {
						report("Sync(%d) returned with Durable %d", seq, m.Durable)
					}
				}
			}
		}(g)
	}
	wg.Add(1)
	go func() { // publisher: as far as the disk has got
		defer wg.Done()
		r := rand.New(rand.NewSource(77))
		for !stop.Load() {
			m := j.Marks()
			if m.Durable > m.Published {
				to := m.Published + 1 + uint64(r.Intn(int(m.Durable-m.Published)))
				if err := j.Publish(to); err != nil {
					report("Publish(%d): %v", to, err)
					return
				}
				if err := j.Reclaim(); err != nil {
					report("Reclaim: %v", err)
					return
				}
			}
			time.Sleep(100 * time.Microsecond)
		}
	}()
	for g := 0; g < 2; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				from := j.Marks().Published
				var prev uint64
				err := j.Replay(func(rec journal.Record) error {
					if prev == 0 && rec.Seq < from+1 {
						// Replay uses the Published of its own snapshot, which can be
						// newer than the one read before; older is wrong.
						return fmt.Errorf("first record %d, published was already %d", rec.Seq, from)
					}
					if prev != 0 && rec.Seq != prev+1 {
						return fmt.Errorf("gap %d -> %d", prev, rec.Seq)
					}
					want, ok := data.Load(rec.Seq)
					if ok && !bytes.Equal(want.([]byte), rec.Data) {
						return fmt.Errorf("record %d differs", rec.Seq)
					}
					prev = rec.Seq
					return nil
				})
				if err != nil {
					report("Replay: %v", err)
					return
				}
			}
		}()
	}
	wg.Add(1)
	go func() { // sampler
		defer wg.Done()
		var last journal.Marks
		for !stop.Load() {
			m := j.Marks()
			if !(m.Published <= m.Durable && m.Durable <= m.Visible) {
				report("marks out of order: %+v", m)
			}
			if m.Visible < last.Visible || m.Durable < last.Durable || m.Published < last.Published {
				report("marks went backwards: %+v then %+v", last, m)
			}
			last = m
			time.Sleep(20 * time.Microsecond)
		}
	}()

	done := make(chan struct{})
	go func() { time.Sleep(2 * time.Second); stop.Store(true); wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		buf := make([]byte, 1<<20)
		buf = buf[:runtime.Stack(buf, true)]
		t.Fatalf("deadlock: workers did not stop\n%s", buf)
	}
	select {
	case f := <-fail:
		t.Fatal(f)
	default:
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
}
