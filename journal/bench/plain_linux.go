package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

// zeroFile makes a file of n bytes with every byte written, the best case for
// a plain write loop: overwriting allocated blocks, as the journal does.
func zeroFile(path string, n int64) *os.File {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	check(err)
	zeros := make([]byte, 1<<20)
	for off := int64(0); off < n; off += int64(len(zeros)) {
		_, err := f.WriteAt(zeros, off)
		check(err)
	}
	check(fdatasync(f))
	return f
}

// plainLatency is the baseline to the journal's latency run: every writer
// does pwrite then fdatasync on a preallocated file, with no journal.
func plainLatency(work string) {
	const per = 16 << 20
	files := make([]*os.File, *writers)
	for w := range files {
		switch {
		case *shared && w > 0:
			files[w] = files[0]
		case *shared:
			files[w] = zeroFile(filepath.Join(work, "shared"), per*int64(*writers))
		default:
			files[w] = zeroFile(filepath.Join(work, fmt.Sprintf("w%d", w)), per)
		}
	}
	streams := make([]*os.File, *bulk)
	for b := range streams {
		streams[b] = zeroFile(filepath.Join(work, fmt.Sprintf("bulk%d", b)), 256<<20)
		defer streams[b].Close()
	}
	buf := make([]byte, *size)
	deadline := time.Now().Add(*dur)
	lats := make([][]time.Duration, *writers)
	var wg sync.WaitGroup
	start := time.Now()
	for w := range files {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			base := int64(0)
			if *shared {
				base = int64(w) * per
			}
			var off int64
			for time.Now().Before(deadline) {
				t0 := time.Now()
				_, err := files[w].WriteAt(buf, base+off)
				check(err)
				check(fdatasync(files[w]))
				lats[w] = append(lats[w], time.Since(t0))
				off = (off + int64(len(buf))) % (per - int64(len(buf)))
			}
		}(w)
	}
	for _, f := range streams {
		wg.Add(1)
		go func(f *os.File) {
			defer wg.Done()
			stream := make([]byte, 1<<20)
			var off int64
			for time.Now().Before(deadline) {
				_, err := f.WriteAt(stream, off)
				check(err)
				off = (off + int64(len(stream))) % (256 << 20)
			}
		}(f)
	}
	wg.Wait()
	elapsed := time.Since(start)
	var all []time.Duration
	for _, l := range lats {
		all = append(all, l...)
	}
	mode := "pwrite+fdatasync"
	if *shared {
		mode += " shared"
	}
	report(mode, len(all), int64(len(all))*int64(*size), elapsed, all, int64(len(all)), int64(len(all)), "", "")
	for _, f := range files {
		f.Close()
	}
}

// plainSequential is the baseline to the journal's throughput run: 1 MiB
// pwrite calls in order, with an fdatasync every -every bytes.
func plainSequential(work string) {
	const span = 2 << 30
	f := zeroFile(filepath.Join(work, "seq"), span)
	defer f.Close()
	chunk := make([]byte, 1<<20)
	if *size != 4096 {
		chunk = make([]byte, *size)
	}
	bytes := parseSize(*total)
	gap := parseSize(*every)
	var lat []time.Duration
	var off, since, syncs int64
	start := time.Now()
	for done := int64(0); done < bytes; done += int64(len(chunk)) {
		t0 := time.Now()
		_, err := f.WriteAt(chunk, off)
		check(err)
		since += int64(len(chunk))
		if gap > 0 && since >= gap {
			check(fdatasync(f))
			syncs++
			since = 0
		}
		lat = append(lat, time.Since(t0))
		off = (off + int64(len(chunk))) % (span - int64(len(chunk)))
	}
	written := time.Since(start)
	check(fdatasync(f))
	syncs++
	durable := time.Since(start)
	report("written", len(lat), bytes, written, append([]time.Duration(nil), lat...), 0, 0, "", "")
	report("durable", len(lat), bytes, durable, lat, syncs, 0, "", fmt.Sprintf("every=%s syncs=%d", *every, syncs))
}

// groupSync is the loop a careful engineer writes by hand to get what the
// journal's sync queue gives: writers append to one shared file, and a syncer
// goroutine runs one fdatasync after another, each serving every writer whose
// write finished before it began. Each waiter has its own channel, so a finished
// sync wakes exactly the writers it covered.
type groupSync struct {
	f       *os.File
	mu      sync.Mutex
	written uint64 // writes completed
	waiting []waiting
	kick    chan struct{}
	calls   atomic.Int64
}

type waiting struct {
	mine uint64
	done chan struct{}
}

func newGroupSync(f *os.File) *groupSync {
	g := &groupSync{f: f, kick: make(chan struct{}, 1)}
	go g.run()
	return g
}

func (g *groupSync) run() {
	for range g.kick {
		g.mu.Lock()
		batch := g.waiting
		g.waiting = nil
		g.mu.Unlock()
		if len(batch) == 0 {
			continue
		}
		check(fdatasync(g.f))
		g.calls.Add(1)
		for _, w := range batch {
			close(w.done)
		}
		select { // writers that arrived during the sync are waiting for the next one
		case g.kick <- struct{}{}:
		default:
		}
	}
}

// wait returns once an fdatasync that began after this writer's write has finished.
func (g *groupSync) wait() {
	g.mu.Lock()
	g.written++
	w := waiting{g.written, make(chan struct{})}
	g.waiting = append(g.waiting, w)
	g.mu.Unlock()
	select {
	case g.kick <- struct{}{}:
	default:
	}
	<-w.done
}

// groupLatency is the plain baseline that does what the journal does for the
// fsync callers and nothing else: no record framing, no replay, no watermarks,
// no deadline, no backpressure. The writers append to one shared file, each at
// the next free offset (so the writes are one sequential stream, as in a log),
// then wait for a shared fdatasync.
func groupLatency(work string) {
	const span = 256 << 20
	f := zeroFile(filepath.Join(work, "group"), span)
	defer f.Close()
	g := newGroupSync(f)
	streams := make([]*os.File, *bulk)
	for b := range streams {
		streams[b] = zeroFile(filepath.Join(work, fmt.Sprintf("bulk%d", b)), 256<<20)
		defer streams[b].Close()
	}
	buf := make([]byte, *size)
	deadline := time.Now().Add(*dur)
	lats := make([][]time.Duration, *writers)
	var next atomic.Int64
	var wg sync.WaitGroup
	start := time.Now()
	for w := 0; w < *writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for time.Now().Before(deadline) {
				t0 := time.Now()
				off := (next.Add(int64(len(buf))) - int64(len(buf))) % (span - int64(len(buf)))
				_, err := f.WriteAt(buf, off)
				check(err)
				g.wait()
				lats[w] = append(lats[w], time.Since(t0))
			}
		}(w)
	}
	for _, s := range streams {
		wg.Add(1)
		go func(f *os.File) {
			defer wg.Done()
			stream := make([]byte, 1<<20)
			var off int64
			for time.Now().Before(deadline) {
				_, err := f.WriteAt(stream, off)
				check(err)
				off = (off + int64(len(stream))) % (256 << 20)
			}
		}(s)
	}
	wg.Wait()
	elapsed := time.Since(start)
	var all []time.Duration
	for _, l := range lats {
		all = append(all, l...)
	}
	report("pwrite+group fdatasync", len(all), int64(len(all))*int64(*size), elapsed, all, g.calls.Load(), int64(len(all)), "", "")
}
