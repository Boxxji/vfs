package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
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
