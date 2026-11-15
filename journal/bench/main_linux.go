// Command bench measures the journal against plain files on the same disk.
//
//	bench -what journal -writers 32 -size 4096     # Append+Sync per writer, group commit
//	bench -what plain   -writers 32 -size 4096     # pwrite+fdatasync per writer, no journal
//	bench -what journal -seq -total 8GiB           # 1 MiB sequential Append, then Sync
//	bench -what plain   -seq -total 8GiB -every 1MiB
//	bench -what strategy                           # how to make a write durable: the measurement behind the journal
//
// Every run prints one line: operations per second, throughput, latency
// percentiles, and for the journal the group-size histogram and the fsync
// calls counted inside the file layer per 1,000 Sync calls. The load average
// is printed with each line because the disk is shared.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hanzoai/vfs/journal"
)

var (
	dir     = flag.String("dir", "/var/tmp/journal-bench", "directory on the disk under test")
	what    = flag.String("what", "journal", "journal or plain")
	writers = flag.Int("writers", 1, "concurrent writers")
	size    = flag.Int("size", 4096, "bytes per operation")
	dur     = flag.Duration("dur", 5*time.Second, "how long a latency run lasts")
	seq     = flag.Bool("seq", false, "sequential throughput instead of per-operation latency")
	total   = flag.String("total", "8GiB", "bytes a sequential run writes")
	every   = flag.String("every", "1MiB", "plain sequential: bytes between fdatasync calls (0 = once at the end)")
	shared  = flag.Bool("shared", false, "plain latency: all writers share one file")
	drain   = flag.Float64("drain", 0, "journal: megabytes per second the publisher drains (0 = instantly)")
	capa    = flag.String("capacity", "1GiB", "journal capacity")
	segment = flag.String("segment", "64MiB", "journal segment size")
	label   = flag.String("label", "", "text added to the output line")
	cpuprof = flag.String("cpuprofile", "", "write a CPU profile")
	mutprof = flag.String("mutexprofile", "", "write a mutex profile")
	header  = flag.Bool("header", false, "print the column names first")
)

func main() {
	flag.Parse()
	if *cpuprof != "" {
		f, err := os.Create(*cpuprof)
		check(err)
		check(pprof.StartCPUProfile(f))
		defer pprof.StopCPUProfile()
	}
	if *mutprof != "" {
		runtime.SetMutexProfileFraction(5)
		defer func() {
			f, err := os.Create(*mutprof)
			check(err)
			check(pprof.Lookup("mutex").WriteTo(f, 0))
			f.Close()
		}()
	}
	work := filepath.Join(*dir, fmt.Sprintf("run-%d", os.Getpid()))
	check(os.MkdirAll(work, 0o755))
	defer os.RemoveAll(work)

	if *header {
		fmt.Println("what\twriters\tsize\tmode\tops/s\tMB/s\tp50\tp95\tp99\tmax\tfsyncs/1000\tgroups\tload\tnote")
	}
	switch {
	case *what == "journal" && !*seq:
		journalLatency(work)
	case *what == "journal":
		journalSequential(work)
	case *what == "plain" && !*seq:
		plainLatency(work)
	case *what == "plain":
		plainSequential(work)
	case *what == "strategy":
		strategy(work)
	default:
		log.Fatalf("unknown -what %q", *what)
	}
}

func check(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func parseSize(s string) int64 {
	var n int64
	var unit string
	if _, err := fmt.Sscanf(s, "%d%s", &n, &unit); err != nil {
		if _, err := fmt.Sscanf(s, "%d", &n); err != nil {
			log.Fatalf("bad size %q", s)
		}
	}
	switch strings.ToLower(unit) {
	case "kib":
		return n << 10
	case "mib":
		return n << 20
	case "gib":
		return n << 30
	}
	return n
}

func load() string {
	b, _ := os.ReadFile("/proc/loadavg")
	if len(b) >= 14 {
		return string(b[:14])
	}
	return ""
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	return sorted[int(float64(len(sorted)-1)*p)]
}

func report(mode string, ops int, bytes int64, elapsed time.Duration, lat []time.Duration, fsyncs, calls int64, groups string, note string) {
	sort.Slice(lat, func(a, b int) bool { return lat[a] < lat[b] })
	per := ""
	if calls > 0 {
		per = fmt.Sprintf("%.1f", float64(fsyncs)*1000/float64(calls))
	}
	fmt.Printf("%s\t%d\t%d\t%s\t%.0f\t%.1f\t%v\t%v\t%v\t%v\t%s\t%s\t%s\t%s %s\n",
		*what, *writers, *size, mode,
		float64(ops)/elapsed.Seconds(), float64(bytes)/elapsed.Seconds()/1e6,
		percentile(lat, .5), percentile(lat, .95), percentile(lat, .99), percentile(lat, 1),
		per, groups, load(), *label, note)
}

// counting wraps the operating system's files and counts fdatasync calls.
type counting struct {
	journal.Disk
	syncs atomic.Int64
}

type countedFile struct {
	journal.File
	d *counting
}

func (c *counting) Create(name string, n int64) (journal.File, error) {
	f, err := c.Disk.Create(name, n)
	return countedFile{f, c}, err
}

func (c *counting) Open(name string) (journal.File, error) {
	f, err := c.Disk.Open(name)
	return countedFile{f, c}, err
}

func (f countedFile) Sync() error {
	f.d.syncs.Add(1)
	return f.File.Sync()
}

func groupsOf(s journal.Stats) string {
	var parts []string
	for i, c := range s.Groups {
		if c > 0 {
			lo := 0
			if i > 0 {
				lo = 1 << (i - 1)
			}
			parts = append(parts, fmt.Sprintf("%d+:%d", lo, c))
		}
	}
	return strings.Join(parts, " ")
}

// open starts a journal with a publisher that drains it as fast as -drain
// allows and returns what stops it.
func open(work string) (*journal.Journal, *counting, func()) {
	files, err := journal.Files(work)
	check(err)
	disk := &counting{Disk: files}
	j, err := journal.Open(work, journal.Options{Capacity: parseSize(*capa), Segment: parseSize(*segment), Disk: disk})
	check(err)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { // the publisher: Due says when, Publish and Reclaim do it
		defer wg.Done()
		published := time.Now()
		var credit float64
		for {
			select {
			case <-j.Due(5 * time.Millisecond):
				m := j.Marks()
				upto := m.Visible
				if *drain > 0 {
					// pace the drain: at most -drain MB/s of records
					credit += time.Since(published).Seconds() * *drain * 1e6
					published = time.Now()
					per := float64(*size + 28)
					n := uint64(credit / per)
					if m.Published+n < upto {
						upto = m.Published + n
					}
					credit -= float64(upto-m.Published) * per
				}
				if upto > m.Published {
					check(j.Publish(upto))
					check(j.Reclaim())
				}
				if *drain > 0 && upto < m.Visible {
					time.Sleep(2 * time.Millisecond) // the store is the slow side
				}
			case <-stop:
				return
			}
		}
	}()
	return j, disk, func() { close(stop); wg.Wait(); check(j.Close()) }
}

// warm cycles the journal through every segment once, so the measured run
// finds recycled files and not files still being created.
func warm(j *journal.Journal) {
	chunk := make([]byte, 1<<20)
	var last uint64
	for written := int64(0); written < parseSize(*capa)*5/4; written += int64(len(chunk)) {
		for {
			seq, err := j.Append(context.Background(), chunk)
			if err == journal.ErrFull {
				runtime.Gosched()
				continue
			}
			check(err)
			last = seq
			break
		}
	}
	check(j.Sync(context.Background(), last))
}

func journalLatency(work string) {
	j, disk, stop := open(work)
	warm(j)
	buf := make([]byte, *size)
	deadline := time.Now().Add(*dur)
	before, syncs0 := j.Stats(), disk.syncs.Load()
	lats := make([][]time.Duration, *writers)
	var wg sync.WaitGroup
	start := time.Now()
	for w := 0; w < *writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			ctx := context.Background()
			for time.Now().Before(deadline) {
				t0 := time.Now()
				var seq uint64
				var err error
				for {
					if seq, err = j.Append(ctx, buf); err != journal.ErrFull {
						break
					}
					runtime.Gosched()
				}
				check(err)
				check(j.Sync(ctx, seq))
				lats[w] = append(lats[w], time.Since(t0))
			}
		}(w)
	}
	wg.Wait()
	elapsed := time.Since(start)
	after, syncs := j.Stats(), disk.syncs.Load()-syncs0
	var all []time.Duration
	for _, l := range lats {
		all = append(all, l...)
	}
	var g journal.Stats
	for i := range g.Groups {
		g.Groups[i] = after.Groups[i] - before.Groups[i]
	}
	report("append+sync", len(all), int64(len(all))*int64(*size), elapsed, all, syncs, int64(len(all)), groupsOf(g), "")
	stop()
}

func journalSequential(work string) {
	j, disk, stop := open(work)
	warm(j)
	ctx := context.Background()
	chunk := make([]byte, *size)
	if *size == 4096 {
		chunk = make([]byte, 1<<20)
	}
	bytes := parseSize(*total)
	n := int(bytes / int64(len(chunk)))
	syncs0 := disk.syncs.Load()
	var last atomic.Uint64
	var wg sync.WaitGroup
	start := time.Now()
	var lat []time.Duration
	var mu sync.Mutex
	for w := 0; w < *writers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var mine []time.Duration
			for i := 0; i < n / *writers; i++ {
				t0 := time.Now()
				for {
					seq, err := j.Append(ctx, chunk)
					if err == journal.ErrFull {
						runtime.Gosched()
						continue
					}
					check(err)
					for {
						old := last.Load()
						if seq <= old || last.CompareAndSwap(old, seq) {
							break
						}
					}
					break
				}
				mine = append(mine, time.Since(t0))
			}
			mu.Lock()
			lat = append(lat, mine...)
			mu.Unlock()
		}()
	}
	wg.Wait()
	appended := time.Since(start)
	check(j.Sync(ctx, last.Load()))
	durable := time.Since(start)
	written := int64(n / *writers * *writers * len(chunk))
	report("append", len(lat), written, appended, append([]time.Duration(nil), lat...), 0, 0, "", "")
	report("durable", len(lat), written, durable, lat, disk.syncs.Load()-syncs0, 0, groupsOf(j.Stats()), fmt.Sprintf("syncs=%d", disk.syncs.Load()-syncs0))
	stop()
}
