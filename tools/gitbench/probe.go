package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)

// probe measures the calls git is made of on a directory, with no git in it:
// what a durable write, a create, a rename, a stat and an unlink cost. These
// rows say which price class a mount is in (microseconds for tmpfs, a
// millisecond for local flash, several for a volume that publishes) before any
// git number is read.
func probe(args []string, out io.Writer) error {
	fl := flag.NewFlagSet("probe", flag.ContinueOnError)
	dir := fl.String("dir", "", "directory on the filesystem under test")
	reps := fl.Int("reps", 5, "repetitions")
	n := fl.Int("n", 300, "operations per repetition")
	if err := fl.Parse(args); err != nil {
		return err
	}
	if *dir == "" {
		return fmt.Errorf("-dir is required")
	}
	abs, err := filepath.Abs(*dir)
	if err != nil {
		return err
	}
	root := filepath.Join(abs, "gitbench-probe-"+strconv.Itoa(os.Getpid()))
	if err := os.MkdirAll(root, 0o755); err != nil {
		return err
	}
	defer os.RemoveAll(root)
	sh, err := newsheet(out, abs, "probe", 0)
	if err != nil {
		return err
	}
	block := make([]byte, 4096)
	for rep := 1; rep <= *reps; rep++ {
		ld := load()
		var sets []series
		sets = append(sets, run("probe_fsync", *n, func(i int) (time.Duration, error) { return syncfile(root, "s", rep, i, block) }))
		sets = append(sets, run("probe_create", *n, func(i int) (time.Duration, error) { return makefile(root, "c", rep, i, block[:1024]) }))
		sets = append(sets, run("probe_rename", *n, func(i int) (time.Duration, error) { return rename(root, rep, i) }))
		sets = append(sets, run("probe_stat", *n, func(i int) (time.Duration, error) {
			t := time.Now()
			_, err := os.Stat(filepath.Join(root, fmt.Sprintf("c%d-%d", rep, i)))
			return time.Since(t), err
		}))
		sets = append(sets, run("probe_unlink", *n, func(i int) (time.Duration, error) {
			t := time.Now()
			err := os.Remove(filepath.Join(root, fmt.Sprintf("c%d-%d", rep, i)))
			return time.Since(t), err
		}))
		for _, s := range sets {
			if s.err != nil {
				return fmt.Errorf("%s: %w", s.op, s.err)
			}
			sort.Slice(s.lats, func(a, b int) bool { return s.lats[a] < s.lats[b] })
			p50, p99 := s.lats[len(s.lats)/2], s.lats[len(s.lats)*99/100]
			m := map[string]string{
				"config": "probe", "op": s.op, "rep": itoa(rep), "wall_ms": ms(p50),
				"load1": strconv.FormatFloat(ld, 'f', 2, 64), "exit": "0",
				"note": fmt.Sprintf("n=%d p50=%sms p99=%sms max=%sms", len(s.lats), ms(p50), ms(p99), ms(s.lats[len(s.lats)-1])),
			}
			if err := sh.put(m); err != nil {
				return err
			}
		}
	}
	return nil
}

// series is the time each of n repetitions of one call took.
type series struct {
	op   string
	lats []time.Duration
	err  error
}

// run times fn n times.
func run(op string, n int, fn func(i int) (time.Duration, error)) series {
	s := series{op: op, lats: make([]time.Duration, 0, n)}
	for i := range n {
		d, err := fn(i)
		if err != nil {
			s.err = err
			return s
		}
		s.lats = append(s.lats, d)
	}
	return s
}

// syncfile writes a block to a new file and returns the time fsync took.
func syncfile(root, tag string, rep, i int, block []byte) (time.Duration, error) {
	f, err := os.Create(filepath.Join(root, fmt.Sprintf("%s%d-%d", tag, rep, i)))
	if err != nil {
		return 0, err
	}
	defer f.Close()
	if _, err := f.Write(block); err != nil {
		return 0, err
	}
	t := time.Now()
	err = f.Sync()
	return time.Since(t), err
}

// makefile creates a small file, writes it and closes it: git's loose object.
func makefile(root, tag string, rep, i int, block []byte) (time.Duration, error) {
	t := time.Now()
	f, err := os.Create(filepath.Join(root, fmt.Sprintf("%s%d-%d", tag, rep, i)))
	if err != nil {
		return 0, err
	}
	if _, err := f.Write(block); err != nil {
		f.Close()
		return 0, err
	}
	err = f.Close()
	return time.Since(t), err
}

// rename moves a file to the name git's lock-file protocol commits it under.
func rename(root string, rep, i int) (time.Duration, error) {
	from := filepath.Join(root, fmt.Sprintf("r%d-%d.lock", rep, i))
	if err := os.WriteFile(from, []byte("x"), 0o644); err != nil {
		return 0, err
	}
	t := time.Now()
	err := os.Rename(from, filepath.Join(root, fmt.Sprintf("r%d-%d", rep, i)))
	return time.Since(t), err
}
