// Command gitbench measures git the way a coding run uses it: a shallow
// partial clone, a branch switch that touches thousands of files, status, add,
// commit, maintenance and a push, repeated, with the wall time, CPU and the
// system calls that matter to a disk that charges per call. It prints CSV and
// runs unchanged on any directory, so the same numbers can be taken on local
// NVMe, tmpfs and a mounted volume.
//
//	gitbench run     -src REPO -dir DIR [-configs a,b+c] [-reps 5]
//	gitbench probe   -dir DIR
//	gitbench net     -url URL [-reps 5]
//	gitbench snapshot -src REPO -dir DIR
//	gitbench summary < results.csv
package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
)

func main() {
	cmd, args := "run", os.Args[1:]
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		cmd, args = args[0], args[1:]
	}
	var err error
	switch cmd {
	case "run":
		err = measure(args, os.Stdout)
	case "probe":
		err = probe(args, os.Stdout)
	case "net":
		err = remote(args, os.Stdout)
	case "snapshot":
		err = duplicate(args, os.Stdout)
	case "summary":
		err = summarize(os.Stdin, os.Stdout)
	default:
		err = fmt.Errorf("unknown command %q (run, probe, net, snapshot, summary)", cmd)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "gitbench:", err)
		os.Exit(1)
	}
}

// header is every column of the CSV. A column that does not apply to a row is
// left empty, so rows from every command share one file.
var header = []string{
	"host", "fs", "git", "repo", "files", "config", "delay_us", "calls_us", "op", "rep",
	"wall_ms", "cpu_ms", "load1", "exit",
	"fsync", "fdatasync", "syncrange", "rename", "openat", "unlink", "stat", "getdents", "write", "syscalls",
	"git_kb", "tree_files", "req", "ttfb_ms", "proto", "note",
}

// sheet writes rows that share the columns every row needs.
type sheet struct {
	w    *csv.Writer
	host string
	fs   string
	git  string
	repo string
	tree int
}

func newsheet(out io.Writer, dir, repo string, tree int) (*sheet, error) {
	ver, err := exec.Command("git", "version").Output()
	if err != nil {
		return nil, fmt.Errorf("git: %w", err)
	}
	s := &sheet{
		w:    csv.NewWriter(out),
		host: fmt.Sprintf("%s/%s/%dcpu", runtime.GOOS, runtime.GOARCH, runtime.NumCPU()),
		fs:   fstype(dir),
		git:  strings.TrimPrefix(strings.TrimSpace(string(ver)), "git version "),
		repo: repo,
		tree: tree,
	}
	return s, s.w.Write(header)
}

// ms prints a duration in milliseconds with microsecond resolution.
func ms(d time.Duration) string { return strconv.FormatFloat(float64(d)/1e6, 'f', 4, 64) }

// put writes one row; cells are the columns from "config" on that the caller
// has, keyed by column name.
func (s *sheet) put(cells map[string]string) error {
	row := make([]string, len(header))
	base := map[string]string{
		"host": s.host, "fs": s.fs, "git": s.git, "repo": s.repo, "files": itoa(s.tree),
	}
	for i, h := range header {
		if v, ok := cells[h]; ok {
			row[i] = v
		} else {
			row[i] = base[h]
		}
	}
	if err := s.w.Write(row); err != nil {
		return err
	}
	s.w.Flush()
	return s.w.Error()
}

// cells turns a timed sample and its counted twin into one row.
func cells(k knob, delay, calls time.Duration, rep int, t sample, c *sample) map[string]string {
	m := map[string]string{
		"config": k.name, "delay_us": strconv.FormatInt(delay.Microseconds(), 10), "calls_us": strconv.FormatInt(calls.Microseconds(), 10),
		"op": t.op, "rep": itoa(rep),
		"wall_ms": ms(t.wall), "cpu_ms": ms(t.cpu),
		"load1": strconv.FormatFloat(t.load, 'f', 2, 64), "exit": itoa(t.exit),
		"note": t.note,
	}
	if t.kb > 0 {
		m["git_kb"], m["tree_files"] = strconv.FormatInt(t.kb, 10), itoa(t.tree)
	}
	if c != nil && c.counted {
		for h, v := range map[string]int{
			"fsync": c.c.fsync, "fdatasync": c.c.fdatasync, "syncrange": c.c.syncrange,
			"rename": c.c.rename, "openat": c.c.openat, "unlink": c.c.unlink,
			"stat": c.c.stat, "getdents": c.c.getdents, "write": c.c.write, "syscalls": c.c.total,
		} {
			m[h] = itoa(v)
		}
	}
	return m
}

// bench is a fixture, a sheet to write rows to, and how to take samples.
type bench struct {
	fx    *fixture
	sh    *sheet
	set   *settings
	root  string
	count bool // a second pass counts system calls
}

// flags are the options every measuring command shares.
type flags struct {
	src, dir, name string
	reps, counts   int
	list           string
	bulk           int
	trace          string
	delay, calls   time.Duration
	until          string
	churn          int
	keep           bool
}

func (f *flags) register(fl *flag.FlagSet) {
	fl.StringVar(&f.src, "src", "", "repository to copy; it is only read")
	fl.StringVar(&f.dir, "dir", "", "directory on the filesystem under test")
	fl.StringVar(&f.name, "name", "", "repository label in the CSV (default: base name of -src)")
	fl.IntVar(&f.reps, "reps", 5, "repetitions of the whole sequence per config")
	fl.IntVar(&f.counts, "counts", 0, "repetitions that also count system calls (default: all)")
	fl.StringVar(&f.list, "configs", "default", "comma list of configs; join catalog names with +; all is the catalog")
	fl.IntVar(&f.bulk, "bulk", 2500, "files the bulk branch changes")
	fl.StringVar(&f.trace, "strace", "auto", "count system calls in a second pass: auto, on, off")
	fl.DurationVar(&f.delay, "delay", 0, "latency added to every fsync and fdatasync (needs strace)")
	fl.DurationVar(&f.calls, "calls", 0, "latency added to every open, create, rename, unlink and directory listing (needs strace)")
	fl.StringVar(&f.until, "until", "push", "stop after this operation")
	fl.IntVar(&f.churn, "churn", 0, "files of a vendored directory to stage and commit (8000 starts gc)")
	fl.BoolVar(&f.keep, "keep", false, "keep the scratch directory")
}

// open builds the fixture and the sheet. The returned function removes the
// scratch directory unless -keep was given.
func (f *flags) open(out io.Writer) (*bench, func(), error) {
	if f.src == "" || f.dir == "" {
		return nil, nil, fmt.Errorf("-src and -dir are required")
	}
	_, lookup := exec.LookPath("strace")
	var count bool
	switch f.trace {
	case "auto":
		count = lookup == nil
	case "on":
		if lookup != nil {
			return nil, nil, fmt.Errorf("-strace=on but strace is not installed")
		}
		count = true
	case "off":
	default:
		return nil, nil, fmt.Errorf("-strace must be auto, on or off")
	}
	if (f.delay > 0 || f.calls > 0) && lookup != nil {
		return nil, nil, fmt.Errorf("-delay and -calls need strace")
	}
	abs, err := filepath.Abs(f.dir)
	if err != nil {
		return nil, nil, err
	}
	root := filepath.Join(abs, "gitbench-"+strconv.Itoa(os.Getpid()))
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, nil, err
	}
	done := func() {
		if !f.keep {
			os.RemoveAll(root)
		}
	}
	fx, err := build(f.src, root, f.bulk)
	if err != nil {
		done()
		return nil, nil, err
	}
	name := f.name
	if name == "" {
		name = filepath.Base(strings.TrimSuffix(f.src, "/"))
	}
	sh, err := newsheet(out, abs, name, len(fx.paths))
	if err != nil {
		done()
		return nil, nil, err
	}
	set := &settings{fx: fx, delay: f.delay, calls: f.calls, until: f.until, churn: f.churn}
	return &bench{fx: fx, sh: sh, set: set, root: root, count: count}, done, nil
}

// loop takes every config through session reps times, in a timed pass and,
// where system calls are counted, a second pass in a fresh directory, and
// writes one row per operation. Repetitions are the outer loop: the configs
// take turns, so a change in the machine's load between minutes lands on all of
// them alike instead of on whichever ran then.
func (b *bench) loop(knobs []knob, reps, counts int, session func(*world)) error {
	if counts == 0 {
		counts = reps
	}
	for rep := 1; rep <= reps; rep++ {
		for _, k := range knobs {
			fmt.Fprintf(os.Stderr, "gitbench: %s rep %d/%d load %.2f\n", k.name, rep, reps, load())
			timed := &world{set: b.set, knob: k, dir: filepath.Join(b.root, "t"+itoa(rep))}
			session(timed)
			os.RemoveAll(timed.dir)
			var counted *world
			if b.count && b.set.delay == 0 && b.set.calls == 0 && rep <= counts {
				plain := *b.set
				counted = &world{set: &plain, knob: k, count: true, dir: filepath.Join(b.root, "c"+itoa(rep))}
				session(counted)
				os.RemoveAll(counted.dir)
			}
			for i, t := range timed.got {
				var c *sample
				if counted != nil && i < len(counted.got) && counted.got[i].op == t.op {
					c = &counted.got[i]
				}
				if err := b.sh.put(cells(k, b.set.delay, b.set.calls, rep, t, c)); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// measure is the run command.
func measure(args []string, out io.Writer) error {
	var f flags
	fl := flag.NewFlagSet("run", flag.ContinueOnError)
	f.register(fl)
	if err := fl.Parse(args); err != nil {
		return err
	}
	knobs, err := names(f.list)
	if err != nil {
		return err
	}
	b, done, err := f.open(out)
	if err != nil {
		return err
	}
	defer done()
	return b.loop(knobs, f.reps, f.counts, func(w *world) { w.session() })
}
