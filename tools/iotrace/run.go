package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// tools whose directories join the minimal PATH of every workflow.
var tools = []string{"sh", "tar", "cp", "rm", "mkdir", "sleep", "find", "sort", "head", "cat", "git", "go", "node", "npm",
	"cargo", "rustc", "uv", "rg", "cc", "gcc", "ld", "as", "ar"}

// run executes one workflow for the given number of runs. Before every
// run the fresh state is rebuilt and the page cache of the trees the
// previous run read is dropped, so each starts from the same cold disk.
// With plain pairs set, it then times the untraced command that many
// times, each cold and then warm, to bound what tracing costs and what
// the disk costs.
func run(name, label string, p Params, data string, runs, plain int, keep bool) error {
	mk, ok := workflows[name]
	if !ok {
		return fmt.Errorf("unknown workflow %q", name)
	}
	if p.State != "" {
		defer os.RemoveAll(p.State)
	}
	var prior []string
	var made []string
	for n := 1; n <= runs; n++ {
		pl, err := mk(p)
		if err != nil {
			return err
		}
		env := environment(pl.env, tools)
		if err := prepare(p, pl, env); err != nil {
			return err
		}
		if _, _, err := evict(drops(pl, prior)); err != nil {
			return err
		}
		out := filepath.Join(data, name, fmt.Sprintf("%s%d", label, n))
		m, err := record(Manifest{Workflow: name, Project: label, Run: fmt.Sprintf("%s%d", label, n),
			Argv: pl.run, Dir: pl.dir, Roots: pl.roots}, env, out, keep)
		if err != nil {
			return err
		}
		if m.Exit > pl.exit {
			return fmt.Errorf("%s run %d exited %d (kept %s)", name, n, m.Exit, out)
		}
		made = append(made, out)
		if prior, err = filesRead(filepath.Join(out, "events.tsv")); err != nil {
			return err
		}
	}
	if plain == 0 {
		return nil
	}
	pl, err := mk(p)
	if err != nil {
		return err
	}
	env := environment(pl.env, tools)
	var cold, warm []int64
	for i := 0; i < plain; i++ {
		if err := prepare(p, pl, env); err != nil {
			return err
		}
		if _, _, err := evict(drops(pl, prior)); err != nil {
			return err
		}
		c, err := timed(pl, env)
		if err != nil {
			return err
		}
		if err := prepare(p, pl, env); err != nil {
			return err
		}
		w, err := timed(pl, env)
		if err != nil {
			return err
		}
		cold, warm = append(cold, c), append(warm, w)
	}
	for _, out := range made {
		path := filepath.Join(out, "manifest.json")
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		var m Manifest
		if err := json.Unmarshal(b, &m); err != nil {
			return err
		}
		m.Plain, m.Warm, m.Colds, m.Warms = median(cold), median(warm), cold, warm
		if b, err = json.MarshalIndent(m, "", "  "); err != nil {
			return err
		}
		if err := os.WriteFile(path, b, 0o644); err != nil {
			return err
		}
	}
	return nil
}

func timed(pl plan, env []string) (int64, error) {
	start := time.Now()
	exit, err := execute(pl.dir, env, pl.run)
	if err == nil && exit > pl.exit {
		err = fmt.Errorf("%s exited %d", strings.Join(pl.run, " "), exit)
	}
	return time.Since(start).Nanoseconds(), err
}

// prepare builds the fresh state of a run.
func prepare(p Params, pl plan, env []string) error {
	if p.State != "" {
		if err := os.MkdirAll(p.State, 0o755); err != nil {
			return err
		}
	}
	for _, c := range pl.prep {
		if exit, err := execute(p.State, env, c); err != nil || exit != 0 {
			return fmt.Errorf("%s: exit %d: %v", strings.Join(c, " "), exit, err)
		}
	}
	return nil
}

// drops lists the trees to evict: those the plan names, and the files
// the previous run read.
func drops(pl plan, prior []string) []string {
	var roots []string
	for _, r := range append(append([]string{}, pl.evict...), prior...) {
		if r != "" {
			roots = append(roots, r)
		}
	}
	return roots
}

// filesRead lists the distinct files a recorded run read, mapped or
// executed.
func filesRead(events string) ([]string, error) {
	f, err := os.Open(events)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	_, _, ev, err := readEvents(bufio.NewReaderSize(f, 1<<20))
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	for _, e := range ev {
		if e.Op == opRead || e.Op == opMap || e.Op == opExec {
			seen[e.Path] = true
		}
	}
	out := make([]string, 0, len(seen))
	for p := range seen {
		out = append(out, p)
	}
	sort.Strings(out)
	return out, nil
}

// Calibration is the cost strace adds to a call that costs nothing.
type Calibration struct {
	Call int64 `json:"call_ns"` // median of cached 1-byte preads
	Read int64 `json:"read_ns"` // median of cached 128 KiB preads, overhead included
	N    int   `json:"samples"`
}

// probe is the workload calibrate traces: cached preads of two sizes.
func probe(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	buf := make([]byte, 128<<10)
	if _, err := io.ReadFull(f, buf); err != nil {
		return err
	}
	for i := 0; i < 20000; i++ {
		if _, err := f.ReadAt(buf[:1], 0); err != nil {
			return err
		}
	}
	for i := 0; i < 5000; i++ {
		if _, err := f.ReadAt(buf, 0); err != nil {
			return err
		}
	}
	return nil
}

func calibrate(dir string) (Calibration, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return Calibration{}, err
	}
	data := filepath.Join(dir, "probe.dat")
	if err := os.WriteFile(data, make([]byte, 1<<20), 0o644); err != nil {
		return Calibration{}, err
	}
	defer os.Remove(data)
	self, err := os.Executable()
	if err != nil {
		return Calibration{}, err
	}
	raw := filepath.Join(dir, "probe.strace")
	defer os.Remove(raw)
	if _, exit, stderr, err := trace(raw, dir, environment(nil, tools), []string{self, "probe", data}); err != nil || exit != 0 {
		return Calibration{}, fmt.Errorf("probe: exit %d: %v %s", exit, err, stderr)
	}
	events, _, err := normalise(func() (io.Reader, error) { return os.Open(raw) }, dir)
	if err != nil {
		return Calibration{}, err
	}
	var small, large []int64
	for _, e := range events {
		if e.Op != opRead || !strings.HasSuffix(e.Path, "probe.dat") {
			continue
		}
		switch e.Len {
		case 1:
			small = append(small, e.Dur)
		case 128 << 10:
			large = append(large, e.Dur)
		}
	}
	if len(small) < 1000 || len(large) < 1000 {
		return Calibration{}, fmt.Errorf("probe produced %d and %d reads", len(small), len(large))
	}
	return Calibration{Call: median(small), Read: median(large), N: len(small) + len(large)}, nil
}

func median(v []int64) int64 {
	s := append([]int64(nil), v...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	return s[len(s)/2]
}
