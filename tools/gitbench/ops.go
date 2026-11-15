package main

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// settings is everything one measurement shares.
type settings struct {
	fx    *fixture
	delay time.Duration // latency added to every fsync and fdatasync; 0 for none
	calls time.Duration // latency added to every call that changes or lists a directory; 0 for none
	until string        // stop after this operation
	churn int           // files a vendored directory adds, for the bulk staging case; 0 for none
}

// sample is one operation's measurement.
type sample struct {
	op        string
	wall, cpu time.Duration
	exit      int
	c         counts
	counted   bool
	kb        int64
	tree      int
	load      float64
	note      string
}

// order is the sequence a coding run performs: get the repository, move to
// the branch it will work on, look, edit, stage, commit, ask what changed,
// keep the repository tidy, deliver.
var order = []string{
	"clone", "sparse", "fetch", "switch_bulk", "switch_base", "switch_new",
	"status_cold", "status_warm", "status_dirty", "add", "commit", "diff", "log",
	"churn_add", "churn_commit", "gc_auto", "push",
	"copy", "status_copy", "status_copy_warm",
}

// world is one working directory taken through the sequence once.
type world struct {
	set   *settings
	knob  knob
	count bool // this pass counts system calls instead of timing
	dir   string
	got   []sample
	dead  bool
}

// origin is the URL a run clones from and pushes to: the fixture's bare repository.
func (s *settings) origin() string { return "file://" + s.fx.origin }

// namespace is the calls a filesystem daemon answers one request each for:
// opening and creating, making and removing directories and links, renaming,
// listing. -calls puts a price on them.
const namespace = "openat,mkdirat,unlinkat,renameat,renameat2,linkat,symlinkat,getdents64"

// prefix is the tracer a pass runs a command under: strace -c to count calls,
// strace with injected latency to price fsync and namespace calls, nothing to
// just time it.
func (s *settings) prefix(count bool, table string) []string {
	switch {
	case count:
		return []string{"strace", "-f", "-c", "-o", table}
	case s.delay > 0 || s.calls > 0:
		line := []string{"strace", "-f", "-qq", "-o", "/dev/null", "--seccomp-bpf"}
		var traced []string
		if s.delay > 0 {
			us := strconv.FormatInt(s.delay.Microseconds(), 10)
			traced = append(traced, "fsync,fdatasync")
			line = append(line, "-e", "inject=fsync,fdatasync:delay_exit="+us+"us")
		}
		if s.calls > 0 {
			us := strconv.FormatInt(s.calls.Microseconds(), 10)
			traced = append(traced, namespace)
			line = append(line, "-e", "inject="+namespace+":delay_exit="+us+"us")
		}
		return append(line, "-e", "trace="+strings.Join(traced, ","))
	}
	return nil
}

// command builds one git invocation. The knob's configuration comes first so
// that a subcommand's own -c arguments cannot be shadowed by it.
func (s *settings) command(k knob, count bool, table string, argv []string) *exec.Cmd {
	line := append(s.prefix(count, table), "git")
	for _, c := range k.conf {
		line = append(line, "-c", c)
	}
	line = append(line, argv...)
	cmd := exec.Command(line[0], line[1:]...)
	cmd.Env = s.fx.sandbox(k.env...)
	return cmd
}

// tail keeps the end of what a command wrote to stderr, where a failure says why.
type tail struct{ b []byte }

func (t *tail) Write(p []byte) (int, error) {
	t.b = append(t.b, p...)
	if len(t.b) > 400 {
		t.b = t.b[len(t.b)-400:]
	}
	return len(p), nil
}

// do runs one git operation and records it. After a failure the rest of the
// sequence is skipped: later operations would measure a broken state.
func (w *world) do(op, stdin string, argv ...string) {
	w.exec(op, stdin, func(table string) *exec.Cmd { return w.set.command(w.knob, w.count, table, argv) })
}

// raw runs a command that is not git (a copy) under the same tracer.
func (w *world) raw(op string, argv ...string) {
	w.exec(op, "", func(table string) *exec.Cmd {
		line := append(w.set.prefix(w.count, table), argv...)
		cmd := exec.Command(line[0], line[1:]...)
		cmd.Env = w.set.fx.sandbox()
		cmd.Dir = w.set.fx.root
		return cmd
	})
}

// exec times a command, reads its system call table when this pass counts,
// and appends the sample.
func (w *world) exec(op, stdin string, build func(table string) *exec.Cmd) {
	if w.dead {
		return
	}
	table := ""
	if w.count {
		f, err := os.CreateTemp(w.set.fx.root, "strace")
		if err != nil {
			w.fail(op, err)
			return
		}
		table = f.Name()
		f.Close()
		defer os.Remove(table)
	}
	cmd := build(table)
	if cmd.Dir == "" {
		cmd.Dir = w.dir
	}
	if stdin != "" {
		cmd.Stdin = strings.NewReader(stdin)
	}
	var errs tail
	cmd.Stderr = &errs
	ld := load()
	start := time.Now()
	err := cmd.Run()
	s := sample{op: op, wall: time.Since(start), load: ld}
	if cmd.ProcessState != nil {
		s.cpu = cmd.ProcessState.UserTime() + cmd.ProcessState.SystemTime()
		s.exit = cmd.ProcessState.ExitCode()
	}
	if err != nil {
		s.exit, s.note = max(s.exit, 1), strings.Join(strings.Fields(string(errs.b)), " ")
		w.dead = true
	}
	if w.count && err == nil {
		raw, rerr := os.ReadFile(table)
		if rerr == nil {
			s.c, rerr = tally(string(raw))
		}
		if rerr != nil {
			s.note = rerr.Error()
		} else {
			s.counted = true
		}
	}
	w.got = append(w.got, s)
	if op == w.set.until {
		w.dead = true
	}
}

func (w *world) fail(op string, err error) {
	w.got = append(w.got, sample{op: op, exit: 1, note: err.Error()})
	w.dead = true
}

// session takes a fresh working directory through the whole sequence.
func (w *world) session() {
	fx := w.set.fx
	if err := os.MkdirAll(w.dir, 0o755); err != nil {
		w.fail("clone", err)
		return
	}
	clone := []string{"clone", "--quiet", "--depth", "1", "--single-branch"}
	filter := []string{"--filter=blob:none"}
	if w.knob.whole {
		filter = nil
	}
	clone = append(clone, filter...)
	if w.knob.sparse {
		clone = append(clone, "--sparse")
	}
	for _, a := range w.knob.clone {
		if strings.Contains(a, "{store}") {
			store, err := fx.cache()
			if err != nil {
				w.fail("clone", err)
				return
			}
			a = strings.ReplaceAll(a, "{store}", store)
		}
		clone = append(clone, a)
	}
	w.do("clone", "", append(clone, w.set.origin(), ".")...)
	if !w.count && len(w.got) == 1 && w.got[0].exit == 0 {
		w.size()
	}
	if w.knob.sparse {
		w.do("sparse", "", "sparse-checkout", "set", "--cone", fx.cone)
	}
	w.do("fetch", "", append(append([]string{"fetch", "--quiet", "--depth", "1"}, filter...), "origin", "+refs/heads/bulk:refs/remotes/origin/bulk")...)
	w.do("switch_bulk", "", "switch", "--quiet", "-c", "bulk", "refs/remotes/origin/bulk")
	w.do("switch_base", "", "switch", "--quiet", fx.base)
	w.do("switch_new", "", "switch", "--quiet", "-c", "agent/run")
	w.do("status_cold", "", "status", "--porcelain")
	w.do("status_warm", "", "status", "--porcelain")
	w.edit()
	w.do("status_dirty", "", "status", "--porcelain")
	w.do("add", "", "add", "-A")
	w.do("commit", "gitbench run\n", "-c", "user.name=gitbench", "-c", "user.email=gitbench@localhost", "commit", "--quiet", "-F", "-")
	w.do("diff", "", "diff", "--stat", fx.base, "HEAD")
	w.do("log", "", "log", "--reverse", "--format=%s", fx.base+"..HEAD")
	w.churn()
	w.do("gc_auto", "", "gc", "--auto", "--quiet")
	w.do("push", "", "push", "--quiet", w.set.origin(), "HEAD:refs/heads/agent/run")
	// The pushed ref goes back so the next repetition pushes to the same origin.
	if n := len(w.got); n > 0 && w.got[n-1].op == "push" {
		if err := fx.reset("refs/heads/agent/run"); err != nil {
			w.fail("reset", err)
		}
	}
}

// size attaches what the clone cost in storage to its sample: kilobytes under
// .git (what a shared object store saves) and files in the working tree.
func (w *world) size() {
	i := len(w.got) - 1
	kb, n := size(filepath.Join(w.dir, ".git"))
	_, all := size(w.dir)
	w.got[i].kb, w.got[i].tree = kb, all-n
}

// edit changes what an agent changes: twenty-five tracked files get a line and
// ten new files appear. It is not timed; what it leaves is what add, status
// and commit then work on.
func (w *world) edit() {
	if w.dead {
		return
	}
	out, err := w.set.fx.git(w.dir, nil, "ls-files", "-z")
	if err != nil {
		w.fail("edit", err)
		return
	}
	list := bytes.Split(bytes.TrimRight(out, "\x00"), []byte{0})
	stride := max(len(list)/100, 1)
	changed := 0
	for i := 0; i < len(list) && changed < 25; i += stride {
		p := filepath.Join(w.dir, string(list[i]))
		f, err := os.OpenFile(p, os.O_APPEND|os.O_WRONLY, 0)
		if err != nil {
			continue // not checked out (sparse) or not a regular file
		}
		_, err = f.WriteString("\ngitbench edit\n")
		f.Close()
		if err != nil {
			w.fail("edit", err)
			return
		}
		changed++
	}
	if changed == 0 {
		w.fail("edit", fmt.Errorf("no tracked file could be edited"))
		return
	}
	dir := filepath.Join(w.dir, "gitbench-new")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		w.fail("edit", err)
		return
	}
	body := bytes.Repeat([]byte("new file from gitbench\n"), 180)
	for i := range 10 {
		if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("%02d.txt", i)), body, 0o644); err != nil {
			w.fail("edit", err)
			return
		}
	}
}

// churn stages a directory of small files that a repository should have
// ignored (a vendored tree, a build output): the case where git writes one
// loose object per file, may start gc, and pays an fsync per object unless the
// settings say otherwise. Garbage collection runs in the foreground here so
// that its cost lands in the commit that started it.
func (w *world) churn() {
	n := w.set.churn
	if n == 0 || w.dead {
		return
	}
	dir := filepath.Join(w.dir, "gitbench-churn")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		w.fail("churn_add", err)
		return
	}
	for i := range n {
		body := fmt.Appendf(nil, "churn file %d\n%s", i, strings.Repeat("x", 1500))
		if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("%05d.txt", i)), body, 0o644); err != nil {
			w.fail("churn_add", err)
			return
		}
	}
	w.do("churn_add", "", "add", "-A")
	w.do("churn_commit", "gitbench churn\n", "-c", "gc.autoDetach=false", "-c", "maintenance.autoDetach=false",
		"-c", "user.name=gitbench", "-c", "user.email=gitbench@localhost", "commit", "--quiet", "-F", "-")
}
