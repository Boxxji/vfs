package main

import (
	"bytes"
	"encoding/csv"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// source builds a small repository of count files spread over three
// directories and returns its path. It is the stand-in for a real repository.
func source(t *testing.T, count int) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "src")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	home := t.TempDir()
	conf := filepath.Join(home, "gitconfig")
	if err := os.WriteFile(conf, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	git := func(args ...string) {
		t.Helper()
		cmd := exec.Command("git", append([]string{"-c", "user.name=t", "-c", "user.email=t@t"}, args...)...)
		cmd.Dir = dir
		cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + home, "GIT_CONFIG_GLOBAL=" + conf, "GIT_CONFIG_NOSYSTEM=1"}
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, out)
		}
	}
	git("init", "--quiet", "-b", "main")
	for i := range count {
		d := filepath.Join(dir, []string{"alpha", "beta", "gamma"}[i%3])
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
		body := strings.Repeat("line of source "+strconv.Itoa(i)+"\n", 20)
		if err := os.WriteFile(filepath.Join(d, "f"+strconv.Itoa(i)+".txt"), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	git("add", "-A")
	git("commit", "--quiet", "-m", "tree")
	return dir
}

// built makes a fixture from a small source.
func built(t *testing.T, count, bulk int) *fixture {
	t.Helper()
	fx, err := build(source(t, count), t.TempDir(), bulk)
	if err != nil {
		t.Fatal(err)
	}
	return fx
}

const sample1 = `% time     seconds  usecs/call     calls    errors syscall
------ ----------- ----------- --------- --------- ----------------
 12.08    0.001145           9       116        52 openat
  7.63    0.000723           1       495        45 newfstatat
  2.85    0.000270         135         2           renameat
  1.90    0.000180           2        62           read
  0.98    0.000093          10         9         5 unlinkat
  0.37    0.000035           5         6           getdents64
  0.37    0.000035           3        10           write
  0.38    0.000036          12         3           fsync
  0.10    0.000010           3         2           fdatasync
  0.10    0.000010           3         4           sync_file_range
------ ----------- ----------- --------- --------- ----------------
100.00    0.009478           8      1074       124 total
`

func TestTally(t *testing.T) {
	c, err := tally(sample1)
	if err != nil {
		t.Fatal(err)
	}
	want := counts{fsync: 3, fdatasync: 2, syncrange: 4, rename: 2, openat: 116, unlink: 9, stat: 495, getdents: 6, write: 10, total: 1074}
	if c != want {
		t.Fatalf("got %+v want %+v", c, want)
	}
	if _, err := tally("no table here"); err == nil {
		t.Fatal("a summary with no total row must be an error")
	}
}

func TestFamilies(t *testing.T) {
	var c counts
	for _, n := range []string{"open", "openat", "openat2", "stat", "lstat", "fstat", "newfstatat", "statx", "getdents", "getdents64", "rename", "renameat2", "unlink", "unlinkat", "write", "pwrite64"} {
		p := c.family(n)
		if p == nil {
			t.Fatalf("%s has no counter", n)
		}
		*p++
	}
	if c.openat != 3 || c.stat != 5 || c.getdents != 2 || c.rename != 2 || c.unlink != 2 || c.write != 2 {
		t.Fatalf("variants of one call must share a counter: %+v", c)
	}
	if c.family("read") != nil || c.family("wait4") != nil {
		t.Fatal("calls that are not counted must not map to a counter")
	}
}

func TestMountType(t *testing.T) {
	// Where the kernel publishes a mount table the filesystem is named from it;
	// elsewhere the answer is "unknown" and nothing else breaks.
	if _, err := os.Stat("/proc/self/mountinfo"); err != nil {
		if got := fstype(t.TempDir()); got != "unknown" {
			t.Fatalf("without a mount table fstype = %q, want unknown", got)
		}
	} else {
		if got := fstype("/proc/self"); got != "proc" {
			t.Fatalf("fstype(/proc/self) = %q, want proc", got)
		}
		if got := fstype(t.TempDir()); got == "unknown" || got == "" {
			t.Fatalf("a temporary directory must be on a known filesystem, got %q", got)
		}
	}
	if got := unescape(`/mnt/with\040space`); got != "/mnt/with space" {
		t.Fatalf("unescape = %q", got)
	}
}

func TestPick(t *testing.T) {
	k, err := pick("fsyncnone+workers+gcoff+locks+whole+sparse")
	if err != nil {
		t.Fatal(err)
	}
	conf := strings.Join(k.conf, " ")
	for _, want := range []string{"core.fsync=none", "checkout.workers=0", "gc.auto=0", "maintenance.auto=false"} {
		if !strings.Contains(conf, want) {
			t.Errorf("config lacks %s: %s", want, conf)
		}
	}
	if len(k.env) != 1 || k.env[0] != "GIT_OPTIONAL_LOCKS=0" || !k.whole || !k.sparse {
		t.Errorf("env, whole or sparse lost in the merge: %+v", k)
	}
	if _, err := pick("fsyncnone+nonesuch"); err == nil {
		t.Fatal("an unknown knob must be an error")
	}
	if _, err := pick(""); err == nil {
		t.Fatal("an empty name must be an error")
	}
	all, err := names("all")
	if err != nil || len(all) != len(catalog) {
		t.Fatalf("all = %d knobs (err %v), want %d", len(all), err, len(catalog))
	}
}

// Every catalog entry is a single change: its settings are what its name says,
// and each one is accepted by git.
func TestCatalogIsAcceptedByGit(t *testing.T) {
	fx := built(t, 6, 3)
	set := &settings{fx: fx}
	for _, k := range catalog {
		for _, kv := range k.conf {
			key, val, _ := strings.Cut(kv, "=")
			cmd := set.command(k, false, "", []string{"config", "--get", key})
			out, err := cmd.Output()
			if err != nil {
				t.Errorf("%s: git config --get %s: %v", k.name, key, err)
				continue
			}
			if got := strings.TrimSpace(string(out)); got != val {
				t.Errorf("%s: -c %s reached git as %q", k.name, kv, got)
			}
		}
	}
}

func TestSandboxHidesTheUsersConfiguration(t *testing.T) {
	user := filepath.Join(t.TempDir(), "gitconfig")
	if err := os.WriteFile(user, []byte("[pack]\n\tthreads = 99\n[url \"ssh://elsewhere/\"]\n\tinsteadOf = file://\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GIT_CONFIG_GLOBAL", user)
	t.Setenv("HOME", filepath.Dir(user))
	fx := built(t, 6, 3)
	if out, err := fx.git(fx.origin, nil, "config", "--get", "pack.threads"); err == nil {
		t.Fatalf("the user's pack.threads reached the sandbox: %s", out)
	}
	if out, err := fx.git(fx.origin, nil, "config", "--get", "safe.directory"); err != nil || strings.TrimSpace(string(out)) != "*" {
		t.Fatalf("the sandbox must trust the leased directory: %q %v", out, err)
	}
}

func TestStraceCommandLine(t *testing.T) {
	fx := built(t, 6, 3)
	s := &settings{fx: fx, delay: 8800 * time.Microsecond}
	k, _ := pick("fsyncnone")

	plain := strings.Join((&settings{fx: fx}).command(k, false, "", []string{"status"}).Args, " ")
	if plain != "git -c core.fsync=none status" {
		t.Errorf("plain pass = %q", plain)
	}
	count := strings.Join(s.command(k, true, "/x/table", []string{"status"}).Args, " ")
	if !strings.HasPrefix(count, "strace -f -c -o /x/table git -c core.fsync=none status") || strings.Contains(count, "inject") {
		t.Errorf("a counting pass must count and never delay: %q", count)
	}
	delayed := strings.Join(s.command(k, false, "", []string{"status"}).Args, " ")
	for _, want := range []string{"--seccomp-bpf", "trace=fsync,fdatasync", "inject=fsync,fdatasync:delay_exit=8800us", "git -c core.fsync=none status"} {
		if !strings.Contains(delayed, want) {
			t.Errorf("a delayed pass lacks %q: %q", want, delayed)
		}
	}
	both := &settings{fx: fx, delay: 4 * time.Millisecond, calls: 100 * time.Microsecond}
	priced := strings.Join(both.command(k, false, "", []string{"status"}).Args, " ")
	for _, want := range []string{"inject=fsync,fdatasync:delay_exit=4000us", "inject=" + namespace + ":delay_exit=100us", "trace=fsync,fdatasync," + namespace} {
		if !strings.Contains(priced, want) {
			t.Errorf("a pass pricing both lacks %q: %q", want, priced)
		}
	}
	only := &settings{fx: fx, calls: 50 * time.Microsecond}
	if line := strings.Join(only.command(k, false, "", []string{"status"}).Args, " "); strings.Contains(line, "fsync,fdatasync") {
		t.Errorf("pricing namespace calls alone must not touch fsync: %q", line)
	}
}

func TestBulkBranchChangesExactlyThatManyFiles(t *testing.T) {
	fx := built(t, 60, 20)
	out, err := fx.git(fx.origin, nil, "diff", "--name-status", fx.base, "bulk")
	if err != nil {
		t.Fatal(err)
	}
	kinds := map[byte]int{}
	for _, l := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		kinds[l[0]]++
	}
	if kinds['M'] != 16 || kinds['D'] != 2 || kinds['A'] != 2 {
		t.Fatalf("bulk 20 must rewrite 16, delete 2, add 2; got %v", kinds)
	}
	if fx.base != "main" || len(fx.paths) != 60 {
		t.Fatalf("base %q with %d regular files", fx.base, len(fx.paths))
	}
	if fx.cone != "alpha" && fx.cone != "beta" && fx.cone != "gamma" {
		t.Fatalf("cone %q is not a top-level directory", fx.cone)
	}
	if _, err := build(source(t, 5), t.TempDir(), 50); err == nil {
		t.Fatal("a bulk branch larger than the tree must be refused")
	}
}

func TestOriginServesPartialClones(t *testing.T) {
	fx := built(t, 10, 4)
	for _, key := range []string{"uploadpack.allowFilter", "uploadpack.allowAnySHA1InWant"} {
		out, err := fx.git(fx.origin, nil, "config", "--get", key)
		if err != nil || strings.TrimSpace(string(out)) != "true" {
			t.Errorf("%s = %q (%v); the origin must serve filters and by-id wants", key, out, err)
		}
	}
}

// The sparse cone is the top-level directory nearest three percent of the tree.
func TestConeIsNearestThreePercent(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "src")
	sizes := map[string]int{"big": 90, "mid": 20, "tiny": 4, "near": 3, ".hidden": 3}
	home := t.TempDir()
	conf := filepath.Join(home, "gitconfig")
	os.WriteFile(conf, nil, 0o644)
	run := func(args ...string) {
		cmd := exec.Command("git", append([]string{"-c", "user.name=t", "-c", "user.email=t@t"}, args...)...)
		cmd.Dir = dir
		cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + home, "GIT_CONFIG_GLOBAL=" + conf, "GIT_CONFIG_NOSYSTEM=1"}
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v %s", args, err, out)
		}
	}
	os.MkdirAll(dir, 0o755)
	run("init", "--quiet", "-b", "main")
	for d, n := range sizes {
		os.MkdirAll(filepath.Join(dir, d), 0o755)
		for i := range n {
			os.WriteFile(filepath.Join(dir, d, "f"+strconv.Itoa(i)), []byte("x"), 0o644)
		}
	}
	run("add", "-A")
	run("commit", "--quiet", "-m", "tree")
	fx, err := build(dir, t.TempDir(), 10)
	if err != nil {
		t.Fatal(err)
	}
	// 120 files: three percent is 3.6, so "near" (3) wins over "tiny" (4) and the hidden directory is never chosen.
	if fx.cone != "near" {
		t.Fatalf("cone = %q, want near", fx.cone)
	}
}

func TestPlainPaths(t *testing.T) {
	for p, want := range map[string]bool{"a/b.txt": true, "a b": false, "a\"b": false, "a\\b": false, "": false, "/abs": false, "é": false, "x\ty": false} {
		if plain(p) != want {
			t.Errorf("plain(%q) = %v", p, !want)
		}
	}
}

// The sequence runs every operation to the end, in order, and the push reaches
// the origin it names: a post-receive hook in the origin leaves a mark.
func TestSessionDeliversThePush(t *testing.T) {
	fx := built(t, 40, 12)
	mark := filepath.Join(t.TempDir(), "received")
	hook := filepath.Join(fx.origin, "hooks", "post-receive")
	if err := os.MkdirAll(filepath.Dir(hook), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(hook, []byte("#!/bin/sh\ncat >> "+mark+"\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	k, _ := pick("default")
	w := &world{set: &settings{fx: fx, until: "push"}, knob: k, dir: filepath.Join(t.TempDir(), "w")}
	w.session()
	var got []string
	for _, s := range w.got {
		if s.exit != 0 {
			t.Fatalf("%s failed: %s", s.op, s.note)
		}
		got = append(got, s.op)
	}
	want := []string{"clone", "fetch", "switch_bulk", "switch_base", "switch_new", "status_cold", "status_warm", "status_dirty", "add", "commit", "diff", "log", "gc_auto", "push"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("operations\n got %v\nwant %v", got, want)
	}
	pushed, err := os.ReadFile(mark)
	if err != nil || !strings.Contains(string(pushed), "refs/heads/agent/run") {
		t.Fatalf("the origin never received refs/heads/agent/run: %q %v", pushed, err)
	}
	if out, _ := fx.git(fx.origin, nil, "rev-parse", "--verify", "-q", "refs/heads/agent/run"); len(out) != 0 {
		t.Fatalf("the pushed ref must be removed so the next repetition starts equal: %s", out)
	}
	if w.got[0].kb == 0 || w.got[0].tree != 40 {
		t.Fatalf("the clone must record its storage: %d KB, %d files", w.got[0].kb, w.got[0].tree)
	}
	// Twenty-five tracked files were edited and ten created: the commit holds 35 paths.
	stat, err := fx.git(fx.origin, nil, "diff", "--name-only", fx.base, "bulk")
	if err != nil || len(strings.Fields(string(stat))) != 12 {
		t.Fatalf("origin's bulk branch changed: %q %v", stat, err)
	}
}

// A failed operation ends the sequence: nothing after it measures a real state.
func TestFailureStopsTheSequence(t *testing.T) {
	fx := built(t, 20, 6)
	k, _ := pick("default")
	if err := os.RemoveAll(fx.origin); err != nil {
		t.Fatal(err)
	}
	set := &settings{fx: fx, until: "push"}
	w := &world{set: set, knob: k, dir: filepath.Join(t.TempDir(), "w")}
	w.session()
	if len(w.got) != 1 || w.got[0].op != "clone" || w.got[0].exit == 0 || w.got[0].note == "" {
		t.Fatalf("a failed clone must be the only sample and say why: %+v", w.got)
	}
}

func TestUntilStopsAfterThatOperation(t *testing.T) {
	fx := built(t, 20, 6)
	k, _ := pick("default")
	w := &world{set: &settings{fx: fx, until: "switch_bulk"}, knob: k, dir: filepath.Join(t.TempDir(), "w")}
	w.session()
	last := w.got[len(w.got)-1]
	if last.op != "switch_bulk" || last.exit != 0 {
		t.Fatalf("stopped at %s", last.op)
	}
}

func TestSparseNarrowsTheCheckout(t *testing.T) {
	fx := built(t, 60, 12)
	k, _ := pick("sparse")
	w := &world{set: &settings{fx: fx, until: "sparse"}, knob: k, dir: filepath.Join(t.TempDir(), "w")}
	w.session()
	if last := w.got[len(w.got)-1]; last.op != "sparse" || last.exit != 0 {
		t.Fatalf("sparse did not run: %+v", w.got)
	}
	var files int
	filepath.WalkDir(w.dir, func(p string, d os.DirEntry, err error) error {
		if err == nil && d.Type().IsRegular() && !strings.Contains(p, "/.git/") {
			files++
		}
		return nil
	})
	if files != 20 {
		t.Fatalf("a cone of %s holds 20 of 60 files, checkout has %d", fx.cone, files)
	}
}

func TestWholeSkipsThePartialFilter(t *testing.T) {
	fx := built(t, 20, 6)
	for name, wantPromisor := range map[string]bool{"default": true, "whole": false} {
		k, _ := pick(name)
		w := &world{set: &settings{fx: fx, until: "clone"}, knob: k, dir: filepath.Join(t.TempDir(), "w")}
		w.session()
		out, err := exec.Command("git", "-C", w.dir, "config", "--get", "remote.origin.promisor").Output()
		got := err == nil && strings.TrimSpace(string(out)) == "true"
		if got != wantPromisor {
			t.Errorf("%s: promisor remote = %v, want %v", name, got, wantPromisor)
		}
	}
}

func TestEditOfAnEmptyTreeFails(t *testing.T) {
	fx := built(t, 10, 4)
	dir := filepath.Join(t.TempDir(), "w")
	if _, err := fx.git(fx.root, nil, "init", "--quiet", dir); err != nil {
		t.Fatal(err)
	}
	k, _ := pick("default")
	w := &world{set: &settings{fx: fx}, knob: k, dir: dir}
	w.edit()
	if !w.dead || len(w.got) != 1 || w.got[0].op != "edit" || w.got[0].exit == 0 {
		t.Fatalf("an edit with no tracked file must fail the sequence: %+v", w.got)
	}
}

// Garbage collection started by the churn commit runs in the foreground, so
// the commit's time includes it and nothing is left running behind it.
func TestChurnCommitRunsGarbageCollectionInTheForeground(t *testing.T) {
	fx := built(t, 20, 6)
	k, _ := pick("default")
	k.conf = append(k.conf, "gc.auto=1", "gc.autoPackLimit=0")
	w := &world{set: &settings{fx: fx, until: "churn_commit", churn: 600}, knob: k, dir: filepath.Join(t.TempDir(), "w")}
	w.session()
	if last := w.got[len(w.got)-1]; last.op != "churn_commit" || last.exit != 0 {
		t.Fatalf("churn did not commit: %+v", last)
	}
	out, err := fx.git(w.dir, nil, "count-objects", "-v")
	if err != nil {
		t.Fatal(err)
	}
	for _, l := range strings.Split(string(out), "\n") {
		if strings.HasPrefix(l, "count: ") && l != "count: 0" {
			t.Fatalf("loose objects remain after the churn commit returned (gc was detached): %s", l)
		}
	}
}

func TestChurnStagesTheVendoredFiles(t *testing.T) {
	fx := built(t, 20, 6)
	k, _ := pick("default")
	w := &world{set: &settings{fx: fx, until: "churn_commit", churn: 50}, knob: k, dir: filepath.Join(t.TempDir(), "w")}
	w.session()
	last := w.got[len(w.got)-1]
	if last.op != "churn_commit" || last.exit != 0 {
		t.Fatalf("churn did not commit: %+v", last)
	}
	out, err := exec.Command("git", "-C", w.dir, "ls-tree", "-r", "--name-only", "HEAD", "gitbench-churn").Output()
	if err != nil || len(strings.Fields(string(out))) != 50 {
		t.Fatalf("committed churn files: %q %v", out, err)
	}
}

// With strace present, a counted pass sees fsyncs where the setting asks for
// them and none where it forbids them; without strace there is nothing to
// count and the pass says so.
func TestCountedPassSeesFsync(t *testing.T) {
	if _, err := exec.LookPath("strace"); err != nil {
		fx := built(t, 10, 4)
		k, _ := pick("default")
		w := &world{set: &settings{fx: fx, until: "clone"}, knob: k, count: true, dir: filepath.Join(t.TempDir(), "w")}
		w.session()
		if w.got[0].exit == 0 || w.got[0].counted {
			t.Fatalf("a counting pass with no strace must fail loudly: %+v", w.got[0])
		}
		return
	}
	total := func(name string) (n int) {
		fx := built(t, 20, 6)
		k, _ := pick(name)
		w := &world{set: &settings{fx: fx, until: "commit"}, knob: k, count: true, dir: filepath.Join(t.TempDir(), "w")}
		w.session()
		for _, s := range w.got {
			if s.exit != 0 || !s.counted {
				t.Fatalf("%s %s: exit %d counted %v %s", name, s.op, s.exit, s.counted, s.note)
			}
			n += s.c.fsync + s.c.fdatasync
		}
		return n
	}
	all, none := total("fsyncall"), total("fsyncnone")
	if none != 0 {
		t.Errorf("core.fsync=none still fsynced %d times", none)
	}
	if all <= 10 {
		t.Errorf("core.fsync=all fsynced only %d times", all)
	}
}

// table reads a CSV the commands wrote into one map per row, keyed by column.
func table(t *testing.T, raw []byte) []map[string]string {
	t.Helper()
	recs, err := csv.NewReader(bytes.NewReader(raw)).ReadAll()
	if err != nil || len(recs) == 0 {
		t.Fatalf("csv: %v (%d records)", err, len(recs))
	}
	var out []map[string]string
	for _, r := range recs[1:] {
		m := map[string]string{}
		for i, h := range recs[0] {
			m[h] = r[i]
		}
		out = append(out, m)
	}
	return out
}

func TestRowsShareOneHeader(t *testing.T) {
	var buf bytes.Buffer
	sh, err := newsheet(&buf, t.TempDir(), "repo", 7)
	if err != nil {
		t.Fatal(err)
	}
	k, _ := pick("batch")
	c := sample{counted: true, c: counts{fsync: 4, total: 99}}
	if err := sh.put(cells(k, 0, 0, 2, sample{op: "commit", wall: 1500 * time.Microsecond, load: 1.25}, &c)); err != nil {
		t.Fatal(err)
	}
	recs, err := csv.NewReader(&buf).ReadAll()
	if err != nil || len(recs) != 2 {
		t.Fatalf("records %v err %v", recs, err)
	}
	row := map[string]string{}
	for i, h := range recs[0] {
		row[h] = recs[1][i]
	}
	for k, v := range map[string]string{"repo": "repo", "files": "7", "config": "batch", "op": "commit", "rep": "2", "wall_ms": "1.5000", "load1": "1.25", "fsync": "4", "syscalls": "99", "exit": "0"} {
		if row[k] != v {
			t.Errorf("%s = %q, want %q", k, row[k], v)
		}
	}
	if len(recs[0]) != len(header) || row["fs"] == "" || row["git"] == "" {
		t.Errorf("header or environment columns missing: %v", recs[0])
	}
}

func TestMeasureWritesOneRowPerOperation(t *testing.T) {
	src := source(t, 30)
	var buf bytes.Buffer
	args := []string{"-src", src, "-dir", t.TempDir(), "-reps", "2", "-bulk", "9", "-configs", "default,fsyncnone", "-strace", "off", "-until", "diff"}
	if err := measure(args, &buf); err != nil {
		t.Fatal(err)
	}
	rows := table(t, buf.Bytes())
	ops := 11 // clone through diff
	if len(rows) != 2*2*ops {
		t.Fatalf("rows = %d, want 2 configs * 2 reps * %d ops", len(rows), ops)
	}
	for _, r := range rows {
		if r["note"] != "" || r["exit"] != "0" {
			t.Errorf("row failed: %v", r)
		}
		if r["fsync"] != "" {
			t.Errorf("-strace off must leave the counts empty: %v", r)
		}
	}
}

func TestMeasureRefusesBadFlags(t *testing.T) {
	for name, args := range map[string][]string{
		"no source":    {"-dir", t.TempDir()},
		"no dir":       {"-src", "x"},
		"bad config":   {"-src", "x", "-dir", t.TempDir(), "-configs", "nonesuch"},
		"bad strace":   {"-src", "x", "-dir", t.TempDir(), "-strace", "maybe"},
		"bad src path": {"-src", filepath.Join(t.TempDir(), "absent"), "-dir", t.TempDir()},
	} {
		if err := measure(args, &bytes.Buffer{}); err == nil {
			t.Errorf("%s: want an error", name)
		}
	}
}

func TestProbeReportsEveryCall(t *testing.T) {
	var buf bytes.Buffer
	if err := probe([]string{"-dir", t.TempDir(), "-reps", "2", "-n", "20"}, &buf); err != nil {
		t.Fatal(err)
	}
	rows := table(t, buf.Bytes())
	if len(rows) != 2*5 {
		t.Fatalf("rows %d", len(rows))
	}
	seen := map[string]bool{}
	for _, r := range rows {
		seen[r["op"]] = true
		if !strings.Contains(r["note"], "p99=") {
			t.Errorf("no percentiles in %v", r)
		}
	}
	for _, op := range []string{"probe_fsync", "probe_create", "probe_rename", "probe_stat", "probe_unlink"} {
		if !seen[op] {
			t.Errorf("no row for %s", op)
		}
	}
	if err := probe(nil, &buf); err == nil {
		t.Fatal("-dir is required")
	}
}

func TestSummaryTakesTheMedian(t *testing.T) {
	var in bytes.Buffer
	w := csv.NewWriter(&in)
	w.Write(header)
	for rep, wall := range []string{"10", "30", "20", "1000"} {
		row := make([]string, len(header))
		for i, h := range header {
			switch h {
			case "fs":
				row[i] = "ext4"
			case "repo":
				row[i] = "r"
			case "config":
				row[i] = "c"
			case "op":
				row[i] = "commit"
			case "wall_ms":
				row[i] = wall
			case "fsync":
				row[i] = strconv.Itoa(rep + 1)
			case "exit":
				row[i] = "0"
			}
		}
		w.Write(row)
	}
	failed := make([]string, len(header))
	for i, h := range header {
		switch h {
		case "fs":
			failed[i] = "ext4"
		case "repo":
			failed[i] = "r"
		case "config":
			failed[i] = "c"
		case "op":
			failed[i] = "commit"
		case "wall_ms":
			failed[i] = "99999"
		case "exit":
			failed[i] = "1"
		}
	}
	w.Write(failed)
	w.Flush()
	var out bytes.Buffer
	if err := summarize(&in, &out); err != nil {
		t.Fatal(err)
	}
	recs, _ := csv.NewReader(&out).ReadAll()
	if len(recs) != 2 {
		t.Fatalf("one group expected, got %v", recs)
	}
	row := map[string]string{}
	for i, h := range recs[0] {
		row[h] = recs[1][i]
	}
	if row["n"] != "4" || row["wall_med_ms"] != "25.000" || row["wall_min_ms"] != "10.000" || row["fsync"] != "2.500" {
		t.Fatalf("summary %v (failed rows must be left out; median of 10,20,30,1000 is 25)", row)
	}
	if err := summarize(strings.NewReader(""), &out); err == nil {
		t.Fatal("empty input must be an error")
	}
	if err := summarize(strings.NewReader("a,b\n1,2\n"), &out); err == nil {
		t.Fatal("a file without the header must be an error")
	}
}

func TestOrderRanksTheSequence(t *testing.T) {
	if !(rank("clone") < rank("push") && rank("status_cold") < rank("add") && rank("unlisted") == len(order)) {
		t.Fatal("rank must follow the sequence and put unknown operations last")
	}
}

// The snapshot command copies a finished checkout and times git's first look at
// the copy; the copy must be the checkout, clean.
func TestSnapshotCopiesACleanCheckout(t *testing.T) {
	src := source(t, 30)
	var buf bytes.Buffer
	if err := duplicate([]string{"-src", src, "-dir", t.TempDir(), "-reps", "1", "-strace", "off"}, &buf); err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, r := range table(t, buf.Bytes()) {
		if r["exit"] != "0" {
			t.Errorf("failed row: %v", r)
		}
		got = append(got, r["config"]+"/"+r["op"])
	}
	want := "default/copy,default/status_copy,default/status_copy_warm,checkstat/copy,checkstat/status_copy,checkstat/status_copy_warm"
	if strings.Join(got, ",") != want {
		t.Fatalf("rows\n got %v\nwant %s", got, want)
	}
}

// A copy that is not clean is a failure, not a slow status.
func TestCheckRefusesADirtyCopy(t *testing.T) {
	fx := built(t, 10, 4)
	dir := filepath.Join(t.TempDir(), "w")
	if _, err := fx.git(fx.root, nil, "clone", "--quiet", "file://"+fx.origin, dir); err != nil {
		t.Fatal(err)
	}
	k, _ := pick("default")
	w := &world{set: &settings{fx: fx}, knob: k, dir: dir}
	w.check()
	if w.dead {
		t.Fatalf("a clean clone must pass: %+v", w.got)
	}
	if err := os.WriteFile(filepath.Join(dir, "stray.txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	w.check()
	if !w.dead || len(w.got) != 1 || w.got[0].op != "check" {
		t.Fatalf("a dirty copy must fail the sequence: %+v", w.got)
	}
}

// Files written before the calls_us column existed still summarise.
func TestSummaryReadsTheOlderHeader(t *testing.T) {
	var old []string
	for _, h := range header {
		if h != "calls_us" {
			old = append(old, h)
		}
	}
	row := make([]string, len(old))
	for i, h := range old {
		switch h {
		case "op":
			row[i] = "clone"
		case "wall_ms":
			row[i] = "5"
		case "exit":
			row[i] = "0"
		}
	}
	var in bytes.Buffer
	w := csv.NewWriter(&in)
	w.Write(old)
	w.Write(row)
	w.Flush()
	var out bytes.Buffer
	if err := summarize(&in, &out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "clone,1,5.000,5.000") {
		t.Fatalf("summary of the older header: %q", out.String())
	}
}

func TestRenameProbeMovesTheFile(t *testing.T) {
	dir := t.TempDir()
	if _, err := rename(dir, 1, 7); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "r1-7.lock")); err == nil {
		t.Fatal("the lock file must be gone after the rename")
	}
	if _, err := os.Stat(filepath.Join(dir, "r1-7")); err != nil {
		t.Fatalf("the renamed file must exist: %v", err)
	}
}
