package main

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// needTrace skips the test when this machine cannot trace a program and
// read the names of the files it opens: strace missing, ptrace refused, a
// strace without the flags record uses, or one that cannot read the
// program's memory.
func needTrace(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("strace"); err != nil {
		t.Skip("strace is not installed")
	}
	dir := t.TempDir()
	probe := filepath.Join(dir, "probe")
	if err := os.WriteFile(probe, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(dir, "out")
	// the flags record uses
	if b, err := exec.Command("strace", "--seccomp-bpf", "-f", "-y", "-s", "0", "-e", "trace=openat", "-o", out, "cat", probe).CombinedOutput(); err != nil {
		t.Skipf("strace cannot trace here (%v): %s", err, b)
	}
	if b, err := os.ReadFile(out); err != nil || !strings.Contains(string(b), probe) {
		t.Skipf("strace here does not show the names of the files a program opens (%v): %s", err, b)
	}
}

func TestRecordCountsEveryByteTheCommandReads(t *testing.T) {
	needTrace(t)
	dir := t.TempDir()
	file := filepath.Join(dir, "data")
	const size = 300000
	if err := os.WriteFile(file, make([]byte, size), 0o644); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(dir, "rec")
	m, err := record(Manifest{Workflow: "w", Run: "r1", Project: "p", Argv: []string{"cat", file}, Dir: dir}, os.Environ(), out, false)
	if err != nil {
		t.Fatal(err)
	}
	if m.Exit != 0 || m.Wall <= 0 || m.Events == 0 {
		t.Errorf("manifest %+v", m)
	}
	if _, err := os.Stat("/proc/loadavg"); err == nil && m.Load[0] == "" {
		t.Errorf("the load average was not recorded: %+v", m.Load)
	}
	if _, err := os.Stat(filepath.Join(out, "raw.strace")); err == nil {
		t.Error("raw strace output kept without -keep")
	}
	f, err := os.Open(filepath.Join(out, "events.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	wf, run, events, err := readEvents(f)
	if err != nil {
		t.Fatal(err)
	}
	if wf != "w" || run != "r1" {
		t.Errorf("identity %q %q", wf, run)
	}
	var got int64
	var last int64 = -1
	for _, e := range events {
		if e.Op == opRead && e.Path == file {
			if e.Off != last+1 && last >= 0 {
				t.Errorf("read at %d does not follow %d", e.Off, last)
			}
			last = e.Off + e.Len - 1
			got += e.Len
		}
	}
	if got != size {
		t.Errorf("read %d bytes of %d", got, size)
	}
	sf, err := os.Open(filepath.Join(out, "sizes.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	defer sf.Close()
	sizes, err := readSizes(sf)
	if err != nil {
		t.Fatal(err)
	}
	if sizes[file] != size {
		t.Errorf("size %d", sizes[file])
	}
	b, err := os.ReadFile(filepath.Join(out, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var back Manifest
	if err := json.Unmarshal(b, &back); err != nil || back.Run != "r1" || back.Events != m.Events {
		t.Errorf("manifest file %s %v", b, err)
	}
}

func TestRecordFollowsChildProcessesAndOffsets(t *testing.T) {
	needTrace(t)
	dir := t.TempDir()
	file := filepath.Join(dir, "data")
	if err := os.WriteFile(file, make([]byte, 100000), 0o644); err != nil {
		t.Fatal(err)
	}
	// The shell opens the file once; two children read from the one
	// shared description in turn, so the second starts where the first stopped.
	script := `exec 3< "$1"; head -c 30000 <&3 > /dev/null; head -c 30000 <&3 > /dev/null`
	out := filepath.Join(dir, "rec")
	if _, err := record(Manifest{Workflow: "w", Run: "r1", Argv: []string{"sh", "-c", script, "sh", file}, Dir: dir}, os.Environ(), out, true); err != nil {
		t.Fatal(err)
	}
	f, _ := os.Open(filepath.Join(out, "events.tsv"))
	defer f.Close()
	_, _, events, err := readEvents(f)
	if err != nil {
		t.Fatal(err)
	}
	var end int64
	for _, e := range events {
		if e.Op == opRead && e.Path == file {
			if e.Off < 0 {
				t.Fatalf("offset lost across a fork: %+v", e)
			}
			end = max(end, e.Off+e.Len)
		}
	}
	// head may read ahead of what it consumes, but it can never read past
	// what the two together took, and the second must have gone beyond 30000.
	if end < 60000 {
		t.Errorf("furthest byte read %d, want at least 60000", end)
	}
}

func TestFinalSizesPreferTheTrace(t *testing.T) {
	dir := t.TempDir()
	disk := filepath.Join(dir, "disk")
	if err := os.WriteFile(disk, make([]byte, 50), 0o644); err != nil {
		t.Fatal(err)
	}
	events := []Event{
		{Op: opStat, Path: "/traced"},
		{Op: opRead, Path: disk, Off: 0, Len: 10},
		{Op: opRead, Path: "/gone", Off: 100, Len: 28},
		{Op: opMap, Path: "/traced", Off: 0, Len: 8192},
	}
	got := finalSizes(events, map[string]int64{"/traced": 1234})
	if got["/traced"] != 1234 || got[disk] != 50 || got["/gone"] != 128 {
		t.Errorf("%v", got)
	}
}

func TestMedian(t *testing.T) {
	v := []int64{9, 1, 5}
	if median(v) != 5 || v[0] != 9 {
		t.Errorf("median %d, input %v", median(v), v)
	}
}

func TestFilesReadListsDataFilesOnce(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "events.tsv")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	err = writeEvents(f, "w", "r", []Event{
		{Op: opRead, Path: "/b"}, {Op: opRead, Path: "/b"}, {Op: opExec, Path: "/a", Len: -1},
		{Op: opMap, Path: "/c"}, {Op: opStat, Path: "/d"}, {Op: opWrite, Path: "/e"},
	})
	f.Close()
	if err != nil {
		t.Fatal(err)
	}
	got, err := filesRead(path)
	if err != nil || strings.Join(got, " ") != "/a /b /c" {
		t.Errorf("%v %v", got, err)
	}
}
