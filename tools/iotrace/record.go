package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// syscalls traced: everything that names a file or moves its bytes,
// under the names of both x86-64 and arm64.
const syscalls = "openat,openat2,open,creat,close,dup,dup2,dup3,fcntl,lseek," +
	"read,pread64,readv,preadv,preadv2,write,pwrite64,writev,pwritev,pwritev2," +
	"copy_file_range,sendfile,mmap,execve,execveat,clone,clone3,fork,vfork," +
	"statx,newfstatat,fstatat64,stat,lstat,stat64,lstat64,fstat,fstat64," +
	"access,faccessat,faccessat2,readlink,readlinkat,getdents,getdents64," +
	"rename,renameat,renameat2,link,linkat,unlink,unlinkat,rmdir,mkdir,mkdirat," +
	"symlink,symlinkat,truncate,ftruncate"

// Manifest describes one recorded run.
type Manifest struct {
	Workflow string            `json:"workflow"`
	Project  string            `json:"project"`
	Run      string            `json:"run"`
	Argv     []string          `json:"argv"`
	Dir      string            `json:"dir"`
	Roots    map[string]string `json:"roots"`
	Wall     int64             `json:"wall_ns"`  // traced
	Plain    int64             `json:"plain_ns"` // untraced, cold page cache, 0 if not measured
	Warm     int64             `json:"warm_ns"`  // untraced, page cache warm from the run before
	Colds    []int64           `json:"colds_ns"` // every cold timing behind Plain
	Warms    []int64           `json:"warms_ns"` // every warm timing behind Warm
	Exit     int               `json:"exit"`
	Load     [2]string         `json:"load"` // /proc/loadavg before and after
	Events   int               `json:"events"`
}

func loadavg() string {
	b, err := os.ReadFile("/proc/loadavg")
	if err != nil {
		return ""
	}
	f := strings.Fields(string(b))
	if len(f) < 3 {
		return ""
	}
	return strings.Join(f[:3], " ")
}

// tail keeps the last bytes written to it.
type tail struct{ b []byte }

func (t *tail) Write(p []byte) (int, error) {
	t.b = append(t.b, p...)
	if len(t.b) > 4096 {
		t.b = t.b[len(t.b)-4096:]
	}
	return len(p), nil
}

// trace runs argv under strace and writes the raw output to raw. What
// the command printed on its error stream, or strace did, is returned
// as stderr.
func trace(raw string, dir string, env, argv []string) (wall time.Duration, exit int, stderr string, err error) {
	args := []string{"--seccomp-bpf", "-f", "-ttt", "-T", "-y", "-s", "0", "-e", "trace=" + syscalls, "-o", raw, "--"}
	cmd := exec.Command("strace", append(args, argv...)...)
	cmd.Dir, cmd.Env = dir, env
	var errs tail
	cmd.Stdout, cmd.Stderr = io.Discard, &errs
	start := time.Now()
	err = cmd.Run()
	wall = time.Since(start)
	var x *exec.ExitError
	if errors.As(err, &x) {
		return wall, x.ExitCode(), string(errs.b), nil
	}
	return wall, 0, string(errs.b), err
}

// record traces argv, normalises the result into out and returns the
// manifest. The raw strace output is kept only when keep is set.
func record(m Manifest, env []string, out string, keep bool) (Manifest, error) {
	out, err := filepath.Abs(out)
	if err != nil {
		return m, err
	}
	if err := os.MkdirAll(out, 0o755); err != nil {
		return m, err
	}
	raw := filepath.Join(out, "raw.strace")
	m.Load[0] = loadavg()
	wall, exit, stderr, err := trace(raw, m.Dir, env, m.Argv)
	m.Load[1] = loadavg()
	if err != nil {
		return m, err
	}
	if err := os.WriteFile(filepath.Join(out, "stderr.txt"), []byte(stderr), 0o644); err != nil {
		return m, err
	}
	m.Wall, m.Exit = wall.Nanoseconds(), exit
	if !keep {
		defer os.Remove(raw)
	}
	events, sizes, err := normalise(func() (io.Reader, error) { return os.Open(raw) }, m.Dir)
	if err != nil {
		return m, err
	}
	m.Events = len(events)
	if err := writeFile(filepath.Join(out, "events.tsv"), func(w io.Writer) error {
		return writeEvents(w, m.Workflow, m.Run, events)
	}); err != nil {
		return m, err
	}
	if err := writeFile(filepath.Join(out, "sizes.tsv"), func(w io.Writer) error {
		return writeSizes(w, m.Workflow, m.Run, finalSizes(events, sizes))
	}); err != nil {
		return m, err
	}
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return m, err
	}
	return m, os.WriteFile(filepath.Join(out, "manifest.json"), b, 0o644)
}

func writeFile(path string, fn func(io.Writer) error) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	w := bufio.NewWriterSize(f, 1<<20)
	if err := fn(w); err != nil {
		f.Close()
		return err
	}
	if err := w.Flush(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// finalSizes gives every touched path a size: the one a stat reported in
// the trace, else what the file system says now, else the furthest byte
// the run read or wrote.
func finalSizes(events []Event, traced map[string]int64) map[string]int64 {
	sizes := map[string]int64{}
	for _, e := range events {
		if _, ok := sizes[e.Path]; ok {
			continue
		}
		if v, ok := traced[e.Path]; ok {
			sizes[e.Path] = v
		} else if st, err := os.Stat(e.Path); err == nil && st.Mode().IsRegular() {
			sizes[e.Path] = st.Size()
		} else {
			sizes[e.Path] = 0
		}
	}
	for _, e := range events {
		if e.Op == opRead || e.Op == opWrite || e.Op == opMap {
			if end := e.Off + e.Len; e.Off >= 0 && end > sizes[e.Path] && (e.Op != opMap || sizes[e.Path] == 0) {
				sizes[e.Path] = end
			}
		}
	}
	return sizes
}

func writeSizes(w io.Writer, workflow, run string, sizes map[string]int64) error {
	paths := make([]string, 0, len(sizes))
	for p := range sizes {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	for _, p := range paths {
		if _, err := fmt.Fprintf(w, "%s\t%s\t%s\t%d\n", escape(workflow), escape(run), escape(p), sizes[p]); err != nil {
			return err
		}
	}
	return nil
}

func readSizes(r io.Reader) (map[string]int64, error) {
	sizes := map[string]int64{}
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 1<<20), 1<<24)
	for sc.Scan() {
		f := strings.Split(sc.Text(), "\t")
		if len(f) != 4 {
			return nil, fmt.Errorf("sizes: %d fields, want 4", len(f))
		}
		var v int64
		if _, err := fmt.Sscan(f[3], &v); err != nil {
			return nil, err
		}
		sizes[unescape(f[2])] = v
	}
	return sizes, sc.Err()
}
