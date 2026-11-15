package journal_test

// Every other test of the file layer runs on an in-memory double. These run the
// real one: the test binary runs again under strace (ptrace, no root), the
// system calls it makes are read from the trace, and an I/O error is injected
// into fdatasync. They pin what the file layer's contract rests on: a created
// file is synced, renamed, and the directory synced, in that order, before the
// first record is written to it; a removal is followed by a directory sync; a
// Sync is a real fdatasync on the segment; and a failed fdatasync poisons the
// journal. strace must be installed: a test that cannot look at the system
// calls proves nothing, so it fails, it does not skip.

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/hanzoai/vfs/journal"
)

var child = flag.String("journal.child", "", "internal: run the helper of the strace tests on this directory")

func TestMain(m *testing.M) {
	flag.Parse()
	if *child != "" {
		os.Exit(runChild(*child))
	}
	os.Exit(m.Run())
}

// runChild is the program the strace tests watch: open, append, sync, append,
// close, and say what each returned.
func runChild(dir string) int {
	j, err := journal.Open(dir, journal.Options{Capacity: 4 * 65536, Segment: 65536})
	if err != nil {
		fmt.Println("OPEN", err)
		return 1
	}
	seq, err := j.Append(ctx, []byte("hello"))
	fmt.Println("APPEND1", seq, err)
	fmt.Println("SYNC1", j.Sync(ctx, seq))
	_, err = j.Append(ctx, []byte("again"))
	fmt.Println("APPEND2", err)
	fmt.Println("CLOSE", j.Close())
	return 0
}

// traced runs the child under strace in a fresh directory and returns what it
// printed and the trace. inject, if set, is an strace inject expression applied
// to the first segment only. prep runs on the directory first.
func traced(t *testing.T, inject string, prep ...func(dir string)) (dir, stdout, trace string) {
	t.Helper()
	if _, err := exec.LookPath("strace"); err != nil {
		t.Fatal("strace is not installed: the tests that read the real system calls cannot run without it")
	}
	dir = filepath.Join(scratch(t), "journal")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, p := range prep {
		p(dir)
	}
	log := filepath.Join(scratch(t), "trace")
	args := []string{"-f", "-y", "-o", log, "-e", "trace=fdatasync,fsync,rename,renameat,renameat2,openat,unlink,unlinkat,flock,mkdir,mkdirat"}
	if inject != "" {
		args = append(args, "-P", filepath.Join(dir, "000000.seg"), "-e", "inject="+inject)
	}
	args = append(args, os.Args[0], "-journal.child="+dir)
	out, err := exec.Command("strace", args...).CombinedOutput()
	b, _ := os.ReadFile(log)
	if len(b) == 0 {
		t.Fatalf("strace could not trace the helper: %v: %s", err, out)
	}
	return dir, string(out), string(b)
}

func lineIndex(lines []string, re string, after int) int {
	r := regexp.MustCompile(re)
	for i := max(after, 0); i < len(lines); i++ {
		if r.MatchString(lines[i]) {
			return i
		}
	}
	return -1
}

// A failed fdatasync: Sync reports it, and the journal refuses every later
// Append. The error is never retried and never swallowed.
func TestFdatasyncFailurePoisonsTheJournal(t *testing.T) {
	_, out, trace := traced(t, "fdatasync:error=EIO")
	if !strings.Contains(trace, "INJECTED") {
		t.Fatalf("no fdatasync was injected:\n%s", trace)
	}
	if !strings.Contains(out, "SYNC1 journal: ") || strings.Contains(out, "SYNC1 <nil>") {
		t.Fatalf("Sync did not report the failed fdatasync:\n%s\n%s", out, trace)
	}
	if strings.Contains(out, "APPEND2 <nil>") {
		t.Fatalf("Append after a failed fdatasync succeeded:\n%s", out)
	}
}

// The order of a Create: the zero-filled file is synced, then renamed into
// place, then the directory is synced; and the Sync of a record is an fdatasync
// of the segment.
func TestCreateSyscallOrder(t *testing.T) {
	dir, _, trace := traced(t, "")
	lines := strings.Split(trace, "\n")
	d := regexp.QuoteMeta(filepath.Base(dir))
	for _, seg := range []string{"000000.seg", "000001.seg"} {
		i1 := lineIndex(lines, `fdatasync\(\d+<[^>]*/`+seg+`\.tmp>`, 0)
		i2 := lineIndex(lines, `renameat2?\(.*/`+seg+`\.tmp".*/`+seg+`"`, 0)
		i3 := -1
		if i2 >= 0 {
			i3 = lineIndex(lines, `\bfsync\(\d+<[^>]*/`+d+`>`, i2)
		}
		if i1 < 0 || i2 < 0 || i3 < 0 || !(i1 < i2 && i2 < i3) {
			t.Fatalf("%s: want fdatasync(tmp) < rename < fsync(dir); got lines %d, %d, %d\n%s", seg, i1, i2, i3, trace)
		}
	}
	if lineIndex(lines, `fdatasync\(\d+<[^>]*/000000\.seg>`, 0) < 0 {
		t.Fatalf("Sync never issued fdatasync on the segment\n%s", trace)
	}
}

// A removal is durable when it returns: the unlink is followed by an fsync of
// the directory. (A stale file of the wrong size is dropped by Open.)
func TestRemoveSyncsTheDirectory(t *testing.T) {
	dir, _, trace := traced(t, "", func(dir string) {
		if err := os.WriteFile(filepath.Join(dir, "000099.seg"), []byte("short"), 0o600); err != nil {
			t.Fatal(err)
		}
	})
	lines := strings.Split(trace, "\n")
	un := lineIndex(lines, `unlinkat?\(.*000099\.seg`, 0)
	if un < 0 {
		t.Fatalf("the stale file was not removed\n%s", trace)
	}
	// Between the unlink and the next Create (its first open of a temporary
	// file), so that a later Create's own directory sync cannot stand in for the
	// removal's.
	next := lineIndex(lines, `openat\(.*\.seg\.tmp`, un)
	if next < 0 {
		next = len(lines)
	}
	if lineIndex(lines[:next], `\bfsync\(\d+<[^>]*/`+regexp.QuoteMeta(filepath.Base(dir))+`>`, un) < 0 {
		t.Fatalf("no fsync of the directory between the unlink and the next Create\n%s", trace)
	}
}

// Files makes the directory readable by its owner alone, makes its entry in the
// parent durable, and takes the lock, before anything is written.
func TestFilesPreparesTheDirectory(t *testing.T) {
	dir, _, trace := traced(t, "")
	lines := strings.Split(trace, "\n")
	parent := regexp.QuoteMeta(filepath.Base(filepath.Dir(dir)))
	create := lineIndex(lines, `openat\(.*\.seg\.tmp`, 0)
	if i := lineIndex(lines, `\bfsync\(\d+<[^>]*/`+parent+`>`, 0); i < 0 || i > create {
		t.Fatalf("the parent of the journal's directory was not synced before the first segment (fsync at %d, first segment at %d)\n%s", i, create, trace)
	}
	if i := lineIndex(lines, `flock\(\d+<[^>]*/`+regexp.QuoteMeta(filepath.Base(dir))+`>, LOCK_EX\|LOCK_NB\)\s*= 0`, 0); i < 0 || i > create {
		t.Fatalf("the directory was not locked before the first segment\n%s", trace)
	}
}
