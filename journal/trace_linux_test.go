package journal_test

// The tests that read the real file layer's system calls. The test binary runs
// again as a child of the test, which traces it with ptrace (no root, no strace:
// a process may trace its own child), records the calls the file layer makes, and
// can fail one. Every other test of the file layer runs on an in-memory double;
// these are the ones that look at what reaches the kernel.

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"

	"github.com/hanzoai/vfs/journal"
)

var child = flag.String("journal.child", "", "internal: run the helper of the tracing tests on this directory")

func TestMain(m *testing.M) {
	flag.Parse()
	if *child != "" {
		os.Exit(runChild(*child))
	}
	journal.MarkEvery(1) // a mark every round, so that every crash state has marks in it
	os.Exit(m.Run())
}

// runChild is the program the tracing tests watch: open, append, sync, append,
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

// call is one system call of interest the child made.
type call struct {
	name   string
	path   string // the file a descriptor argument refers to, or the path argument
	to     string // rename: the new path
	arg    uintptr
	inject bool // the tracer made it fail with EIO
}

func (c call) String() string { return fmt.Sprintf("%s(%s %s %#x)", c.name, c.path, c.to, c.arg) }

var names = map[uintptr]string{
	syscall.SYS_FDATASYNC: "fdatasync",
	syscall.SYS_FSYNC:     "fsync",
	syscall.SYS_RENAMEAT:  "rename",
	renameat2:             "rename", // not in the frozen syscall package: the number is per architecture
	syscall.SYS_UNLINKAT:  "unlink",
	syscall.SYS_OPENAT:    "open",
	syscall.SYS_FLOCK:     "flock",
	syscall.SYS_MKDIRAT:   "mkdir",
}

// tracedRun runs the child on dir under the tracer and returns what it printed
// and the calls it made, in order. fail picks a call to fail with EIO.
func tracedRun(t *testing.T, dir string, fail func(call) bool) (string, []call) {
	t.Helper()
	// ptrace requests must all come from the thread that started the child.
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	cmd := exec.Command(os.Args[0], "-journal.child="+dir)
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	cmd.SysProcAttr = &syscall.SysProcAttr{Ptrace: true}
	if err := cmd.Start(); err != nil {
		t.Fatalf("starting the traced child: %v", err)
	}
	pid := cmd.Process.Pid
	var ws syscall.WaitStatus
	if _, err := syscall.Wait4(pid, &ws, 0, nil); err != nil || !ws.Stopped() {
		t.Fatalf("the traced child did not stop at exec: %v %v", ws, err)
	}
	const exitkill = 0x100000 // PTRACE_O_EXITKILL: the child dies with the tracer
	if err := syscall.PtraceSetOptions(pid, syscall.PTRACE_O_TRACESYSGOOD|syscall.PTRACE_O_TRACECLONE|exitkill); err != nil {
		t.Fatalf("ptrace is not available here: %v", err)
	}
	if err := syscall.PtraceSyscall(pid, 0); err != nil {
		t.Fatal(err)
	}

	var calls []call
	live := map[int]bool{pid: true}
	inside := map[int]bool{}
	current := map[int]*call{}
	for len(live) > 0 {
		tid, err := syscall.Wait4(-1, &ws, syscall.WALL, nil)
		if errors.Is(err, syscall.EINTR) {
			continue
		}
		if err != nil {
			t.Fatalf("waiting for the traced child: %v", err)
		}
		if ws.Exited() || ws.Signaled() {
			delete(live, tid)
			delete(inside, tid)
			delete(current, tid)
			continue
		}
		if !ws.Stopped() {
			continue
		}
		live[tid] = true
		deliver := 0
		switch sig := ws.StopSignal(); sig {
		case syscall.SIGTRAP | 0x80: // a system call, at entry or at exit
			var r regs
			if err := syscall.PtraceGetRegs(tid, &r); err != nil {
				break
			}
			if !inside[tid] {
				delete(current, tid)
				if c, ok := decode(tid, &r); ok {
					c.inject = fail != nil && fail(c)
					calls = append(calls, c)
					current[tid] = &calls[len(calls)-1]
				}
			} else if c := current[tid]; c != nil && c.inject {
				setResult(&r, -int(syscall.EIO))
				if err := syscall.PtraceSetRegs(tid, &r); err != nil {
					t.Fatalf("failing a system call: %v", err)
				}
			}
			inside[tid] = !inside[tid]
		case syscall.SIGTRAP, syscall.SIGSTOP: // a new thread, or a clone event: nothing to deliver
		default:
			deliver = int(sig) // a signal for the child, such as the runtime's preemption signal
		}
		if err := syscall.PtraceSyscall(tid, deliver); err != nil && !errors.Is(err, syscall.ESRCH) {
			t.Fatal(err)
		}
	}
	_ = cmd.Wait()
	return out.String(), calls
}

// decode reads the arguments of the calls the tests care about.
func decode(tid int, r *regs) (call, bool) {
	name, ok := names[sysnum(r)]
	if !ok {
		return call{}, false
	}
	c := call{name: name}
	switch name {
	case "fdatasync", "fsync":
		c.path, _ = os.Readlink(fmt.Sprintf("/proc/%d/fd/%d", tid, int(sysarg(r, 0))))
	case "flock":
		c.path, _ = os.Readlink(fmt.Sprintf("/proc/%d/fd/%d", tid, int(sysarg(r, 0))))
		c.arg = sysarg(r, 1)
	case "rename":
		c.path, c.to = readString(tid, sysarg(r, 1)), readString(tid, sysarg(r, 3))
	case "unlink", "mkdir":
		c.path = readString(tid, sysarg(r, 1))
	case "open":
		c.path, c.arg = readString(tid, sysarg(r, 1)), sysarg(r, 2)
	}
	return c, true
}

// readString reads a NUL-terminated string from the child's memory.
func readString(tid int, addr uintptr) string {
	f, err := os.Open(fmt.Sprintf("/proc/%d/mem", tid))
	if err != nil {
		return ""
	}
	defer f.Close()
	buf := make([]byte, 4096)
	n, _ := f.ReadAt(buf, int64(addr))
	if i := bytes.IndexByte(buf[:n], 0); i >= 0 {
		n = i
	}
	return string(buf[:n])
}

func index(calls []call, after int, match func(call) bool) int {
	for i := max(after, 0); i < len(calls); i++ {
		if match(calls[i]) {
			return i
		}
	}
	return -1
}

func is(name, suffix string) func(call) bool {
	return func(c call) bool { return c.name == name && strings.HasSuffix(c.path, suffix) }
}

// journalDir makes the directory the child opens, with the files prep puts in it.
func journalDir(t *testing.T, prep ...func(dir string)) string {
	t.Helper()
	dir := filepath.Join(scratch(t), "journal")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	// The kernel reports a descriptor's path with its symbolic links resolved;
	// the child is given the same path so the two can be compared.
	dir, err := filepath.EvalSymlinks(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range prep {
		p(dir)
	}
	return dir
}

func dump(calls []call) string {
	var b strings.Builder
	for i, c := range calls {
		fmt.Fprintf(&b, "%3d %s\n", i, c)
	}
	return b.String()
}

// A failed fdatasync: Sync reports it, and the journal refuses every later
// Append. The error is never retried and never swallowed.
func TestFdatasyncFailurePoisonsTheJournal(t *testing.T) {
	dir := journalDir(t)
	out, calls := tracedRun(t, dir, func(c call) bool { return c.name == "fdatasync" && strings.HasSuffix(c.path, "000000.seg") })
	if index(calls, 0, func(c call) bool { return c.inject }) < 0 {
		t.Fatalf("no fdatasync of the segment was failed:\n%s", dump(calls))
	}
	if !strings.Contains(out, "SYNC1 journal: ") || strings.Contains(out, "SYNC1 <nil>") {
		t.Fatalf("Sync did not report the failed fdatasync:\n%s\n%s", out, dump(calls))
	}
	if strings.Contains(out, "APPEND2 <nil>") {
		t.Fatalf("Append after a failed fdatasync succeeded:\n%s", out)
	}
}

// The order of a Create: the zero-filled file is synced, then renamed into
// place, then the directory is synced; and the Sync of a record is an fdatasync
// of the segment.
func TestCreateSyscallOrder(t *testing.T) {
	dir := journalDir(t)
	_, calls := tracedRun(t, dir, nil)
	for _, seg := range []string{"000000.seg", "000001.seg"} {
		i1 := index(calls, 0, is("fdatasync", seg+".tmp"))
		i2 := index(calls, 0, func(c call) bool {
			return c.name == "rename" && strings.HasSuffix(c.path, seg+".tmp") && strings.HasSuffix(c.to, seg)
		})
		i3 := -1
		if i2 >= 0 {
			i3 = index(calls, i2, func(c call) bool { return c.name == "fsync" && c.path == dir })
		}
		if i1 < 0 || i2 < 0 || i3 < 0 || !(i1 < i2 && i2 < i3) {
			t.Fatalf("%s: want fdatasync(tmp) < rename < fsync(dir); got calls %d, %d, %d\n%s", seg, i1, i2, i3, dump(calls))
		}
	}
	if index(calls, 0, is("fdatasync", "000000.seg")) < 0 {
		t.Fatalf("Sync never issued fdatasync on the segment\n%s", dump(calls))
	}
}

// A removal is durable when it returns: the unlink is followed by an fsync of
// the directory. (A stale file of the wrong size is dropped by Open.)
func TestRemoveSyncsTheDirectory(t *testing.T) {
	dir := journalDir(t, func(dir string) {
		if err := os.WriteFile(filepath.Join(dir, "000099.seg"), []byte("short"), 0o600); err != nil {
			t.Fatal(err)
		}
	})
	_, calls := tracedRun(t, dir, nil)
	un := index(calls, 0, is("unlink", "000099.seg"))
	if un < 0 {
		t.Fatalf("the stale file was not removed\n%s", dump(calls))
	}
	// Between the unlink and the next Create (its first open of a temporary
	// file), so that a later Create's own directory sync cannot stand in for the
	// removal's.
	next := index(calls, un, func(c call) bool { return c.name == "open" && strings.HasSuffix(c.path, ".seg.tmp") })
	if next < 0 {
		next = len(calls)
	}
	if index(calls[:next], un, func(c call) bool { return c.name == "fsync" && c.path == dir }) < 0 {
		t.Fatalf("no fsync of the directory between the unlink and the next Create\n%s", dump(calls))
	}
}

// Files makes the directory readable by its owner alone, makes its entry in the
// parent durable, and takes the lock, before anything is written.
func TestFilesPreparesTheDirectory(t *testing.T) {
	dir := journalDir(t)
	_, calls := tracedRun(t, dir, nil)
	create := index(calls, 0, func(c call) bool { return c.name == "open" && strings.HasSuffix(c.path, ".seg.tmp") })
	if create < 0 {
		t.Fatalf("no segment was created\n%s", dump(calls))
	}
	if i := index(calls, 0, func(c call) bool { return c.name == "fsync" && c.path == filepath.Dir(dir) }); i < 0 || i > create {
		t.Fatalf("the parent of the journal's directory was not synced before the first segment (fsync at %d, first segment at %d)\n%s", i, create, dump(calls))
	}
	if i := index(calls, 0, func(c call) bool {
		return c.name == "flock" && c.path == dir && c.arg == syscall.LOCK_EX|syscall.LOCK_NB
	}); i < 0 || i > create {
		t.Fatalf("the directory was not locked before the first segment\n%s", dump(calls))
	}
}
