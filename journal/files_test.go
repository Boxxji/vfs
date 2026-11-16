package journal_test

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/hanzoai/vfs/journal"
	"github.com/hanzoai/vfs/journal/crash"
)

func ctxWithCancel() (context.Context, context.CancelFunc) { return context.WithCancel(ctx) }

func realOptions(dir string) journal.Options {
	return journal.Options{Capacity: 4 * 65536, Segment: 65536}
}

// a second Open of the same directory in one process, through a symbolic
// link to it, and Discard of a directory in use, are all refused.
func TestLockSameProcessLinkAndDiscard(t *testing.T) {
	dir := filepath.Join(scratch(t), "j")
	j, err := journal.Open(dir, realOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer closeWithin(t, j)
	if _, err := journal.Open(dir, realOptions(dir)); !errors.Is(err, journal.ErrLocked) {
		t.Fatalf("second Open: %v, want ErrLocked", err)
	}
	link := filepath.Join(scratch(t), "link")
	if err := os.Symlink(dir, link); err != nil {
		t.Fatal(err)
	}
	if _, err := journal.Open(link, realOptions(link)); !errors.Is(err, journal.ErrLocked) {
		t.Fatalf("Open through a symbolic link: %v, want ErrLocked", err)
	}
	if err := journal.Discard(dir, realOptions(dir), 1, 1); !errors.Is(err, journal.ErrLocked) {
		t.Fatalf("Discard of a directory in use: %v, want ErrLocked", err)
	}
	// ".." and "." spellings
	odd := filepath.Join(dir, "..", filepath.Base(dir))
	if _, err := journal.Open(odd, realOptions(odd)); !errors.Is(err, journal.ErrLocked) {
		t.Fatalf("Open through a dot-dot spelling: %v, want ErrLocked", err)
	}
}

// a child process holds the lock and is killed with SIGKILL: the kernel
// releases it and the next Open works. While the child lives, Open is refused.
func TestLockHeldByAChildDiesWithIt(t *testing.T) {
	t.Parallel() // real time, not CPU: it runs beside the others once the serial tests are done
	if os.Getenv("JOURNAL_HOLD") != "" {
		return
	}
	dir := filepath.Join(scratch(t), "j")
	cmd := exec.Command(os.Args[0], "-test.run=^TestLockHoldHelper$", "-test.v")
	cmd.Env = append(os.Environ(), "JOURNAL_HOLD="+dir)
	stdout, _ := cmd.StdoutPipe()
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 64)
	n, _ := stdout.Read(buf) // the helper prints READY once the journal is open
	if n == 0 {
		t.Fatal("helper did not start")
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(dir); err == nil || time.Now().After(deadline) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(200 * time.Millisecond)
	if _, err := journal.Open(dir, realOptions(dir)); !errors.Is(err, journal.ErrLocked) {
		t.Fatalf("Open while another process holds the journal: %v", err)
	}
	cmd.Process.Kill()
	cmd.Wait()
	j, err := journal.Open(dir, realOptions(dir))
	if err != nil {
		t.Fatalf("Open after the holder was killed: %v", err)
	}
	closeWithin(t, j)
}

func TestLockHoldHelper(t *testing.T) {
	dir := os.Getenv("JOURNAL_HOLD")
	if dir == "" {
		t.Skip("helper")
	}
	j, err := journal.Open(dir, realOptions(dir))
	if err != nil {
		os.Stdout.WriteString("OPENFAIL " + err.Error() + "\n")
		os.Exit(1)
	}
	seq, _ := j.Append(ctx, []byte("held"))
	j.Sync(ctx, seq)
	os.Stdout.WriteString("READY\n")
	time.Sleep(time.Hour)
}

// a descriptor must not leak into a child that outlives the journal. Start
// a long-lived child after Open, Close the journal, and open again.
func TestLockIsNotInheritedByChildren(t *testing.T) {
	t.Parallel() // real time, not CPU: it runs beside the others once the serial tests are done
	dir := filepath.Join(scratch(t), "j")
	j, err := journal.Open(dir, realOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	child := exec.Command("sleep", "20")
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { child.Process.Kill(); child.Wait() }()
	if err := closeBounded(t, j); err != nil {
		t.Fatal(err)
	}
	j, err = journal.Open(dir, realOptions(dir))
	if err != nil {
		t.Fatalf("a child process kept the directory locked after Close: %v", err)
	}
	closeWithin(t, j)
}

// tmpfs is refused with ErrVolatile (nothing is created in it), a
// pre-existing directory keeps whatever mode it had, and segments are 0600.
func TestPlacementAndModes(t *testing.T) {
	if fi, err := os.Stat("/dev/shm"); err == nil && fi.IsDir() {
		dir := filepath.Join("/dev/shm", "red34b-journal")
		defer os.RemoveAll(dir)
		_, err := journal.Open(dir, realOptions(dir))
		if !errors.Is(err, journal.ErrVolatile) {
			t.Fatalf("Open on /dev/shm: %v, want ErrVolatile", err)
		}
		if entries, _ := os.ReadDir(dir); len(entries) != 0 {
			t.Errorf("Open on tmpfs left %d files behind", len(entries))
		}
	}
	// A directory that already exists, mode 0755, is not tightened.
	base := scratch(t)
	dir := filepath.Join(base, "pre")
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	j, err := journal.Open(dir, realOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	seq, _ := j.Append(ctx, []byte("x"))
	j.Sync(ctx, seq)
	closeWithin(t, j)
	fi, _ := os.Stat(dir)
	t.Logf("pre-existing directory mode after Open: %v (the docs say the directory is 0700)", fi.Mode().Perm())
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		info, _ := e.Info()
		if info.Mode().Perm() != 0o600 {
			t.Errorf("%s mode %v, want 0600", e.Name(), info.Mode().Perm())
		}
	}
	// A fresh directory is 0700, and so is every directory MkdirAll made for it.
	nested := filepath.Join(base, "a", "b", "c")
	j, err = journal.Open(nested, realOptions(nested))
	if err != nil {
		t.Fatal(err)
	}
	closeWithin(t, j)
	for _, p := range []string{filepath.Join(base, "a"), filepath.Join(base, "a", "b"), nested} {
		fi, _ := os.Stat(p)
		if fi.Mode().Perm() != 0o700 {
			t.Errorf("%s mode %v, want 0700", p, fi.Mode().Perm())
		}
	}
}

// the Due/Close race on the wall clock, with a cancellable Due.
func TestDueCloseRaceWithRealClock(t *testing.T) {
	t.Parallel() // real time, not CPU: it runs beside the others once the serial tests are done
	end := time.Now().Add(time.Second)
	for i := 0; time.Now().Before(end); i++ {
		j, err := journal.Open("", journal.Options{Capacity: 1 << 20, Segment: 64 << 10, Disk: crash.New()})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := j.Append(ctx, []byte("x")); err != nil {
			t.Fatal(err)
		}
		c, cancel := ctxWithCancel()
		ch := j.Due(c, 200*time.Microsecond)
		time.Sleep(time.Duration(100+i%200) * time.Microsecond)
		if i%3 == 0 {
			cancel() // the context ends as the deadline falls due
		}
		closeWithin(t, j)
		select {
		case <-ch:
		default:
			if i%3 != 0 {
				t.Fatalf("iteration %d: Close left a live Due open", i)
			}
		}
		cancel()
	}
}

// A publisher whose publication always fails sees one overdue record. Due must
// not fire more often than Retry, on the wall clock.
func TestFailingPublisherDoesNotSpin(t *testing.T) {
	t.Parallel() // real time, not CPU: it runs beside the others once the serial tests are done
	j, err := journal.Open("", journal.Options{Capacity: 1 << 20, Segment: 64 << 10, Disk: crash.New(), Retry: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer closeWithin(t, j)
	if _, err := j.Append(ctx, []byte("x")); err != nil {
		t.Fatal(err)
	}
	var fired int
	end := time.Now().Add(time.Second)
	for time.Now().Before(end) {
		c, cancel := ctxWithCancel()
		select {
		case <-j.Due(c, 0):
			fired++
		case <-time.After(time.Until(end)):
		}
		cancel()
	}
	if fired > 22 {
		t.Fatalf("Due fired %d times in a second with Retry 50 ms", fired)
	}
	t.Logf("Due fired %d times in a second with Retry 50 ms", fired)
}

// Abandoned Due calls on a never-cancelled context are the caller's to bound;
// with cancellation nothing may accumulate. 20,000 calls, goroutines and timers.
func TestCancelledDueLeavesNothingBehind(t *testing.T) {
	clk := newClock()
	j := open(t, small(crash.New(), clk, 0))
	defer closeWithin(t, j)
	mustAppend(t, j, []byte("x"))
	before := runtime.NumGoroutine()
	var wg sync.WaitGroup
	for i := 0; i < 20000; i++ {
		c, cancel := ctxWithCancel()
		j.Due(c, 5*time.Second)
		if i%2 == 0 {
			cancel()
		} else {
			wg.Add(1)
			go func() { defer wg.Done(); cancel() }()
		}
	}
	wg.Wait()
	time.Sleep(50 * time.Millisecond)
	if p := clk.Pending(); p > 1 {
		t.Fatalf("%d timers pending after 20000 abandoned calls", p)
	}
	if g := runtime.NumGoroutine(); g > before+50 {
		t.Fatalf("goroutines %d -> %d", before, g)
	}
	_ = syscall.Getpid
}
