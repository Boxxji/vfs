package vfs_test

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/luxfi/age"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/internal/storetest"
	"github.com/hanzoai/vfs/pkg/backend"
	_ "github.com/hanzoai/vfs/pkg/backend/file"
)

// expected returns the snapshots of the two states a crash test moves between.
func expected(t *testing.T) (one, two map[string]string) {
	_, _, v := newMem(t)
	stateOne(t, v)
	flush(t, v)
	one = snapshot(t, v)
	stateTwo(t, v)
	flush(t, v)
	return one, snapshot(t, v)
}

// TestFailureAtEveryWrite: a Flush that fails at its Nth write, for every N it
// makes, leaves the store at the previous Head: a fresh Open sees exactly state
// one, whatever chunks, packs and shards the failed Flush managed to upload. A
// retry on the same Volume then commits state two.
func TestFailureAtEveryWrite(t *testing.T) {
	one, two := expected(t)

	// Count the writes of the Flush that moves from one to two.
	writes := func(failAt int) (*storetest.Mem, vfs.Cipher, *vfs.Volume, *int, *error) {
		mem := storetest.NewMem()
		hook := &storetest.Hook{Backend: mem}
		c := vfsAge(t)
		if err := vfs.Create(ctx, cfg(hook, c, 0)); err != nil {
			t.Fatal(err)
		}
		v, err := vfs.Attach(ctx, cfg(hook, c, 1))
		if err != nil {
			t.Fatal(err)
		}
		stateOne(t, v)
		flush(t, v)
		stateTwo(t, v)
		n := 0
		var boom error = errors.New("injected")
		hook.Before = func(op, key string) error {
			n++
			if n == failAt {
				return boom
			}
			return nil
		}
		return mem, c, v, &n, &boom
	}

	_, _, v, n, _ := writes(0)
	flush(t, v)
	total := *n
	if total < 4 {
		t.Fatalf("the flush made only %d writes; the sweep would prove little", total)
	}
	t.Logf("the flush from state one to state two makes %d writes", total)

	for failAt := 1; failAt <= total; failAt++ {
		// Under -short try the first write, the last two and every fourth between:
		// the full sweep runs without it.
		if testing.Short() && failAt > 1 && failAt < total-1 && failAt%4 != 0 {
			continue
		}
		mem, c, v, _, boom := writes(failAt)
		if err := v.Flush(ctx); !errors.Is(err, *boom) {
			t.Fatalf("write %d/%d: Flush = %v, want the injected error", failAt, total, err)
		}
		sameSnapshot(t, fmt.Sprintf("failure at write %d/%d", failAt, total), snapshot(t, reopen(t, mem, c)), one)

		// The Volume kept its changes: a retry commits them.
		flush(t, v)
		sameSnapshot(t, fmt.Sprintf("retry after write %d/%d", failAt, total), snapshot(t, reopen(t, mem, c)), two)
	}
}

// TestLostAcknowledgementOfHead: the Head write lands but its reply is lost. The
// caller sees an error, the store holds the new state whole, and a retry is safe.
func TestLostAcknowledgementOfHead(t *testing.T) {
	_, two := expected(t)
	mem := storetest.NewMem()
	var lose atomic.Bool
	hook := &storetest.Hook{Backend: mem, After: func(op, key string) error {
		if lose.Load() && key == "head" {
			return errors.New("reply lost")
		}
		return nil
	}}
	c := vfsAge(t)
	if err := vfs.Create(ctx, cfg(hook, c, 0)); err != nil {
		t.Fatal(err)
	}
	v, err := vfs.Attach(ctx, cfg(hook, c, 1))
	if err != nil {
		t.Fatal(err)
	}
	stateOne(t, v)
	flush(t, v)
	stateTwo(t, v)
	lose.Store(true)
	if err := v.Flush(ctx); err == nil {
		t.Fatal("Flush hid a lost reply")
	}
	sameSnapshot(t, "head landed", snapshot(t, reopen(t, mem, c)), two)
	lose.Store(false)
	flush(t, v) // the retry is a no-op or a re-commit of the same tree
	sameSnapshot(t, "after retry", snapshot(t, reopen(t, mem, c)), two)
}

// crashChild is the process the crash test kills. It is the test binary run
// again with the arguments "crashchild <dir> <stage>"; it takes the Volume key
// from standard input, as the vfs command does, and never from the environment.
func TestCrashChild(t *testing.T) {
	if flag.Arg(0) != "crashchild" {
		t.Skip("runs only as the child of TestSigkill")
	}
	dir, stage := flag.Arg(1), flag.Arg(2)
	line, err := bufio.NewReader(os.Stdin).ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	key, err := age.ParseHybridIdentity(strings.TrimSpace(line))
	if err != nil {
		t.Fatal(err)
	}
	be, err := backend.Open(ctx, "file://"+dir)
	if err != nil {
		t.Fatal(err)
	}
	// The stop is armed once Attach has returned, so the claim's own write of the
	// Head is not mistaken for the commit.
	var armed, stopped atomic.Bool
	stop := func(op, key string) {
		if armed.Load() && stopped.CompareAndSwap(false, true) {
			fmt.Println("READY")
			select {} // wait for the parent's SIGKILL
		}
	}
	hook := &storetest.Hook{Backend: be}
	switch stage {
	case "pack":
		hook.Before = func(op, key string) error {
			if strings.HasPrefix(key, "pack/") {
				stop(op, key)
			}
			return nil
		}
	case "packed":
		hook.After = func(op, key string) error {
			if strings.HasPrefix(key, "pack/") {
				stop(op, key)
			}
			return nil
		}
	case "shard":
		hook.Before = func(op, key string) error {
			if strings.HasPrefix(key, "meta/") {
				stop(op, key)
			}
			return nil
		}
	case "sharded":
		hook.After = func(op, key string) error {
			if strings.HasPrefix(key, "meta/") {
				stop(op, key)
			}
			return nil
		}
	case "head":
		hook.Before = func(op, key string) error {
			if key == "head" {
				stop(op, key)
			}
			return nil
		}
	case "landed":
		hook.After = func(op, key string) error {
			if key == "head" {
				stop(op, key)
			}
			return nil
		}
	default:
		t.Fatalf("unknown stage %q", stage)
	}
	v, err := vfs.Attach(ctx, cfg(hook, vfs.Hybrid(key), 2))
	if err != nil {
		t.Fatal(err)
	}
	armed.Store(true)
	stateTwo(t, v)
	if err := v.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	t.Fatal("the flush finished without reaching the stop")
}

// TestSigkill kills a real process between uploading chunks, uploading shards and
// committing the Head, and after the commit, and reopens the store from another
// process: the tree is exactly the last committed state, every time.
func TestSigkill(t *testing.T) {
	one, two := expected(t)
	for _, tc := range []struct {
		stage string
		want  map[string]string
	}{
		{"pack", one}, {"packed", one}, {"shard", one}, {"sharded", one}, {"head", one}, {"landed", two},
	} {
		t.Run(tc.stage, func(t *testing.T) {
			dir := t.TempDir()
			key := newKey(t)
			c := vfs.Hybrid(key)
			be, err := backend.Open(ctx, "file://"+dir)
			if err != nil {
				t.Fatal(err)
			}
			if err := vfs.Create(ctx, cfg(be, c, 0)); err != nil {
				t.Fatal(err)
			}
			v, err := vfs.Attach(ctx, cfg(be, c, 1))
			if err != nil {
				t.Fatal(err)
			}
			stateOne(t, v)
			flush(t, v)

			cmd := exec.Command(os.Args[0], "-test.run=^TestCrashChild$", "-test.count=1", "crashchild", dir, tc.stage)
			cmd.Stdin = strings.NewReader(key.String() + "\n")
			out, err := cmd.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			cmd.Stderr = os.Stderr
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			ready := make(chan bool, 1)
			go func() {
				sc := bufio.NewScanner(out)
				for sc.Scan() {
					if sc.Text() == "READY" {
						ready <- true
						return
					}
				}
				ready <- false
			}()
			select {
			case ok := <-ready:
				if !ok {
					_ = cmd.Wait()
					t.Fatal("the child exited before reaching the stop")
				}
			case <-time.After(2 * time.Minute):
				_ = cmd.Process.Kill()
				t.Fatal("the child never reached the stop")
			}
			if err := cmd.Process.Signal(syscall.SIGKILL); err != nil {
				t.Fatal(err)
			}
			_ = cmd.Wait()

			// Read-only first, then a takeover: both see the committed state.
			ro, err := vfs.Open(ctx, cfg(reopenBackend(t, dir), c, 0))
			if err != nil {
				t.Fatal(err)
			}
			sameSnapshot(t, "after SIGKILL at "+tc.stage, snapshot(t, ro), tc.want)
			next, err := vfs.Attach(ctx, cfg(reopenBackend(t, dir), c, 3))
			if err != nil {
				t.Fatalf("takeover after the kill: %v", err)
			}
			sameSnapshot(t, "takeover after SIGKILL at "+tc.stage, snapshot(t, next), tc.want)
			// And the survivor can carry on: it commits state two over what is there.
			if tc.stage != "landed" {
				stateTwo(t, next)
				flush(t, next)
				sameSnapshot(t, "carried on after "+tc.stage, snapshot(t, reopen(t, reopenBackend(t, dir), c)), two)
			}
		})
	}
}

func reopenBackend(t *testing.T, dir string) backend.Backend {
	be, err := backend.Open(ctx, "file://"+dir)
	if err != nil {
		t.Fatal(err)
	}
	return be
}
