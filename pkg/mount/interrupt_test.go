//go:build fuse && linux

package mount

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"bazil.org/fuse"
	"github.com/luxfi/age"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/internal/storetest"
	"github.com/hanzoai/vfs/pkg/backend"
)

// TestInterruptIsEINTR: a request the kernel interrupted ends with EINTR, however
// deep the Volume wrapped the cancellation, never with EIO.
func TestInterruptIsEINTR(t *testing.T) {
	for name, err := range map[string]error{
		"bare":    context.Canceled,
		"wrapped": fmt.Errorf("vfs: write shard 1: %w", context.Canceled),
		"twice":   fmt.Errorf("vfs: commit: %w", fmt.Errorf("vfs: read chunk 2: %w", context.Canceled)),
	} {
		if got := errno(err); got != syscall.EINTR {
			t.Errorf("%s: errno = %v, want EINTR", name, got)
		}
	}
	if got := errno(fmt.Errorf("x: %w", context.DeadlineExceeded)); got != syscall.ETIMEDOUT {
		t.Errorf("deadline: errno = %v, want ETIMEDOUT", got)
	}
	if got := errno(errors.New("the store is down")); got != syscall.EIO {
		t.Errorf("a store fault: errno = %v, want EIO", got)
	}
}

// gate holds the calls it is armed for until released, and counts them. After the
// release a call whose context has ended fails, as an HTTP request to a store does.
type gate struct {
	backend.Backend
	armed   atomic.Bool
	reached chan struct{}
	release chan struct{}
	landed  chan struct{} // closed when a Head write has returned success
	passed  chan struct{} // closed when the first held call is released
	heldErr error         // what that call's context said then; read after passed
	once    sync.Once
	second  sync.Once
	freed   sync.Once
	ranges  atomic.Int64
}

func newGate(be backend.Backend) *gate {
	return &gate{Backend: be, reached: make(chan struct{}), release: make(chan struct{}), landed: make(chan struct{}), passed: make(chan struct{})}
}

// free lets every held call go; a test that fails with calls still held defers it so
// that closing its files does not wait on them.
func (g *gate) free() { g.freed.Do(func() { close(g.release) }) }

func (g *gate) hold(ctx context.Context) error {
	if g.armed.Load() {
		g.once.Do(func() { close(g.reached) })
		<-g.release
		g.second.Do(func() { g.heldErr = ctx.Err(); close(g.passed) })
	}
	return ctx.Err()
}

func (g *gate) PutIfVersion(ctx context.Context, key string, data []byte, v backend.Version) (backend.Version, error) {
	if key == "head" {
		if err := g.hold(ctx); err != nil {
			return "", err
		}
	}
	nv, err := g.Backend.PutIfVersion(ctx, key, data, v)
	if err == nil && key == "head" && g.armed.Load() {
		close(g.landed)
	}
	return nv, err
}

func (g *gate) GetRange(ctx context.Context, key string, off, n int64) ([]byte, error) {
	g.ranges.Add(1)
	if err := g.hold(ctx); err != nil {
		return nil, err
	}
	return g.Backend.GetRange(ctx, key, off, n)
}

func testVolume(t *testing.T) (*gate, *vfs.Volume, vfs.Cipher, *storetest.Mem) {
	t.Helper()
	k, err := age.GenerateHybridIdentity()
	if err != nil {
		t.Fatal(err)
	}
	mem := storetest.NewMem()
	g := newGate(storetest.Strict{Backend: mem})
	c := vfs.Hybrid(k)
	bg := context.Background()
	if err := vfs.Create(bg, vfs.Config{Backend: g, Cipher: c}); err != nil {
		t.Fatal(err)
	}
	v, err := vfs.Attach(bg, vfs.Config{Backend: g, Cipher: c, Round: 1})
	if err != nil {
		t.Fatal(err)
	}
	return g, v, c, mem
}

// TestCommitSurvivesItsCaller: the caller of fsync(2) or close(2) is interrupted
// while the commit is writing the Head. It stops waiting and is answered EINTR at
// once; the commit is not cancelled, lands by itself, and the restarted call finds
// nothing left to commit.
func TestCommitSurvivesItsCaller(t *testing.T) {
	g, v, c, mem := testVolume(t)
	bg := context.Background()
	in, err := v.Create(bg, vfs.RootInode, "f", 0o644)
	if err != nil {
		t.Fatal(err)
	}
	f, err := v.Open(bg, in.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	defer g.free()
	if _, err := f.WriteAt(bg, make([]byte, 200_000), 0); err != nil {
		t.Fatal(err)
	}

	g.armed.Store(true)
	req, interrupt := context.WithCancel(bg)
	result := make(chan error, 1)
	go func() { result <- errno(newCommitter(v).commit(req)) }()
	select {
	case <-g.reached:
	case <-time.After(20 * time.Second):
		t.Fatal("the commit never reached the Head")
	}
	interrupt() // the kernel's FUSE_INTERRUPT: the request's context ends mid-PUT
	select {
	case err := <-result:
		if err != syscall.EINTR {
			t.Fatalf("an interrupted commit answered %v, want EINTR", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("an interrupted request kept waiting for the commit")
	}
	g.free() // the store answers
	select {
	case <-g.landed: // the Head was written by the commit the caller walked away from
	case <-time.After(20 * time.Second):
		t.Fatal("the commit did not land once its caller was gone")
	}
	if err := newCommitter(v).commit(bg); err != nil { // the restarted call: nothing is left to commit
		t.Fatalf("the restarted commit: %v", err)
	}
	ro, err := vfs.Open(bg, vfs.Config{Backend: mem, Cipher: c})
	if err != nil {
		t.Fatal(err)
	}
	got, err := ro.Resolve(bg, "/f")
	if err != nil || got.Size != 200_000 {
		t.Fatalf("the interrupted commit did not land whole: %+v %v", got, err)
	}
}

// TestCloseWaitsForItsCommit: close(2) is not restarted after EINTR (Linux has dropped
// the descriptor and Go's os.File.Close hands the error to its caller), so an
// interrupt that reaches a commit asked for by a close does not end the wait: the
// caller gets the commit's own result.
func TestCloseWaitsForItsCommit(t *testing.T) {
	g, v, _, _ := testVolume(t)
	bg := context.Background()
	in, err := v.Create(bg, vfs.RootInode, "f", 0o644)
	if err != nil {
		t.Fatal(err)
	}
	f, err := v.Open(bg, in.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt(bg, make([]byte, 100_000), 0); err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	defer g.free()
	h := newSession(bg, v).handle(f)

	g.armed.Store(true)
	req, interrupt := context.WithCancel(bg)
	result := make(chan error, 1)
	go func() { result <- h.Flush(req, &fuse.FlushRequest{}) }()
	select {
	case <-g.reached:
	case <-time.After(20 * time.Second):
		t.Fatal("the commit never reached the Head")
	}
	interrupt()
	select {
	case err := <-result:
		t.Fatalf("close was answered (%v) while its commit was still being written", err)
	case <-time.After(300 * time.Millisecond):
	}
	g.free()
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("close after an interrupt: %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("close never returned")
	}
}

// TestReadWaitsForItsData: a read answered EINTR reaches the caller as EIO when the
// kernel's page cache was filling pages, so an interrupt does not end a read: it waits
// for its chunk and returns the bytes. The fetch runs on the mount's own context, not
// on the request that ended.
func TestReadWaitsForItsData(t *testing.T) {
	g, v, c, _ := testVolume(t)
	bg := context.Background()
	in, err := v.Create(bg, vfs.RootInode, "f", 0o644)
	if err != nil {
		t.Fatal(err)
	}
	f, err := v.Open(bg, in.ID)
	if err != nil {
		t.Fatal(err)
	}
	data := make([]byte, 300_000)
	for i := range data {
		data[i] = byte(i * 7)
	}
	if _, err := f.WriteAt(bg, data, 0); err != nil {
		t.Fatal(err)
	}
	if err := v.Flush(bg); err != nil {
		t.Fatal(err)
	}
	f.Close()

	// A cold view of the store, so the read has to fetch the chunk.
	cold, err := vfs.Open(bg, vfs.Config{Backend: g, Cipher: c})
	if err != nil {
		t.Fatal(err)
	}
	file, err := cold.Open(bg, in.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	defer g.free()
	h := newSession(bg, cold).handle(file)

	g.armed.Store(true)
	req, interrupt := context.WithCancel(bg)
	resp := &fuse.ReadResponse{}
	result := make(chan error, 1)
	go func() { result <- h.Read(req, &fuse.ReadRequest{Size: 50_000, Offset: 0}, resp) }()
	select {
	case <-g.reached:
	case <-time.After(20 * time.Second):
		t.Fatal("the read never reached the store")
	}
	interrupt()
	select {
	case err := <-result:
		t.Fatalf("a read was answered (%v) while its chunk was still being fetched", err)
	case <-time.After(300 * time.Millisecond):
	}
	g.free()
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("the read after an interrupt: %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("the read never returned")
	}
	if g.heldErr != nil {
		t.Fatalf("the fetch was cancelled with the request that asked for it: %v", g.heldErr)
	}
	if string(resp.Data) != string(data[:50_000]) {
		t.Fatalf("the read returned %d wrong bytes", len(resp.Data))
	}
}

// TestInterruptedFsyncRetriesDoNotPileUp: a caller retries fsync(2) on every EINTR
// while the store is stalled. The commits are shared: however many retries arrive,
// one commit is running and at most one more is waiting to start, never one per retry.
func TestInterruptedFsyncRetriesDoNotPileUp(t *testing.T) {
	g, v, c, mem := testVolume(t)
	bg := context.Background()
	in, err := v.Create(bg, vfs.RootInode, "f", 0o644)
	if err != nil {
		t.Fatal(err)
	}
	f, err := v.Open(bg, in.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	defer g.free()
	if _, err := f.WriteAt(bg, make([]byte, 100_000), 0); err != nil {
		t.Fatal(err)
	}
	cm := newCommitter(v)
	base := runtime.NumGoroutine()
	g.armed.Store(true)
	first, cancelFirst := context.WithCancel(bg)
	go func() { _ = cm.commit(first) }()
	select {
	case <-g.reached:
	case <-time.After(20 * time.Second):
		t.Fatal("the commit never reached the Head")
	}
	cancelFirst()
	const retries = 5000
	for range retries {
		req, cancel := context.WithTimeout(bg, time.Microsecond)
		_ = cm.commit(req) // answered EINTR
		cancel()
	}
	if held := runtime.NumGoroutine() - base; held > 8 {
		t.Fatalf("%d interrupted retries left %d goroutines running commits: the work is not shared", retries, held)
	}

	// The store answers: the writes are committed once, by the flights that were
	// there, and every caller after that finds nothing left.
	g.free()
	if err := cm.commit(bg); err != nil {
		t.Fatalf("a commit after the store answered: %v", err)
	}
	ro, err := vfs.Open(bg, vfs.Config{Backend: mem, Cipher: c})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := ro.Resolve(bg, "/f"); err != nil || got.Size != 100_000 {
		t.Fatalf("the write is not in the store: %+v %v", got, err)
	}
	if heads := mem.Puts()["head"]; heads > 4 {
		t.Fatalf("%d Head writes for one write and %d retries", heads, retries)
	}
}

// TestSessionRefusesANilContext: a session without the mount's context would hand
// every node a nil one, and the first read of a chunk against a real store would
// panic. It cannot be built.
func TestSessionRefusesANilContext(t *testing.T) {
	_, v, _, _ := testVolume(t)
	defer func() {
		if recover() == nil {
			t.Fatal("a session was built without a context")
		}
	}()
	newSession(nil, v)
}

// TestLookedUpFileReadsOnTheMountContext: the kernel finds every file that is not new
// to the mount session with Lookup and reads it through the node Lookup returns. The
// store here panics on a nil context, as every real store's client does; the
// directory backend would never have noticed.
func TestLookedUpFileReadsOnTheMountContext(t *testing.T) {
	_, v, c, mem := testVolume(t)
	bg := context.Background()
	in, err := v.Create(bg, vfs.RootInode, "f", 0o644)
	if err != nil {
		t.Fatal(err)
	}
	f, err := v.Open(bg, in.ID)
	if err != nil {
		t.Fatal(err)
	}
	want := make([]byte, 300_000)
	for i := range want {
		want[i] = byte(i * 7)
	}
	if _, err := f.WriteAt(bg, want, 0); err != nil {
		t.Fatal(err)
	}
	f.Close()
	if err := v.Flush(bg); err != nil {
		t.Fatal(err)
	}
	// A fresh server over the same store: nothing cached.
	v2, err := vfs.Attach(bg, vfs.Config{Backend: storetest.Strict{Backend: mem}, Cipher: c, Round: 2})
	if err != nil {
		t.Fatal(err)
	}
	root := newSession(bg, v2).node(vfs.RootInode, vfs.RootInode)
	// What the kernel does for open("/f"): LOOKUP, OPEN, READ.
	found, err := root.Lookup(bg, "f")
	if err != nil {
		t.Fatal(err)
	}
	h, err := found.(*node).Open(bg, &fuse.OpenRequest{}, &fuse.OpenResponse{})
	if err != nil {
		t.Fatal(err)
	}
	resp := &fuse.ReadResponse{}
	if err := h.(*handle).Read(bg, &fuse.ReadRequest{Size: 50_000, Offset: 0}, resp); err != nil {
		t.Fatalf("read of a file found by Lookup: %v", err)
	}
	if string(resp.Data) != string(want[:50_000]) {
		t.Fatalf("read of a file found by Lookup returned %d wrong bytes", len(resp.Data))
	}
	dir, err := root.Mkdir(bg, &fuse.MkdirRequest{Name: "d", Mode: 0o755})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := dir.(*node).Lookup(bg, "nothing"); err == nil {
		t.Fatal("a lookup in a new directory found a file that is not there")
	}
}
