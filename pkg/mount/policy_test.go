//go:build fuse && linux

package mount

import (
	"context"
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

// holdsMeta holds the first read of a shard until released, as a slow store does, and then
// answers like an HTTP client whose request context has ended.
type holdsMeta struct {
	backend.Backend
	armed   atomic.Bool
	reached chan struct{}
	release chan struct{}
	once    atomic.Bool
}

func (h *holdsMeta) Get(ctx context.Context, key string, max int64) ([]byte, backend.Version, error) {
	if h.armed.Load() && len(key) > 5 && key[:5] == "meta/" && h.once.CompareAndSwap(false, true) {
		close(h.reached)
		<-h.release
		if err := ctx.Err(); err != nil {
			return nil, "", err
		}
	}
	return h.Backend.Get(ctx, key, max)
}

func coldWriter(t *testing.T) (*holdsMeta, *vfs.Volume) {
	t.Helper()
	k, _ := age.GenerateHybridIdentity()
	c := vfs.Hybrid(k)
	bg := context.Background()
	h := &holdsMeta{Backend: storetest.NewMem(), reached: make(chan struct{}), release: make(chan struct{})}
	if err := vfs.Create(bg, vfs.Config{Backend: h, Cipher: c}); err != nil {
		t.Fatal(err)
	}
	w, err := vfs.Attach(bg, vfs.Config{Backend: h, Cipher: c, Round: 1})
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"a", "b", "c"} {
		if _, err := w.Create(bg, vfs.RootInode, name, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Flush(bg); err != nil {
		t.Fatal(err)
	}
	cold, err := vfs.Attach(bg, vfs.Config{Backend: h, Cipher: c, Round: 2})
	if err != nil {
		t.Fatal(err)
	}
	return h, cold
}

func interruptedAt(t *testing.T, h *holdsMeta, call func(ctx context.Context) error) error {
	t.Helper()
	req, interrupt := context.WithCancel(context.Background())
	res := make(chan error, 1)
	h.armed.Store(true)
	go func() { res <- call(req) }()
	select {
	case <-h.reached:
	case <-time.After(20 * time.Second):
		t.Fatal("the call never reached the store")
	}
	interrupt()
	time.Sleep(50 * time.Millisecond)
	close(h.release)
	select {
	case err := <-res:
		return err
	case <-time.After(20 * time.Second):
		t.Fatal("the call never returned")
	}
	return nil
}

// No request is answered EINTR (see errno.go). A lookup of a cold name that is interrupted
// while the store is slow must be answered by its own result.
func TestLookupIsNotAnsweredEINTR(t *testing.T) {
	h, v := coldWriter(t)
	n := newSession(context.Background(), v).node(vfs.RootInode, vfs.RootInode)
	err := interruptedAt(t, h, func(ctx context.Context) error {
		_, err := n.Lookup(ctx, "b")
		return err
	})
	if err == syscall.EINTR {
		t.Errorf("Lookup interrupted while the store was slow was answered EINTR: C callers do not restart stat(2), and SA_RESTART does not restart a FUSE EINTR")
	} else if err != nil {
		t.Errorf("Lookup: %v", err)
	}
}

func TestAttrIsNotAnsweredEINTR(t *testing.T) {
	h, v := coldWriter(t)
	n := newSession(context.Background(), v).node(vfs.RootInode, vfs.RootInode)
	err := interruptedAt(t, h, func(ctx context.Context) error {
		var a fuse.Attr
		return n.Attr(ctx, &a)
	})
	if err == syscall.EINTR {
		t.Errorf("Attr interrupted while the store was slow was answered EINTR")
	}
}

func TestReaddirIsNotAnsweredEINTR(t *testing.T) {
	h, v := coldWriter(t)
	n := newSession(context.Background(), v).node(vfs.RootInode, vfs.RootInode)
	err := interruptedAt(t, h, func(ctx context.Context) error {
		_, err := n.ReadDirAll(ctx)
		return err
	})
	if err == syscall.EINTR {
		t.Errorf("ReadDirAll interrupted while the store was slow was answered EINTR")
	}
}

// Tree-changing calls wait: these fail on a mutant that drops context.WithoutCancel.
func TestMkdirIsNotAnsweredEINTR(t *testing.T) {
	h, v := coldWriter(t)
	n := newSession(context.Background(), v).node(vfs.RootInode, vfs.RootInode)
	err := interruptedAt(t, h, func(ctx context.Context) error {
		_, err := n.Mkdir(ctx, &fuse.MkdirRequest{Name: "new", Mode: 0o755})
		return err
	})
	if err != nil {
		t.Errorf("Mkdir interrupted mid-read = %v, want its own result", err)
	}
}

func TestRemoveIsNotAnsweredEINTR(t *testing.T) {
	h, v := coldWriter(t)
	n := newSession(context.Background(), v).node(vfs.RootInode, vfs.RootInode)
	err := interruptedAt(t, h, func(ctx context.Context) error {
		return n.Remove(ctx, &fuse.RemoveRequest{Name: "b"})
	})
	if err != nil {
		t.Errorf("Remove interrupted mid-read = %v, want its own result", err)
	}
}

func TestCreateIsNotAnsweredEINTR(t *testing.T) {
	h, v := coldWriter(t)
	n := newSession(context.Background(), v).node(vfs.RootInode, vfs.RootInode)
	err := interruptedAt(t, h, func(ctx context.Context) error {
		_, _, err := n.Create(ctx, &fuse.CreateRequest{Name: "fresh", Mode: 0o644}, &fuse.CreateResponse{})
		return err
	})
	if err != nil {
		t.Errorf("Create interrupted mid-read = %v, want its own result", err)
	}
}
