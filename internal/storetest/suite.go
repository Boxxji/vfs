package storetest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/hanzoai/vfs/pkg/backend"
)

// Run is the conformance suite: what every Backend must do, in particular the
// two conditional writes the fence and the Head commit rest on. open returns a
// fresh, empty backend.
func Run(t *testing.T, open func(t *testing.T) backend.Backend) {
	ctx := context.Background()

	t.Run("get put range delete", func(t *testing.T) {
		be := open(t)
		if _, _, err := be.Get(ctx, "a/b", backend.Unbounded); !errors.Is(err, backend.ErrNotFound) {
			t.Fatalf("Get absent: %v, want ErrNotFound", err)
		}
		if err := be.Put(ctx, "a/b", []byte("0123456789")); err != nil {
			t.Fatal(err)
		}
		got, v, err := be.Get(ctx, "a/b", backend.Unbounded)
		if err != nil || string(got) != "0123456789" || v == "" {
			t.Fatalf("Get: %q %q %v", got, v, err)
		}
		if _, v2, _ := be.Get(ctx, "a/b", backend.Unbounded); v2 != v {
			t.Fatalf("version of unchanged key moved: %q then %q", v, v2)
		}
		r, err := be.GetRange(ctx, "a/b", 3, 4)
		if err != nil || string(r) != "3456" {
			t.Fatalf("GetRange: %q %v", r, err)
		}
		if _, err := be.GetRange(ctx, "a/b", 8, 5); err == nil {
			t.Fatal("GetRange past the end returned no error")
		}
		if _, err := be.GetRange(ctx, "a/none", 0, 1); !errors.Is(err, backend.ErrNotFound) {
			t.Fatalf("GetRange absent: %v, want ErrNotFound", err)
		}
		if n, err := be.Stat(ctx, "a/b"); err != nil || n != 10 {
			t.Fatalf("Stat: %d %v", n, err)
		}
		if err := be.Delete(ctx, "a/b"); err != nil {
			t.Fatal(err)
		}
		if err := be.Delete(ctx, "a/b"); err != nil {
			t.Fatalf("Delete absent: %v", err)
		}
		if _, _, err := be.Get(ctx, "a/b", backend.Unbounded); !errors.Is(err, backend.ErrNotFound) {
			t.Fatalf("Get after Delete: %v", err)
		}
	})

	t.Run("put if absent", func(t *testing.T) {
		be := open(t)
		v1, err := be.PutIfAbsent(ctx, "head", []byte("one"))
		if err != nil || v1 == "" {
			t.Fatalf("first PutIfAbsent: %q %v", v1, err)
		}
		if _, err := be.PutIfAbsent(ctx, "head", []byte("two")); !errors.Is(err, backend.ErrConflict) {
			t.Fatalf("second PutIfAbsent: %v, want ErrConflict", err)
		}
		got, v, _ := be.Get(ctx, "head", backend.Unbounded)
		if string(got) != "one" || v != v1 {
			t.Fatalf("a refused PutIfAbsent changed the object: %q %q (want one %q)", got, v, v1)
		}
	})

	t.Run("put if version", func(t *testing.T) {
		be := open(t)
		if _, err := be.PutIfVersion(ctx, "head", []byte("x"), "nothing"); !errors.Is(err, backend.ErrConflict) {
			t.Fatalf("PutIfVersion on absent: %v, want ErrConflict", err)
		}
		if _, _, err := be.Get(ctx, "head", backend.Unbounded); err == nil {
			t.Fatal("PutIfVersion on an absent key created it")
		}
		v1, _ := be.PutIfAbsent(ctx, "head", []byte("one"))
		v2, err := be.PutIfVersion(ctx, "head", []byte("two"), v1)
		if err != nil || v2 == v1 {
			t.Fatalf("PutIfVersion at the current version: %q %v", v2, err)
		}
		if _, err := be.PutIfVersion(ctx, "head", []byte("three"), v1); !errors.Is(err, backend.ErrConflict) {
			t.Fatalf("PutIfVersion at a stale version: %v, want ErrConflict", err)
		}
		got, v, _ := be.Get(ctx, "head", backend.Unbounded)
		if string(got) != "two" || v != v2 {
			t.Fatalf("object after a refused write: %q %q, want two %q", got, v, v2)
		}
		if _, err := be.PutIfVersion(ctx, "head", []byte("three"), v2); err != nil {
			t.Fatalf("PutIfVersion at the returned version: %v", err)
		}
	})

	t.Run("exactly one winner", func(t *testing.T) {
		be := open(t)
		v0, _ := be.PutIfAbsent(ctx, "head", []byte("base"))
		for round := range 20 {
			_, v, _ := be.Get(ctx, "head", backend.Unbounded)
			const racers = 8
			var wins atomic.Int32
			var wg sync.WaitGroup
			start := make(chan struct{})
			for i := range racers {
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					_, err := be.PutIfVersion(ctx, "head", []byte(fmt.Sprintf("r%d-w%d", round, i)), v)
					switch {
					case err == nil:
						wins.Add(1)
					case !errors.Is(err, backend.ErrConflict):
						t.Errorf("racer %d: %v", i, err)
					}
				}()
			}
			close(start)
			wg.Wait()
			if wins.Load() != 1 {
				t.Fatalf("round %d: %d of %d racers on version %q won, want exactly 1", round, wins.Load(), racers, v)
			}
		}
		_ = v0

		// The same race for creation: exactly one PutIfAbsent wins.
		var wins atomic.Int32
		var wg sync.WaitGroup
		start := make(chan struct{})
		for i := range 8 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				if _, err := be.PutIfAbsent(ctx, "fresh", []byte{byte(i)}); err == nil {
					wins.Add(1)
				} else if !errors.Is(err, backend.ErrConflict) {
					t.Errorf("creator %d: %v", i, err)
				}
			}()
		}
		close(start)
		wg.Wait()
		if wins.Load() != 1 {
			t.Fatalf("%d creators won, want exactly 1", wins.Load())
		}
	})

	t.Run("list", func(t *testing.T) {
		be := open(t)
		for _, k := range []string{"pack/aa/1", "pack/aa/2", "pack/bb/3", "head"} {
			if err := be.Put(ctx, k, []byte("x")); err != nil {
				t.Fatal(err)
			}
		}
		ch, errCh := be.List(ctx, "pack/aa")
		var got []string
		for k := range ch {
			got = append(got, k)
		}
		if err := <-errCh; err != nil {
			t.Fatal(err)
		}
		if len(got) != 2 {
			t.Fatalf("List(pack/aa) = %v, want 2 keys", got)
		}
	})

	t.Run("get is bounded", func(t *testing.T) {
		be := open(t)
		if err := be.Put(ctx, "k", []byte("0123456789")); err != nil {
			t.Fatal(err)
		}
		if b, _, err := be.Get(ctx, "k", 10); err != nil || len(b) != 10 {
			t.Fatalf("Get at exactly max: %d bytes, %v", len(b), err)
		}
		if b, _, err := be.Get(ctx, "k", 9); !errors.Is(err, backend.ErrTooLarge) || len(b) != 0 {
			t.Fatalf("Get over max = %d bytes, %v; want ErrTooLarge and nothing", len(b), err)
		}
	})

	t.Run("large object", func(t *testing.T) {
		be := open(t)
		big := bytes.Repeat([]byte("0123456789abcdef"), 1<<18) // 4 MiB
		if err := be.Put(ctx, "pack/aa/big", big); err != nil {
			t.Fatal(err)
		}
		got, _, err := be.Get(ctx, "pack/aa/big", backend.Unbounded)
		if err != nil || !bytes.Equal(got, big) {
			t.Fatalf("large object round trip failed: %v", err)
		}
		r, err := be.GetRange(ctx, "pack/aa/big", 1<<20+5, 1000)
		if err != nil || !bytes.Equal(r, big[1<<20+5:1<<20+5+1000]) {
			t.Fatalf("large object range failed: %v", err)
		}
	})
}
