package vfs_test

import (
	"bytes"
	"fmt"
	"io"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/internal/storetest"
)

// TestConcurrentWorkers: eight workers, each with its own directory and its
// own byte-exact model, mix writes, truncates, removes, reads and Flushes on one
// Volume while another goroutine Flushes and another reads every directory.
// Run with -race.
func TestConcurrentWorkers(t *testing.T) {
	mem := storetest.NewMem()
	c := vfsAge(t)
	if err := vfs.Create(ctx, cfg(mem, c, 0)); err != nil {
		t.Fatal(err)
	}
	cf := cfg(mem, c, 1)
	cf.Dirty, cf.Overlay, cf.Cache = 2<<20, 50, 4<<20
	v, err := vfs.Attach(ctx, cf)
	if err != nil {
		t.Fatal(err)
	}
	const workers = 8
	models := make([]map[string][]byte, workers)
	var wg sync.WaitGroup
	stop := make(chan struct{})
	dirs := make([]vfs.InodeID, workers)
	for w := range workers {
		dirs[w] = mkdir(t, v, vfs.RootInode, fmt.Sprint("w", w))
		models[w] = map[string][]byte{}
	}
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := rand.New(rand.NewPCG(uint64(w), 5))
			m := models[w]
			for step := 0; step < 120; step++ {
				name := fmt.Sprintf("f%d", r.IntN(6))
				cur, exists := m[name]
				switch op := r.IntN(10); {
				case !exists || op == 0:
					if exists {
						if err := v.Remove(ctx, dirs[w], name); err != nil {
							t.Errorf("remove: %v", err)
							return
						}
						delete(m, name)
						continue
					}
					in, err := v.Create(ctx, dirs[w], name, 0o644)
					if err != nil {
						t.Errorf("create: %v", err)
						return
					}
					d := make([]byte, r.IntN(300<<10))
					for i := range d {
						d[i] = byte(r.Uint32())
					}
					f, _ := v.Open(ctx, in.ID)
					f.WriteAt(ctx, d, 0)
					f.Close()
					m[name] = d
				case op < 5:
					in, err := v.Lookup(ctx, dirs[w], name)
					if err != nil {
						t.Errorf("lookup %s: %v", name, err)
						return
					}
					off := r.IntN(len(cur) + 1)
					d := make([]byte, 1+r.IntN(200<<10))
					for i := range d {
						d[i] = byte(r.Uint32())
					}
					f, _ := v.Open(ctx, in.ID)
					if _, err := f.WriteAt(ctx, d, int64(off)); err != nil {
						t.Errorf("write: %v", err)
					}
					f.Close()
					if off+len(d) > len(cur) {
						cur = append(cur, make([]byte, off+len(d)-len(cur))...)
					}
					copy(cur[off:], d)
					m[name] = cur
				case op < 6:
					in, _ := v.Lookup(ctx, dirs[w], name)
					sz := r.IntN(len(cur) + 100<<10)
					if err := v.Truncate(ctx, in.ID, uint64(sz)); err != nil {
						t.Errorf("truncate: %v", err)
					}
					if sz < len(cur) {
						cur = cur[:sz]
					} else {
						cur = append(cur, make([]byte, sz-len(cur))...)
					}
					m[name] = cur
				case op < 8:
					in, err := v.Lookup(ctx, dirs[w], name)
					if err != nil {
						t.Errorf("lookup %s: %v", name, err)
						return
					}
					f, _ := v.Open(ctx, in.ID)
					buf := make([]byte, len(cur)+1)
					n, err := f.ReadAt(ctx, buf, 0)
					f.Close()
					if err != nil && err != io.EOF || !bytes.Equal(buf[:n], cur) {
						t.Errorf("worker %d: %s reads %d bytes (%v), model %d", w, name, n, err, len(cur))
						return
					}
				default:
					if err := v.Flush(ctx); err != nil {
						t.Errorf("flush: %v", err)
						return
					}
				}
			}
		}()
	}
	var bg sync.WaitGroup
	bg.Add(2)
	go func() {
		defer bg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				_ = v.Flush(ctx)
				time.Sleep(time.Millisecond)
			}
		}
	}()
	go func() {
		defer bg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				for _, d := range dirs {
					if _, err := v.ReadDir(ctx, d); err != nil {
						t.Errorf("readdir: %v", err)
					}
				}
				_ = v.Stats()
			}
		}
	}()
	wg.Wait()
	close(stop)
	bg.Wait()
	flush(t, v)
	if st := v.Stats(); st.Dirty != 0 || st.Pending != 0 {
		t.Fatalf("after Flush: Dirty=%d Pending=%d, want 0 0 (accounting drift)", st.Dirty, st.Pending)
	}
	ro := reopen(t, mem, c)
	for w := range workers {
		for name, want := range models[w] {
			in, err := ro.Resolve(ctx, fmt.Sprintf("/w%d/%s", w, name))
			if err != nil {
				t.Fatalf("committed /w%d/%s: %v", w, name, err)
			}
			mustEqual(t, fmt.Sprintf("/w%d/%s", w, name), readAll(t, ro, in.ID), want)
		}
	}
}
