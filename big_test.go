package vfs_test

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/pkg/backend"
)

// discard keeps heads and shards and throws packs away (counting their bytes),
// so a 50 GiB write measures the Volume's memory, not the store's.
type discard struct {
	mu    sync.Mutex
	objs  map[string][]byte
	packs int64
	n     int
}

func (d *discard) Get(_ context.Context, k string, _ int64) ([]byte, backend.Version, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	b, ok := d.objs[k]
	if !ok {
		return nil, "", fmt.Errorf("%w: %s", backend.ErrNotFound, k)
	}
	return b, backend.Version(fmt.Sprintf("%x", len(b)) + k + fmt.Sprint(d.n)), nil
}
func (d *discard) GetRange(context.Context, string, int64, int64) ([]byte, error) {
	return nil, fmt.Errorf("discard: pack bytes are not kept")
}
func (d *discard) put(k string, b []byte) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(k) > 5 && k[:5] == "pack/" {
		d.packs += int64(len(b))
		d.n++
		return
	}
	d.objs[k] = append([]byte(nil), b...)
	d.n++
}
func (d *discard) Put(_ context.Context, k string, b []byte) error { d.put(k, b); return nil }
func (d *discard) PutIfAbsent(_ context.Context, k string, b []byte) (backend.Version, error) {
	d.mu.Lock()
	_, ok := d.objs[k]
	d.mu.Unlock()
	if ok {
		return "", backend.ErrConflict
	}
	d.put(k, b)
	return "v", nil
}
func (d *discard) PutIfVersion(_ context.Context, k string, b []byte, v backend.Version) (backend.Version, error) {
	d.mu.Lock()
	cur, ok := d.objs[k]
	d.mu.Unlock()
	if !ok || backend.Version(fmt.Sprintf("%x", len(cur))+k+fmt.Sprint(d.n)) != v {
		return "", backend.ErrConflict
	}
	d.put(k, b)
	return "v", nil
}
func (d *discard) Delete(context.Context, string) error { return nil }
func (d *discard) List(context.Context, string) (<-chan string, <-chan error) {
	c, e := make(chan string), make(chan error)
	close(c)
	close(e)
	return c, e
}
func (d *discard) Stat(context.Context, string) (int64, error) { return 0, nil }
func (d *discard) Close() error                                { return nil }
func (d *discard) String() string                              { return "discard://" }

// TestSequentialWriteMemory writes N GiB sequentially (sequentialGiB; 4 GiB by default)
// through one handle with a Flush every 1 GiB (as a closing tool would) and
// reports the Volume's memory. Run by name.
func TestSequentialWriteMemory(t *testing.T) {
	if testing.Short() {
		t.Skip("a measurement: the scale step runs it without the race detector")
	}
	mib := int64(sequentialGiB) << 10
	if sequentialGiB == 0 {
		mib = 4096
	}
	d := &discard{objs: map[string][]byte{}}
	c := vfsAge(t)
	if err := vfs.Create(ctx, cfg(d, c, 0)); err != nil {
		t.Fatal(err)
	}
	v, err := vfs.Attach(ctx, cfg(d, c, 1))
	if err != nil {
		t.Fatal(err)
	}
	in, _ := v.Create(ctx, vfs.RootInode, "big", 0o644)
	f, _ := v.Open(ctx, in.ID)
	buf := make([]byte, 1<<20)
	var s uint64 = 0x9e3779b97f4a7c15
	var peak uint64
	var m runtime.MemStats
	start := time.Now()
	total := mib << 20
	for off := int64(0); off < total; off += int64(len(buf)) {
		for i := 0; i < len(buf); i += 8 { // xorshift fill: incompressible, fast
			s ^= s << 13
			s ^= s >> 7
			s ^= s << 17
			buf[i], buf[i+1], buf[i+2], buf[i+3] = byte(s), byte(s>>8), byte(s>>16), byte(s>>24)
			buf[i+4], buf[i+5], buf[i+6], buf[i+7] = byte(s>>32), byte(s>>40), byte(s>>48), byte(s>>56)
		}
		if _, err := f.WriteAt(ctx, buf, off); err != nil {
			t.Fatal(err)
		}
		if off%(512<<20) == 0 {
			runtime.ReadMemStats(&m)
			peak = max(peak, m.HeapInuse)
		}
		if off%(1<<30) == 0 && off > 0 {
			if err := v.Flush(ctx); err != nil {
				t.Fatal(err)
			}
			t.Logf("%d GiB at %v: heap in use %d MiB, sys %d MiB, packs %d MiB in %d objects", off>>30, time.Since(start).Round(time.Second), m.HeapInuse>>20, m.Sys>>20, d.packs>>20, d.n)
		}
	}
	if err := v.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	runtime.ReadMemStats(&m)
	t.Logf("wrote %d MiB in %v (%.0f MB/s); peak heap %d MiB; final heap %d MiB sys %d MiB; %d objects, %d MiB of packs",
		mib, time.Since(start).Round(time.Second), float64(total)/1e6/time.Since(start).Seconds(), peak>>20, m.HeapInuse>>20, m.Sys>>20, d.n, d.packs>>20)
	if peak > 1<<30 {
		t.Fatalf("peak heap %d MiB for a sequential write: memory grows with the file", peak>>20)
	}
}

// sequentialGiB is how much TestSequentialWriteMemory writes: 4 GiB, a quarter of a
// GiB under -short, 50 when TestSequentialWrite50GiB runs it.
var sequentialGiB = 0

func TestSequentialWrite50GiB(t *testing.T) {
	if testing.Short() {
		t.Skip("50 GiB")
	}
	sequentialGiB = 50
	TestSequentialWriteMemory(t)
}

// TestMillionEntryDirectory lists one directory of 1,000,000 entries and
// reports the heap one ReadDir costs, and the cost of the rmdir emptiness check.
func TestMillionEntryDirectory(t *testing.T) {
	if testing.Short() {
		t.Skip("1M entries")
	}
	_, c, v := newMem(t)
	_ = c
	dir := mkdir(t, v, vfs.RootInode, "huge")
	for i := range 1_000_000 {
		if _, err := v.Create(ctx, dir, fmt.Sprintf("entry-%08d", i), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	flush(t, v)
	runtime.GC()
	var m0, m1 runtime.MemStats
	runtime.ReadMemStats(&m0)
	start := time.Now()
	ents, err := v.ReadDir(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	runtime.ReadMemStats(&m1)
	t.Logf("ReadDir of %d entries: %v, allocated %d MiB in one call (live %d MiB), lock held throughout", len(ents), time.Since(start).Round(time.Millisecond), (m1.TotalAlloc-m0.TotalAlloc)>>20, (m1.HeapInuse-m0.HeapInuse)>>20)
	runtime.KeepAlive(ents)
	// rmdir of a non-empty directory: the emptiness check scans all.
	start = time.Now()
	runtime.ReadMemStats(&m0)
	err = v.Remove(ctx, vfs.RootInode, "huge")
	runtime.ReadMemStats(&m1)
	t.Logf("rmdir of the non-empty directory: %v after %v, allocated %d MiB", err, time.Since(start).Round(time.Millisecond), (m1.TotalAlloc-m0.TotalAlloc)>>20)
	if got := (m1.TotalAlloc - m0.TotalAlloc) >> 20; got > 64 {
		t.Fatalf("rmdir's emptiness check allocated %d MiB to learn one entry exists", got)
	}
}
