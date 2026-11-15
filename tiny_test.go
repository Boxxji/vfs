package vfs_test

// What sparse extents cost. Flush time, objects, bytes and RSS must
// follow the number of extents. Measured for a few scattered writes in 1 PiB and
// for 10,000 and 100,000 tiny extents, distinct and identical.

import (
	"bytes"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/hanzoai/vfs"
)

func TestTinyExtentsCost(t *testing.T) {
	for _, tc := range []struct {
		n        int
		distinct bool
	}{{1000, true}, {10_000, true}, {100_000, true}, {100_000, false}} {
		t.Run(fmt.Sprintf("n=%d distinct=%v", tc.n, tc.distinct), func(t *testing.T) {
			if testing.Short() && tc.n > 1000 {
				t.Skip("a measurement: the scale step runs it without the race detector")
			}
			be, c, v := newMem(t)
			in, err := v.Create(ctx, vfs.RootInode, "sieve", 0o644)
			if err != nil {
				t.Fatal(err)
			}
			f, _ := v.Open(ctx, in.ID)
			be.Reset()
			runtime.GC()
			var m0, m1 runtime.MemStats
			runtime.ReadMemStats(&m0)
			rss0 := rss(t)
			start := time.Now()
			if err := f.Truncate(ctx, 1<<50); err != nil {
				t.Fatal(err)
			}
			stride := int64(10 << 20)
			for i := 0; i < tc.n; i++ {
				d := []byte(fmt.Sprintf("tiny extent %010d", i))
				if !tc.distinct {
					d = []byte("tiny extent same")
				}
				if _, err := f.WriteAt(ctx, d, int64(i)*stride); err != nil {
					t.Fatal(err)
				}
			}
			wrote := time.Since(start)
			peak := rss(t)
			t0 := time.Now()
			flush(t, v)
			fl := time.Since(t0)
			runtime.ReadMemStats(&m1)
			puts := be.Puts()
			var shardBytes int
			for k, b := range be.Dump() {
				if strings.HasPrefix(k, "meta/") {
					shardBytes += len(b)
				}
			}
			t.Logf("n=%d distinct=%v: %d writes in %v; Flush %v; puts meta=%d pack=%d head=%d; shards %d KiB; packs %d KiB; heap +%d MiB; RSS +%d MiB (after writes) +%d MiB (after flush)",
				tc.n, tc.distinct, tc.n, wrote.Round(time.Millisecond), fl.Round(time.Millisecond), puts["meta"], puts["pack"], puts["head"],
				shardBytes>>10, packBytes(be)>>10, (int64(m1.HeapInuse)-int64(m0.HeapInuse))>>20, (int64(peak)-int64(rss0))>>20, (int64(rss(t))-int64(rss0))>>20)

			// One more tiny edit: the second Flush's cost.
			if _, err := f.WriteAt(ctx, []byte("edit"), 5*stride+100); err != nil {
				t.Fatal(err)
			}
			be.Reset()
			t0 = time.Now()
			flush(t, v)
			t.Logf("second Flush after one 4-byte edit: %v; puts %v", time.Since(t0).Round(time.Millisecond), be.Puts())

			// Cold read of every extent.
			cold := reopen(t, be, c)
			h, _ := cold.Open(ctx, in.ID)
			t0 = time.Now()
			buf := make([]byte, 32)
			bad := 0
			step := max(1, tc.n/2000)
			for i := 0; i < tc.n; i += step {
				want := []byte(fmt.Sprintf("tiny extent %010d", i))
				if !tc.distinct {
					want = []byte("tiny extent same")
				}
				if _, err := h.ReadAt(ctx, buf, int64(i)*stride); err != nil && err.Error() != "EOF" {
					t.Fatal(err)
				}
				if !bytes.Equal(buf[:len(want)], want) {
					bad++
				}
			}
			t.Logf("cold read of %d spots: %v, %d wrong", tc.n/step, time.Since(t0).Round(time.Millisecond), bad)
			if bad != 0 {
				t.Fatalf("%d wrong reads", bad)
			}
		})
	}
}

func TestPetabyteScattered(t *testing.T) {
	be, c, v := newMem(t)
	in, _ := v.Create(ctx, vfs.RootInode, "pib", 0o644)
	f, _ := v.Open(ctx, in.ID)
	be.Reset()
	rss0 := rss(t)
	t0 := time.Now()
	if err := f.Truncate(ctx, 1<<50); err != nil {
		t.Fatal(err)
	}
	offs := []int64{0, 1, 16383, 16384, 65535, 65536, 4<<20 - 1, 4 << 20, 1 << 30, 1 << 40, 1<<50 - 1}
	for i, off := range offs {
		if _, err := f.WriteAt(ctx, []byte{byte(i + 1)}, off); err != nil {
			t.Fatal(err)
		}
	}
	flush(t, v)
	t.Logf("1 PiB, %d scattered bytes: %v, puts %v, RSS +%d MiB", len(offs), time.Since(t0).Round(time.Millisecond), be.Puts(), (int64(rss(t))-int64(rss0))>>20)
	cold := reopen(t, be, c)
	h, _ := cold.Open(ctx, in.ID)
	for i, off := range offs {
		b := make([]byte, 1)
		if _, err := h.ReadAt(ctx, b, off); err != nil && err.Error() != "EOF" {
			t.Fatal(err)
		}
		if b[0] != byte(i+1) {
			t.Fatalf("byte at %d = %d, want %d", off, b[0], i+1)
		}
	}
	// Remove: cost of removing a sparse file with many extents.
	t0 = time.Now()
	if err := v.Remove(ctx, vfs.RootInode, "pib"); err != nil {
		t.Fatal(err)
	}
	flush(t, v)
	t.Logf("remove + flush: %v", time.Since(t0).Round(time.Millisecond))
}
