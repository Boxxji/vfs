package vfs_test

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/hanzoai/vfs"
)

// fileVolume attaches a new Volume on the file backend under the test's temp dir.
func fileVolume(tb testing.TB) (dir string, c vfs.Cipher, v *vfs.Volume) {
	dir = tb.TempDir()
	c = vfs.Hybrid(newKey(tb))
	if err := vfs.Create(ctx, cfg(reopenBackendTB(tb, dir), c, 0)); err != nil {
		tb.Fatal(err)
	}
	v, err := vfs.Attach(ctx, cfg(reopenBackendTB(tb, dir), c, 1))
	if err != nil {
		tb.Fatal(err)
	}
	return dir, c, v
}

func percentile(ds []time.Duration, p float64) time.Duration {
	s := slices.Clone(ds)
	slices.Sort(s)
	return s[min(len(s)-1, int(float64(len(s))*p))]
}

// BenchmarkSequentialWrite writes 128 MiB in 1 MiB writes through the file
// backend and commits.
func BenchmarkSequentialWrite(b *testing.B) {
	data := content(1, 128<<20) // distinct bytes throughout: nothing to dedupe
	b.SetBytes(128 << 20)
	for range b.N {
		b.StopTimer()
		_, _, v := fileVolume(b)
		in, _ := v.Create(ctx, vfs.RootInode, "f", 0o644)
		f, _ := v.Open(ctx, in.ID)
		b.StartTimer()
		for i := range 128 {
			if _, err := f.WriteAt(ctx, data[i<<20:(i+1)<<20], int64(i)<<20); err != nil {
				b.Fatal(err)
			}
		}
		flush(b, v)
	}
}

// BenchmarkSequentialRead reads back 128 MiB through a cold read-only Volume.
func BenchmarkSequentialRead(b *testing.B) {
	dir, c, v := fileVolume(b)
	in, _ := v.Create(ctx, vfs.RootInode, "f", 0o644)
	f, _ := v.Open(ctx, in.ID)
	data := content(2, 128<<20)
	for i := range 128 {
		f.WriteAt(ctx, data[i<<20:(i+1)<<20], int64(i)<<20)
	}
	flush(b, v)
	b.SetBytes(128 << 20)
	b.ResetTimer()
	for range b.N {
		ro, err := vfs.Open(ctx, cfg(reopenBackendTB(b, dir), c, 0))
		if err != nil {
			b.Fatal(err)
		}
		g, _ := ro.Open(ctx, in.ID)
		buf := make([]byte, 1<<20)
		for i := range 128 {
			if _, err := g.ReadAt(ctx, buf, int64(i)<<20); err != nil {
				b.Fatal(err)
			}
		}
	}
}

// TestFlushLatency measures what an fsync costs: a 4 KiB overwrite in a 16 MiB
// file followed by Flush, on the file backend (a tmpfs, where the file backend's
// own fsync costs nothing, so this is the engine: chunking, sealing, shard and
// Head writes).
func TestFlushLatency(t *testing.T) {
	if testing.Short() {
		t.Skip("measurement")
	}
	_, _, v := fileVolume(t)
	in, _ := v.Create(ctx, vfs.RootInode, "db", 0o644)
	f, _ := v.Open(ctx, in.ID)
	f.WriteAt(ctx, content(3, 16<<20), 0)
	flush(t, v)
	page := content(4, 4096)
	var ds []time.Duration
	for i := range 100 {
		f.WriteAt(ctx, page, int64(i*37%4000)*4096)
		start := time.Now()
		flush(t, v)
		ds = append(ds, time.Since(start))
	}
	t.Logf("4 KiB write + Flush in a 16 MiB file, 100 rounds: p50 %v, p90 %v, p98 %v, max %v (%.1f/s at p50)",
		percentile(ds, 0.5).Round(time.Millisecond), percentile(ds, 0.9).Round(time.Millisecond), percentile(ds, 0.98).Round(time.Millisecond), percentile(ds, 1).Round(time.Millisecond), 1/percentile(ds, 0.5).Seconds())
}

// TestSustainedWriteWithFsync is the workload the previous engine's notes measured: 64 KiB
// writes round-robin over 16 files, a Flush after every write, for 10 seconds.
func TestSustainedWriteWithFsync(t *testing.T) {
	if testing.Short() {
		t.Skip("measurement")
	}
	_, _, v := fileVolume(t)
	files := make([]*vfs.File, 16)
	for i := range files {
		in, _ := v.Create(ctx, vfs.RootInode, fmt.Sprint("f", i), 0o644)
		files[i], _ = v.Open(ctx, in.ID)
	}
	chunk := content(5, 64<<10)
	var ds []time.Duration
	end := time.Now().Add(10 * time.Second)
	for i := 0; time.Now().Before(end); i++ {
		start := time.Now()
		if _, err := files[i%16].WriteAt(ctx, chunk, int64(i/16)*(64<<10)); err != nil {
			t.Fatal(err)
		}
		flush(t, v)
		ds = append(ds, time.Since(start))
	}
	var sum time.Duration
	for _, d := range ds {
		sum += d
	}
	t.Logf("64 KiB write + Flush over 16 files, 10 s: %d ops (%.1f/s), mean %v, p50 %v, p99 %v, max %v",
		len(ds), float64(len(ds))/10, (sum / time.Duration(len(ds))).Round(time.Millisecond),
		percentile(ds, 0.5).Round(time.Millisecond), percentile(ds, 0.99).Round(time.Millisecond), percentile(ds, 1).Round(time.Millisecond))
}

// TestObjectsPerThousandSmallFiles counts the objects 1,000 small files cost.
func TestObjectsPerThousandSmallFiles(t *testing.T) {
	if testing.Short() {
		t.Skip("a measurement: the scale step runs it without the race detector")
	}
	be, _, v := newMem(t)
	dir := mkdir(t, v, vfs.RootInode, "d")
	be.Reset()
	for i := range 1000 {
		write(t, v, dir, fmt.Sprintf("f%04d", i), content(uint64(i), 200))
	}
	flush(t, v)
	puts := be.Puts()
	t.Logf("1,000 files of 200 bytes: %d shards, %d packs, %d head = %d objects", puts["meta"], puts["pack"], puts["head"], be.Total())
	if be.Total() > 10 {
		t.Fatalf("1,000 files of 200 bytes cost %d objects", be.Total())
	}
	be.Reset()
	for i := range 1000 {
		write(t, v, dir, fmt.Sprintf("m%04d", i), content(uint64(i), 2000+i))
	}
	flush(t, v)
	puts = be.Puts()
	t.Logf("1,000 files of about 2.5 KiB: %d shards, %d packs, %d head = %d objects (their bytes are in the shards: about one shard per 64 KiB)", puts["meta"], puts["pack"], puts["head"], be.Total())
	if be.Total() > 60 {
		t.Fatalf("1,000 files of 2.5 KiB cost %d objects", be.Total())
	}
	// And 1,000 files of 100 KiB: they are chunked, and pack together.
	be.Reset()
	for i := range 1000 {
		write(t, v, dir, fmt.Sprintf("g%04d", i), content(uint64(5000+i), 100<<10))
	}
	flush(t, v)
	puts = be.Puts()
	t.Logf("1,000 files of 100 KiB: %d shards, %d packs, %d head = %d objects", puts["meta"], puts["pack"], puts["head"], be.Total())
}
