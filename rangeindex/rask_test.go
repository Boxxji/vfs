// Copyright 2026 Hanzo AI, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rangeindex_test

import (
	"fmt"
	"math/rand/v2"
	"reflect"
	"runtime"
	"slices"
	"sort"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/hanzoai/vfs/rangeindex"
)

func TestRaskSplitsOverwritesMerges(t *testing.T) {
	idx := rangeindex.New()

	// 1. Insert contiguous physical extents -> automatically coalesce into one.
	if err := idx.Insert(rangeindex.Extent{Off: 0, Len: 4096, Phys: 10000, Tag: 1}); err != nil {
		t.Fatal(err)
	}
	if err := idx.Insert(rangeindex.Extent{Off: 4096, Len: 4096, Phys: 14096, Tag: 1}); err != nil {
		t.Fatal(err)
	}
	if idx.Len() != 1 || idx.CoveredBytes() != 8192 {
		t.Fatalf("expected coalesced single extent len=1 covered=8192, got len=%d covered=%d", idx.Len(), idx.CoveredBytes())
	}

	// 2. Non-contiguous physical offset or different tag must NOT merge.
	if err := idx.Insert(rangeindex.Extent{Off: 8192, Len: 4096, Phys: 90000, Tag: 1}); err != nil {
		t.Fatal(err)
	}
	if idx.Len() != 2 {
		t.Fatalf("expected non-contiguous Phys not to merge, len=%d", idx.Len())
	}

	// 3. Split in the middle of [0, 8192) with a different tag/phys.
	if err := idx.Insert(rangeindex.Extent{Off: 2048, Len: 2048, Phys: 50000, Tag: 2}); err != nil {
		t.Fatal(err)
	}
	want := []rangeindex.Extent{
		{Off: 0, Len: 2048, Phys: 10000, Tag: 1},
		{Off: 2048, Len: 2048, Phys: 50000, Tag: 2},
		{Off: 4096, Len: 4096, Phys: 14096, Tag: 1},
		{Off: 8192, Len: 4096, Phys: 90000, Tag: 1},
	}
	if got := idx.Extents(); !reflect.DeepEqual(got, want) {
		t.Fatalf("after middle split:\n got %+v\nwant %+v", got, want)
	}

	// 4. Overwrite the middle back with the original contiguous Phys/Tag -> three-way merge!
	if err := idx.Insert(rangeindex.Extent{Off: 2048, Len: 2048, Phys: 12048, Tag: 1}); err != nil {
		t.Fatal(err)
	}
	wantMerged := []rangeindex.Extent{
		{Off: 0, Len: 8192, Phys: 10000, Tag: 1},
		{Off: 8192, Len: 4096, Phys: 90000, Tag: 1},
	}
	if got := idx.Extents(); !reflect.DeepEqual(got, wantMerged) {
		t.Fatalf("after three-way merge:\n got %+v\nwant %+v", got, wantMerged)
	}

	// 5. Hole punch splits [0, 8192) into [0, 1000) and [3000, 8192).
	if err := idx.Delete(1000, 2000); err != nil {
		t.Fatal(err)
	}
	if e, ok := idx.Lookup(1500); ok {
		t.Fatalf("lookup inside punched hole returned %+v", e)
	}
	eRight, ok := idx.Lookup(4000)
	if !ok || eRight != (rangeindex.Extent{Off: 3000, Len: 5192, Phys: 13000, Tag: 1}) {
		t.Fatalf("right split piece mismatch: ok=%v ext=%+v", ok, eRight)
	}

	// 6. Truncate inside [3000, 8192) drops [8192, 12288) and trims right piece.
	if err := idx.Truncate(5000); err != nil {
		t.Fatal(err)
	}
	wantTrunc := []rangeindex.Extent{
		{Off: 0, Len: 1000, Phys: 10000, Tag: 1},
		{Off: 3000, Len: 2000, Phys: 13000, Tag: 1},
	}
	if got := idx.Extents(); !reflect.DeepEqual(got, wantTrunc) {
		t.Fatalf("after truncate:\n got %+v\nwant %+v", got, wantTrunc)
	}
}

func TestRaskMergeAdjacentPolicy(t *testing.T) {
	idx := rangeindex.New(rangeindex.WithMergePolicy(rangeindex.MergeAdjacent))
	_ = idx.Insert(rangeindex.Extent{Off: 0, Len: 4096, Phys: 111, Tag: 1})
	_ = idx.Insert(rangeindex.Extent{Off: 4096, Len: 4096, Phys: 999, Tag: 1})
	if idx.Len() != 1 || idx.CoveredBytes() != 8192 {
		t.Fatalf("MergeAdjacent did not coalesce adjacent same-tag extents: len=%d", idx.Len())
	}
	_ = idx.Insert(rangeindex.Extent{Off: 8192, Len: 4096, Phys: 123, Tag: 2})
	if idx.Len() != 2 {
		t.Fatalf("MergeAdjacent merged differing tags: len=%d", idx.Len())
	}
}

func TestRask1PiBSparseFile(t *testing.T) {
	idx := rangeindex.New()
	const pib = rangeindex.PiB

	// Cluster near 0, cluster near 512 TiB, cluster near 1 PiB, and edge at MaxSize.
	spots := []rangeindex.Extent{
		{Off: 0, Len: 4096, Phys: 100, Tag: 1},
		{Off: pib / 2, Len: 64 << 10, Phys: 200, Tag: 2},
		{Off: pib/2 + (1 << 30), Len: 4096, Phys: 300, Tag: 3},
		{Off: pib - 4096, Len: 4096, Phys: 400, Tag: 4},
		{Off: rangeindex.MaxSize - 4096, Len: 4096, Phys: 500, Tag: 5},
	}
	for _, e := range spots {
		if err := idx.Insert(e); err != nil {
			t.Fatalf("Insert(%+v): %v", e, err)
		}
	}

	// Out-of-range inserts/deletes/truncates must fail cleanly.
	if err := idx.Insert(rangeindex.Extent{Off: rangeindex.MaxSize, Len: 1}); err == nil {
		t.Fatal("expected ErrOutOfRange at MaxSize")
	}
	if err := idx.Insert(rangeindex.Extent{Off: rangeindex.MaxSize - 10, Len: 11}); err == nil {
		t.Fatal("expected ErrOutOfRange crossing MaxSize")
	}

	for _, s := range spots {
		got, ok := idx.Lookup(s.Off + s.Len/2)
		if !ok || got != s {
			t.Fatalf("Lookup(%d): got %+v ok=%v, want %+v", s.Off+s.Len/2, got, ok, s)
		}
	}
	for _, holeOff := range []uint64{8192, pib / 4, pib/2 - 1, pib - 8192} {
		if e, ok := idx.Lookup(holeOff); ok {
			t.Fatalf("hole at %d returned %+v", holeOff, e)
		}
	}

	// Verify sparse clusters keep compact memory even with 1 PiB gaps.
	for i := range 500 {
		off := pib + uint64(i)*8192
		if err := idx.Insert(rangeindex.Extent{Off: off, Len: 4096, Phys: uint64(i) * 4096, Tag: 9}); err != nil {
			t.Fatal(err)
		}
	}
	st := idx.Stats()
	if st.BytesPerExtent > 32 {
		t.Fatalf("1 PiB sparse index bloated memory: %.1f bytes/extent (%+v)", st.BytesPerExtent, st)
	}

	// Range scan across 1 PiB boundary.
	var scanned int
	idx.LookupRange(pib-8192, 32768, func(e rangeindex.Extent) bool {
		scanned++
		return true
	})
	if scanned != 4 {
		t.Fatalf("LookupRange across 1 PiB got %d extents, want 4", scanned)
	}
}

func TestRaskMultiLeafSpanningOverwriteAndSplits(t *testing.T) {
	idx := rangeindex.New()
	// Insert 600 disjoint extents so multiple leaves and internal levels exist.
	for i := range 600 {
		if err := idx.Insert(rangeindex.Extent{
			Off:  uint64(i) * 100,
			Len:  50,
			Phys: uint64(i) * 1000,
			Tag:  uint32(i % 3),
		}); err != nil {
			t.Fatal(err)
		}
	}
	if st := idx.Stats(); st.Leaves < 4 || st.InternalNodes == 0 {
		t.Fatalf("expected multi-level B+ tree, got %+v", st)
	}

	// Overwrite from inside extent 10 across ~400 extents to inside extent 410.
	ins := rangeindex.Extent{Off: 1020, Len: 40010, Phys: 777777, Tag: 99}
	if err := idx.Insert(ins); err != nil {
		t.Fatal(err)
	}
	ext := idx.Extents()
	for i := 1; i < len(ext); i++ {
		if ext[i-1].End() > ext[i].Off {
			t.Fatalf("overlapping extents at %d: %+v vs %+v", i, ext[i-1], ext[i])
		}
	}
	got, ok := idx.Lookup(15000)
	if !ok || got != ins {
		t.Fatalf("Lookup inside spanning overwrite: got %+v ok=%v, want %+v", got, ok, ins)
	}

	// Punch a spanning hole across multiple leaves.
	if err := idx.Delete(5000, 20000); err != nil {
		t.Fatal(err)
	}
	if _, ok := idx.Lookup(15000); ok {
		t.Fatal("hole still mapped after multi-leaf Delete")
	}
}

// sliceOracle is a simple sorted-slice model for differential testing.
type sliceOracle struct {
	exts   []rangeindex.Extent
	policy rangeindex.MergePolicy
}

func (o *sliceOracle) canMerge(a, b rangeindex.Extent) bool {
	if a.End() != b.Off || a.Tag != b.Tag {
		return false
	}
	if o.policy == rangeindex.MergeAdjacent {
		return true
	}
	return a.Phys+a.Len == b.Phys
}

func (o *sliceOracle) insert(e rangeindex.Extent) {
	if e.Len == 0 {
		return
	}
	o.delete(e.Off, e.Len)
	var out []rangeindex.Extent
	inserted := false
	for _, cur := range o.exts {
		if !inserted && cur.Off > e.Off {
			out = append(out, e)
			inserted = true
		}
		out = append(out, cur)
	}
	if !inserted {
		out = append(out, e)
	}
	merged := out[:0]
	for _, cur := range out {
		if n := len(merged); n > 0 && o.canMerge(merged[n-1], cur) {
			merged[n-1].Len += cur.Len
		} else {
			merged = append(merged, cur)
		}
	}
	o.exts = merged
}

func (o *sliceOracle) delete(off, length uint64) {
	if length == 0 {
		return
	}
	end := off + length
	var out []rangeindex.Extent
	for _, cur := range o.exts {
		if cur.End() <= off || cur.Off >= end {
			out = append(out, cur)
			continue
		}
		if cur.Off < off {
			left := cur
			left.Len = off - cur.Off
			out = append(out, left)
		}
		if cur.End() > end {
			delta := end - cur.Off
			right := cur
			right.Off = end
			right.Phys += delta
			right.Len -= delta
			out = append(out, right)
		}
	}
	o.exts = out
}

func (o *sliceOracle) truncate(size uint64) {
	var out []rangeindex.Extent
	for _, cur := range o.exts {
		if cur.Off >= size {
			continue
		}
		if cur.End() > size {
			cur.Len = size - cur.Off
		}
		out = append(out, cur)
	}
	o.exts = out
}

func (o *sliceOracle) lookup(off uint64) (rangeindex.Extent, bool) {
	i := sort.Search(len(o.exts), func(i int) bool { return o.exts[i].Off > off }) - 1
	if i >= 0 && off < o.exts[i].End() {
		return o.exts[i], true
	}
	return rangeindex.Extent{}, false
}

func TestRaskDifferentialOracle(t *testing.T) {
	for _, policy := range []rangeindex.MergePolicy{rangeindex.MergeContiguous, rangeindex.MergeAdjacent} {
		t.Run(fmt.Sprintf("policy-%d", policy), func(t *testing.T) {
			idx := rangeindex.New(rangeindex.WithMergePolicy(policy))
			oracle := &sliceOracle{policy: policy}
			rng := rand.New(rand.NewPCG(uint64(policy)+42, 1337))

			bases := []uint64{0, 1 << 30, rangeindex.PiB / 2, rangeindex.PiB}
			for step := range 4000 {
				base := bases[rng.IntN(len(bases))]
				switch op := rng.IntN(100); {
				case op < 55:
					off := base + uint64(rng.IntN(256*1024))
					ln := uint64(1 + rng.IntN(32768))
					var phys uint64
					if rng.IntN(3) == 0 && len(oracle.exts) > 0 {
						// Encourage contiguous merges across neighbor boundaries.
						prev := oracle.exts[rng.IntN(len(oracle.exts))]
						off = prev.End()
						phys = prev.Phys + prev.Len
					} else {
						phys = uint64(rng.IntN(1 << 28))
					}
					e := rangeindex.Extent{
						Off:  off,
						Len:  ln,
						Phys: phys,
						Tag:  uint32(rng.IntN(3)),
					}
					if err := idx.Insert(e); err != nil {
						t.Fatalf("step %d Insert: %v", step, err)
					}
					oracle.insert(e)
				case op < 75:
					off := base + uint64(rng.IntN(256*1024))
					ln := uint64(1 + rng.IntN(32768))
					if err := idx.Delete(off, ln); err != nil {
						t.Fatalf("step %d Delete: %v", step, err)
					}
					oracle.delete(off, ln)
				case op < 82:
					sz := bases[rng.IntN(len(bases))] + uint64(rng.IntN(256*1024))
					if err := idx.Truncate(sz); err != nil {
						t.Fatalf("step %d Truncate: %v", step, err)
					}
					oracle.truncate(sz)
				default:
					q := base + uint64(rng.IntN(300*1024))
					ge, gok := idx.Lookup(q)
					we, wok := oracle.lookup(q)
					if gok != wok || (gok && ge != we) {
						t.Fatalf("step %d Lookup(%d): got (%+v,%v), want (%+v,%v)", step, q, ge, gok, we, wok)
					}
				}

				if step%250 == 249 {
					got := idx.Extents()
					if !reflect.DeepEqual(got, oracle.exts) {
						t.Fatalf("step %d Extents mismatch: got %d extents, want %d", step, len(got), len(oracle.exts))
					}
					var cov uint64
					for _, e := range oracle.exts {
						cov += e.Len
					}
					if idx.CoveredBytes() != cov || idx.Len() != len(oracle.exts) {
						t.Fatalf("step %d accounting mismatch: len=%d/%d covered=%d/%d", step, idx.Len(), len(oracle.exts), idx.CoveredBytes(), cov)
					}
				}
			}
			if got := idx.Extents(); !reflect.DeepEqual(got, oracle.exts) {
				t.Fatalf("final Extents mismatch: got %d, want %d", len(got), len(oracle.exts))
			}
		})
	}
}

func TestRaskConcurrentReadersWriters(t *testing.T) {
	idx := rangeindex.New()
	for i := range 200 {
		_ = idx.Insert(rangeindex.Extent{Off: uint64(i) * 8192, Len: 4096, Phys: uint64(i) * 4096, Tag: 1})
	}
	var wg sync.WaitGroup
	for g := range 4 {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(uint64(id+1), 55))
			for i := range 500 {
				off := uint64(rng.IntN(200)) * 4096
				if id%2 == 0 {
					_ = idx.Insert(rangeindex.Extent{Off: off, Len: 4096, Phys: uint64(i * 4096), Tag: uint32(id + 1)})
				} else {
					_, _ = idx.Lookup(off + 128)
					idx.LookupRange(off, 16384, func(rangeindex.Extent) bool { return true })
				}
			}
		}(g)
	}
	wg.Wait()
}

// naiveBSTNode represents a classic pointer-heavy RB/BST extent index node for
// memory-per-extent baseline comparisons.
type naiveBSTNode struct {
	ext         rangeindex.Extent
	left, right *naiveBSTNode
	color       bool
}

// BenchmarkRaskMemoryPerExtent measures live Go heap bytes/extent and reported
// structural bytes/extent across dense, random-split, and 1 PiB sparse workloads
// versus pointer-BST and flat-slice baselines.
func BenchmarkRaskMemoryPerExtent(b *testing.B) {
	const numExtents = 50_000

	measureHeap := func(build func() any) float64 {
		runtime.GC()
		var m0, m1 runtime.MemStats
		runtime.ReadMemStats(&m0)
		keep := build()
		runtime.GC()
		runtime.ReadMemStats(&m1)
		runtime.KeepAlive(keep)
		if m1.HeapAlloc <= m0.HeapAlloc {
			return 0
		}
		return float64(m1.HeapAlloc-m0.HeapAlloc) / float64(numExtents)
	}

	b.Run("rask-compact12-clustered", func(b *testing.B) {
		var st rangeindex.Stats
		var heapPer float64
		for i := 0; i < b.N; i++ {
			heapPer = measureHeap(func() any {
				idx := rangeindex.New()
				for j := range numExtents {
					_ = idx.Insert(rangeindex.Extent{
						Off:  uint64(j) * 8192,
						Len:  4096,
						Phys: uint64(j) * 4096,
						Tag:  1,
					})
				}
				st = idx.Stats()
				return idx
			})
		}
		b.ReportMetric(st.BytesPerExtent, "struct-B/extent")
		b.ReportMetric(heapPer, "heap-B/extent")
	})

	b.Run("rask-1PiB-sparse-jumps", func(b *testing.B) {
		var st rangeindex.Stats
		var heapPer float64
		for i := 0; i < b.N; i++ {
			heapPer = measureHeap(func() any {
				idx := rangeindex.New()
				// 100 sparse regions across [0 .. 1 PiB], 500 extents per region.
				for r := range 100 {
					base := (rangeindex.PiB / 100) * uint64(r)
					for k := range 500 {
						_ = idx.Insert(rangeindex.Extent{
							Off:  base + uint64(k)*8192,
							Len:  4096,
							Phys: uint64(r*500+k) * 4096,
							Tag:  1,
						})
					}
				}
				st = idx.Stats()
				return idx
			})
		}
		b.ReportMetric(st.BytesPerExtent, "struct-B/extent")
		b.ReportMetric(heapPer, "heap-B/extent")
	})

	b.Run("baseline-pointer-bst", func(b *testing.B) {
		var heapPer float64
		structBytes := float64(unsafe.Sizeof(naiveBSTNode{}))
		for i := 0; i < b.N; i++ {
			heapPer = measureHeap(func() any {
				nodes := make([]*naiveBSTNode, numExtents)
				for j := range numExtents {
					nodes[j] = &naiveBSTNode{
						ext: rangeindex.Extent{
							Off:  uint64(j) * 8192,
							Len:  4096,
							Phys: uint64(j) * 4096,
							Tag:  1,
						},
					}
					if j > 0 {
						nodes[j-1].right = nodes[j]
					}
				}
				return nodes
			})
		}
		b.ReportMetric(structBytes, "struct-B/extent")
		b.ReportMetric(heapPer, "heap-B/extent")
	})

	b.Run("baseline-flat-slice", func(b *testing.B) {
		var heapPer float64
		structBytes := float64(unsafe.Sizeof(rangeindex.Extent{}))
		for i := 0; i < b.N; i++ {
			heapPer = measureHeap(func() any {
				s := make([]rangeindex.Extent, 0, numExtents)
				for j := range numExtents {
					s = append(s, rangeindex.Extent{
						Off:  uint64(j) * 8192,
						Len:  4096,
						Phys: uint64(j) * 4096,
						Tag:  1,
					})
				}
				return s
			})
		}
		b.ReportMetric(structBytes, "struct-B/extent")
		b.ReportMetric(heapPer, "heap-B/extent")
	})
}

// BenchmarkRaskLookupLatency measures point lookup throughput and p50/p95/p99
// tail latencies across 100,000 sparse extents spanning up to 1 PiB.
func BenchmarkRaskLookupLatency(b *testing.B) {
	const numExtents = 100_000
	idx := rangeindex.New()
	queries := make([]uint64, 4096)
	flat := make([]rangeindex.Extent, 0, numExtents)

	for r := range 100 {
		base := (rangeindex.PiB / 100) * uint64(r)
		for k := range 1000 {
			e := rangeindex.Extent{
				Off:  base + uint64(k)*8192,
				Len:  4096,
				Phys: uint64(r*1000+k) * 4096,
				Tag:  1,
			}
			_ = idx.Insert(e)
			flat = append(flat, e)
		}
	}
	rng := rand.New(rand.NewPCG(7, 11))
	for i := range queries {
		if i%4 == 0 {
			queries[i] = flat[rng.IntN(len(flat))].Off + 20000 // hole
		} else {
			queries[i] = flat[rng.IntN(len(flat))].Off + 1024 // hit
		}
	}

	b.Run("rask-index", func(b *testing.B) {
		samples := make([]int64, 0, 8192)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			q := queries[i&4095]
			sample := i&7 == 0 && len(samples) < cap(samples)
			var t0 time.Time
			if sample {
				t0 = time.Now()
			}
			_, _ = idx.Lookup(q)
			if sample {
				samples = append(samples, time.Since(t0).Nanoseconds())
			}
		}
		b.StopTimer()
		if len(samples) > 0 {
			slices.Sort(samples)
			b.ReportMetric(float64(samples[len(samples)/2]), "p50-ns")
			b.ReportMetric(float64(samples[int(float64(len(samples)-1)*0.95)]), "p95-ns")
			b.ReportMetric(float64(samples[int(float64(len(samples)-1)*0.99)]), "p99-ns")
		}
		b.ReportMetric(idx.Stats().BytesPerExtent, "B/extent")
	})

	b.Run("baseline-flat-binarysearch", func(b *testing.B) {
		samples := make([]int64, 0, 8192)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			q := queries[i&4095]
			sample := i&7 == 0 && len(samples) < cap(samples)
			var t0 time.Time
			if sample {
				t0 = time.Now()
			}
			pos := sort.Search(len(flat), func(j int) bool { return flat[j].Off > q }) - 1
			if pos >= 0 && q < flat[pos].End() {
				_ = flat[pos]
			}
			if sample {
				samples = append(samples, time.Since(t0).Nanoseconds())
			}
		}
		b.StopTimer()
		if len(samples) > 0 {
			slices.Sort(samples)
			b.ReportMetric(float64(samples[len(samples)/2]), "p50-ns")
			b.ReportMetric(float64(samples[int(float64(len(samples)-1)*0.95)]), "p95-ns")
			b.ReportMetric(float64(samples[int(float64(len(samples)-1)*0.99)]), "p99-ns")
		}
		b.ReportMetric(float64(unsafe.Sizeof(rangeindex.Extent{})), "B/extent")
	})
}

func BenchmarkRaskInsertSplitOverwrite(b *testing.B) {
	b.ReportAllocs()
	idx := rangeindex.New()
	rng := rand.New(rand.NewPCG(99, 101))
	for i := 0; i < b.N; i++ {
		off := uint64(rng.IntN(4096)) * 4096
		_ = idx.Insert(rangeindex.Extent{
			Off:  off,
			Len:  uint64(1+rng.IntN(4)) * 2048,
			Phys: uint64(i) * 4096,
			Tag:  uint32(i & 1),
		})
		if idx.Len() > 8192 {
			idx.Clear()
		}
	}
}
