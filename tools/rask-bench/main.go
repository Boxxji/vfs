// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

// Command rask-bench runs the reproducible HIP-1344 RASK compact sparse-range
// index evaluation suite comparing four matched in-memory RangeIndex baselines:
//   - sorted_slice_baseline
//   - pointer_btree_baseline
//   - rask_compact_range_index
//   - flat_page_bitmap_negative (documented negative control)
//
// Usage:
//
//	go run ./tools/rask-bench
//	go run ./tools/rask-bench -json
//	go run ./tools/rask-bench -write-dossier docs/research/hip-1344-rask
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"time"

	"github.com/hanzoai/vfs/internal/index/rask"
)

type IndexMetrics struct {
	Name             string  `json:"name"`
	Extents          int     `json:"extents"`
	StructBytes      int64   `json:"struct_bytes"`
	BytesPerExtent   float64 `json:"bytes_per_extent"`
	HeapAllocDelta   int64   `json:"heap_alloc_delta_bytes"`
	HeapBytesPerExt  float64 `json:"heap_bytes_per_extent"`
	PointerCount     int     `json:"pointer_count"`
	GCPauseUs        float64 `json:"gc_pause_median_us"`
	LookupHitP50Ns   float64 `json:"lookup_hit_p50_ns"`
	LookupHitP95Ns   float64 `json:"lookup_hit_p95_ns"`
	LookupHitP99Ns   float64 `json:"lookup_hit_p99_ns"`
	LookupHoleP50Ns  float64 `json:"lookup_hole_p50_ns"`
	LookupHoleP99Ns  float64 `json:"lookup_hole_p99_ns"`
	RangeScanMExtSec float64 `json:"range_scan_mext_per_sec"`
	RandomMutKOpsSec float64 `json:"random_mutate_kops_per_sec"`
	PunchHoleKOpsSec float64 `json:"punch_hole_kops_per_sec"`
	Status           string  `json:"status"`
}

type Sparse1PiBMetrics struct {
	Name             string  `json:"name"`
	Extents          int     `json:"extents"`
	LogicalSpanBytes uint64  `json:"logical_span_bytes"`
	StructBytes      int64   `json:"struct_bytes"`
	AnalyticalRAMGiB float64 `json:"analytical_ram_gib,omitempty"`
	PointerCount     int     `json:"pointer_count"`
	HitP50Ns         float64 `json:"hit_p50_ns"`
	HitP99Ns         float64 `json:"hit_p99_ns"`
	HoleP50Ns        float64 `json:"hole_p50_ns"`
	HoleP99Ns        float64 `json:"hole_p99_ns"`
	Status           string  `json:"status"`
}

type BoundaryFragmentedMetrics struct {
	Name           string  `json:"name"`
	Writes         int     `json:"writes"`
	Extents        int     `json:"extents"`
	InsertKOpsSec  float64 `json:"insert_kops_per_sec"`
	BytesPerExtent float64 `json:"bytes_per_extent"`
	PointerCount   int     `json:"pointer_count"`
	LookupHitP99Ns float64 `json:"lookup_hit_p99_ns"`
}

type BoundaryTinyMetrics struct {
	Name           string  `json:"name"`
	Extents        int     `json:"extents"`
	StructBytes    int64   `json:"struct_bytes"`
	LookupHitP50Ns float64 `json:"lookup_hit_p50_ns"`
	LookupHitP99Ns float64 `json:"lookup_hit_p99_ns"`
}

type FlatExplosionRow struct {
	LogicalLabel    string  `json:"logical_label"`
	LogicalBytes    uint64  `json:"logical_bytes"`
	FlatTableBytes  uint64  `json:"flat_table_bytes"`
	FlatTableHuman  string  `json:"flat_table_human"`
	RASK100ExtBytes int64   `json:"rask_100_extents_bytes"`
	MemoryRatio     float64 `json:"memory_ratio_flat_over_rask"`
}

type Report struct {
	Environment struct {
		GOOS      string `json:"goos"`
		GOARCH    string `json:"goarch"`
		GoVersion string `json:"go_version"`
		NumCPU    int    `json:"num_cpu"`
		Timestamp string `json:"timestamp_utc"`
	} `json:"environment"`
	WorkloadCompact32   []IndexMetrics              `json:"workload_100k_compact32"`
	WorkloadRandomVal64 []IndexMetrics              `json:"workload_100k_random_val64"`
	WorkloadSparse1PiB  []Sparse1PiBMetrics         `json:"workload_1pib_sparse"`
	BoundaryFragmented  []BoundaryFragmentedMetrics `json:"boundary_dense_fragmented_writes"`
	BoundaryTiny16      []BoundaryTinyMetrics       `json:"boundary_tiny_file_under_16_extents"`
	BoundaryFlatExplode []FlatExplosionRow          `json:"boundary_flat_page_table_explosion"`
	RASKDiagnostics     rask.ModeStats              `json:"rask_100k_compact32_diagnostics"`
}

func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(math.Ceil(p/100.0*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

// measureLookups runs batched point lookups (batchSize ops per timing sample)
// so vDSO clock overhead is amortized below 1 ns/op while preserving tail
// distribution across cache-line and page misses.
func measureLookups(idx rask.RangeIndex, offsets []uint64, batches, batchSize int) (p50, p95, p99 float64) {
	samples := make([]float64, batches)
	n := len(offsets)
	var sink uint64
	cursor := 0
	// Warmup
	for i := 0; i < 1000; i++ {
		if v, ok := idx.Lookup(offsets[i%n]); ok {
			sink += v
		}
	}
	for b := 0; b < batches; b++ {
		t0 := time.Now()
		for k := 0; k < batchSize; k++ {
			if v, ok := idx.Lookup(offsets[cursor]); ok {
				sink += v
			}
			cursor++
			if cursor == n {
				cursor = 0
			}
		}
		dt := time.Since(t0)
		samples[b] = float64(dt.Nanoseconds()) / float64(batchSize)
	}
	if sink == math.MaxUint64 {
		fmt.Fprintln(os.Stderr, sink)
	}
	slices.Sort(samples)
	return percentile(samples, 50), percentile(samples, 95), percentile(samples, 99)
}

func measureGCPauseUs(keep any) float64 {
	const rounds = 7
	pauses := make([]float64, rounds)
	var ms runtime.MemStats
	runtime.GC() // settle prior allocations
	for i := 0; i < rounds; i++ {
		runtime.GC()
		runtime.ReadMemStats(&ms)
		pauses[i] = float64(ms.PauseNs[(ms.NumGC+255)%256]) / 1000.0
	}
	runtime.KeepAlive(keep)
	slices.Sort(pauses)
	return pauses[rounds/2]
}

func measureRangeScan(idx rask.RangeIndex, maxOff uint64, windows int) float64 {
	rng := rand.New(rand.NewPCG(77, 88))
	winWidth := uint64(8 << 20) // ~1024 extents at 8 KiB stride
	if winWidth > maxOff {
		winWidth = maxOff
	}
	var visited int64
	t0 := time.Now()
	for i := 0; i < windows; i++ {
		start := uint64(0)
		if maxOff > winWidth {
			start = rng.Uint64N(maxOff - winWidth)
		}
		idx.RangeScan(start, start+winWidth, func(e rask.Extent) bool {
			visited++
			return true
		})
	}
	dur := time.Since(t0).Seconds()
	if dur <= 0 {
		return 0
	}
	return float64(visited) / 1e6 / dur
}

func runLargeWorkload(numExtents int, randomVal64 bool) ([]IndexMetrics, rask.ModeStats) {
	type factory struct {
		name string
		mk   func() rask.RangeIndex
		// SortedSlice is O(N) per random insert; cap random mutation sample
		// so the benchmark finishes in seconds while accurately measuring rate.
		mutOps int
	}
	factories := []factory{
		{name: "sorted_slice_baseline", mk: func() rask.RangeIndex { return rask.NewSortedSlice() }, mutOps: 2000},
		{name: "pointer_btree_baseline", mk: func() rask.RangeIndex { return rask.NewPointerBTree() }, mutOps: 20000},
		{name: "rask_compact_range_index", mk: func() rask.RangeIndex { return rask.NewRASKIndex() }, mutOps: 20000},
	}

	// Deterministic synthetic extents: 4 KiB data, 4 KiB hole (stride 8 KiB).
	vals := make([]uint64, numExtents)
	valRng := rand.New(rand.NewPCG(101, 202))
	for i := 0; i < numExtents; i++ {
		if randomVal64 {
			vals[i] = valRng.Uint64()
		} else {
			vals[i] = uint64(i) + 1
		}
	}

	maxOff := uint64(numExtents) * 8192

	// Pre-generate lookup keys (hits and holes).
	const probeCount = 65536
	hitOffsets := make([]uint64, probeCount)
	holeOffsets := make([]uint64, probeCount)
	pRng := rand.New(rand.NewPCG(303, 404))
	for i := 0; i < probeCount; i++ {
		idx := uint64(pRng.IntN(numExtents))
		hitOffsets[i] = idx*8192 + uint64(pRng.IntN(4096))
		holeOffsets[i] = idx*8192 + 4096 + uint64(pRng.IntN(4096))
	}

	results := make([]IndexMetrics, 0, len(factories)+1)
	var raskDiag rask.ModeStats

	for _, f := range factories {
		runtime.GC()
		runtime.GC()
		var m0, m1 runtime.MemStats
		runtime.ReadMemStats(&m0)

		idx := f.mk()
		for i := 0; i < numExtents; i++ {
			off := uint64(i) * 8192
			idx.Insert(off, off+4096, vals[i])
		}

		runtime.GC()
		runtime.ReadMemStats(&m1)
		heapDelta := int64(m1.HeapAlloc) - int64(m0.HeapAlloc)
		if heapDelta < 0 {
			heapDelta = 0
		}

		if rIdx, ok := idx.(*rask.RASKIndex); ok && !randomVal64 {
			raskDiag = rIdx.Stats()
		}

		extCount := idx.Len()
		structBytes := idx.MemoryBytes()
		ptrs := idx.PointerCount()
		gcUs := measureGCPauseUs(idx)

		hitP50, hitP95, hitP99 := measureLookups(idx, hitOffsets, 4000, 32)
		holeP50, _, holeP99 := measureLookups(idx, holeOffsets, 4000, 32)
		scanMExt := measureRangeScan(idx, maxOff, 500)

		// Random insert/overwrite throughput on a fresh copy so memory stats stay clean.
		mutRng := rand.New(rand.NewPCG(505, 606))
		t0 := time.Now()
		for i := 0; i < f.mutOps; i++ {
			off := mutRng.Uint64N(maxOff)
			length := uint64(512 + mutRng.IntN(4096))
			v := uint64(i) + 1
			if randomVal64 {
				v = mutRng.Uint64()
			}
			idx.Insert(off, off+length, v)
		}
		mutDur := time.Since(t0).Seconds()
		mutKOps := float64(f.mutOps) / 1e3 / mutDur

		// Random punch-hole throughput.
		punchOps := f.mutOps / 2
		t1 := time.Now()
		for i := 0; i < punchOps; i++ {
			off := mutRng.Uint64N(maxOff)
			length := uint64(512 + mutRng.IntN(8192))
			idx.DeleteRange(off, off+length)
		}
		punchDur := time.Since(t1).Seconds()
		punchKOps := float64(punchOps) / 1e3 / punchDur

		results = append(results, IndexMetrics{
			Name:             f.name,
			Extents:          extCount,
			StructBytes:      structBytes,
			BytesPerExtent:   float64(structBytes) / float64(extCount),
			HeapAllocDelta:   heapDelta,
			HeapBytesPerExt:  float64(heapDelta) / float64(extCount),
			PointerCount:     ptrs,
			GCPauseUs:        gcUs,
			LookupHitP50Ns:   hitP50,
			LookupHitP95Ns:   hitP95,
			LookupHitP99Ns:   hitP99,
			LookupHoleP50Ns:  holeP50,
			LookupHoleP99Ns:  holeP99,
			RangeScanMExtSec: scanMExt,
			RandomMutKOpsSec: mutKOps,
			PunchHoleKOpsSec: punchKOps,
			Status:           "ok",
		})
	}

	// Documented negative control: FlatPageTable at maxOff (~781 MiB logical span).
	flat := rask.NewFlatPageTable(64 << 20)
	_ = flat.TryInsert(maxOff-4096, maxOff, 1)
	analBytes := int64(rask.AnalyticalMemoryBytes(maxOff))
	results = append(results, IndexMetrics{
		Name:           "flat_page_bitmap_negative",
		Extents:        numExtents,
		StructBytes:    analBytes,
		BytesPerExtent: float64(analBytes) / float64(numExtents),
		Status:         fmt.Sprintf("OOM_CAPPED (requires %.2f MiB dense table for %.0f MiB logical span)", float64(analBytes)/(1<<20), float64(maxOff)/(1<<20)),
	})

	return results, raskDiag
}

func run1PiBSparseWorkload() []Sparse1PiBMetrics {
	anchors := []uint64{
		0,
		1 << 40,   // 1 TiB
		256 << 40, // 256 TiB
		512 << 40, // 512 TiB
		rask.PiB - (1 << 20),
	}

	populate := func(idx rask.RangeIndex) {
		for aIdx, base := range anchors {
			for j := uint64(0); j < 20; j++ {
				s := base + j*(8<<10)
				idx.Insert(s, s+(4<<10), uint64(aIdx*1000+int(j)+1))
			}
		}
		idx.Insert(rask.PiB-4096, rask.PiB, 999999)
	}

	// Build probe lists: hits at the 5 petabyte clusters + random holes across 1 PiB.
	const probeN = 32768
	hits := make([]uint64, probeN)
	holes := make([]uint64, probeN)
	rng := rand.New(rand.NewPCG(909, 1010))
	for i := 0; i < probeN; i++ {
		if i%21 == 0 {
			hits[i] = rask.PiB - 1
		} else {
			base := anchors[i%len(anchors)]
			j := uint64(rng.IntN(20))
			hits[i] = base + j*(8<<10) + uint64(rng.IntN(4096))
		}
		// Random offset inside empty 1 PiB sparse space (avoiding anchor clusters).
		holes[i] = (uint64(rng.IntN(200)+2) << 40) + rng.Uint64N(1<<30)
	}

	factories := []struct {
		name string
		mk   func() rask.RangeIndex
	}{
		{"sorted_slice_baseline", func() rask.RangeIndex { return rask.NewSortedSlice() }},
		{"pointer_btree_baseline", func() rask.RangeIndex { return rask.NewPointerBTree() }},
		{"rask_compact_range_index", func() rask.RangeIndex { return rask.NewRASKIndex() }},
	}

	out := make([]Sparse1PiBMetrics, 0, 4)
	for _, f := range factories {
		idx := f.mk()
		populate(idx)
		hitP50, _, hitP99 := measureLookups(idx, hits, 3000, 32)
		holeP50, _, holeP99 := measureLookups(idx, holes, 3000, 32)
		out = append(out, Sparse1PiBMetrics{
			Name:             f.name,
			Extents:          idx.Len(),
			LogicalSpanBytes: rask.PiB,
			StructBytes:      idx.MemoryBytes(),
			PointerCount:     idx.PointerCount(),
			HitP50Ns:         hitP50,
			HitP99Ns:         hitP99,
			HoleP50Ns:        holeP50,
			HoleP99Ns:        holeP99,
			Status:           "ok",
		})
	}

	dense1PiB := rask.AnalyticalMemoryBytes(rask.PiB)
	out = append(out, Sparse1PiBMetrics{
		Name:             "flat_page_bitmap_negative",
		Extents:          101,
		LogicalSpanBytes: rask.PiB,
		StructBytes:      int64(dense1PiB),
		AnalyticalRAMGiB: float64(dense1PiB) / (1 << 30),
		Status:           fmt.Sprintf("OOM_IMPOSSIBLE (%.2f TiB RAM required for 1 PiB @ 4 KiB)", float64(dense1PiB)/(1<<40)),
	})
	return out
}

func runBoundaryFragmented() []BoundaryFragmentedMetrics {
	const writes = 25_000
	const spanPages = 32_768 // 128 MiB window
	offsets := make([]uint64, writes)
	vals := make([]uint64, writes)
	rng := rand.New(rand.NewPCG(777, 888))
	for i := 0; i < writes; i++ {
		offsets[i] = uint64(rng.IntN(spanPages)) * 4096
		vals[i] = rng.Uint64() // distinct uncoalescable 64-bit values
	}

	factories := []struct {
		name string
		mk   func() rask.RangeIndex
	}{
		{"sorted_slice_baseline", func() rask.RangeIndex { return rask.NewSortedSlice() }},
		{"pointer_btree_baseline", func() rask.RangeIndex { return rask.NewPointerBTree() }},
		{"rask_compact_range_index", func() rask.RangeIndex { return rask.NewRASKIndex() }},
	}

	out := make([]BoundaryFragmentedMetrics, 0, len(factories))
	for _, f := range factories {
		idx := f.mk()
		t0 := time.Now()
		for i := 0; i < writes; i++ {
			idx.Insert(offsets[i], offsets[i]+4096, vals[i])
		}
		dur := time.Since(t0).Seconds()
		_, _, p99 := measureLookups(idx, offsets, 2000, 32)
		out = append(out, BoundaryFragmentedMetrics{
			Name:           f.name,
			Writes:         writes,
			Extents:        idx.Len(),
			InsertKOpsSec:  float64(writes) / 1e3 / dur,
			BytesPerExtent: float64(idx.MemoryBytes()) / float64(idx.Len()),
			PointerCount:   idx.PointerCount(),
			LookupHitP99Ns: p99,
		})
	}
	return out
}

func runBoundaryTiny16() []BoundaryTinyMetrics {
	const n = 8
	probes := make([]uint64, 1024)
	for i := range probes {
		probes[i] = uint64(i%n)*8192 + 512
	}
	factories := []struct {
		name string
		mk   func() rask.RangeIndex
	}{
		{"sorted_slice_baseline", func() rask.RangeIndex { return rask.NewSortedSlice() }},
		{"pointer_btree_baseline", func() rask.RangeIndex { return rask.NewPointerBTree() }},
		{"rask_compact_range_index", func() rask.RangeIndex { return rask.NewRASKIndex() }},
	}
	out := make([]BoundaryTinyMetrics, 0, len(factories))
	for _, f := range factories {
		idx := f.mk()
		for i := uint64(0); i < n; i++ {
			idx.Insert(i*8192, i*8192+4096, i+1)
		}
		p50, _, p99 := measureLookups(idx, probes, 4000, 64)
		out = append(out, BoundaryTinyMetrics{
			Name:           f.name,
			Extents:        idx.Len(),
			StructBytes:    idx.MemoryBytes(),
			LookupHitP50Ns: p50,
			LookupHitP99Ns: p99,
		})
	}
	return out
}

func humanBytes(b uint64) string {
	switch {
	case b >= 1<<40:
		return fmt.Sprintf("%.2f TiB", float64(b)/(1<<40))
	case b >= 1<<30:
		return fmt.Sprintf("%.2f GiB", float64(b)/(1<<30))
	case b >= 1<<20:
		return fmt.Sprintf("%.2f MiB", float64(b)/(1<<20))
	case b >= 1<<10:
		return fmt.Sprintf("%.2f KiB", float64(b)/(1<<10))
	default:
		return fmt.Sprintf("%d B", b)
	}
}

func runFlatExplosionTable(rask1PiBBytes int64) []FlatExplosionRow {
	scales := []struct {
		label string
		size  uint64
	}{
		{"64 MiB", 64 << 20},
		{"1 GiB", 1 << 30},
		{"1 TiB", 1 << 40},
		{"1 PiB (1<<50 B)", rask.PiB},
	}
	rows := make([]FlatExplosionRow, 0, len(scales))
	for _, sc := range scales {
		fb := rask.AnalyticalMemoryBytes(sc.size)
		rows = append(rows, FlatExplosionRow{
			LogicalLabel:    sc.label,
			LogicalBytes:    sc.size,
			FlatTableBytes:  fb,
			FlatTableHuman:  humanBytes(fb),
			RASK100ExtBytes: rask1PiBBytes,
			MemoryRatio:     float64(fb) / float64(rask1PiBBytes),
		})
	}
	return rows
}

func buildScoreTableMarkdown(rep *Report) string {
	var out string
	out += "# HIP-1344 RASK Compact Sparse-Range Index — Measured Score Table\n\n"
	out += fmt.Sprintf("Environment: `%s/%s`, `%s`, `%d` CPUs (`%s`).\n\n",
		rep.Environment.GOOS, rep.Environment.GOARCH, rep.Environment.GoVersion, rep.Environment.NumCPU, rep.Environment.Timestamp)

	out += "## 1. Primary Workload: 100,000 Sparse-Run Extents (4 KiB Data / 4 KiB Hole, Compact Values)\n\n"
	out += "| Baseline | Struct Bytes | B/Extent | HeapAlloc Delta | Heap B/Ext | GC Pointers | GC Pause (us) | Hit p50 / p95 / p99 (ns) | Hole p50 / p99 (ns) | RangeScan (M ext/s) | Random Insert (K ops/s) | PunchHole (K ops/s) |\n"
	out += "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n"
	for _, m := range rep.WorkloadCompact32 {
		if m.Status != "ok" {
			out += fmt.Sprintf("| `%s` | %d | %.2f | n/a | n/a | n/a | n/a | n/a | n/a | n/a | n/a | %s |\n",
				m.Name, m.StructBytes, m.BytesPerExtent, m.Status)
			continue
		}
		out += fmt.Sprintf("| `%s` | %d | **%.2f** | %d | %.2f | %d | %.1f | %.1f / %.1f / %.1f | %.1f / %.1f | %.2f | %.1f | %.1f |\n",
			m.Name, m.StructBytes, m.BytesPerExtent, m.HeapAllocDelta, m.HeapBytesPerExt,
			m.PointerCount, m.GCPauseUs, m.LookupHitP50Ns, m.LookupHitP95Ns, m.LookupHitP99Ns,
			m.LookupHoleP50Ns, m.LookupHoleP99Ns, m.RangeScanMExtSec, m.RandomMutKOpsSec, m.PunchHoleKOpsSec)
	}

	out += "\n## 2. Arbitrary 64-Bit Random Values Workload (100,000 Extents, `modeCompact64`)\n\n"
	out += "| Baseline | Struct Bytes | B/Extent | Heap B/Ext | GC Pointers | Hit p50 / p99 (ns) | Random Insert (K ops/s) | PunchHole (K ops/s) |\n"
	out += "|---|---:|---:|---:|---:|---:|---:|---:|\n"
	for _, m := range rep.WorkloadRandomVal64 {
		if m.Status != "ok" {
			continue
		}
		out += fmt.Sprintf("| `%s` | %d | **%.2f** | %.2f | %d | %.1f / %.1f | %.1f | %.1f |\n",
			m.Name, m.StructBytes, m.BytesPerExtent, m.HeapBytesPerExt, m.PointerCount,
			m.LookupHitP50Ns, m.LookupHitP99Ns, m.RandomMutKOpsSec, m.PunchHoleKOpsSec)
	}

	out += "\n## 3. 1 PiB (`1 << 50` B) Sparse-File Stress Workload (101 Extents Across `0`, `1 TiB`, `256 TiB`, `512 TiB`, `1 PiB - 4 KiB`)\n\n"
	out += "| Baseline | Extents | Struct Memory | GC Pointers | Hit p50 / p99 (ns) | 1 PiB Hole p50 / p99 (ns) | Status |\n"
	out += "|---|---:|---:|---:|---:|---:|---|\n"
	for _, m := range rep.WorkloadSparse1PiB {
		if m.Status != "ok" {
			out += fmt.Sprintf("| `%s` | %d | **%s** | n/a | n/a | n/a | %s |\n",
				m.Name, m.Extents, humanBytes(uint64(m.StructBytes)), m.Status)
			continue
		}
		out += fmt.Sprintf("| `%s` | %d | **%s** (%d B) | %d | %.1f / %.1f | **%.1f / %.1f** | %s |\n",
			m.Name, m.Extents, humanBytes(uint64(m.StructBytes)), m.StructBytes, m.PointerCount,
			m.HitP50Ns, m.HitP99Ns, m.HoleP50Ns, m.HoleP99Ns, m.Status)
	}

	out += "\n## 4. Documented Honest Negative & Boundary Regimes\n\n"
	out += "### A. `dense_tiny_random_100pct_fragmented_writes` (25,000 Random 4 KiB Writes, Uncoalescable 64-bit Values)\n\n"
	out += "| Baseline | Extents | Insert Throughput (K ops/s) | B/Extent | GC Pointers | Hit p99 (ns) |\n"
	out += "|---|---:|---:|---:|---:|---:|\n"
	for _, m := range rep.BoundaryFragmented {
		out += fmt.Sprintf("| `%s` | %d | %.1f | %.2f | %d | %.1f |\n",
			m.Name, m.Extents, m.InsertKOpsSec, m.BytesPerExtent, m.PointerCount, m.LookupHitP99Ns)
	}

	out += "\n### B. `tiny_file_under_16_extents` (8 Extents)\n\n"
	out += "| Baseline | Extents | Struct Bytes | Hit p50 (ns) | Hit p99 (ns) |\n"
	out += "|---|---:|---:|---:|---:|\n"
	for _, m := range rep.BoundaryTiny16 {
		out += fmt.Sprintf("| `%s` | %d | %d | %.1f | %.1f |\n",
			m.Name, m.Extents, m.StructBytes, m.LookupHitP50Ns, m.LookupHitP99Ns)
	}

	out += "\n### C. `flat_page_table_1pib_explosion` (Dense 4 KiB Page Table vs RASK on 101 Sparse Extents)\n\n"
	out += "| Logical File Size | Dense 4 KiB Page Table RAM | RASK (101 Sparse Extents) | Memory Amplification |\n"
	out += "|---|---:|---:|---:|\n"
	for _, row := range rep.BoundaryFlatExplode {
		out += fmt.Sprintf("| %s | **%s** (%d B) | %d B | %.1fx |\n",
			row.LogicalLabel, row.FlatTableHuman, row.FlatTableBytes, row.RASK100ExtBytes, row.MemoryRatio)
	}
	return out
}

func main() {
	asJSON := flag.Bool("json", false, "emit machine-readable JSON to stdout")
	writeDir := flag.String("write-dossier", "", "optional directory to write raw_results.json and score_table.md")
	numExtents := flag.Int("extents", 100_000, "extent count for large workloads")
	flag.Parse()

	var rep Report
	rep.Environment.GOOS = runtime.GOOS
	rep.Environment.GOARCH = runtime.GOARCH
	rep.Environment.GoVersion = runtime.Version()
	rep.Environment.NumCPU = runtime.NumCPU()
	rep.Environment.Timestamp = time.Now().UTC().Format(time.RFC3339)

	rep.WorkloadCompact32, rep.RASKDiagnostics = runLargeWorkload(*numExtents, false)
	rep.WorkloadRandomVal64, _ = runLargeWorkload(*numExtents, true)
	rep.WorkloadSparse1PiB = run1PiBSparseWorkload()
	rep.BoundaryFragmented = runBoundaryFragmented()
	rep.BoundaryTiny16 = runBoundaryTiny16()

	var rask1PiBBytes int64 = 1
	for _, m := range rep.WorkloadSparse1PiB {
		if m.Name == "rask_compact_range_index" {
			rask1PiBBytes = m.StructBytes
		}
	}
	rep.BoundaryFlatExplode = runFlatExplosionTable(rask1PiBBytes)

	if *writeDir != "" {
		if err := os.MkdirAll(*writeDir, 0o755); err != nil {
			fmt.Fprintf(os.Stderr, "rask-bench: mkdir: %v\n", err)
			os.Exit(1)
		}
		raw, err := json.MarshalIndent(rep, "", "  ")
		if err != nil {
			fmt.Fprintf(os.Stderr, "rask-bench: marshal: %v\n", err)
			os.Exit(1)
		}
		if err := os.WriteFile(filepath.Join(*writeDir, "raw_results.json"), append(raw, '\n'), 0o644); err != nil {
			fmt.Fprintf(os.Stderr, "rask-bench: write raw_results.json: %v\n", err)
			os.Exit(1)
		}
		md := buildScoreTableMarkdown(&rep)
		if err := os.WriteFile(filepath.Join(*writeDir, "score_table.md"), []byte(md), 0o644); err != nil {
			fmt.Fprintf(os.Stderr, "rask-bench: write score_table.md: %v\n", err)
			os.Exit(1)
		}
	}

	if *asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(rep); err != nil {
			fmt.Fprintf(os.Stderr, "rask-bench: encode: %v\n", err)
			os.Exit(1)
		}
		return
	}

	fmt.Print(buildScoreTableMarkdown(&rep))
}
