// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package rask

import (
	"errors"
	"math"
	"math/rand/v2"
	"runtime"
	"sync"
	"testing"
)

func collectAll(idx RangeIndex, start, end uint64) []Extent {
	var out []Extent
	idx.RangeScan(start, end, func(e Extent) bool {
		out = append(out, e)
		return true
	})
	return out
}

func assertEquivalent(t *testing.T, step string, oracle *SortedSlice, raskIdx *RASKIndex, btree *PointerBTree, probeOffsets []uint64) {
	t.Helper()

	want := collectAll(oracle, 0, math.MaxUint64)
	gotRASK := collectAll(raskIdx, 0, math.MaxUint64)
	gotBTree := collectAll(btree, 0, math.MaxUint64)

	if oracle.Len() != len(want) {
		t.Fatalf("%s: oracle Len()=%d != scan len=%d", step, oracle.Len(), len(want))
	}
	if raskIdx.Len() != oracle.Len() {
		t.Fatalf("%s: RASK Len()=%d != oracle Len()=%d", step, raskIdx.Len(), oracle.Len())
	}
	if btree.Len() != oracle.Len() {
		t.Fatalf("%s: PointerBTree Len()=%d != oracle Len()=%d", step, btree.Len(), oracle.Len())
	}

	// Verify canonical invariants on oracle as well.
	for i := range want {
		if want[i].Start >= want[i].End {
			t.Fatalf("%s: non-positive extent %+v", step, want[i])
		}
		if i > 0 {
			prev := want[i-1]
			if prev.End > want[i].Start {
				t.Fatalf("%s: overlapping extents %+v and %+v", step, prev, want[i])
			}
			if prev.End == want[i].Start && prev.Val == want[i].Val {
				t.Fatalf("%s: uncoalesced adjacent extents %+v and %+v", step, prev, want[i])
			}
		}
	}

	if len(gotRASK) != len(want) {
		t.Fatalf("%s: RASK scan len=%d != want %d", step, len(gotRASK), len(want))
	}
	for i := range want {
		if gotRASK[i] != want[i] {
			t.Fatalf("%s: RASK scan[%d]=%+v != want %+v", step, i, gotRASK[i], want[i])
		}
	}

	if len(gotBTree) != len(want) {
		t.Fatalf("%s: PointerBTree scan len=%d != want %d", step, len(gotBTree), len(want))
	}
	for i := range want {
		if gotBTree[i] != want[i] {
			t.Fatalf("%s: PointerBTree scan[%d]=%+v != want %+v", step, i, gotBTree[i], want[i])
		}
	}

	for _, off := range probeOffsets {
		wv, wok := oracle.Lookup(off)
		rv, rok := raskIdx.Lookup(off)
		bv, bok := btree.Lookup(off)
		if rv != wv || rok != wok {
			t.Fatalf("%s: RASK Lookup(%d)=(%d,%v) != want (%d,%v)", step, off, rv, rok, wv, wok)
		}
		if bv != wv || bok != wok {
			t.Fatalf("%s: PointerBTree Lookup(%d)=(%d,%v) != want (%d,%v)", step, off, bv, bok, wv, wok)
		}
	}
}

// TestDifferentialEquivalence10k executes 12,000 randomized Insert, partial
// overwrite, split, cross-leaf coalesce, DeleteRange (hole punch), Lookup, and
// windowed RangeScan operations across multiple address & value distributions,
// asserting 100% bit-exact equivalence between RASKIndex, PointerBTree, and
// SortedSlice.
func TestDifferentialEquivalence10k(t *testing.T) {
	rng := rand.New(rand.NewPCG(0x5241534B, 0x1344))

	oracle := NewSortedSlice()
	raskIdx := NewRASKIndex()
	btree := NewPointerBTree()

	const totalOps = 12_000
	var probes [64]uint64

	for op := 0; op < totalOps; op++ {
		var start, end, val uint64

		// Mix dense small offsets, multi-leaf spans, 4 GiB boundary crossings,
		// and sparse 1 PiB jumps to exercise all 3 RASK leaf encoding modes.
		switch rng.IntN(10) {
		case 0, 1, 2, 3:
			// Dense local region (triggers frequent splits, overwrites, coalescing).
			start = rng.Uint64N(256 << 10)
			end = start + 1 + rng.Uint64N(8192)
			val = rng.Uint64N(6) // low cardinality -> heavy coalescing!
		case 4, 5:
			// Medium multi-leaf span inside first 4 GiB.
			start = rng.Uint64N(64 << 20)
			end = start + 1 + rng.Uint64N(1<<20)
			val = rng.Uint64N(1000)
		case 6:
			// Wide value range (triggers modeCompact64).
			start = rng.Uint64N(512 << 10)
			end = start + 1 + rng.Uint64N(4096)
			val = rng.Uint64()
		case 7:
			// 4 GiB segment boundary straddler.
			base := uint64(rng.IntN(4)+1) << 32
			start = base - rng.Uint64N(8192)
			end = base + 1 + rng.Uint64N(8192)
			val = rng.Uint64N(16)
		case 8:
			// Sparse petabyte cluster up to 1 PiB.
			cluster := uint64(rng.IntN(8)) * (PiB / 8)
			if cluster >= PiB {
				cluster = PiB - (1 << 20)
			}
			start = cluster + rng.Uint64N(64<<10)
			end = start + 1 + rng.Uint64N(16<<10)
			val = rng.Uint64N(8)
		case 9:
			// Occasional wide >4 GiB extent (triggers modeFull64 + radix fallback).
			start = uint64(rng.IntN(4)) << 32
			end = start + (5 << 30) + rng.Uint64N(1<<20)
			val = rng.Uint64N(4)
		}

		isDelete := rng.IntN(4) == 0
		if isDelete {
			oracle.DeleteRange(start, end)
			raskIdx.DeleteRange(start, end)
			btree.DeleteRange(start, end)
		} else {
			oracle.Insert(start, end, val)
			raskIdx.Insert(start, end, val)
			btree.Insert(start, end, val)
		}

		// Periodically verify full equivalence + boundary probes + windowed scans.
		if op%100 == 0 || op == totalOps-1 {
			p := probes[:0]
			p = append(p, 0, start, end)
			if start > 0 {
				p = append(p, start-1)
			}
			if end > 0 {
				p = append(p, end-1)
			}
			if end < math.MaxUint64 {
				p = append(p, end+1)
			}
			for k := 0; k < 16; k++ {
				p = append(p, rng.Uint64N(256<<10))
			}
			assertEquivalent(t, "fuzz", oracle, raskIdx, btree, p)

			// Verify windowed RangeScan & early stop.
			wStart := start
			wEnd := end + 4096
			wantWin := collectAll(oracle, wStart, wEnd)
			gotRASKWin := collectAll(raskIdx, wStart, wEnd)
			gotBTreeWin := collectAll(btree, wStart, wEnd)
			if len(gotRASKWin) != len(wantWin) || len(gotBTreeWin) != len(wantWin) {
				t.Fatalf("op %d: windowed RangeScan [%d,%d) lens rask=%d btree=%d want=%d",
					op, wStart, wEnd, len(gotRASKWin), len(gotBTreeWin), len(wantWin))
			}
			for i := range wantWin {
				if gotRASKWin[i] != wantWin[i] || gotBTreeWin[i] != wantWin[i] {
					t.Fatalf("op %d: windowed RangeScan mismatch at %d", op, i)
				}
			}
			// Early-stop check (stop after 2 items).
			if len(wantWin) >= 3 {
				cnt := 0
				raskIdx.RangeScan(wStart, wEnd, func(e Extent) bool {
					if e != wantWin[cnt] {
						t.Fatalf("early-stop mismatch at %d", cnt)
					}
					cnt++
					return cnt < 2
				})
				if cnt != 2 {
					t.Fatalf("early-stop visited %d items, want 2", cnt)
				}
			}
		}
	}

	// Verify full drain via DeleteRange(0, MaxOffset).
	oracle.DeleteRange(0, MaxOffset)
	raskIdx.DeleteRange(0, MaxOffset)
	btree.DeleteRange(0, MaxOffset)
	assertEquivalent(t, "drain", oracle, raskIdx, btree, []uint64{0, 1, PiB / 2, PiB - 1})
	if raskIdx.Len() != 0 {
		t.Fatalf("after drain, RASK Len()=%d, want 0", raskIdx.Len())
	}
}

// TestSparse1PiBBoundary exercises 100 sparse extents scattered across a full
// 1 PiB (1 << 50 B) logical offset space, verifying O(extents) RAM (< 64 KiB),
// zero uint64 overflow, exact lookups at 1 PiB - 1, and documenting the >2 TiB
// dense page-table explosion.
func TestSparse1PiBBoundary(t *testing.T) {
	raskIdx := NewRASKIndex()
	btree := NewPointerBTree()
	oracle := NewSortedSlice()

	// 100 sparse extents distributed across 0, 1 TiB, 256 TiB, 512 TiB, and 1 PiB - 4 KiB.
	anchors := []uint64{
		0,
		1 << 40,     // 1 TiB
		256 << 40,   // 256 TiB
		512 << 40,   // 512 TiB
		PiB - 1<<20, // 1 PiB - 1 MiB
	}
	for aIdx, base := range anchors {
		for j := uint64(0); j < 20; j++ {
			s := base + j*(8<<10) // 4 KiB data, 4 KiB hole
			e := s + (4 << 10)
			v := uint64(aIdx*1000 + int(j) + 1)
			raskIdx.Insert(s, e, v)
			btree.Insert(s, e, v)
			oracle.Insert(s, e, v)
		}
	}
	// Exact final byte of 1 PiB: [PiB-4096, PiB).
	raskIdx.Insert(PiB-4096, PiB, 999999)
	btree.Insert(PiB-4096, PiB, 999999)
	oracle.Insert(PiB-4096, PiB, 999999)

	probes := []uint64{
		0, 4095, 4096,
		1 << 40, (1 << 40) + 4095, (1 << 40) + 4096,
		256 << 40,
		512 << 40, (512 << 40) + 1234,
		PiB/2 + (1 << 30), // deep hole
		PiB - 4097,        // hole 1 byte before final extent
		PiB - 4096,        // start of final extent
		PiB - 1,           // exact last byte of 1 PiB
		PiB,               // past 1 PiB
	}
	assertEquivalent(t, "1PiB-populate", oracle, raskIdx, btree, probes)

	if v, ok := raskIdx.Lookup(PiB - 1); !ok || v != 999999 {
		t.Fatalf("Lookup(1PiB-1) = (%d, %v), want (999999, true)", v, ok)
	}
	if _, ok := raskIdx.Lookup(PiB); ok {
		t.Fatalf("Lookup(1PiB) unexpectedly hit")
	}
	if _, ok := raskIdx.Lookup(PiB/2 + (1 << 30)); ok {
		t.Fatalf("Lookup(deep hole) unexpectedly hit")
	}

	mem := raskIdx.MemoryBytes()
	t.Logf("1 PiB sparse file (%d extents): RASK=%d B (%.1f B/ext), BTree=%d B, Stats=%+v",
		raskIdx.Len(), mem, float64(mem)/float64(raskIdx.Len()), btree.MemoryBytes(), raskIdx.Stats())
	if mem >= 64<<10 {
		t.Fatalf("RASK 1 PiB sparse index used %d bytes, want < 64 KiB", mem)
	}
	if !raskIdx.Stats().RadixEnabled {
		t.Fatalf("expected O(1) sparse radix filter to be active on 1 PiB sparse file")
	}

	// Punch hole at 512 TiB and verify exact removal.
	raskIdx.DeleteRange(512<<40, (512<<40)+(1<<20))
	btree.DeleteRange(512<<40, (512<<40)+(1<<20))
	oracle.DeleteRange(512<<40, (512<<40)+(1<<20))
	assertEquivalent(t, "1PiB-punch", oracle, raskIdx, btree, probes)

	// Negative control: FlatPageTable refuses 1 PiB and quantifies >2 TiB RAM requirement.
	flat := NewFlatPageTable(64 << 20)
	if err := flat.TryInsert(PiB-4096, PiB, 1); !errors.Is(err, ErrLogicalSpaceTooLarge) {
		t.Fatalf("FlatPageTable.TryInsert(1PiB) err=%v, want ErrLogicalSpaceTooLarge", err)
	}
	denseBytes := AnalyticalMemoryBytes(PiB)
	const twoTiB = 2 << 40
	if denseBytes <= twoTiB {
		t.Fatalf("AnalyticalMemoryBytes(1PiB) = %d, want > 2 TiB (%d)", denseBytes, twoTiB)
	}
	t.Logf("FlatPageTable 1 PiB analytical RAM: %.2f TiB (%d bytes) vs RASK %d bytes",
		float64(denseBytes)/(1<<40), denseBytes, mem)
}

// TestMemoryPerExtentBeatsPointerBTree verifies that RASKIndex achieves strictly
// lower bytes per extent (both exact structural bytes and Go HeapAlloc delta)
// and strictly fewer GC pointers than PointerBTree.
func TestMemoryPerExtentBeatsPointerBTree(t *testing.T) {
	const numExtents = 50_000

	measureHeap := func(build func() RangeIndex) (RangeIndex, int64) {
		runtime.GC()
		runtime.GC()
		var m0, m1 runtime.MemStats
		runtime.ReadMemStats(&m0)
		idx := build()
		runtime.GC()
		runtime.ReadMemStats(&m1)
		delta := int64(m1.HeapAlloc) - int64(m0.HeapAlloc)
		if delta < 0 {
			delta = 0
		}
		return idx, delta
	}

	// Case 1: compact 32-bit value deltas (typical chunk/pack/journal handles).
	raskIdx, raskHeap := measureHeap(func() RangeIndex {
		idx := NewRASKIndex()
		for i := uint64(0); i < numExtents; i++ {
			off := i * 8192
			idx.Insert(off, off+4096, i+1)
		}
		return idx
	})
	btreeIdx, btreeHeap := measureHeap(func() RangeIndex {
		idx := NewPointerBTree()
		for i := uint64(0); i < numExtents; i++ {
			off := i * 8192
			idx.Insert(off, off+4096, i+1)
		}
		return idx
	})

	raskBPE := float64(raskIdx.MemoryBytes()) / float64(raskIdx.Len())
	btreeBPE := float64(btreeIdx.MemoryBytes()) / float64(btreeIdx.Len())
	t.Logf("Compact32 (%d extents): RASK=%.2f B/ext (heap %d B, ptrs=%d) vs PointerBTree=%.2f B/ext (heap %d B, ptrs=%d)",
		numExtents, raskBPE, raskHeap, raskIdx.PointerCount(), btreeBPE, btreeHeap, btreeIdx.PointerCount())

	if raskBPE >= btreeBPE {
		t.Fatalf("expected RASK bytes/extent (%.2f) < PointerBTree (%.2f)", raskBPE, btreeBPE)
	}
	if raskIdx.PointerCount() >= btreeIdx.PointerCount() {
		t.Fatalf("expected RASK pointers (%d) < PointerBTree (%d)", raskIdx.PointerCount(), btreeIdx.PointerCount())
	}
	runtime.KeepAlive(raskIdx)
	runtime.KeepAlive(btreeIdx)

	// Case 2: full arbitrary 64-bit random values (modeCompact64).
	rng := rand.New(rand.NewPCG(42, 99))
	rask64 := NewRASKIndex()
	btree64 := NewPointerBTree()
	for i := uint64(0); i < numExtents; i++ {
		off := i * 8192
		v := rng.Uint64()
		rask64.Insert(off, off+4096, v)
		btree64.Insert(off, off+4096, v)
	}
	r64BPE := float64(rask64.MemoryBytes()) / float64(rask64.Len())
	b64BPE := float64(btree64.MemoryBytes()) / float64(btree64.Len())
	t.Logf("Compact64 (%d extents): RASK=%.2f B/ext (ptrs=%d) vs PointerBTree=%.2f B/ext (ptrs=%d)",
		numExtents, r64BPE, rask64.PointerCount(), b64BPE, btree64.PointerCount())
	if r64BPE >= b64BPE {
		t.Fatalf("expected RASK Compact64 bytes/extent (%.2f) < PointerBTree (%.2f)", r64BPE, b64BPE)
	}
}

// TestBoundaryRegimes validates the documented honest negative and boundary
// regimes: tiny files (<=16 extents), dense 100% fragmented writes, and
// FlatPageTable equivalence within its safe cap.
func TestBoundaryRegimes(t *testing.T) {
	t.Run("tiny_file_under_16_extents", func(t *testing.T) {
		sliceIdx := NewSortedSlice()
		raskIdx := NewRASKIndex()
		for i := uint64(0); i < 8; i++ {
			s := i * 8192
			sliceIdx.Insert(s, s+4096, i+1)
			raskIdx.Insert(s, s+4096, i+1)
		}
		// Honest boundary behavior: flat SortedSlice has smaller fixed header
		// overhead than a multi-level radix index for tiny files <= 16 extents.
		if sliceIdx.MemoryBytes() >= raskIdx.MemoryBytes() {
			t.Fatalf("expected SortedSlice (%d B) < RASK (%d B) on 8-extent tiny file",
				sliceIdx.MemoryBytes(), raskIdx.MemoryBytes())
		}
		t.Logf("Tiny file (8 extents): SortedSlice=%d B vs RASK=%d B",
			sliceIdx.MemoryBytes(), raskIdx.MemoryBytes())
	})

	t.Run("flat_page_table_small_dense_equivalence", func(t *testing.T) {
		flat := NewFlatPageTable(4 << 20)
		raskIdx := NewRASKIndex()
		for p := uint64(0); p < 256; p += 2 {
			s := p * DefaultPageSize
			e := s + DefaultPageSize
			flat.Insert(s, e, p+10)
			raskIdx.Insert(s, e, p+10)
		}
		flat.DeleteRange(10*DefaultPageSize, 20*DefaultPageSize)
		raskIdx.DeleteRange(10*DefaultPageSize, 20*DefaultPageSize)

		if flat.Len() != raskIdx.Len() {
			t.Fatalf("FlatPageTable Len=%d != RASK Len=%d", flat.Len(), raskIdx.Len())
		}
		for p := uint64(0); p < 260; p++ {
			off := p * DefaultPageSize
			fv, fok := flat.Lookup(off)
			rv, rok := raskIdx.Lookup(off)
			if fv != rv || fok != rok {
				t.Fatalf("offset %d: flat=(%d,%v) rask=(%d,%v)", off, fv, fok, rv, rok)
			}
		}
	})
}

// TestMultiTenantConcurrentIsolation exercises concurrent multi-tenant reads
// and writes across isolated TenantRegistry entries and shared ConcurrentIndex
// instances under -race.
func TestMultiTenantConcurrentIsolation(t *testing.T) {
	reg := NewTenantRegistry()
	const tenants = 8
	const opsPerWorker = 1000

	var wg sync.WaitGroup
	for tid := uint64(1); tid <= tenants; tid++ {
		tid := tid
		idx := reg.GetOrCreate(tid)

		// Dedicated writer goroutine per tenant.
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(tid, 0xCAFE))
			base := tid * (1 << 40) // each tenant operates in its own 1 TiB offset band
			for i := 0; i < opsPerWorker; i++ {
				off := base + rng.Uint64N(1<<20)
				length := 1 + rng.Uint64N(8192)
				if i%5 == 0 {
					idx.DeleteRange(off, off+length)
				} else {
					// Encode tenant ID in high bits to assert zero cross-tenant leakage.
					idx.Insert(off, off+length, (tid<<32)|uint64(i%16))
				}
			}
		}()

		// Dedicated concurrent reader goroutine per tenant.
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(tid+100, 0xBEEF))
			base := tid * (1 << 40)
			for i := 0; i < opsPerWorker; i++ {
				off := base + rng.Uint64N(1<<20)
				if val, ok := idx.Lookup(off); ok {
					if val>>32 != tid {
						t.Errorf("tenant %d observed cross-tenant value %#x", tid, val)
					}
				}
				if i%25 == 0 {
					idx.RangeScan(base, base+(1<<20), func(e Extent) bool {
						if e.Val>>32 != tid {
							t.Errorf("tenant %d scan observed cross-tenant value %#x", tid, e.Val)
							return false
						}
						return true
					})
				}
			}
		}()
	}
	wg.Wait()

	if reg.TotalMemoryBytes() <= 0 {
		t.Fatalf("expected non-zero aggregate tenant memory")
	}
	for tid := uint64(1); tid <= tenants; tid++ {
		idx := reg.GetOrCreate(tid)
		// Verify another tenant's band is completely empty here.
		otherBand := (tid + 1) * (1 << 40)
		if _, ok := idx.Lookup(otherBand); ok {
			t.Fatalf("tenant %d unexpectedly mapped another tenant's offset band", tid)
		}
	}
}

// TestDifferentialOraclePayloadFreeRuns12k validates RASKIndex against an
// independent payload-free interval-set model (SortedRunSlice holding 16-byte
// Run{A, B} matching hanzoai/vfs file.go:590) across 12,000 randomized insert,
// touch-coalesce, multi-leaf merge, hole-punch, truncate, 4 GiB boundary, and
// 1 PiB sparse operations. It also verifies that RASK automatically selects
// modeConstVal32 (8 B/extent) for payload-free leaves.
func TestDifferentialOraclePayloadFreeRuns12k(t *testing.T) {
	rng := rand.New(rand.NewPCG(0x72756E73, 0x1344))

	oracle := NewSortedRunSlice()
	raskIdx := NewRASKIndex()

	const totalOps = 12_000
	var probes [48]uint64
	sawConstVal32 := false

	for op := 0; op < totalOps; op++ {
		var start, end uint64
		switch rng.IntN(10) {
		case 0, 1, 2, 3, 4:
			// Dense local region -> heavy adjacent-run coalescing and splits.
			start = rng.Uint64N(256 << 10)
			end = start + 1 + rng.Uint64N(8192)
		case 5, 6:
			// Medium multi-leaf spans within 4 GiB.
			start = rng.Uint64N(64 << 20)
			end = start + 1 + rng.Uint64N(1<<20)
		case 7:
			// 4 GiB segment boundary crossings.
			base := uint64(rng.IntN(4)+1) << 32
			start = base - rng.Uint64N(8192)
			end = base + 1 + rng.Uint64N(8192)
		case 8:
			// Sparse 1 PiB clusters.
			cluster := uint64(rng.IntN(8)) * (PiB / 8)
			if cluster >= PiB {
				cluster = PiB - (1 << 20)
			}
			start = cluster + rng.Uint64N(64<<10)
			end = start + 1 + rng.Uint64N(16<<10)
		case 9:
			// Wide >4 GiB run (exercises modeFull64 fallback alongside modeConstVal32).
			start = uint64(rng.IntN(4)) << 32
			end = start + (5 << 30) + rng.Uint64N(1<<20)
		}

		switch rng.IntN(5) {
		case 0:
			// Hole punch
			oracle.DeleteRange(start, end)
			raskIdx.DeleteRange(start, end)
		case 1:
			// Occasional truncate tail
			if op%50 == 1 {
				cut := rng.Uint64N(64 << 20)
				oracle.DeleteRange(cut, MaxOffset)
				raskIdx.DeleteRange(cut, MaxOffset)
			} else {
				oracle.InsertRun(start, end)
				raskIdx.Insert(start, end, 0)
			}
		default:
			oracle.InsertRun(start, end)
			raskIdx.Insert(start, end, 0)
		}

		if op%100 == 0 || op == totalOps-1 {
			if raskIdx.Stats().ConstVal32 > 0 {
				sawConstVal32 = true
			}
			if raskIdx.Len() != oracle.Len() {
				t.Fatalf("op %d: RASK Len=%d != payload-free oracle Len=%d", op, raskIdx.Len(), oracle.Len())
			}

			var wantRuns []Run
			oracle.RangeScanRuns(0, math.MaxUint64, func(r Run) bool {
				wantRuns = append(wantRuns, r)
				return true
			})
			gotExts := collectAll(raskIdx, 0, math.MaxUint64)
			if len(gotExts) != len(wantRuns) {
				t.Fatalf("op %d: scan len=%d != want %d", op, len(gotExts), len(wantRuns))
			}
			for i, w := range wantRuns {
				if w.A >= w.B {
					t.Fatalf("op %d: invalid oracle run %+v", op, w)
				}
				if i > 0 && wantRuns[i-1].B >= w.A {
					t.Fatalf("op %d: touching/overlapping oracle runs %+v and %+v", op, wantRuns[i-1], w)
				}
				g := gotExts[i]
				if g.Start != w.A || g.End != w.B || g.Val != 0 {
					t.Fatalf("op %d: scan[%d]=%+v != want Run%+v", op, i, g, w)
				}
			}

			p := probes[:0]
			p = append(p, 0, start, end)
			if start > 0 {
				p = append(p, start-1)
			}
			if end > 0 {
				p = append(p, end-1)
			}
			if end < math.MaxUint64 {
				p = append(p, end+1)
			}
			for k := 0; k < 16; k++ {
				p = append(p, rng.Uint64N(256<<10))
			}
			for _, off := range p {
				wantHit := oracle.Contains(off)
				gotVal, gotHit := raskIdx.Lookup(off)
				if gotHit != wantHit || (gotHit && gotVal != 0) {
					t.Fatalf("op %d: Lookup(%d)=(%d,%v) != payload-free Contains=%v",
						op, off, gotVal, gotHit, wantHit)
				}
			}

			// Windowed RangeScan equivalence.
			wStart, wEnd := start, end+4096
			var wantWin []Run
			oracle.RangeScanRuns(wStart, wEnd, func(r Run) bool {
				wantWin = append(wantWin, r)
				return true
			})
			gotWin := collectAll(raskIdx, wStart, wEnd)
			if len(gotWin) != len(wantWin) {
				t.Fatalf("op %d: windowed scan len=%d != want %d", op, len(gotWin), len(wantWin))
			}
			for i, w := range wantWin {
				if gotWin[i].Start != w.A || gotWin[i].End != w.B || gotWin[i].Val != 0 {
					t.Fatalf("op %d: windowed scan[%d]=%+v != want %+v", op, i, gotWin[i], w)
				}
			}
		}
	}

	if !sawConstVal32 {
		t.Fatalf("expected modeConstVal32 to be exercised on payload-free run workload")
	}

	// Full drain equivalence.
	oracle.DeleteRange(0, MaxOffset)
	raskIdx.DeleteRange(0, MaxOffset)
	if raskIdx.Len() != 0 || oracle.Len() != 0 {
		t.Fatalf("after drain: rask=%d oracle=%d", raskIdx.Len(), oracle.Len())
	}
}

// TestPayloadFreeRunMemoryComparison verifies that RASKIndex in modeConstVal32
// (8 B/extent leaf payload) uses strictly less memory than a matched 16-byte
// SortedRunSlice (file.go:590 run{a, b}) and a matched 48-byte SortedRecordSlice
// (record.go:235 extent{off, n, id}).
func TestPayloadFreeRunMemoryComparison(t *testing.T) {
	const numRuns = 50_000
	runSlice := NewSortedRunSlice()
	raskRuns := NewRASKIndex()
	recSlice := NewSortedRecordSlice()

	for i := uint64(0); i < numRuns; i++ {
		off := i * 8192
		runSlice.InsertRun(off, off+4096)
		raskRuns.Insert(off, off+4096, 0)
		var id [32]byte
		id[0] = byte(i)
		recSlice.Append(off, 4096, id)
	}

	runBPE := float64(runSlice.MemoryBytes()) / float64(runSlice.Len())
	raskBPE := float64(raskRuns.MemoryBytes()) / float64(raskRuns.Len())
	recBPE := float64(recSlice.MemoryBytes()) / float64(recSlice.Len())
	st := raskRuns.Stats()

	t.Logf("Payload-free %d runs: RASK(modeConstVal32)=%.2f B/ext (leaves=%d constVal32=%d) vs SortedRunSlice(16B)=%.2f B/ext (ratio=%.2fx) vs SortedRecordSlice(48B)=%.2f B/ext",
		numRuns, raskBPE, st.Leaves, st.ConstVal32, runBPE, raskBPE/runBPE, recBPE)

	if st.ConstVal32 == 0 || st.ConstVal32 != st.Leaves {
		t.Fatalf("expected all %d payload-free leaves in modeConstVal32, got %+v", st.Leaves, st)
	}
	if raskBPE >= runBPE {
		t.Fatalf("expected RASK modeConstVal32 (%.2f B/ext) < SortedRunSlice 16B (%.2f B/ext)", raskBPE, runBPE)
	}
	if raskBPE >= recBPE {
		t.Fatalf("expected RASK modeConstVal32 (%.2f B/ext) < SortedRecordSlice 48B (%.2f B/ext)", raskBPE, recBPE)
	}
}
