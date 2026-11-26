// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package rask

import (
	"math"
	"sort"
	"unsafe"
)

const (
	// raskLeafMax is the maximum number of extents packed into one contiguous
	// zero-pointer leaf block before splitting.
	raskLeafMax = 128

	// raskGroupMax is the maximum number of leaves per cache-aligned directory
	// group before splitting the group.
	raskGroupMax = 64

	// raskRadixShift partitions the 64-bit logical address space into 4 GiB
	// high-order radix segments (uint32 segment IDs cover up to 16 EiB, far
	// beyond 1 PiB = 1<<50 and vfs.MaxSize = 1<<62).
	raskRadixShift = 32

	// raskMaxLeafSpanSegments caps how many 4 GiB segments a single leaf may
	// span while keeping the O(1) sparse radix filter active.
	raskMaxLeafSpanSegments = 8
)

type leafMode uint8

const (
	// modeConstVal32 packs each extent as 2 uint32 words (8 bytes/extent):
	//   [relStart, length]
	// Valid when maxEnd - baseOff <= math.MaxUint32 and every extent in the
	// leaf shares the same Val (stored once in baseVal). This covers payload-free
	// run{a, b} indexes (Val == 0) as well as uniform-tag spans.
	modeConstVal32 leafMode = iota

	// modeCompact32 packs each extent as 3 uint32 words (12 bytes/extent):
	//   [relStart, length, relVal]
	// Valid when maxEnd - baseOff <= math.MaxUint32 and every extent value
	// satisfies baseVal <= val <= baseVal + math.MaxUint32.
	modeCompact32

	// modeCompact64 packs each extent as 4 uint32 words (16 bytes/extent):
	//   [relStart, length, valLo, valHi]
	// Valid when maxEnd - baseOff <= math.MaxUint32 with arbitrary 64-bit Val.
	modeCompact64

	// modeFull64 packs each extent as 6 uint32 words (24 bytes/extent):
	//   [startLo, startHi, endLo, endHi, valLo, valHi]
	// Always valid for arbitrary 64-bit offsets and >4 GiB single extents.
	modeFull64
)

func modeStride(m leafMode) int {
	switch m {
	case modeConstVal32:
		return 2
	case modeCompact32:
		return 3
	case modeCompact64:
		return 4
	default:
		return 6
	}
}

// leafAllocEntries chooses a leaf capacity aligned with Go's allocator size
// classes to keep HeapAlloc tightly bounded while amortizing appends.
func leafAllocEntries(n int) int {
	switch {
	case n <= 8:
		return 8
	case n <= 16:
		return 16
	case n <= 32:
		return 32
	case n <= 48:
		return 48
	case n <= 64:
		return 64
	case n <= 84:
		return 84
	case n <= 104:
		return 104
	default:
		return raskLeafMax
	}
}

// raskLeaf is a packed-delta, zero-pointer leaf block holding up to raskLeafMax
// sorted, disjoint, coalesced extents. All payload data resides in a single
// pointer-free []uint32 backing slice (allocated in Go's noscan spans).
type raskLeaf struct {
	baseOff uint64
	baseVal uint64
	maxEnd  uint64
	words   []uint32
	count   uint16
	mode    leafMode
}

func (l *raskLeaf) get(i int) Extent {
	w := l.words
	switch l.mode {
	case modeConstVal32:
		idx := i * 2
		s := l.baseOff + uint64(w[idx])
		return Extent{
			Start: s,
			End:   s + uint64(w[idx+1]),
			Val:   l.baseVal,
		}
	case modeCompact32:
		idx := i * 3
		s := l.baseOff + uint64(w[idx])
		return Extent{
			Start: s,
			End:   s + uint64(w[idx+1]),
			Val:   l.baseVal + uint64(w[idx+2]),
		}
	case modeCompact64:
		idx := i * 4
		s := l.baseOff + uint64(w[idx])
		v := uint64(w[idx+2]) | (uint64(w[idx+3]) << 32)
		return Extent{
			Start: s,
			End:   s + uint64(w[idx+1]),
			Val:   v,
		}
	default:
		idx := i * 6
		s := uint64(w[idx]) | (uint64(w[idx+1]) << 32)
		e := uint64(w[idx+2]) | (uint64(w[idx+3]) << 32)
		v := uint64(w[idx+4]) | (uint64(w[idx+5]) << 32)
		return Extent{Start: s, End: e, Val: v}
	}
}

func (l *raskLeaf) lastVal() uint64 {
	i := int(l.count) - 1
	w := l.words
	switch l.mode {
	case modeConstVal32:
		return l.baseVal
	case modeCompact32:
		return l.baseVal + uint64(w[i*3+2])
	case modeCompact64:
		idx := i * 4
		return uint64(w[idx+2]) | (uint64(w[idx+3]) << 32)
	default:
		idx := i * 6
		return uint64(w[idx+4]) | (uint64(w[idx+5]) << 32)
	}
}

func (l *raskLeaf) decodeAll(dst []Extent) []Extent {
	n := int(l.count)
	dst = dst[:0]
	w := l.words
	switch l.mode {
	case modeConstVal32:
		baseOff, baseVal := l.baseOff, l.baseVal
		for i := 0; i < n; i++ {
			idx := i * 2
			s := baseOff + uint64(w[idx])
			dst = append(dst, Extent{
				Start: s,
				End:   s + uint64(w[idx+1]),
				Val:   baseVal,
			})
		}
	case modeCompact32:
		baseOff, baseVal := l.baseOff, l.baseVal
		for i := 0; i < n; i++ {
			idx := i * 3
			s := baseOff + uint64(w[idx])
			dst = append(dst, Extent{
				Start: s,
				End:   s + uint64(w[idx+1]),
				Val:   baseVal + uint64(w[idx+2]),
			})
		}
	case modeCompact64:
		baseOff := l.baseOff
		for i := 0; i < n; i++ {
			idx := i * 4
			s := baseOff + uint64(w[idx])
			v := uint64(w[idx+2]) | (uint64(w[idx+3]) << 32)
			dst = append(dst, Extent{
				Start: s,
				End:   s + uint64(w[idx+1]),
				Val:   v,
			})
		}
	default:
		for i := 0; i < n; i++ {
			idx := i * 6
			s := uint64(w[idx]) | (uint64(w[idx+1]) << 32)
			e := uint64(w[idx+2]) | (uint64(w[idx+3]) << 32)
			v := uint64(w[idx+4]) | (uint64(w[idx+5]) << 32)
			dst = append(dst, Extent{Start: s, End: e, Val: v})
		}
	}
	return dst
}

// packLeafInto encodes a non-empty canonical slice (1 <= len(exts) <= raskLeafMax)
// into l, reusing l.words in place whenever its capacity is sufficient.
func packLeafInto(l *raskLeaf, exts []Extent) {
	n := len(exts)
	baseOff := exts[0].Start
	maxEnd := exts[n-1].End

	var mode leafMode
	var baseVal uint64

	if maxEnd-baseOff <= math.MaxUint32 {
		minV, maxV := exts[0].Val, exts[0].Val
		for i := 1; i < n; i++ {
			v := exts[i].Val
			if v < minV {
				minV = v
			}
			if v > maxV {
				maxV = v
			}
		}
		if minV == maxV {
			mode = modeConstVal32
			baseVal = minV
		} else if maxV-minV <= math.MaxUint32 {
			mode = modeCompact32
			baseVal = minV
		} else {
			mode = modeCompact64
		}
	} else {
		mode = modeFull64
	}

	stride := modeStride(mode)
	need := n * stride
	words := l.words
	if cap(words) >= need {
		words = words[:need]
	} else {
		capEntries := leafAllocEntries(n)
		words = make([]uint32, need, capEntries*stride)
	}

	switch mode {
	case modeConstVal32:
		for i, e := range exts {
			idx := i * 2
			words[idx] = uint32(e.Start - baseOff)
			words[idx+1] = uint32(e.End - e.Start)
		}
	case modeCompact32:
		for i, e := range exts {
			idx := i * 3
			words[idx] = uint32(e.Start - baseOff)
			words[idx+1] = uint32(e.End - e.Start)
			words[idx+2] = uint32(e.Val - baseVal)
		}
	case modeCompact64:
		for i, e := range exts {
			idx := i * 4
			words[idx] = uint32(e.Start - baseOff)
			words[idx+1] = uint32(e.End - e.Start)
			words[idx+2] = uint32(e.Val)
			words[idx+3] = uint32(e.Val >> 32)
		}
	default:
		for i, e := range exts {
			idx := i * 6
			words[idx] = uint32(e.Start)
			words[idx+1] = uint32(e.Start >> 32)
			words[idx+2] = uint32(e.End)
			words[idx+3] = uint32(e.End >> 32)
			words[idx+4] = uint32(e.Val)
			words[idx+5] = uint32(e.Val >> 32)
		}
	}

	l.baseOff = baseOff
	l.baseVal = baseVal
	l.maxEnd = maxEnd
	l.words = words
	l.count = uint16(n)
	l.mode = mode
}

// packLeaf encodes a non-empty canonical slice (1 <= len(exts) <= raskLeafMax)
// into a fresh leaf using the tightest eligible encoding mode.
func packLeaf(exts []Extent) raskLeaf {
	var l raskLeaf
	packLeafInto(&l, exts)
	return l
}

// packExtentsIntoLeaves partitions a canonical slice of extents into balanced
// leaves, splitting both across >4 GiB sparse gaps (so sparse clusters remain
// in modeCompact32) and evenly (50/50) whenever a cluster exceeds raskLeafMax.
func packExtentsIntoLeaves(exts []Extent) []raskLeaf {
	if len(exts) == 0 {
		return nil
	}
	var out []raskLeaf
	i := 0
	for i < len(exts) {
		// Group contiguous extents that fit inside one 4 GiB offset window.
		clusterEnd := i + 1
		base := exts[i].Start
		for clusterEnd < len(exts) && exts[clusterEnd].End-base <= math.MaxUint32 {
			clusterEnd++
		}
		cluster := exts[i:clusterEnd]
		if len(cluster) <= raskLeafMax {
			out = append(out, packLeaf(cluster))
		} else {
			// Split evenly (e.g. 129 -> 64 + 65) to maintain ~70-75% fill factor.
			numLeaves := (len(cluster) + raskLeafMax - 1) / raskLeafMax
			pos := 0
			for k := 0; k < numLeaves; k++ {
				rem := len(cluster) - pos
				leavesLeft := numLeaves - k
				take := rem / leavesLeft
				out = append(out, packLeaf(cluster[pos:pos+take]))
				pos += take
			}
		}
		i = clusterEnd
	}
	return out
}

func (l *raskLeaf) lookup(offset uint64) (uint64, bool) {
	if offset < l.baseOff || offset >= l.maxEnd {
		return 0, false
	}
	n := int(l.count)
	w := l.words
	switch l.mode {
	case modeConstVal32:
		target := uint32(offset - l.baseOff)
		lo, hi := 0, n
		for lo < hi {
			mid := int(uint(lo+hi) >> 1)
			if w[mid*2] <= target {
				lo = mid + 1
			} else {
				hi = mid
			}
		}
		idx := lo - 1
		if idx < 0 {
			return 0, false
		}
		base := idx * 2
		relStart := w[base]
		length := w[base+1]
		if target-relStart < length {
			return l.baseVal, true
		}
		return 0, false

	case modeCompact32:
		target := uint32(offset - l.baseOff)
		lo, hi := 0, n
		for lo < hi {
			mid := int(uint(lo+hi) >> 1)
			if w[mid*3] <= target {
				lo = mid + 1
			} else {
				hi = mid
			}
		}
		idx := lo - 1
		if idx < 0 {
			return 0, false
		}
		base := idx * 3
		relStart := w[base]
		length := w[base+1]
		if target-relStart < length {
			return l.baseVal + uint64(w[base+2]), true
		}
		return 0, false

	case modeCompact64:
		target := uint32(offset - l.baseOff)
		lo, hi := 0, n
		for lo < hi {
			mid := int(uint(lo+hi) >> 1)
			if w[mid*4] <= target {
				lo = mid + 1
			} else {
				hi = mid
			}
		}
		idx := lo - 1
		if idx < 0 {
			return 0, false
		}
		base := idx * 4
		relStart := w[base]
		length := w[base+1]
		if target-relStart < length {
			return uint64(w[base+2]) | (uint64(w[base+3]) << 32), true
		}
		return 0, false

	default:
		lo, hi := 0, n
		for lo < hi {
			mid := int(uint(lo+hi) >> 1)
			base := mid * 6
			s := uint64(w[base]) | (uint64(w[base+1]) << 32)
			if s <= offset {
				lo = mid + 1
			} else {
				hi = mid
			}
		}
		idx := lo - 1
		if idx < 0 {
			return 0, false
		}
		base := idx * 6
		e := uint64(w[base+2]) | (uint64(w[base+3]) << 32)
		if offset < e {
			return uint64(w[base+4]) | (uint64(w[base+5]) << 32), true
		}
		return 0, false
	}
}

func (l *raskLeaf) ensureCap(entries int) {
	stride := modeStride(l.mode)
	need := entries * stride
	if cap(l.words) >= need {
		return
	}
	newCap := leafAllocEntries(entries) * stride
	if newCap < need {
		newCap = need
	}
	nw := make([]uint32, len(l.words), newCap)
	copy(nw, l.words)
	l.words = nw
}

// tryFastInsert performs an in-place O(1) append or packed-array gap insert
// when the insert does not overlap, split, rebase, or overflow the leaf.
func (l *raskLeaf) tryFastInsert(start, end, val uint64) (delta int, ok bool) {
	if l.mode == modeFull64 {
		return 0, false
	}
	if start < l.baseOff || end-l.baseOff > math.MaxUint32 {
		return 0, false
	}
	if l.mode == modeConstVal32 && val != l.baseVal {
		return 0, false
	}
	if l.mode == modeCompact32 && (val < l.baseVal || val-l.baseVal > math.MaxUint32) {
		return 0, false
	}

	n := int(l.count)
	stride := modeStride(l.mode)

	// Fast path 1: contiguous extend or disjoint append at end of leaf.
	if start >= l.maxEnd {
		if start == l.maxEnd && l.lastVal() == val {
			lastBase := (n - 1) * stride
			l.words[lastBase+1] += uint32(end - start)
			l.maxEnd = end
			return 0, true
		}
		if n >= raskLeafMax {
			return 0, false
		}
		l.ensureCap(n + 1)
		l.words = l.words[:(n+1)*stride]
		base := n * stride
		l.words[base] = uint32(start - l.baseOff)
		l.words[base+1] = uint32(end - start)
		switch l.mode {
		case modeConstVal32:
			// Value is implicit in l.baseVal.
		case modeCompact32:
			l.words[base+2] = uint32(val - l.baseVal)
		default:
			l.words[base+2] = uint32(val)
			l.words[base+3] = uint32(val >> 32)
		}
		l.count++
		l.maxEnd = end
		return 1, true
	}

	// Fast path 2: strictly disjoint insertion into an interior gap.
	if n >= raskLeafMax {
		return 0, false
	}
	relStart := uint32(start - l.baseOff)
	relEnd := uint32(end - l.baseOff)
	w := l.words
	lo, hi := 0, n
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if w[mid*stride] < relStart {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	// Check left neighbor does not touch or overlap.
	if lo > 0 {
		prevBase := (lo - 1) * stride
		prevEnd := w[prevBase] + w[prevBase+1]
		if prevEnd >= relStart {
			return 0, false
		}
	}
	// Check right neighbor does not touch or overlap.
	if lo < n {
		nextStart := w[lo*stride]
		if nextStart <= relEnd {
			return 0, false
		}
	}

	l.ensureCap(n + 1)
	l.words = l.words[:(n+1)*stride]
	copy(l.words[(lo+1)*stride:], l.words[lo*stride:n*stride])
	base := lo * stride
	l.words[base] = relStart
	l.words[base+1] = uint32(end - start)
	switch l.mode {
	case modeConstVal32:
		// Value is implicit in l.baseVal.
	case modeCompact32:
		l.words[base+2] = uint32(val - l.baseVal)
	default:
		l.words[base+2] = uint32(val)
		l.words[base+3] = uint32(val >> 32)
	}
	l.count++
	if end > l.maxEnd {
		l.maxEnd = end
	}
	return 1, true
}

func (l *raskLeaf) scan(start, end uint64, fn func(Extent) bool) bool {
	if l.maxEnd <= start || l.baseOff >= end {
		return true
	}
	n := int(l.count)
	w := l.words
	baseOff := l.baseOff

	switch l.mode {
	case modeConstVal32:
		baseVal := l.baseVal
		lo := 0
		if start > baseOff {
			relTarget := uint32(start - baseOff)
			hi := n
			for lo < hi {
				mid := int(uint(lo+hi) >> 1)
				idx := mid * 2
				if w[idx]+w[idx+1] <= relTarget {
					lo = mid + 1
				} else {
					hi = mid
				}
			}
		}
		for i := lo; i < n; i++ {
			idx := i * 2
			s := baseOff + uint64(w[idx])
			if s >= end {
				break
			}
			if !fn(Extent{Start: s, End: s + uint64(w[idx+1]), Val: baseVal}) {
				return false
			}
		}
		return true

	case modeCompact32:
		baseVal := l.baseVal
		lo := 0
		if start > baseOff {
			relTarget := uint32(start - baseOff)
			hi := n
			for lo < hi {
				mid := int(uint(lo+hi) >> 1)
				idx := mid * 3
				if w[idx]+w[idx+1] <= relTarget {
					lo = mid + 1
				} else {
					hi = mid
				}
			}
		}
		for i := lo; i < n; i++ {
			idx := i * 3
			s := baseOff + uint64(w[idx])
			if s >= end {
				break
			}
			if !fn(Extent{Start: s, End: s + uint64(w[idx+1]), Val: baseVal + uint64(w[idx+2])}) {
				return false
			}
		}
		return true

	case modeCompact64:
		lo := 0
		if start > baseOff {
			relTarget := uint32(start - baseOff)
			hi := n
			for lo < hi {
				mid := int(uint(lo+hi) >> 1)
				idx := mid * 4
				if w[idx]+w[idx+1] <= relTarget {
					lo = mid + 1
				} else {
					hi = mid
				}
			}
		}
		for i := lo; i < n; i++ {
			idx := i * 4
			s := baseOff + uint64(w[idx])
			if s >= end {
				break
			}
			v := uint64(w[idx+2]) | (uint64(w[idx+3]) << 32)
			if !fn(Extent{Start: s, End: s + uint64(w[idx+1]), Val: v}) {
				return false
			}
		}
		return true

	default:
		lo, hi := 0, n
		for lo < hi {
			mid := int(uint(lo+hi) >> 1)
			idx := mid * 6
			e := uint64(w[idx+2]) | (uint64(w[idx+3]) << 32)
			if e <= start {
				lo = mid + 1
			} else {
				hi = mid
			}
		}
		for i := lo; i < n; i++ {
			idx := i * 6
			s := uint64(w[idx]) | (uint64(w[idx+1]) << 32)
			if s >= end {
				break
			}
			e := uint64(w[idx+2]) | (uint64(w[idx+3]) << 32)
			v := uint64(w[idx+4]) | (uint64(w[idx+5]) << 32)
			if !fn(Extent{Start: s, End: e, Val: v}) {
				return false
			}
		}
		return true
	}
}

// raskGroup is a cache-aligned directory block holding up to raskGroupMax
// leaves stored contiguously by value, paired with a compact leafMaxEnd fence
// array for L1-resident binary search.
type raskGroup struct {
	baseOff    uint64
	maxEnd     uint64
	leafMaxEnd []uint64
	leaves     []raskLeaf
}

func newGroup(leaves []raskLeaf) raskGroup {
	g := raskGroup{
		leafMaxEnd: make([]uint64, len(leaves)),
		leaves:     make([]raskLeaf, len(leaves)),
	}
	copy(g.leaves, leaves)
	for i := range g.leaves {
		g.leafMaxEnd[i] = g.leaves[i].maxEnd
	}
	g.syncBounds()
	return g
}

func (g *raskGroup) syncBounds() {
	if len(g.leaves) == 0 {
		g.baseOff = 0
		g.maxEnd = 0
		return
	}
	g.baseOff = g.leaves[0].baseOff
	g.maxEnd = g.leaves[len(g.leaves)-1].maxEnd
}

// radixSlot is an entry in the open-addressed, pointer-free 4 GiB segment
// radix directory used for O(1) sparse-hole skipping across 1 PiB files.
type radixSlot struct {
	seg        uint32
	used       uint32
	firstGroup uint32
	lastGroup  uint32
}

// RASKIndex is a compact, cache-friendly, radix-prefixed in-memory sparse-range
// index inspired by RASK (Zhao et al., USENIX FAST '26).
//
// Design highlights:
//   - Two-level flat group directory storing leaf headers by value (no per-extent
//     or per-node pointer chains).
//   - Zero-pointer packed-delta leaf blocks (8 B/extent in modeConstVal32 for
//     payload-free run{a,b} / uniform-value leaves, 12 B/extent in modeCompact32,
//     16 B/extent in modeCompact64, 24 B/extent fallback in modeFull64).
//   - Open-addressed 4 GiB segment radix table giving O(1) hole rejection on
//     petabyte-scale sparse files with O(extents) memory.
type RASKIndex struct {
	groups     []raskGroup
	radix      []radixSlot
	radixMask  uint32
	radixValid bool
	totalLen   int
}

// NewRASKIndex constructs an empty RASKIndex.
func NewRASKIndex() *RASKIndex {
	return &RASKIndex{radixValid: true}
}

// Len returns the canonical number of stored extents.
func (r *RASKIndex) Len() int {
	return r.totalLen
}

// Lookup returns the value covering offset, or (0, false) for a sparse hole.
// Lookup is strictly read-only and safe for concurrent readers under RLock.
func (r *RASKIndex) Lookup(offset uint64) (uint64, bool) {
	if r.totalLen == 0 {
		return 0, false
	}
	gLo, gHi := 0, len(r.groups)-1
	if r.radixValid && len(r.radix) > 0 {
		seg := uint32(offset >> raskRadixShift)
		idx := (seg * 0x9e3779b1) & r.radixMask
		found := false
		for {
			s := r.radix[idx]
			if s.used == 0 {
				break
			}
			if s.seg == seg {
				gLo = int(s.firstGroup)
				gHi = int(s.lastGroup)
				found = true
				break
			}
			idx = (idx + 1) & r.radixMask
		}
		if !found {
			return 0, false
		}
	}

	// Find the single group in [gLo, gHi] with maxEnd > offset.
	gi := gLo
	if gLo < gHi {
		lo, hi := gLo, gHi+1
		for lo < hi {
			mid := int(uint(lo+hi) >> 1)
			if r.groups[mid].maxEnd <= offset {
				lo = mid + 1
			} else {
				hi = mid
			}
		}
		if lo > gHi {
			return 0, false
		}
		gi = lo
	}
	g := &r.groups[gi]
	if offset < g.baseOff || offset >= g.maxEnd {
		return 0, false
	}

	// Binary search compact leafMaxEnd fence array inside the group.
	fences := g.leafMaxEnd
	lo, hi := 0, len(fences)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if fences[mid] <= offset {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo >= len(g.leaves) {
		return 0, false
	}
	return g.leaves[lo].lookup(offset)
}

// RangeScan visits all stored extents intersecting [start, end) in ascending
// Start order. RangeScan is strictly read-only.
func (r *RASKIndex) RangeScan(start, end uint64, fn func(Extent) bool) {
	if start >= end || r.totalLen == 0 {
		return
	}
	// Find first group with maxEnd > start.
	gi := sort.Search(len(r.groups), func(i int) bool {
		return r.groups[i].maxEnd > start
	})
	for ; gi < len(r.groups); gi++ {
		g := &r.groups[gi]
		if g.baseOff >= end {
			break
		}
		li := sort.Search(len(g.leafMaxEnd), func(i int) bool {
			return g.leafMaxEnd[i] > start
		})
		for ; li < len(g.leaves); li++ {
			leaf := &g.leaves[li]
			if leaf.baseOff >= end {
				return
			}
			if !leaf.scan(start, end, fn) {
				return
			}
		}
	}
}

// leafLoc identifies a leaf position inside r.groups.
type leafLoc struct {
	g int
	l int
}

func (a leafLoc) valid() bool { return a.g >= 0 }

func (a leafLoc) cmp(b leafLoc) int {
	if a.g != b.g {
		if a.g < b.g {
			return -1
		}
		return 1
	}
	if a.l < b.l {
		return -1
	}
	if a.l > b.l {
		return 1
	}
	return 0
}

// firstLeafMaxEndGE returns the first leaf whose maxEnd >= offset.
func (r *RASKIndex) firstLeafMaxEndGE(offset uint64) leafLoc {
	gi := sort.Search(len(r.groups), func(i int) bool {
		return r.groups[i].maxEnd >= offset
	})
	if gi >= len(r.groups) {
		return leafLoc{-1, -1}
	}
	g := &r.groups[gi]
	li := sort.Search(len(g.leafMaxEnd), func(i int) bool {
		return g.leafMaxEnd[i] >= offset
	})
	if li >= len(g.leaves) {
		return leafLoc{-1, -1}
	}
	return leafLoc{gi, li}
}

// firstLeafMaxEndGT returns the first leaf whose maxEnd > offset.
func (r *RASKIndex) firstLeafMaxEndGT(offset uint64) leafLoc {
	gi := sort.Search(len(r.groups), func(i int) bool {
		return r.groups[i].maxEnd > offset
	})
	if gi >= len(r.groups) {
		return leafLoc{-1, -1}
	}
	g := &r.groups[gi]
	li := sort.Search(len(g.leafMaxEnd), func(i int) bool {
		return g.leafMaxEnd[i] > offset
	})
	if li >= len(g.leaves) {
		return leafLoc{-1, -1}
	}
	return leafLoc{gi, li}
}

// lastLeafBaseOffLE returns the last leaf whose baseOff <= offset.
func (r *RASKIndex) lastLeafBaseOffLE(offset uint64) leafLoc {
	gi := sort.Search(len(r.groups), func(i int) bool {
		return r.groups[i].baseOff > offset
	}) - 1
	if gi < 0 {
		return leafLoc{-1, -1}
	}
	g := &r.groups[gi]
	li := sort.Search(len(g.leaves), func(i int) bool {
		return g.leaves[i].baseOff > offset
	}) - 1
	if li < 0 {
		return leafLoc{-1, -1}
	}
	return leafLoc{gi, li}
}

// lastLeafBaseOffLT returns the last leaf whose baseOff < offset.
func (r *RASKIndex) lastLeafBaseOffLT(offset uint64) leafLoc {
	gi := sort.Search(len(r.groups), func(i int) bool {
		return r.groups[i].baseOff >= offset
	}) - 1
	if gi < 0 {
		return leafLoc{-1, -1}
	}
	g := &r.groups[gi]
	li := sort.Search(len(g.leaves), func(i int) bool {
		return g.leaves[i].baseOff >= offset
	}) - 1
	if li < 0 {
		return leafLoc{-1, -1}
	}
	return leafLoc{gi, li}
}

// Insert maps [start, end) -> val with exact overlap splitting, overwriting,
// and global adjacent run coalescing.
func (r *RASKIndex) Insert(start, end, val uint64) {
	if start >= end {
		return
	}
	if r.totalLen == 0 {
		leaf := packLeaf([]Extent{{Start: start, End: end, Val: val}})
		r.groups = []raskGroup{newGroup([]raskLeaf{leaf})}
		r.totalLen = 1
		r.rebuildRadix()
		return
	}

	first := r.firstLeafMaxEndGE(start)
	last := r.lastLeafBaseOffLE(end)

	// Case 1: strictly after all existing extents.
	if !first.valid() {
		gi := len(r.groups) - 1
		g := &r.groups[gi]
		li := len(g.leaves) - 1
		leaf := &g.leaves[li]
		if leaf.count < raskLeafMax && end-leaf.baseOff <= math.MaxUint32 {
			r.insertIntoSingleLeaf(gi, li, start, end, val)
			return
		}
		newLeaf := packLeaf([]Extent{{Start: start, End: end, Val: val}})
		r.totalLen++
		r.insertLeavesAt(gi, li+1, []raskLeaf{newLeaf})
		return
	}

	// Case 2: strictly before all existing extents.
	if !last.valid() {
		g := &r.groups[0]
		leaf := &g.leaves[0]
		if leaf.count < raskLeafMax && leaf.maxEnd-start <= math.MaxUint32 {
			r.insertIntoSingleLeaf(0, 0, start, end, val)
			return
		}
		newLeaf := packLeaf([]Extent{{Start: start, End: end, Val: val}})
		r.totalLen++
		r.insertLeavesAt(0, 0, []raskLeaf{newLeaf})
		return
	}

	cmp := first.cmp(last)

	// Case 3: strictly in the gap between last (prev) and first (next).
	if cmp > 0 {
		prev := &r.groups[last.g].leaves[last.l]
		if prev.count < raskLeafMax && end-prev.baseOff <= math.MaxUint32 {
			r.insertIntoSingleLeaf(last.g, last.l, start, end, val)
			return
		}
		next := &r.groups[first.g].leaves[first.l]
		if next.count < raskLeafMax && next.maxEnd-start <= math.MaxUint32 {
			r.insertIntoSingleLeaf(first.g, first.l, start, end, val)
			return
		}
		newLeaf := packLeaf([]Extent{{Start: start, End: end, Val: val}})
		r.totalLen++
		r.insertLeavesAt(last.g, last.l+1, []raskLeaf{newLeaf})
		return
	}

	// Case 4: isolated to a single leaf.
	if cmp == 0 {
		r.insertIntoSingleLeaf(first.g, first.l, start, end, val)
		return
	}

	// Case 5: spans multiple leaves [first .. last].
	var leftSrc [raskLeafMax]Extent
	var rightSrc [raskLeafMax]Extent
	var merged [2*raskLeafMax + 2]Extent

	firstLeaf := &r.groups[first.g].leaves[first.l]
	lastLeaf := &r.groups[last.g].leaves[last.l]
	lExts := firstLeaf.decodeAll(leftSrc[:0])
	rExts := lastLeaf.decodeAll(rightSrc[:0])

	m := merged[:0]
	for _, e := range lExts {
		if e.End < start {
			m = append(m, e)
		} else if e.Start < start {
			if e.Val == val {
				start = e.Start
			} else {
				m = append(m, Extent{Start: e.Start, End: start, Val: e.Val})
			}
			break
		} else {
			break
		}
	}

	// Collect right extents after end, absorbing any right coalescing run.
	rightStartIdx := len(rExts)
	var rightHead Extent
	hasRightHead := false
	for i, e := range rExts {
		if e.End <= end {
			continue
		}
		if e.Start <= end {
			if e.Val == val {
				end = e.End
				rightStartIdx = i + 1
			} else if e.Start < end {
				rightHead = Extent{Start: end, End: e.End, Val: e.Val}
				hasRightHead = true
				rightStartIdx = i + 1
			} else {
				rightStartIdx = i
			}
		} else {
			rightStartIdx = i
		}
		break
	}

	m = append(m, Extent{Start: start, End: end, Val: val})
	if hasRightHead {
		m = append(m, rightHead)
	}
	if rightStartIdx < len(rExts) {
		m = append(m, rExts[rightStartIdx:]...)
	}

	newLeaves := packExtentsIntoLeaves(m)
	r.replaceLeafRange(first, last, newLeaves, len(m))
}

func (r *RASKIndex) insertIntoSingleLeaf(gi, li int, start, end, val uint64) {
	g := &r.groups[gi]
	leaf := &g.leaves[li]
	oldSegA := uint32(leaf.baseOff >> raskRadixShift)
	oldSegB := uint32((leaf.maxEnd - 1) >> raskRadixShift)

	if delta, ok := leaf.tryFastInsert(start, end, val); ok {
		r.totalLen += delta
		g.leafMaxEnd[li] = leaf.maxEnd
		g.syncBounds()
		newSegA := uint32(leaf.baseOff >> raskRadixShift)
		newSegB := uint32((leaf.maxEnd - 1) >> raskRadixShift)
		if newSegA != oldSegA || newSegB != oldSegB {
			r.rebuildRadix()
		}
		return
	}

	var src [raskLeafMax]Extent
	var dst [raskLeafMax + 2]Extent
	oldCount := int(leaf.count)
	s := leaf.decodeAll(src[:0])
	out := spliceExtents(dst[:0], s, start, end, val)
	r.totalLen += len(out) - oldCount

	// Zero-allocation in-place repack whenever out fits cleanly inside one leaf.
	if len(out) <= raskLeafMax && (len(out) == 1 || out[len(out)-1].End-out[0].Start <= math.MaxUint32) {
		packLeafInto(leaf, out)
		g.leafMaxEnd[li] = leaf.maxEnd
		g.syncBounds()
		newSegA := uint32(leaf.baseOff >> raskRadixShift)
		newSegB := uint32((leaf.maxEnd - 1) >> raskRadixShift)
		if !r.radixValid || newSegA != oldSegA || newSegB != oldSegB {
			r.rebuildRadix()
		}
		return
	}

	newLeaves := packExtentsIntoLeaves(out)
	if len(newLeaves) == 1 {
		g.leaves[li] = newLeaves[0]
		g.leafMaxEnd[li] = newLeaves[0].maxEnd
		g.syncBounds()
		newSegA := uint32(newLeaves[0].baseOff >> raskRadixShift)
		newSegB := uint32((newLeaves[0].maxEnd - 1) >> raskRadixShift)
		if !r.radixValid || newSegA != oldSegA || newSegB != oldSegB {
			r.rebuildRadix()
		}
		return
	}

	// Replace single leaf with 2+ balanced split leaves in group gi.
	r.replaceLeavesInGroup(gi, li, li+1, newLeaves)
}

// DeleteRange removes [start, end) (hole punch / truncate).
func (r *RASKIndex) DeleteRange(start, end uint64) {
	if start >= end || r.totalLen == 0 {
		return
	}
	first := r.firstLeafMaxEndGT(start)
	last := r.lastLeafBaseOffLT(end)
	if !first.valid() || !last.valid() {
		return
	}
	cmp := first.cmp(last)
	if cmp > 0 {
		return
	}

	if cmp == 0 {
		g := &r.groups[first.g]
		leaf := &g.leaves[first.l]
		oldSegA := uint32(leaf.baseOff >> raskRadixShift)
		oldSegB := uint32((leaf.maxEnd - 1) >> raskRadixShift)
		oldCount := int(leaf.count)

		var src [raskLeafMax]Extent
		var dst [raskLeafMax + 1]Extent
		out := deleteExtents(dst[:0], leaf.decodeAll(src[:0]), start, end)
		r.totalLen += len(out) - oldCount

		if len(out) == 0 {
			r.replaceLeavesInGroup(first.g, first.l, first.l+1, nil)
			return
		}
		if len(out) <= raskLeafMax && (len(out) == 1 || out[len(out)-1].End-out[0].Start <= math.MaxUint32) {
			packLeafInto(leaf, out)
			g.leafMaxEnd[first.l] = leaf.maxEnd
			g.syncBounds()
			newSegA := uint32(leaf.baseOff >> raskRadixShift)
			newSegB := uint32((leaf.maxEnd - 1) >> raskRadixShift)
			if !r.radixValid || newSegA != oldSegA || newSegB != oldSegB {
				r.rebuildRadix()
			}
			return
		}
		newLeaves := packExtentsIntoLeaves(out)
		if len(newLeaves) == 1 {
			g.leaves[first.l] = newLeaves[0]
			g.leafMaxEnd[first.l] = newLeaves[0].maxEnd
			g.syncBounds()
			newSegA := uint32(newLeaves[0].baseOff >> raskRadixShift)
			newSegB := uint32((newLeaves[0].maxEnd - 1) >> raskRadixShift)
			if !r.radixValid || newSegA != oldSegA || newSegB != oldSegB {
				r.rebuildRadix()
			}
			return
		}
		r.replaceLeavesInGroup(first.g, first.l, first.l+1, newLeaves)
		return
	}

	// Multi-leaf delete across [first .. last].
	var leftSrc [raskLeafMax]Extent
	var rightSrc [raskLeafMax]Extent
	var merged [2 * raskLeafMax]Extent

	firstLeaf := &r.groups[first.g].leaves[first.l]
	lastLeaf := &r.groups[last.g].leaves[last.l]
	m := merged[:0]
	for _, e := range firstLeaf.decodeAll(leftSrc[:0]) {
		if e.End <= start {
			m = append(m, e)
		} else if e.Start < start {
			m = append(m, Extent{Start: e.Start, End: start, Val: e.Val})
			break
		} else {
			break
		}
	}
	for _, e := range lastLeaf.decodeAll(rightSrc[:0]) {
		if e.End <= end {
			continue
		}
		if e.Start < end {
			m = append(m, Extent{Start: end, End: e.End, Val: e.Val})
		} else {
			m = append(m, e)
		}
	}

	newLeaves := packExtentsIntoLeaves(m)
	r.replaceLeafRange(first, last, newLeaves, len(m))
}

func (r *RASKIndex) insertLeavesAt(gi, li int, add []raskLeaf) {
	r.replaceLeavesInGroup(gi, li, li, add)
}

func groupAllocCap(n int) int {
	switch {
	case n <= 8:
		return 8
	case n <= 16:
		return 16
	case n <= 32:
		return 32
	default:
		return raskGroupMax
	}
}

// replaceLeavesInGroup replaces g.leaves[lo:hi] with rep inside group gi,
// mutating in place when the group stays within raskGroupMax, splitting when
// it overflows, or removing the group if it becomes empty.
func (r *RASKIndex) replaceLeavesInGroup(gi, lo, hi int, rep []raskLeaf) {
	g := &r.groups[gi]
	oldLen := len(g.leaves)
	newLen := oldLen - (hi - lo) + len(rep)

	if newLen == 0 {
		copy(r.groups[gi:], r.groups[gi+1:])
		r.groups[len(r.groups)-1] = raskGroup{}
		r.groups = r.groups[:len(r.groups)-1]
		r.rebuildRadix()
		return
	}

	if newLen <= raskGroupMax {
		if cap(g.leaves) >= newLen {
			if newLen > oldLen {
				g.leaves = g.leaves[:newLen]
				copy(g.leaves[lo+len(rep):], g.leaves[hi:oldLen])
			} else {
				copy(g.leaves[lo+len(rep):], g.leaves[hi:oldLen])
				for k := newLen; k < oldLen; k++ {
					g.leaves[k] = raskLeaf{}
				}
				g.leaves = g.leaves[:newLen]
			}
			copy(g.leaves[lo:], rep)
		} else {
			c := groupAllocCap(newLen)
			updated := make([]raskLeaf, newLen, c)
			copy(updated[:lo], g.leaves[:lo])
			copy(updated[lo:], rep)
			copy(updated[lo+len(rep):], g.leaves[hi:oldLen])
			g.leaves = updated
		}
		if cap(g.leafMaxEnd) >= newLen {
			g.leafMaxEnd = g.leafMaxEnd[:newLen]
		} else {
			g.leafMaxEnd = make([]uint64, newLen, groupAllocCap(newLen))
		}
		for i := range g.leaves {
			g.leafMaxEnd[i] = g.leaves[i].maxEnd
		}
		g.syncBounds()
		if !r.radixValid || !r.radixCoversLeaves(gi, rep) {
			r.rebuildRadix()
		}
		return
	}

	// Overflow: materialize and split evenly into balanced groups <= raskGroupMax.
	updated := make([]raskLeaf, 0, newLen)
	updated = append(updated, g.leaves[:lo]...)
	updated = append(updated, rep...)
	updated = append(updated, g.leaves[hi:]...)

	var splitGroups []raskGroup
	for len(updated) > 0 {
		take := len(updated)
		if take > raskGroupMax {
			if len(updated) <= 2*raskGroupMax {
				take = len(updated) / 2
			} else {
				take = raskGroupMax / 2
			}
		}
		splitGroups = append(splitGroups, newGroup(updated[:take]))
		updated = updated[take:]
	}

	newGroups := make([]raskGroup, 0, len(r.groups)-1+len(splitGroups))
	newGroups = append(newGroups, r.groups[:gi]...)
	newGroups = append(newGroups, splitGroups...)
	newGroups = append(newGroups, r.groups[gi+1:]...)
	r.groups = newGroups
	r.rebuildRadix()
}

// replaceLeafRange replaces all leaves in the inclusive range [first .. last]
// with rep (containing newExtentCount extents total).
func (r *RASKIndex) replaceLeafRange(first, last leafLoc, rep []raskLeaf, newExtentCount int) {
	// Count removed extents across [first .. last].
	removed := 0
	for gi := first.g; gi <= last.g; gi++ {
		g := &r.groups[gi]
		lo := 0
		if gi == first.g {
			lo = first.l
		}
		hi := len(g.leaves) - 1
		if gi == last.g {
			hi = last.l
		}
		for li := lo; li <= hi; li++ {
			removed += int(g.leaves[li].count)
		}
	}
	r.totalLen += newExtentCount - removed

	if first.g == last.g {
		r.replaceLeavesInGroup(first.g, first.l, last.l+1, rep)
		return
	}

	// Combine surviving prefix of first.g, rep, and surviving suffix of last.g.
	prefix := r.groups[first.g].leaves[:first.l]
	suffix := r.groups[last.g].leaves[last.l+1:]
	combined := make([]raskLeaf, 0, len(prefix)+len(rep)+len(suffix))
	combined = append(combined, prefix...)
	combined = append(combined, rep...)
	combined = append(combined, suffix...)

	var midGroups []raskGroup
	for len(combined) > 0 {
		take := len(combined)
		if take > raskGroupMax {
			if len(combined) <= 2*raskGroupMax {
				take = len(combined) / 2
			} else {
				take = raskGroupMax
			}
		}
		midGroups = append(midGroups, newGroup(combined[:take]))
		combined = combined[take:]
	}

	newGroups := make([]raskGroup, 0, first.g+len(midGroups)+(len(r.groups)-1-last.g))
	newGroups = append(newGroups, r.groups[:first.g]...)
	newGroups = append(newGroups, midGroups...)
	newGroups = append(newGroups, r.groups[last.g+1:]...)
	r.groups = newGroups
	r.rebuildRadix()
}

func (r *RASKIndex) radixCoversLeaves(gi int, leaves []raskLeaf) bool {
	if !r.radixValid || len(r.radix) == 0 {
		return false
	}
	for i := range leaves {
		segA := uint32(leaves[i].baseOff >> raskRadixShift)
		segB := uint32((leaves[i].maxEnd - 1) >> raskRadixShift)
		if segB-segA > raskMaxLeafSpanSegments {
			return false
		}
		for seg := segA; ; seg++ {
			idx := (seg * 0x9e3779b1) & r.radixMask
			ok := false
			for {
				s := r.radix[idx]
				if s.used == 0 {
					break
				}
				if s.seg == seg {
					if uint32(gi) >= s.firstGroup && uint32(gi) <= s.lastGroup {
						ok = true
					}
					break
				}
				idx = (idx + 1) & r.radixMask
			}
			if !ok {
				return false
			}
			if seg == segB {
				break
			}
		}
	}
	return true
}

func (r *RASKIndex) rebuildRadix() {
	if len(r.groups) == 0 {
		r.radix = nil
		r.radixMask = 0
		r.radixValid = true
		return
	}

	// Pass 1: verify no leaf spans more than raskMaxLeafSpanSegments 4 GiB segments.
	for gi := range r.groups {
		leaves := r.groups[gi].leaves
		for li := range leaves {
			segA := uint32(leaves[li].baseOff >> raskRadixShift)
			segB := uint32((leaves[li].maxEnd - 1) >> raskRadixShift)
			if segB-segA > raskMaxLeafSpanSegments {
				r.radix = nil
				r.radixMask = 0
				r.radixValid = false
				return
			}
		}
	}

	// Start compact (16 slots = 256 B) and grow only as distinct 4 GiB segments appear.
	size := uint32(16)
	table := make([]radixSlot, size)
	mask := size - 1
	usedCount := uint32(0)

	insertSeg := func(seg, gi uint32) {
		if (usedCount+1)*2 > uint32(len(table)) {
			newSize := uint32(len(table)) << 1
			newTable := make([]radixSlot, newSize)
			newMask := newSize - 1
			for _, s := range table {
				if s.used == 0 {
					continue
				}
				p := (s.seg * 0x9e3779b1) & newMask
				for newTable[p].used != 0 {
					p = (p + 1) & newMask
				}
				newTable[p] = s
			}
			table = newTable
			mask = newMask
		}
		p := (seg * 0x9e3779b1) & mask
		for {
			s := &table[p]
			if s.used == 0 {
				*s = radixSlot{seg: seg, used: 1, firstGroup: gi, lastGroup: gi}
				usedCount++
				return
			}
			if s.seg == seg {
				if gi < s.firstGroup {
					s.firstGroup = gi
				}
				if gi > s.lastGroup {
					s.lastGroup = gi
				}
				return
			}
			p = (p + 1) & mask
		}
	}

	for gi := range r.groups {
		leaves := r.groups[gi].leaves
		for li := range leaves {
			segA := uint32(leaves[li].baseOff >> raskRadixShift)
			segB := uint32((leaves[li].maxEnd - 1) >> raskRadixShift)
			for seg := segA; ; seg++ {
				insertSeg(seg, uint32(gi))
				if seg == segB {
					break
				}
			}
		}
	}

	r.radix = table
	r.radixMask = mask
	r.radixValid = true
}

// MemoryBytes returns the exact structural memory footprint in bytes.
func (r *RASKIndex) MemoryBytes() int64 {
	total := int64(unsafe.Sizeof(*r))
	total += int64(cap(r.groups)) * int64(unsafe.Sizeof(raskGroup{}))
	total += int64(cap(r.radix)) * int64(unsafe.Sizeof(radixSlot{}))
	for i := range r.groups {
		g := &r.groups[i]
		total += int64(cap(g.leafMaxEnd)) * 8
		total += int64(cap(g.leaves)) * int64(unsafe.Sizeof(raskLeaf{}))
		for j := range g.leaves {
			total += int64(cap(g.leaves[j].words)) * 4
		}
	}
	return total
}

// PointerCount returns the exact number of heap pointers retained in the index.
// Notice that leaf payloads ([]uint32) and radix slots contain zero pointers.
func (r *RASKIndex) PointerCount() int {
	ptrs := 0
	if cap(r.groups) > 0 {
		ptrs++
	}
	if cap(r.radix) > 0 {
		ptrs++
	}
	for i := range r.groups {
		g := &r.groups[i]
		if cap(g.leafMaxEnd) > 0 {
			ptrs++
		}
		if cap(g.leaves) > 0 {
			ptrs++
		}
		for j := range g.leaves {
			if cap(g.leaves[j].words) > 0 {
				ptrs++
			}
		}
	}
	return ptrs
}

// ModeStats returns counts of leaves in each encoding mode for diagnostics.
type ModeStats struct {
	Groups       int
	Leaves       int
	ConstVal32   int
	Compact32    int
	Compact64    int
	Full64       int
	RadixSlots   int
	RadixEnabled bool
}

// Stats returns structural diagnostics for benchmarking and reporting.
func (r *RASKIndex) Stats() ModeStats {
	st := ModeStats{
		Groups:       len(r.groups),
		RadixSlots:   len(r.radix),
		RadixEnabled: r.radixValid && len(r.radix) > 0,
	}
	for i := range r.groups {
		for j := range r.groups[i].leaves {
			st.Leaves++
			switch r.groups[i].leaves[j].mode {
			case modeConstVal32:
				st.ConstVal32++
			case modeCompact32:
				st.Compact32++
			case modeCompact64:
				st.Compact64++
			default:
				st.Full64++
			}
		}
	}
	return st
}
