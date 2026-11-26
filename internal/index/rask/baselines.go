// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package rask

import (
	"sort"
	"unsafe"
)

// ============================================================================
// Baseline 1: SortedSlice (sorted_slice_baseline)
// ============================================================================

// SortedSlice is a plain sorted slice of Extent values. It has minimal header
// overhead (ideal for tiny files <= 16 extents) and O(log N) binary search,
// but O(N) splice/shift cost on random inserts and hole punches.
type SortedSlice struct {
	extents []Extent
}

// NewSortedSlice constructs an empty SortedSlice baseline index.
func NewSortedSlice() *SortedSlice {
	return &SortedSlice{}
}

func (s *SortedSlice) Len() int { return len(s.extents) }

func (s *SortedSlice) Lookup(offset uint64) (uint64, bool) {
	n := len(s.extents)
	if n == 0 {
		return 0, false
	}
	lo, hi := 0, n
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if s.extents[mid].Start <= offset {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	idx := lo - 1
	if idx >= 0 && offset < s.extents[idx].End {
		return s.extents[idx].Val, true
	}
	return 0, false
}

func (s *SortedSlice) RangeScan(start, end uint64, fn func(Extent) bool) {
	if start >= end || len(s.extents) == 0 {
		return
	}
	i := sort.Search(len(s.extents), func(i int) bool {
		return s.extents[i].End > start
	})
	for ; i < len(s.extents); i++ {
		e := s.extents[i]
		if e.Start >= end {
			break
		}
		if !fn(e) {
			break
		}
	}
}

func (s *SortedSlice) Insert(start, end, val uint64) {
	if start >= end {
		return
	}
	n := len(s.extents)
	// Fast append path for monotonic sequential writes.
	if n == 0 {
		s.extents = append(s.extents, Extent{Start: start, End: end, Val: val})
		return
	}
	last := &s.extents[n-1]
	if start >= last.End {
		if start == last.End && last.Val == val {
			last.End = end
			return
		}
		s.extents = append(s.extents, Extent{Start: start, End: end, Val: val})
		return
	}

	// Binary search first extent with End >= start.
	lo := sort.Search(n, func(i int) bool {
		return s.extents[i].End >= start
	})
	// Binary search first extent with Start > end.
	hi := sort.Search(n, func(i int) bool {
		return s.extents[i].Start > end
	})

	// Pure gap insert with zero overlap and zero coalescing.
	if lo == hi {
		s.extents = append(s.extents, Extent{})
		copy(s.extents[lo+1:], s.extents[lo:n])
		s.extents[lo] = Extent{Start: start, End: end, Val: val}
		return
	}

	// Build replacement for s.extents[lo:hi] on a tiny stack buffer (at most 3 items).
	var repBuf [3]Extent
	rep := spliceExtents(repBuf[:0], s.extents[lo:hi], start, end, val)

	oldSpan := hi - lo
	newSpan := len(rep)
	diff := newSpan - oldSpan
	if diff > 0 {
		for k := 0; k < diff; k++ {
			s.extents = append(s.extents, Extent{})
		}
		copy(s.extents[hi+diff:], s.extents[hi:n])
	} else if diff < 0 {
		copy(s.extents[lo+newSpan:], s.extents[hi:n])
		s.extents = s.extents[:n+diff]
	}
	copy(s.extents[lo:lo+newSpan], rep)
}

func (s *SortedSlice) DeleteRange(start, end uint64) {
	if start >= end || len(s.extents) == 0 {
		return
	}
	n := len(s.extents)
	lo := sort.Search(n, func(i int) bool {
		return s.extents[i].End > start
	})
	if lo >= n || s.extents[lo].Start >= end {
		return
	}
	hi := sort.Search(n, func(i int) bool {
		return s.extents[i].Start >= end
	})

	var repBuf [2]Extent
	rep := deleteExtents(repBuf[:0], s.extents[lo:hi], start, end)

	oldSpan := hi - lo
	newSpan := len(rep)
	diff := newSpan - oldSpan
	if diff > 0 {
		for k := 0; k < diff; k++ {
			s.extents = append(s.extents, Extent{})
		}
		copy(s.extents[hi+diff:], s.extents[hi:n])
	} else if diff < 0 {
		copy(s.extents[lo+newSpan:], s.extents[hi:n])
		s.extents = s.extents[:n+diff]
	}
	copy(s.extents[lo:lo+newSpan], rep)
}

func (s *SortedSlice) MemoryBytes() int64 {
	return int64(unsafe.Sizeof(*s)) + int64(cap(s.extents))*int64(unsafe.Sizeof(Extent{}))
}

func (s *SortedSlice) PointerCount() int {
	if cap(s.extents) > 0 {
		return 1
	}
	return 0
}

// ============================================================================
// Baseline 2: PointerBTree (pointer_btree_baseline)
// ============================================================================

const (
	// btreeLeafMax and btreeInternalMax implement an idiomatic in-memory
	// degree-16/32 pointer B+tree (common for filesystem extent trees).
	btreeLeafMax     = 32
	btreeInternalMax = 32
)

type btLeaf struct {
	parent  *btInternal
	prev    *btLeaf
	next    *btLeaf
	n       int16
	extents [btreeLeafMax]Extent
}

func (l *btLeaf) minStart() uint64 { return l.extents[0].Start }
func (l *btLeaf) maxEnd() uint64   { return l.extents[l.n-1].End }

type btInternal struct {
	parent      *btInternal
	height      int16 // 1 = children are *btLeaf; >1 = children are *btInternal
	n           int16
	keys        [btreeInternalMax]uint64 // maxEnd of each child i in [0, n)
	childLeaves []*btLeaf
	childInts   []*btInternal
}

func (in *btInternal) maxEnd() uint64 { return in.keys[in.n-1] }

// PointerBTree is an idiomatic pointer-based in-memory B+tree of order 32
// storing uncompressed 24-byte Extents with parent and doubly-linked leaf
// pointers. It provides O(log N) inserts, deletes, and lookups, at the cost
// of higher memory per extent and greater Go GC pointer density.
type PointerBTree struct {
	root     *btInternal
	single   *btLeaf // used when tree has only 1 leaf (height 0)
	first    *btLeaf
	last     *btLeaf
	totalLen int
}

// NewPointerBTree constructs an empty PointerBTree baseline index.
func NewPointerBTree() *PointerBTree {
	return &PointerBTree{}
}

func (b *PointerBTree) Len() int { return b.totalLen }

func (b *PointerBTree) findLeafMaxEndGT(offset uint64) *btLeaf {
	if b.totalLen == 0 {
		return nil
	}
	if b.root == nil {
		if b.single.maxEnd() > offset {
			return b.single
		}
		return nil
	}
	cur := b.root
	for {
		n := int(cur.n)
		lo, hi := 0, n
		for lo < hi {
			mid := int(uint(lo+hi) >> 1)
			if cur.keys[mid] <= offset {
				lo = mid + 1
			} else {
				hi = mid
			}
		}
		if lo >= n {
			return nil
		}
		if cur.height == 1 {
			return cur.childLeaves[lo]
		}
		cur = cur.childInts[lo]
	}
}

func (b *PointerBTree) findLeafMaxEndGE(offset uint64) *btLeaf {
	if b.totalLen == 0 {
		return nil
	}
	if b.root == nil {
		if b.single.maxEnd() >= offset {
			return b.single
		}
		return nil
	}
	cur := b.root
	for {
		n := int(cur.n)
		lo, hi := 0, n
		for lo < hi {
			mid := int(uint(lo+hi) >> 1)
			if cur.keys[mid] < offset {
				lo = mid + 1
			} else {
				hi = mid
			}
		}
		if lo >= n {
			return nil
		}
		if cur.height == 1 {
			return cur.childLeaves[lo]
		}
		cur = cur.childInts[lo]
	}
}

func (b *PointerBTree) Lookup(offset uint64) (uint64, bool) {
	leaf := b.findLeafMaxEndGT(offset)
	if leaf == nil {
		return 0, false
	}
	n := int(leaf.n)
	lo, hi := 0, n
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if leaf.extents[mid].Start <= offset {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	idx := lo - 1
	if idx >= 0 && offset < leaf.extents[idx].End {
		return leaf.extents[idx].Val, true
	}
	return 0, false
}

func (b *PointerBTree) RangeScan(start, end uint64, fn func(Extent) bool) {
	if start >= end || b.totalLen == 0 {
		return
	}
	leaf := b.findLeafMaxEndGT(start)
	for leaf != nil && leaf.minStart() < end {
		n := int(leaf.n)
		i := sort.Search(n, func(i int) bool {
			return leaf.extents[i].End > start
		})
		for ; i < n; i++ {
			e := leaf.extents[i]
			if e.Start >= end {
				return
			}
			if !fn(e) {
				return
			}
		}
		leaf = leaf.next
	}
}

func (b *PointerBTree) updateMaxEndUp(leaf *btLeaf) {
	p := leaf.parent
	if p == nil {
		return
	}
	idx := -1
	for i := 0; i < int(p.n); i++ {
		if p.childLeaves[i] == leaf {
			idx = i
			break
		}
	}
	if idx < 0 {
		return
	}
	newMax := leaf.maxEnd()
	if p.keys[idx] == newMax {
		return
	}
	p.keys[idx] = newMax
	cur := p
	for cur.parent != nil {
		gp := cur.parent
		gIdx := -1
		for i := 0; i < int(gp.n); i++ {
			if gp.childInts[i] == cur {
				gIdx = i
				break
			}
		}
		if gIdx < 0 {
			break
		}
		m := cur.maxEnd()
		if gp.keys[gIdx] == m {
			break
		}
		gp.keys[gIdx] = m
		cur = gp
	}
}

func (b *PointerBTree) insertLeafAfter(target, right *btLeaf) {
	// Link doubly-linked leaf chain.
	right.prev = target
	right.next = target.next
	if target.next != nil {
		target.next.prev = right
	} else {
		b.last = right
	}
	target.next = right

	p := target.parent
	if p == nil {
		// Promote single leaf to height-1 root.
		root := &btInternal{
			height:      1,
			n:           2,
			childLeaves: make([]*btLeaf, btreeInternalMax),
		}
		root.childLeaves[0] = target
		root.childLeaves[1] = right
		root.keys[0] = target.maxEnd()
		root.keys[1] = right.maxEnd()
		target.parent = root
		right.parent = root
		b.root = root
		b.single = nil
		return
	}

	idx := -1
	for i := 0; i < int(p.n); i++ {
		if p.childLeaves[i] == target {
			idx = i
			break
		}
	}
	p.keys[idx] = target.maxEnd()
	b.insertLeafIntoInternal(p, idx+1, right)
}

func (b *PointerBTree) insertLeafIntoInternal(p *btInternal, pos int, leaf *btLeaf) {
	if int(p.n) < btreeInternalMax {
		copy(p.childLeaves[pos+1:], p.childLeaves[pos:p.n])
		copy(p.keys[pos+1:], p.keys[pos:p.n])
		p.childLeaves[pos] = leaf
		p.keys[pos] = leaf.maxEnd()
		leaf.parent = p
		p.n++
		b.refreshInternalUp(p)
		return
	}

	// Split height-1 internal node.
	var tmpLeaves [btreeInternalMax + 1]*btLeaf
	var tmpKeys [btreeInternalMax + 1]uint64
	copy(tmpLeaves[:pos], p.childLeaves[:pos])
	tmpLeaves[pos] = leaf
	copy(tmpLeaves[pos+1:], p.childLeaves[pos:p.n])

	copy(tmpKeys[:pos], p.keys[:pos])
	tmpKeys[pos] = leaf.maxEnd()
	copy(tmpKeys[pos+1:], p.keys[pos:p.n])

	total := int(p.n) + 1
	mid := total / 2

	p.n = int16(mid)
	for i := 0; i < mid; i++ {
		p.childLeaves[i] = tmpLeaves[i]
		p.keys[i] = tmpKeys[i]
		p.childLeaves[i].parent = p
	}
	for i := mid; i < btreeInternalMax; i++ {
		p.childLeaves[i] = nil
		p.keys[i] = 0
	}

	right := &btInternal{
		height:      1,
		n:           int16(total - mid),
		childLeaves: make([]*btLeaf, btreeInternalMax),
	}
	for i := mid; i < total; i++ {
		right.childLeaves[i-mid] = tmpLeaves[i]
		right.keys[i-mid] = tmpKeys[i]
		right.childLeaves[i-mid].parent = right
	}

	b.insertInternalAfter(p, right)
}

func (b *PointerBTree) insertInternalAfter(left, right *btInternal) {
	p := left.parent
	if p == nil {
		root := &btInternal{
			height:    left.height + 1,
			n:         2,
			childInts: make([]*btInternal, btreeInternalMax),
		}
		root.childInts[0] = left
		root.childInts[1] = right
		root.keys[0] = left.maxEnd()
		root.keys[1] = right.maxEnd()
		left.parent = root
		right.parent = root
		b.root = root
		return
	}

	idx := -1
	for i := 0; i < int(p.n); i++ {
		if p.childInts[i] == left {
			idx = i
			break
		}
	}
	p.keys[idx] = left.maxEnd()
	pos := idx + 1

	if int(p.n) < btreeInternalMax {
		copy(p.childInts[pos+1:], p.childInts[pos:p.n])
		copy(p.keys[pos+1:], p.keys[pos:p.n])
		p.childInts[pos] = right
		p.keys[pos] = right.maxEnd()
		right.parent = p
		p.n++
		b.refreshInternalUp(p)
		return
	}

	var tmpInts [btreeInternalMax + 1]*btInternal
	var tmpKeys [btreeInternalMax + 1]uint64
	copy(tmpInts[:pos], p.childInts[:pos])
	tmpInts[pos] = right
	copy(tmpInts[pos+1:], p.childInts[pos:p.n])

	copy(tmpKeys[:pos], p.keys[:pos])
	tmpKeys[pos] = right.maxEnd()
	copy(tmpKeys[pos+1:], p.keys[pos:p.n])

	total := int(p.n) + 1
	mid := total / 2

	p.n = int16(mid)
	for i := 0; i < mid; i++ {
		p.childInts[i] = tmpInts[i]
		p.keys[i] = tmpKeys[i]
		p.childInts[i].parent = p
	}
	for i := mid; i < btreeInternalMax; i++ {
		p.childInts[i] = nil
		p.keys[i] = 0
	}

	rightP := &btInternal{
		height:    p.height,
		n:         int16(total - mid),
		childInts: make([]*btInternal, btreeInternalMax),
	}
	for i := mid; i < total; i++ {
		rightP.childInts[i-mid] = tmpInts[i]
		rightP.keys[i-mid] = tmpKeys[i]
		rightP.childInts[i-mid].parent = rightP
	}

	b.insertInternalAfter(p, rightP)
}

func (b *PointerBTree) refreshInternalUp(cur *btInternal) {
	for cur.parent != nil {
		p := cur.parent
		for i := 0; i < int(p.n); i++ {
			if p.childInts[i] == cur {
				p.keys[i] = cur.maxEnd()
				break
			}
		}
		cur = p
	}
}

func (b *PointerBTree) removeLeaf(leaf *btLeaf) {
	// Unlink doubly-linked leaf list.
	if leaf.prev != nil {
		leaf.prev.next = leaf.next
	} else {
		b.first = leaf.next
	}
	if leaf.next != nil {
		leaf.next.prev = leaf.prev
	} else {
		b.last = leaf.prev
	}
	leaf.prev = nil
	leaf.next = nil

	p := leaf.parent
	if p == nil {
		b.single = nil
		return
	}
	leaf.parent = nil

	idx := -1
	for i := 0; i < int(p.n); i++ {
		if p.childLeaves[i] == leaf {
			idx = i
			break
		}
	}
	if idx < 0 {
		return
	}
	copy(p.childLeaves[idx:], p.childLeaves[idx+1:p.n])
	copy(p.keys[idx:], p.keys[idx+1:p.n])
	p.childLeaves[p.n-1] = nil
	p.keys[p.n-1] = 0
	p.n--

	if p.n > 0 {
		b.refreshInternalUp(p)
		b.collapseRootIfSingle()
		return
	}
	b.removeEmptyInternal(p)
}

func (b *PointerBTree) removeEmptyInternal(cur *btInternal) {
	p := cur.parent
	if p == nil {
		b.root = nil
		b.single = nil
		return
	}
	cur.parent = nil
	idx := -1
	for i := 0; i < int(p.n); i++ {
		if p.childInts[i] == cur {
			idx = i
			break
		}
	}
	if idx >= 0 {
		copy(p.childInts[idx:], p.childInts[idx+1:p.n])
		copy(p.keys[idx:], p.keys[idx+1:p.n])
		p.childInts[p.n-1] = nil
		p.keys[p.n-1] = 0
		p.n--
	}
	if p.n > 0 {
		b.refreshInternalUp(p)
		b.collapseRootIfSingle()
		return
	}
	b.removeEmptyInternal(p)
}

func (b *PointerBTree) collapseRootIfSingle() {
	for b.root != nil && b.root.n == 1 {
		if b.root.height == 1 {
			leaf := b.root.childLeaves[0]
			leaf.parent = nil
			b.root = nil
			b.single = leaf
			return
		}
		child := b.root.childInts[0]
		child.parent = nil
		b.root = child
	}
}

func (b *PointerBTree) writeLeafAndSplit(leaf *btLeaf, exts []Extent) {
	if len(exts) == 0 {
		b.removeLeaf(leaf)
		return
	}
	if len(exts) <= btreeLeafMax {
		copy(leaf.extents[:len(exts)], exts)
		leaf.n = int16(len(exts))
		b.updateMaxEndUp(leaf)
		return
	}
	// Split into chunks of <= btreeLeafMax.
	cur := leaf
	first := true
	for len(exts) > 0 {
		take := len(exts)
		if take > btreeLeafMax {
			if len(exts) <= 2*btreeLeafMax {
				take = len(exts) / 2
			} else {
				take = btreeLeafMax / 2
			}
		}
		if first {
			copy(cur.extents[:take], exts[:take])
			cur.n = int16(take)
			b.updateMaxEndUp(cur)
			first = false
		} else {
			nl := &btLeaf{n: int16(take)}
			copy(nl.extents[:take], exts[:take])
			b.insertLeafAfter(cur, nl)
			cur = nl
		}
		exts = exts[take:]
	}
}

func (b *PointerBTree) Insert(start, end, val uint64) {
	if start >= end {
		return
	}
	if b.totalLen == 0 {
		leaf := &btLeaf{n: 1}
		leaf.extents[0] = Extent{Start: start, End: end, Val: val}
		b.single = leaf
		b.first = leaf
		b.last = leaf
		b.totalLen = 1
		return
	}

	first := b.findLeafMaxEndGE(start)
	if first == nil {
		// Strictly after all leaves -> append into b.last.
		leaf := b.last
		n := int(leaf.n)
		var buf [btreeLeafMax + 2]Extent
		out := spliceExtents(buf[:0], leaf.extents[:n], start, end, val)
		b.totalLen += len(out) - n
		b.writeLeafAndSplit(leaf, out)
		return
	}

	// If [start, end) ends strictly before first.minStart(), insert into first.prev (or first).
	if end < first.minStart() {
		target := first
		if first.prev != nil && int(first.prev.n) < btreeLeafMax {
			target = first.prev
		}
		n := int(target.n)
		var buf [btreeLeafMax + 2]Extent
		out := spliceExtents(buf[:0], target.extents[:n], start, end, val)
		b.totalLen += len(out) - n
		b.writeLeafAndSplit(target, out)
		return
	}

	// Single-leaf isolated case: next leaf starts strictly after end.
	if first.next == nil || first.next.minStart() > end {
		n := int(first.n)
		var buf [btreeLeafMax + 2]Extent
		out := spliceExtents(buf[:0], first.extents[:n], start, end, val)
		b.totalLen += len(out) - n
		b.writeLeafAndSplit(first, out)
		return
	}

	// Multi-leaf span: collect all leaves starting at first whose minStart <= end.
	last := first
	for last.next != nil && last.next.minStart() <= end {
		last = last.next
	}

	var merged [2*btreeLeafMax + 2]Extent
	m := merged[:0]
	for i := 0; i < int(first.n); i++ {
		e := first.extents[i]
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

	rightStartIdx := int(last.n)
	var rightHead Extent
	hasRightHead := false
	for i := 0; i < int(last.n); i++ {
		e := last.extents[i]
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
	for i := rightStartIdx; i < int(last.n); i++ {
		m = append(m, last.extents[i])
	}

	// Remove all leaves strictly after first up through last.
	removed := int(first.n)
	cur := first.next
	for cur != nil {
		nxt := cur.next
		removed += int(cur.n)
		isLast := cur == last
		b.removeLeaf(cur)
		if isLast {
			break
		}
		cur = nxt
	}

	b.totalLen += len(m) - removed
	b.writeLeafAndSplit(first, m)
}

func (b *PointerBTree) DeleteRange(start, end uint64) {
	if start >= end || b.totalLen == 0 {
		return
	}
	first := b.findLeafMaxEndGT(start)
	if first == nil || first.minStart() >= end {
		return
	}

	if first.next == nil || first.next.minStart() >= end {
		n := int(first.n)
		var buf [btreeLeafMax + 1]Extent
		out := deleteExtents(buf[:0], first.extents[:n], start, end)
		b.totalLen += len(out) - n
		b.writeLeafAndSplit(first, out)
		return
	}

	last := first
	for last.next != nil && last.next.minStart() < end {
		last = last.next
	}

	var merged [2 * btreeLeafMax]Extent
	m := merged[:0]
	for i := 0; i < int(first.n); i++ {
		e := first.extents[i]
		if e.End <= start {
			m = append(m, e)
		} else if e.Start < start {
			m = append(m, Extent{Start: e.Start, End: start, Val: e.Val})
			break
		} else {
			break
		}
	}
	for i := 0; i < int(last.n); i++ {
		e := last.extents[i]
		if e.End <= end {
			continue
		}
		if e.Start < end {
			m = append(m, Extent{Start: end, End: e.End, Val: e.Val})
		} else {
			m = append(m, e)
		}
	}

	removed := int(first.n)
	cur := first.next
	for cur != nil {
		nxt := cur.next
		removed += int(cur.n)
		isLast := cur == last
		b.removeLeaf(cur)
		if isLast {
			break
		}
		cur = nxt
	}

	b.totalLen += len(m) - removed
	b.writeLeafAndSplit(first, m)
}

func (b *PointerBTree) MemoryBytes() int64 {
	total := int64(unsafe.Sizeof(*b))
	for leaf := b.first; leaf != nil; leaf = leaf.next {
		total += int64(unsafe.Sizeof(*leaf))
	}
	if b.root != nil {
		total += b.internalBytes(b.root)
	}
	return total
}

func (b *PointerBTree) internalBytes(in *btInternal) int64 {
	total := int64(unsafe.Sizeof(*in))
	total += int64(cap(in.childLeaves)) * int64(unsafe.Sizeof((*btLeaf)(nil)))
	total += int64(cap(in.childInts)) * int64(unsafe.Sizeof((*btInternal)(nil)))
	if in.height > 1 {
		for i := 0; i < int(in.n); i++ {
			total += b.internalBytes(in.childInts[i])
		}
	}
	return total
}

func (b *PointerBTree) PointerCount() int {
	ptrs := 0
	if b.root != nil {
		ptrs++
	}
	if b.single != nil {
		ptrs++
	}
	if b.first != nil {
		ptrs++
	}
	if b.last != nil {
		ptrs++
	}
	for leaf := b.first; leaf != nil; leaf = leaf.next {
		if leaf.parent != nil {
			ptrs++
		}
		if leaf.prev != nil {
			ptrs++
		}
		if leaf.next != nil {
			ptrs++
		}
	}
	if b.root != nil {
		ptrs += b.internalPointers(b.root)
	}
	return ptrs
}

func (b *PointerBTree) internalPointers(in *btInternal) int {
	ptrs := 0
	if in.parent != nil {
		ptrs++
	}
	if cap(in.childLeaves) > 0 {
		ptrs += 1 + int(in.n)
	}
	if cap(in.childInts) > 0 {
		ptrs += 1 + int(in.n)
		for i := 0; i < int(in.n); i++ {
			ptrs += b.internalPointers(in.childInts[i])
		}
	}
	return ptrs
}

// ============================================================================
// Baseline 3 (Negative Control): FlatPageTable (flat_page_bitmap_negative)
// ============================================================================

// FlatPageTable is the documented negative control: a dense fixed-page table
// (4 KiB granularity) mapping page index (offset >> 12) directly to a 64-bit
// value plus a presence bitmap (8.125 bytes per logical 4 KiB page).
//
// It achieves O(1) lookup on small dense files, but its memory scales as
// O(logical_size / page_size) rather than O(extents): representing a 1 PiB
// sparse file with 100 extents requires >2.03 TiB of RAM. To keep benchmarks
// and tests safe on bounded hosts, FlatPageTable refuses allocations beyond
// maxLogicalSize and records the rejected logical ceiling.
type FlatPageTable struct {
	pageSize       uint64
	maxLogicalSize uint64
	vals           []uint64
	present        []uint64 // 1 bit per page
	maxRequested   uint64
	overflowed     bool
}

// NewFlatPageTable constructs a dense 4 KiB page-table negative control capped
// at maxLogicalSize bytes (defaults to 64 MiB if 0).
func NewFlatPageTable(maxLogicalSize uint64) *FlatPageTable {
	if maxLogicalSize == 0 {
		maxLogicalSize = 64 << 20
	}
	return &FlatPageTable{
		pageSize:       DefaultPageSize,
		maxLogicalSize: maxLogicalSize,
	}
}

// AnalyticalMemoryBytes computes the exact RAM (in bytes) required by a dense
// 4 KiB page table + presence bitmap for a file of logicalSize bytes:
//
//	pages = ceil(logicalSize / 4096)
//	bytes = pages*8 + ceil(pages/64)*8  (~8.125 bytes per 4 KiB page)
func AnalyticalMemoryBytes(logicalSize uint64) uint64 {
	if logicalSize == 0 {
		return 0
	}
	pages := (logicalSize + DefaultPageSize - 1) / DefaultPageSize
	bitmapWords := (pages + 63) / 64
	return pages*8 + bitmapWords*8
}

// TryInsert attempts to map [start, end) -> val at page granularity, returning
// ErrLogicalSpaceTooLarge if end exceeds the configured dense-table RAM cap.
func (f *FlatPageTable) TryInsert(start, end, val uint64) error {
	if start >= end {
		return nil
	}
	if end > f.maxRequested {
		f.maxRequested = end
	}
	if end > f.maxLogicalSize {
		f.overflowed = true
		return ErrLogicalSpaceTooLarge
	}
	pStart := start / f.pageSize
	pEnd := (end + f.pageSize - 1) / f.pageSize
	f.ensurePages(int(pEnd))
	for p := pStart; p < pEnd; p++ {
		f.vals[p] = val
		f.present[p>>6] |= 1 << (p & 63)
	}
	return nil
}

// Overflowed reports whether any Insert exceeded maxLogicalSize.
func (f *FlatPageTable) Overflowed() bool { return f.overflowed }

// MaxRequestedLogicalSize returns the highest logical end offset requested.
func (f *FlatPageTable) MaxRequestedLogicalSize() uint64 { return f.maxRequested }

func (f *FlatPageTable) ensurePages(pages int) {
	if pages <= len(f.vals) {
		return
	}
	nv := make([]uint64, pages)
	copy(nv, f.vals)
	f.vals = nv
	words := (pages + 63) / 64
	if words > len(f.present) {
		np := make([]uint64, words)
		copy(np, f.present)
		f.present = np
	}
}

func (f *FlatPageTable) Insert(start, end, val uint64) {
	_ = f.TryInsert(start, end, val)
}

func (f *FlatPageTable) DeleteRange(start, end uint64) {
	if start >= end || len(f.vals) == 0 {
		return
	}
	pStart := start / f.pageSize
	pEnd := (end + f.pageSize - 1) / f.pageSize
	if pEnd > uint64(len(f.vals)) {
		pEnd = uint64(len(f.vals))
	}
	for p := pStart; p < pEnd; p++ {
		f.present[p>>6] &^= 1 << (p & 63)
		f.vals[p] = 0
	}
}

func (f *FlatPageTable) Lookup(offset uint64) (uint64, bool) {
	p := offset / f.pageSize
	if p >= uint64(len(f.vals)) {
		return 0, false
	}
	if (f.present[p>>6] & (1 << (p & 63))) == 0 {
		return 0, false
	}
	return f.vals[p], true
}

func (f *FlatPageTable) RangeScan(start, end uint64, fn func(Extent) bool) {
	if start >= end || len(f.vals) == 0 {
		return
	}
	pStart := start / f.pageSize
	pEnd := (end + f.pageSize - 1) / f.pageSize
	if pEnd > uint64(len(f.vals)) {
		pEnd = uint64(len(f.vals))
	}
	var cur Extent
	active := false
	for p := pStart; p < pEnd; p++ {
		if (f.present[p>>6] & (1 << (p & 63))) == 0 {
			if active {
				if !fn(cur) {
					return
				}
				active = false
			}
			continue
		}
		v := f.vals[p]
		off := p * f.pageSize
		if active && cur.Val == v && cur.End == off {
			cur.End = off + f.pageSize
		} else {
			if active {
				if !fn(cur) {
					return
				}
			}
			cur = Extent{Start: off, End: off + f.pageSize, Val: v}
			active = true
		}
	}
	if active {
		fn(cur)
	}
}

func (f *FlatPageTable) Len() int {
	count := 0
	active := false
	var lastVal uint64
	for p := 0; p < len(f.vals); p++ {
		if (f.present[p>>6] & (1 << (uint(p) & 63))) == 0 {
			active = false
			continue
		}
		v := f.vals[p]
		if !active || v != lastVal {
			count++
			lastVal = v
			active = true
		}
	}
	return count
}

func (f *FlatPageTable) MemoryBytes() int64 {
	return int64(unsafe.Sizeof(*f)) + int64(cap(f.vals))*8 + int64(cap(f.present))*8
}

func (f *FlatPageTable) PointerCount() int {
	ptrs := 0
	if cap(f.vals) > 0 {
		ptrs++
	}
	if cap(f.present) > 0 {
		ptrs++
	}
	return ptrs
}

// ============================================================================
// Baseline 4 (Engine Payload-Free Run Slice): SortedRunSlice (16 B/element)
// ============================================================================

// Run mirrors hanzoai/vfs file.go's payload-free `type run struct{ a, b uint64 }`
// (16 bytes, file.go:590), used by layer.void to track sorted, disjoint,
// non-touching half-open byte intervals [A, B) with no value field.
type Run struct {
	A uint64
	B uint64
}

// SortedRunSlice is a payload-free sorted slice of 16-byte Run intervals
// matching hanzoai/vfs's in-memory `[]run` representation. It serves both as
// the apples-to-apples 16-byte/element baseline and as the independent
// payload-free differential oracle for RASKIndex.
type SortedRunSlice struct {
	runs []Run
}

// NewSortedRunSlice constructs an empty payload-free SortedRunSlice.
func NewSortedRunSlice() *SortedRunSlice {
	return &SortedRunSlice{}
}

func (s *SortedRunSlice) Len() int { return len(s.runs) }

// Contains reports whether offset falls inside any covered run [A, B).
func (s *SortedRunSlice) Contains(offset uint64) bool {
	n := len(s.runs)
	if n == 0 {
		return false
	}
	lo, hi := 0, n
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if s.runs[mid].A <= offset {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	idx := lo - 1
	return idx >= 0 && offset < s.runs[idx].B
}

func (s *SortedRunSlice) Lookup(offset uint64) (uint64, bool) {
	if s.Contains(offset) {
		return 0, true
	}
	return 0, false
}

// RangeScanRuns visits all runs intersecting [start, end) in ascending order.
func (s *SortedRunSlice) RangeScanRuns(start, end uint64, fn func(Run) bool) {
	if start >= end || len(s.runs) == 0 {
		return
	}
	i := sort.Search(len(s.runs), func(i int) bool {
		return s.runs[i].B > start
	})
	for ; i < len(s.runs); i++ {
		r := s.runs[i]
		if r.A >= end {
			break
		}
		if !fn(r) {
			break
		}
	}
}

func (s *SortedRunSlice) RangeScan(start, end uint64, fn func(Extent) bool) {
	s.RangeScanRuns(start, end, func(r Run) bool {
		return fn(Extent{Start: r.A, End: r.B, Val: 0})
	})
}

// InsertRun adds the half-open interval [a, b), merging any overlapping or
// adjacent touching runs so invariants (sorted, disjoint, never touching) hold.
func (s *SortedRunSlice) InsertRun(a, b uint64) {
	if a >= b {
		return
	}
	n := len(s.runs)
	if n == 0 {
		s.runs = append(s.runs, Run{A: a, B: b})
		return
	}
	last := &s.runs[n-1]
	if a > last.B {
		s.runs = append(s.runs, Run{A: a, B: b})
		return
	}
	if a == last.B {
		last.B = b
		return
	}

	// First run that could touch or overlap [a, b) has B >= a.
	lo := sort.Search(n, func(i int) bool {
		return s.runs[i].B >= a
	})
	// First run strictly past [a, b) has A > b.
	hi := sort.Search(n, func(i int) bool {
		return s.runs[i].A > b
	})

	if lo == hi {
		s.runs = append(s.runs, Run{})
		copy(s.runs[lo+1:], s.runs[lo:n])
		s.runs[lo] = Run{A: a, B: b}
		return
	}

	if s.runs[lo].A < a {
		a = s.runs[lo].A
	}
	if s.runs[hi-1].B > b {
		b = s.runs[hi-1].B
	}
	s.runs[lo] = Run{A: a, B: b}
	if hi-lo > 1 {
		copy(s.runs[lo+1:], s.runs[hi:n])
		s.runs = s.runs[:n-(hi-lo-1)]
	}
}

// Insert implements RangeIndex by ignoring val and storing payload-free [start, end).
func (s *SortedRunSlice) Insert(start, end, _ uint64) {
	s.InsertRun(start, end)
}

func (s *SortedRunSlice) DeleteRange(start, end uint64) {
	if start >= end || len(s.runs) == 0 {
		return
	}
	n := len(s.runs)
	lo := sort.Search(n, func(i int) bool {
		return s.runs[i].B > start
	})
	if lo >= n || s.runs[lo].A >= end {
		return
	}
	hi := sort.Search(n, func(i int) bool {
		return s.runs[i].A >= end
	})

	var rep [2]Run
	repN := 0
	if s.runs[lo].A < start {
		rep[repN] = Run{A: s.runs[lo].A, B: start}
		repN++
	}
	if s.runs[hi-1].B > end {
		rep[repN] = Run{A: end, B: s.runs[hi-1].B}
		repN++
	}

	oldSpan := hi - lo
	diff := repN - oldSpan
	if diff > 0 {
		s.runs = append(s.runs, Run{})
		copy(s.runs[hi+1:], s.runs[hi:n])
	} else if diff < 0 {
		copy(s.runs[lo+repN:], s.runs[hi:n])
		s.runs = s.runs[:n+diff]
	}
	copy(s.runs[lo:lo+repN], rep[:repN])
}

func (s *SortedRunSlice) MemoryBytes() int64 {
	return int64(unsafe.Sizeof(*s)) + int64(cap(s.runs))*int64(unsafe.Sizeof(Run{}))
}

func (s *SortedRunSlice) PointerCount() int {
	if cap(s.runs) > 0 {
		return 1
	}
	return 0
}

// ============================================================================
// Baseline 5 (Engine Settled Record Slice): SortedRecordSlice (48 B/element)
// ============================================================================

// RecordExtent mirrors hanzoai/vfs record.go's settled `type extent struct`
// (`off uint64; n uint32; id chunkID`, record.go:235, where `chunkID` is
// `[32]byte` at chunk.go:87). Go aligns this struct to 8 bytes: 44 payload
// bytes + 4 bytes tail padding = 48 bytes/element.
type RecordExtent struct {
	Off uint64
	N   uint32
	ID  [32]byte
}

// SortedRecordSlice is a sorted slice of RecordExtent matching fileState.ext
// (`[]extent`) in hanzoai/vfs, for apples-to-apples settled-record memory
// comparison.
type SortedRecordSlice struct {
	recs []RecordExtent
}

// NewSortedRecordSlice constructs an empty SortedRecordSlice.
func NewSortedRecordSlice() *SortedRecordSlice {
	return &SortedRecordSlice{}
}

func (s *SortedRecordSlice) Len() int { return len(s.recs) }

func (s *SortedRecordSlice) Append(off uint64, length uint32, id [32]byte) {
	s.recs = append(s.recs, RecordExtent{Off: off, N: length, ID: id})
}

func (s *SortedRecordSlice) Lookup(offset uint64) (uint64, bool) {
	n := len(s.recs)
	if n == 0 {
		return 0, false
	}
	lo, hi := 0, n
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if s.recs[mid].Off <= offset {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	idx := lo - 1
	if idx >= 0 && offset < s.recs[idx].Off+uint64(s.recs[idx].N) {
		// Decode first 8 bytes of chunk ID as a handle tag.
		id := s.recs[idx].ID
		v := uint64(id[0]) | uint64(id[1])<<8 | uint64(id[2])<<16 | uint64(id[3])<<24 |
			uint64(id[4])<<32 | uint64(id[5])<<40 | uint64(id[6])<<48 | uint64(id[7])<<56
		return v, true
	}
	return 0, false
}

func (s *SortedRecordSlice) MemoryBytes() int64 {
	return int64(unsafe.Sizeof(*s)) + int64(cap(s.recs))*int64(unsafe.Sizeof(RecordExtent{}))
}

func (s *SortedRecordSlice) PointerCount() int {
	if cap(s.recs) > 0 {
		return 1
	}
	return 0
}
