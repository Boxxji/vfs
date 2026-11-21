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

// Package rangeindex implements an in-memory compact sparse-range extent index
// (RASK) supporting 1 PiB+ logical file ranges (up to MaxSize = 2^62) without
// changing the frozen format-v2 persistent metadata B-tree.
package rangeindex

import (
	"errors"
	"math"
	"sort"
	"sync"
	"unsafe"
)

const (
	// PiB is 1 pebibyte (2^50 bytes).
	PiB uint64 = 1 << 50

	// MaxSize matches vfs.MaxSize (4 EiB = 2^62 bytes), keeping offsets and
	// lengths free of 64-bit signed/unsigned overflow.
	MaxSize uint64 = 1 << 62

	leafCap     = 128 // max extents per B+ leaf slab
	nodeCap     = 32  // max children per B+ internal node
	sparseSplit = 8   // minimum entries in a compact leaf before splitting on >4 GiB sparse jump
)

// ErrOutOfRange is returned when an offset or range exceeds MaxSize.
var ErrOutOfRange = errors.New("rangeindex: offset or length exceeds MaxSize")

// MergePolicy controls when adjacent extents are coalesced.
type MergePolicy uint8

const (
	// MergeContiguous coalesces adjacent extents when a.End() == b.Off,
	// a.Tag == b.Tag, and a.Phys+a.Len == b.Phys.
	MergeContiguous MergePolicy = iota

	// MergeAdjacent coalesces adjacent extents whenever a.End() == b.Off and
	// a.Tag == b.Tag (for logical dirty-span or coverage tracking).
	MergeAdjacent
)

// Extent maps a contiguous logical byte range [Off, Off+Len) to a backing
// physical offset/handle Phys and metadata Tag.
type Extent struct {
	Off  uint64
	Len  uint64
	Phys uint64
	Tag  uint32
}

// End returns Off + Len.
func (e Extent) End() uint64 { return e.Off + e.Len }

type leafMode uint8

const (
	modeC12  leafMode = iota // 12 B/entry: deltaOff(u32), len(u32), deltaPhys(u32), uniform baseTag
	modeC16                  // 16 B/entry: deltaOff(u32), len(u32), phys(u64), uniform baseTag
	modeWide                 // 24-28 B/entry: full u64 off/len/phys, optional u32 tag
)

type entry12 struct {
	offDelta  uint32
	length    uint32
	physDelta uint32
}

type entry16 struct {
	offDelta uint32
	length   uint32
	phys     uint64
}

type leafNode struct {
	parent   *internalNode
	prev     *leafNode
	next     *leafNode
	mode     leafMode
	baseOff  uint64
	basePhys uint64
	baseTag  uint32

	c12      []entry12
	c16      []entry16
	wideOff  []uint64
	wideLen  []uint64
	widePhys []uint64
	wideTag  []uint32 // nil when all entries equal baseTag
}

func (l *leafNode) count() int {
	switch l.mode {
	case modeC12:
		return len(l.c12)
	case modeC16:
		return len(l.c16)
	default:
		return len(l.wideOff)
	}
}

func (l *leafNode) offAt(i int) uint64 {
	switch l.mode {
	case modeC12:
		return l.baseOff + uint64(l.c12[i].offDelta)
	case modeC16:
		return l.baseOff + uint64(l.c16[i].offDelta)
	default:
		return l.wideOff[i]
	}
}

func (l *leafNode) endAt(i int) uint64 {
	switch l.mode {
	case modeC12:
		return l.baseOff + uint64(l.c12[i].offDelta) + uint64(l.c12[i].length)
	case modeC16:
		return l.baseOff + uint64(l.c16[i].offDelta) + uint64(l.c16[i].length)
	default:
		return l.wideOff[i] + l.wideLen[i]
	}
}

func (l *leafNode) get(i int) Extent {
	switch l.mode {
	case modeC12:
		e := l.c12[i]
		return Extent{
			Off:  l.baseOff + uint64(e.offDelta),
			Len:  uint64(e.length),
			Phys: l.basePhys + uint64(e.physDelta),
			Tag:  l.baseTag,
		}
	case modeC16:
		e := l.c16[i]
		return Extent{
			Off:  l.baseOff + uint64(e.offDelta),
			Len:  uint64(e.length),
			Phys: e.phys,
			Tag:  l.baseTag,
		}
	default:
		tag := l.baseTag
		if l.wideTag != nil {
			tag = l.wideTag[i]
		}
		return Extent{
			Off:  l.wideOff[i],
			Len:  l.wideLen[i],
			Phys: l.widePhys[i],
			Tag:  tag,
		}
	}
}

func (l *leafNode) firstOff() uint64 { return l.offAt(0) }
func (l *leafNode) lastEnd() uint64  { return l.endAt(l.count() - 1) }

func (l *leafNode) searchEndGE(off uint64) int {
	n := l.count()
	return sort.Search(n, func(i int) bool { return l.endAt(i) >= off })
}

func (l *leafNode) searchPoint(off uint64) int {
	lo, hi := 0, l.count()
	switch l.mode {
	case modeC12:
		if off < l.baseOff {
			return -1
		}
		d := off - l.baseOff
		c12 := l.c12
		for lo < hi {
			mid := int(uint(lo+hi) >> 1)
			if uint64(c12[mid].offDelta) <= d {
				lo = mid + 1
			} else {
				hi = mid
			}
		}
		return lo - 1
	case modeC16:
		if off < l.baseOff {
			return -1
		}
		d := off - l.baseOff
		c16 := l.c16
		for lo < hi {
			mid := int(uint(lo+hi) >> 1)
			if uint64(c16[mid].offDelta) <= d {
				lo = mid + 1
			} else {
				hi = mid
			}
		}
		return lo - 1
	default:
		wOff := l.wideOff
		for lo < hi {
			mid := int(uint(lo+hi) >> 1)
			if wOff[mid] <= off {
				lo = mid + 1
			} else {
				hi = mid
			}
		}
		return lo - 1
	}
}

// packExtents builds the tightest possible leaf encoding for a sorted slice of extents.
func (l *leafNode) packExtents(ext []Extent) {
	l.c12 = nil
	l.c16 = nil
	l.wideOff = nil
	l.wideLen = nil
	l.widePhys = nil
	l.wideTag = nil
	if len(ext) == 0 {
		l.mode = modeC12
		return
	}

	minOff := ext[0].Off
	maxOff := ext[len(ext)-1].Off
	minPhys := ext[0].Phys
	maxPhys := ext[0].Phys
	maxLen := ext[0].Len
	baseTag := ext[0].Tag
	uniformTag := true
	for _, e := range ext[1:] {
		if e.Phys < minPhys {
			minPhys = e.Phys
		}
		if e.Phys > maxPhys {
			maxPhys = e.Phys
		}
		if e.Len > maxLen {
			maxLen = e.Len
		}
		if e.Tag != baseTag {
			uniformTag = false
		}
	}

	if uniformTag && maxOff-minOff <= math.MaxUint32 && maxLen <= math.MaxUint32 {
		l.baseOff = minOff
		l.baseTag = baseTag
		if maxPhys-minPhys <= math.MaxUint32 {
			l.mode = modeC12
			l.basePhys = minPhys
			c12 := make([]entry12, len(ext))
			for i, e := range ext {
				c12[i] = entry12{
					offDelta:  uint32(e.Off - minOff),
					length:    uint32(e.Len),
					physDelta: uint32(e.Phys - minPhys),
				}
			}
			l.c12 = c12
			return
		}
		l.mode = modeC16
		l.basePhys = 0
		c16 := make([]entry16, len(ext))
		for i, e := range ext {
			c16[i] = entry16{
				offDelta: uint32(e.Off - minOff),
				length:   uint32(e.Len),
				phys:     e.Phys,
			}
		}
		l.c16 = c16
		return
	}

	l.mode = modeWide
	l.baseOff = 0
	l.basePhys = 0
	l.baseTag = baseTag
	l.wideOff = make([]uint64, len(ext))
	l.wideLen = make([]uint64, len(ext))
	l.widePhys = make([]uint64, len(ext))
	if !uniformTag {
		l.wideTag = make([]uint32, len(ext))
	}
	for i, e := range ext {
		l.wideOff[i] = e.Off
		l.wideLen[i] = e.Len
		l.widePhys[i] = e.Phys
		if !uniformTag {
			l.wideTag[i] = e.Tag
		}
	}
}

func (l *leafNode) fitsFast(e Extent) bool {
	switch l.mode {
	case modeC12:
		return e.Tag == l.baseTag &&
			e.Off >= l.baseOff && e.Off-l.baseOff <= math.MaxUint32 &&
			e.Len <= math.MaxUint32 &&
			e.Phys >= l.basePhys && e.Phys-l.basePhys <= math.MaxUint32
	case modeC16:
		return e.Tag == l.baseTag &&
			e.Off >= l.baseOff && e.Off-l.baseOff <= math.MaxUint32 &&
			e.Len <= math.MaxUint32
	default:
		return l.wideTag != nil || e.Tag == l.baseTag
	}
}

func (l *leafNode) encodeC12(e Extent) entry12 {
	return entry12{
		offDelta:  uint32(e.Off - l.baseOff),
		length:    uint32(e.Len),
		physDelta: uint32(e.Phys - l.basePhys),
	}
}

func (l *leafNode) encodeC16(e Extent) entry16 {
	return entry16{
		offDelta: uint32(e.Off - l.baseOff),
		length:   uint32(e.Len),
		phys:     e.Phys,
	}
}

// splice replaces entries [lo:hi) in l with repl. If l overflows leafCap, the
// caller splits l via splitLeaf.
func (l *leafNode) splice(lo, hi int, repl []Extent) {
	n := l.count()
	if n == 0 {
		l.packExtents(repl)
		return
	}

	allFit := true
	for _, e := range repl {
		if !l.fitsFast(e) {
			allFit = false
			break
		}
	}
	if !allFit {
		var buf [leafCap + 4]Extent
		tmp := buf[:0]
		for i := 0; i < lo; i++ {
			tmp = append(tmp, l.get(i))
		}
		tmp = append(tmp, repl...)
		for i := hi; i < n; i++ {
			tmp = append(tmp, l.get(i))
		}
		l.packExtents(tmp)
		return
	}

	newN := n - (hi - lo) + len(repl)
	switch l.mode {
	case modeC12:
		l.c12 = growAndSplice(l.c12, lo, hi, len(repl), newN)
		for i, e := range repl {
			l.c12[lo+i] = l.encodeC12(e)
		}
	case modeC16:
		l.c16 = growAndSplice(l.c16, lo, hi, len(repl), newN)
		for i, e := range repl {
			l.c16[lo+i] = l.encodeC16(e)
		}
	default:
		l.wideOff = growAndSplice(l.wideOff, lo, hi, len(repl), newN)
		l.wideLen = growAndSplice(l.wideLen, lo, hi, len(repl), newN)
		l.widePhys = growAndSplice(l.widePhys, lo, hi, len(repl), newN)
		if l.wideTag != nil {
			l.wideTag = growAndSplice(l.wideTag, lo, hi, len(repl), newN)
		}
		for i, e := range repl {
			l.wideOff[lo+i] = e.Off
			l.wideLen[lo+i] = e.Len
			l.widePhys[lo+i] = e.Phys
			if l.wideTag != nil {
				l.wideTag[lo+i] = e.Tag
			}
		}
	}
}

func growAndSplice[T any](s []T, lo, hi, replLen, newN int) []T {
	oldLen := len(s)
	if newN > cap(s) {
		newCap := max(newN, min(leafCap, max(8, cap(s)*2)))
		if newN > leafCap {
			newCap = newN
		}
		ns := make([]T, newN, newCap)
		copy(ns[:lo], s[:lo])
		copy(ns[lo+replLen:], s[hi:oldLen])
		return ns
	}
	s = s[:newN]
	if lo+replLen != hi {
		copy(s[lo+replLen:], s[hi:oldLen])
	}
	return s
}

func (l *leafNode) splitInto(cut int) *leafNode {
	n := l.count()
	var leftBuf, rightBuf [leafCap + 4]Extent
	leftExt := leftBuf[:0]
	for i := 0; i < cut; i++ {
		leftExt = append(leftExt, l.get(i))
	}
	rightExt := rightBuf[:0]
	for i := cut; i < n; i++ {
		rightExt = append(rightExt, l.get(i))
	}
	r := &leafNode{}
	l.packExtents(leftExt)
	r.packExtents(rightExt)
	return r
}

type internalNode struct {
	parent   *internalNode
	level    uint8 // >= 1; level 1 children are *leafNode, >1 are *internalNode
	keys     []uint64
	children []any
}

func childFirstOff(child any) uint64 {
	switch c := child.(type) {
	case *leafNode:
		return c.firstOff()
	case *internalNode:
		return c.keys[0]
	default:
		panic("rangeindex: unknown child node type")
	}
}

func setChildParent(child any, p *internalNode) {
	switch c := child.(type) {
	case *leafNode:
		c.parent = p
	case *internalNode:
		c.parent = p
	}
}

// Stats reports memory footprint and structural distribution of an Index.
type Stats struct {
	Extents         int
	Leaves          int
	InternalNodes   int
	Compact12Leaves int
	Compact16Leaves int
	WideLeaves      int
	CoveredBytes    uint64
	ApproxBytes     uint64
	BytesPerExtent  float64
}

// Index is a concurrent-safe, in-memory compact sparse-range extent index.
type Index struct {
	mu      sync.RWMutex
	policy  MergePolicy
	root    any // *leafNode or *internalNode; nil when empty
	head    *leafNode
	tail    *leafNode
	count   int
	covered uint64
}

// Option configures an Index.
type Option func(*Index)

// WithMergePolicy selects how adjacent extents are coalesced.
func WithMergePolicy(p MergePolicy) Option {
	return func(idx *Index) { idx.policy = p }
}

// New creates an empty sparse-range Index.
func New(opts ...Option) *Index {
	idx := &Index{policy: MergeContiguous}
	for _, opt := range opts {
		opt(idx)
	}
	return idx
}

func (idx *Index) canMerge(a, b Extent) bool {
	if a.End() != b.Off || a.Tag != b.Tag {
		return false
	}
	if idx.policy == MergeAdjacent {
		return true
	}
	return a.Phys+a.Len == b.Phys
}

// Clear removes all extents from the index.
func (idx *Index) Clear() {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.root = nil
	idx.head = nil
	idx.tail = nil
	idx.count = 0
	idx.covered = 0
}

// Len returns the number of extents stored in the index.
func (idx *Index) Len() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.count
}

// CoveredBytes returns the total logical bytes covered by all extents.
func (idx *Index) CoveredBytes() uint64 {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.covered
}

// Lookup returns the extent covering logical offset off, or false if off lies
// in a hole or beyond all extents.
func (idx *Index) Lookup(off uint64) (Extent, bool) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	if idx.root == nil || off >= MaxSize {
		return Extent{}, false
	}
	l := idx.findLeafForPoint(off)
	if l == nil {
		return Extent{}, false
	}
	i := l.searchPoint(off)
	if i < 0 {
		return Extent{}, false
	}
	e := l.get(i)
	if off < e.End() {
		return e, true
	}
	return Extent{}, false
}

// LookupRange invokes fn in ascending offset order for every extent overlapping
// [off, off+length). Iteration stops early if fn returns false.
func (idx *Index) LookupRange(off, length uint64, fn func(Extent) bool) {
	if length == 0 || off >= MaxSize {
		return
	}
	end := MaxSize
	if length < MaxSize-off {
		end = off + length
	}

	idx.mu.RLock()
	defer idx.mu.RUnlock()
	if idx.root == nil {
		return
	}
	l := idx.findLeafEndGE(off)
	if l == nil {
		return
	}
	i := l.searchEndGE(off)
	for l != nil {
		n := l.count()
		for ; i < n; i++ {
			e := l.get(i)
			if e.Off >= end {
				return
			}
			if e.End() > off {
				if !fn(e) {
					return
				}
			}
		}
		l = l.next
		i = 0
	}
}

// Extents returns a slice of all extents in ascending logical offset order.
func (idx *Index) Extents() []Extent {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	out := make([]Extent, 0, idx.count)
	for l := idx.head; l != nil; l = l.next {
		n := l.count()
		for i := 0; i < n; i++ {
			out = append(out, l.get(i))
		}
	}
	return out
}

// Insert inserts or overwrites extent e into the index, splitting partially
// overlapped extents and coalescing adjacent compatible extents.
func (idx *Index) Insert(e Extent) error {
	if e.Len == 0 {
		return nil
	}
	if e.Off >= MaxSize || e.Len > MaxSize-e.Off {
		return ErrOutOfRange
	}

	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.insertLocked(e)
	return nil
}

// Delete punches a hole across [off, off+length), splitting or trimming any
// overlapping extents so holes consume zero metadata.
func (idx *Index) Delete(off, length uint64) error {
	if length == 0 {
		return nil
	}
	if off >= MaxSize || length > MaxSize-off {
		return ErrOutOfRange
	}

	idx.mu.Lock()
	defer idx.mu.Unlock()
	if idx.root == nil {
		return nil
	}
	idx.replaceRangeLocked(off, off+length, nil)
	return nil
}

// Truncate shrinks the indexed range to size, dropping extents at or past size
// and trimming any extent crossing size.
func (idx *Index) Truncate(size uint64) error {
	if size > MaxSize {
		return ErrOutOfRange
	}
	idx.mu.Lock()
	defer idx.mu.Unlock()
	if idx.root == nil {
		return nil
	}
	for idx.tail != nil {
		l := idx.tail
		n := l.count()
		if n == 0 {
			idx.removeLeaf(l)
			continue
		}
		if l.firstOff() >= size {
			for i := 0; i < n; i++ {
				idx.covered -= l.get(i).Len
			}
			idx.count -= n
			idx.removeLeaf(l)
			continue
		}
		// Find first entry with End() > size.
		cut := sort.Search(n, func(i int) bool { return l.endAt(i) > size })
		if cut == n {
			return nil
		}
		var repl [1]Extent
		replLen := 0
		first := l.get(cut)
		if first.Off < size {
			trimmed := first
			trimmed.Len = size - first.Off
			repl[0] = trimmed
			replLen = 1
		}
		for i := cut; i < n; i++ {
			idx.covered -= l.get(i).Len
		}
		idx.count -= n - cut
		if replLen == 1 {
			idx.covered += repl[0].Len
			idx.count++
		}
		l.splice(cut, n, repl[:replLen])
		if l.count() == 0 {
			idx.removeLeaf(l)
		}
		return nil
	}
	return nil
}

func (idx *Index) findLeafForPoint(off uint64) *leafNode {
	cur := idx.root
	for cur != nil {
		switch n := cur.(type) {
		case *leafNode:
			return n
		case *internalNode:
			keys := n.keys
			lo, hi := 0, len(keys)
			for lo < hi {
				mid := int(uint(lo+hi) >> 1)
				if keys[mid] <= off {
					lo = mid + 1
				} else {
					hi = mid
				}
			}
			pos := lo - 1
			if pos < 0 {
				pos = 0
			}
			cur = n.children[pos]
		}
	}
	return nil
}

// findLeafEndGE returns the first leaf that could hold an extent with End() >= off.
func (idx *Index) findLeafEndGE(off uint64) *leafNode {
	l := idx.findLeafForPoint(off)
	if l == nil {
		return nil
	}
	if l.prev != nil && l.prev.lastEnd() >= off {
		return l.prev
	}
	return l
}

func (idx *Index) insertLocked(e Extent) {
	if idx.root == nil {
		l := &leafNode{}
		l.packExtents([]Extent{e})
		idx.root = l
		idx.head = l
		idx.tail = l
		idx.count = 1
		idx.covered = e.Len
		return
	}
	idx.replaceRangeLocked(e.Off, e.End(), &e)
}

func (idx *Index) replaceRangeLocked(insStart, insEnd uint64, ins *Extent) {
	lStart := idx.findLeafEndGE(insStart)
	idxStart := lStart.searchEndGE(insStart)

	// If insStart falls strictly after lStart's last entry, move to lStart.next
	// if lStart.next starts at or before insEnd; otherwise insert at the tail of lStart.
	if idxStart == lStart.count() {
		if lStart.next != nil && lStart.next.firstOff() <= insEnd {
			lStart = lStart.next
			idxStart = 0
		} else {
			if ins != nil {
				// Pure hole insert at the end of lStart (not touching lStart's last extent).
				idx.appendOrInsertInLeaf(lStart, idxStart, *ins)
			}
			return
		}
	}

	// Check if there are actually any extents overlapping or touching [insStart, insEnd].
	if lStart.offAt(idxStart) > insEnd {
		if ins != nil {
			idx.appendOrInsertInLeaf(lStart, idxStart, *ins)
		}
		return
	}

	// Walk forward from (lStart, idxStart) to find the exclusive end (lEnd, idxEnd)
	// of extents with Off <= insEnd.
	firstExt := lStart.get(idxStart)
	var lastExt Extent
	var removedBytes uint64
	var removedCount int

	curL := lStart
	curI := idxStart
	var lEnd *leafNode
	var idxEnd int
	for curL != nil {
		n := curL.count()
		for curI < n {
			ext := curL.get(curI)
			if ext.Off > insEnd {
				lEnd = curL
				idxEnd = curI
				goto foundEnd
			}
			lastExt = ext
			removedBytes += ext.Len
			removedCount++
			curI++
		}
		if curL.next == nil {
			lEnd = curL
			idxEnd = n
			goto foundEnd
		}
		// Check if next leaf begins past insEnd; if so, stop at end of curL.
		if curL.next.firstOff() > insEnd {
			lEnd = curL
			idxEnd = n
			goto foundEnd
		}
		curL = curL.next
		curI = 0
	}

foundEnd:
	var replBuf [3]Extent
	repl := replBuf[:0]
	if firstExt.Off < insStart {
		left := firstExt
		left.Len = insStart - firstExt.Off
		repl = append(repl, left)
	}
	if ins != nil {
		if n := len(repl); n > 0 && idx.canMerge(repl[n-1], *ins) {
			repl[n-1].Len += ins.Len
		} else {
			repl = append(repl, *ins)
		}
	}
	if lastExt.End() > insEnd {
		right := lastExt
		if right.Off < insEnd {
			delta := insEnd - right.Off
			right.Off = insEnd
			right.Phys += delta
			right.Len -= delta
		}
		if n := len(repl); n > 0 && idx.canMerge(repl[n-1], right) {
			repl[n-1].Len += right.Len
		} else {
			repl = append(repl, right)
		}
	} else if ins == nil && firstExt.End() > insEnd && lastExt.End() == firstExt.End() {
		// Already handled by lastExt.End() > insEnd above.
	}

	var addedBytes uint64
	for _, r := range repl {
		addedBytes += r.Len
	}
	idx.covered = idx.covered - removedBytes + addedBytes
	idx.count = idx.count - removedCount + len(repl)

	if lStart == lEnd {
		oldFirst := lStart.firstOff()
		lStart.splice(idxStart, idxEnd, repl)
		if lStart.count() == 0 {
			idx.removeLeaf(lStart)
			return
		}
		if lStart.firstOff() != oldFirst {
			idx.fixParentKey(lStart)
		}
		idx.ensureLeafCap(lStart)
		return
	}

	// Multi-leaf replacement across lStart .. lEnd.
	var mids []*leafNode
	for m := lStart.next; m != nil && m != lEnd; m = m.next {
		mids = append(mids, m)
	}
	for _, m := range mids {
		idx.removeLeaf(m)
	}

	// Trim trailing [idxStart:] from lStart, and leading [:idxEnd] from lEnd.
	lStart.splice(idxStart, lStart.count(), nil)

	oldEndFirst := lEnd.firstOff()
	lEnd.splice(0, idxEnd, nil)
	if lEnd.count() == 0 {
		idx.removeLeaf(lEnd)
		lEnd = nil
	} else if lEnd.firstOff() != oldEndFirst {
		idx.fixParentKey(lEnd)
	}

	if len(repl) == 0 {
		if lStart.count() == 0 {
			idx.removeLeaf(lStart)
		}
		return
	}

	if lStart.count() > 0 {
		lStart.splice(lStart.count(), lStart.count(), repl)
		idx.ensureLeafCap(lStart)
		return
	}

	// lStart was completely emptied.
	if lEnd != nil {
		lEnd.splice(0, 0, repl)
		idx.fixParentKey(lEnd)
		idx.removeLeaf(lStart)
		idx.ensureLeafCap(lEnd)
		return
	}
	lStart.packExtents(repl)
	idx.fixParentKey(lStart)
	idx.ensureLeafCap(lStart)
}

func (idx *Index) appendOrInsertInLeaf(l *leafNode, pos int, e Extent) {
	idx.count++
	idx.covered += e.Len

	// Keep compact encoding when appending after a >= 4 GiB sparse jump
	// (e.g. clustered 1 PiB offsets) once the leaf has accumulated entries.
	if pos == l.count() && l.count() >= sparseSplit && l.mode != modeWide &&
		(e.Off < l.baseOff || e.Off-l.baseOff > math.MaxUint32) {
		r := &leafNode{}
		r.packExtents([]Extent{e})
		idx.linkAfter(l, r)
		idx.insertChildAbove(l, r)
		return
	}

	var oldFirst uint64
	if l.count() > 0 {
		oldFirst = l.firstOff()
	}
	l.splice(pos, pos, []Extent{e})
	if pos == 0 && l.firstOff() != oldFirst {
		idx.fixParentKey(l)
	}
	idx.ensureLeafCap(l)
}

func (idx *Index) linkAfter(l, r *leafNode) {
	r.prev = l
	r.next = l.next
	if l.next != nil {
		l.next.prev = r
	} else {
		idx.tail = r
	}
	l.next = r
}

func (idx *Index) ensureLeafCap(l *leafNode) {
	for l.count() > leafCap {
		cut := l.count() / 2
		r := l.splitInto(cut)
		idx.linkAfter(l, r)
		idx.insertChildAbove(l, r)
		// If l was split multiple times (e.g. bulk), check r next if needed.
		l = r
	}
}

func (idx *Index) fixParentKey(child any) {
	key := childFirstOff(child)
	var parent *internalNode
	switch c := child.(type) {
	case *leafNode:
		parent = c.parent
	case *internalNode:
		parent = c.parent
	}
	curChild := child
	for parent != nil {
		pos := -1
		for i, ch := range parent.children {
			if ch == curChild {
				pos = i
				break
			}
		}
		if pos < 0 {
			return
		}
		if parent.keys[pos] == key {
			return
		}
		parent.keys[pos] = key
		if pos != 0 {
			return
		}
		curChild = parent
		parent = parent.parent
	}
}

func (idx *Index) insertChildAbove(left, right any) {
	var parent *internalNode
	switch c := left.(type) {
	case *leafNode:
		parent = c.parent
	case *internalNode:
		parent = c.parent
	}
	if parent == nil {
		var lvl uint8 = 1
		if in, ok := left.(*internalNode); ok {
			lvl = in.level + 1
		}
		newRoot := &internalNode{
			level:    lvl,
			keys:     []uint64{childFirstOff(left), childFirstOff(right)},
			children: []any{left, right},
		}
		setChildParent(left, newRoot)
		setChildParent(right, newRoot)
		idx.root = newRoot
		return
	}

	pos := -1
	for i, ch := range parent.children {
		if ch == left {
			pos = i
			break
		}
	}
	rKey := childFirstOff(right)
	parent.keys = append(parent.keys, 0)
	copy(parent.keys[pos+2:], parent.keys[pos+1:])
	parent.keys[pos+1] = rKey

	parent.children = append(parent.children, nil)
	copy(parent.children[pos+2:], parent.children[pos+1:])
	parent.children[pos+1] = right
	setChildParent(right, parent)

	if len(parent.children) > nodeCap {
		cut := len(parent.children) / 2
		rNode := &internalNode{
			level:    parent.level,
			keys:     append([]uint64(nil), parent.keys[cut:]...),
			children: append([]any(nil), parent.children[cut:]...),
		}
		for _, ch := range rNode.children {
			setChildParent(ch, rNode)
		}
		parent.keys = parent.keys[:cut]
		parent.children = parent.children[:cut]
		idx.insertChildAbove(parent, rNode)
	}
}

func (idx *Index) removeLeaf(l *leafNode) {
	if l.prev != nil {
		l.prev.next = l.next
	} else {
		idx.head = l.next
	}
	if l.next != nil {
		l.next.prev = l.prev
	} else {
		idx.tail = l.prev
	}
	l.prev = nil
	l.next = nil

	if l.parent == nil {
		idx.root = nil
		return
	}
	idx.removeChildFrom(l.parent, l)
}

func (idx *Index) removeChildFrom(parent *internalNode, child any) {
	for parent != nil {
		pos := -1
		for i, ch := range parent.children {
			if ch == child {
				pos = i
				break
			}
		}
		if pos < 0 {
			return
		}
		parent.keys = append(parent.keys[:pos], parent.keys[pos+1:]...)
		parent.children = append(parent.children[:pos], parent.children[pos+1:]...)

		if len(parent.children) == 0 {
			grand := parent.parent
			if grand == nil {
				idx.root = nil
				return
			}
			child = parent
			parent = grand
			continue
		}
		if parent.parent == nil && len(parent.children) == 1 {
			only := parent.children[0]
			setChildParent(only, nil)
			idx.root = only
			return
		}
		if pos == 0 {
			idx.fixParentKey(parent)
		}
		return
	}
}

// Stats computes exact live structural statistics and approximate memory usage.
func (idx *Index) Stats() Stats {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	st := Stats{
		Extents:      idx.count,
		CoveredBytes: idx.covered,
	}
	if idx.root == nil {
		return st
	}
	for l := idx.head; l != nil; l = l.next {
		st.Leaves++
		st.ApproxBytes += uint64(unsafe.Sizeof(*l))
		switch l.mode {
		case modeC12:
			st.Compact12Leaves++
			st.ApproxBytes += uint64(cap(l.c12)) * uint64(unsafe.Sizeof(entry12{}))
		case modeC16:
			st.Compact16Leaves++
			st.ApproxBytes += uint64(cap(l.c16)) * uint64(unsafe.Sizeof(entry16{}))
		default:
			st.WideLeaves++
			st.ApproxBytes += uint64(cap(l.wideOff)) * 24
			if l.wideTag != nil {
				st.ApproxBytes += uint64(cap(l.wideTag)) * 4
			}
		}
	}
	var walk func(any)
	walk = func(cur any) {
		n, ok := cur.(*internalNode)
		if !ok {
			return
		}
		st.InternalNodes++
		st.ApproxBytes += uint64(unsafe.Sizeof(*n)) + uint64(cap(n.keys))*8 + uint64(cap(n.children))*16
		if n.level > 1 {
			for _, ch := range n.children {
				walk(ch)
			}
		}
	}
	walk(idx.root)
	if st.Extents > 0 {
		st.BytesPerExtent = float64(st.ApproxBytes) / float64(st.Extents)
	}
	return st
}
