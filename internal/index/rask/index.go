// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

// Package rask implements a clean-room RASK-inspired compact sparse-range
// index (Zhao et al., USENIX FAST '26; HIP-1344) alongside matched in-memory
// interval-index baselines for hanzoai/vfs sparse files and journal spans.
//
// The index operates purely in memory and leaves the persistent format-v2
// prolly/Merkle tree layout untouched. Every RangeIndex implementation enforces
// canonical half-open interval semantics over [0, 1<<62) (including 1 PiB
// sparse files with O(extents) memory):
//   - Half-open intervals [Start, End) with Start < End
//   - Strictly sorted and pairwise disjoint
//   - Adjacent intervals with identical Val are always coalesced
package rask

import "errors"

const (
	// PiB is 1 pebibyte (1 << 50 bytes), the sparse logical address scale
	// exercised by hanzoai/vfs sparse-file tests.
	PiB uint64 = 1 << 50

	// MaxOffset matches vfs.MaxSize (4 EiB = 1 << 62 bytes).
	MaxOffset uint64 = 1 << 62

	// DefaultPageSize is the 4 KiB granularity used by FlatPageTable.
	DefaultPageSize uint64 = 4096
)

// ErrLogicalSpaceTooLarge is returned by FlatPageTable when a sparse logical
// offset exceeds its configured dense-table RAM budget.
var ErrLogicalSpaceTooLarge = errors.New("rask: logical offset exceeds dense page-table memory cap")

// Extent maps a half-open logical byte range [Start, End) to a 64-bit metadata
// value (for example a chunk/pack descriptor, journal span offset, or write tag).
type Extent struct {
	Start uint64
	End   uint64
	Val   uint64
}

// Len returns the byte length of the extent.
func (e Extent) Len() uint64 {
	if e.End <= e.Start {
		return 0
	}
	return e.End - e.Start
}

// RangeIndex is the shared in-memory interval index contract implemented by
// RASKIndex and every matched baseline in this package.
type RangeIndex interface {
	// Insert maps [start, end) -> val, overwriting, trimming, or splitting any
	// overlapping extents and coalescing adjacent runs that share val.
	// Calls with start >= end are no-ops.
	Insert(start, end, val uint64)

	// DeleteRange removes [start, end) (hole punch or truncate), trimming or
	// splitting partially overlapping extents. Calls with start >= end are no-ops.
	DeleteRange(start, end uint64)

	// Lookup returns the value covering offset, or (0, false) if offset falls
	// in an unmapped sparse hole.
	Lookup(offset uint64) (val uint64, ok bool)

	// RangeScan visits every stored extent intersecting [start, end)
	// (e.End > start && e.Start < end) in ascending Start order. Iteration stops
	// early if fn returns false. Calls with start >= end are no-ops.
	RangeScan(start, end uint64, fn func(Extent) bool)

	// Len returns the canonical count of disjoint, coalesced extents.
	Len() int

	// MemoryBytes returns the exact structural in-memory footprint in bytes
	// (backing slice capacities, node structs, and directory tables).
	MemoryBytes() int64

	// PointerCount returns the exact number of heap pointers retained inside the
	// index structure (reflecting Go GC mark-phase scan work).
	PointerCount() int
}

// spliceExtents applies Insert(start, end, val) to a canonical sorted slice of
// extents, appending the canonical result into dst[:0].
func spliceExtents(dst, src []Extent, start, end, val uint64) []Extent {
	dst = dst[:0]
	n := len(src)
	i := 0
	for i < n && src[i].End < start {
		dst = append(dst, src[i])
		i++
	}
	// Handle left boundary: may touch at start or straddle start.
	if i < n && src[i].Start < start {
		e := src[i]
		if e.Val == val {
			// Coalesce with left neighbor / straddling extent of same value.
			start = e.Start
			if e.End > end {
				end = e.End
			}
			i++
		} else {
			// Emit trimmed left prefix [e.Start, start).
			dst = append(dst, Extent{Start: e.Start, End: start, Val: e.Val})
			if e.End > end {
				// Single extent strictly straddles [start, end) with a different val.
				dst = append(dst, Extent{Start: start, End: end, Val: val})
				dst = append(dst, Extent{Start: end, End: e.End, Val: e.Val})
				dst = append(dst, src[i+1:]...)
				return dst
			}
			i++
		}
	}
	// Skip all extents fully covered by [start, end).
	for i < n && src[i].End <= end {
		i++
	}
	// Handle right boundary: may straddle end or touch at end.
	var tail Extent
	hasTail := false
	if i < n && src[i].Start <= end {
		e := src[i]
		if e.Val == val {
			if e.End > end {
				end = e.End
			}
			i++
		} else if e.Start < end {
			tail = Extent{Start: end, End: e.End, Val: e.Val}
			hasTail = true
			i++
		}
	}
	dst = append(dst, Extent{Start: start, End: end, Val: val})
	if hasTail {
		dst = append(dst, tail)
	}
	dst = append(dst, src[i:]...)
	return dst
}

// deleteExtents applies DeleteRange(start, end) to a canonical sorted slice of
// extents, appending the canonical result into dst[:0].
func deleteExtents(dst, src []Extent, start, end uint64) []Extent {
	dst = dst[:0]
	n := len(src)
	i := 0
	for i < n && src[i].End <= start {
		dst = append(dst, src[i])
		i++
	}
	for i < n && src[i].Start < end {
		e := src[i]
		if e.Start < start {
			dst = append(dst, Extent{Start: e.Start, End: start, Val: e.Val})
		}
		if e.End > end {
			dst = append(dst, Extent{Start: end, End: e.End, Val: e.Val})
		}
		i++
	}
	dst = append(dst, src[i:]...)
	return dst
}
