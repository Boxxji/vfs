package main

import (
	"sort"
)

// coverage collects the byte ranges of a run's base accesses. With maps
// false only ranges returned by read calls count.
func (r *Run) coverage(maps bool) cover {
	c := cover{}
	for _, a := range r.Base {
		if a.Kind == kindMap && !maps {
			continue
		}
		c.add(a.File, a.Lo, a.Hi)
	}
	return c
}

// Predict compares a run B with an earlier run A of the same workflow
// on the same state: how much of what B reads did A already read.
type Predict struct {
	FilesB, FilesShared     int
	BytesB, BytesShared     int64 // exact byte ranges
	PagesB, PagesShared     int64 // 4 KiB pages
	Ext1B, Ext1Shared       int64 // 1 MiB aligned extents
	Ext4B, Ext4Shared       int64 // 4 MiB aligned extents
	PagesA, PagesUsed       int64 // A's pages, and those B also reads
	TouchedB, TouchedShared int   // paths touched by any call
	Order                   []Order
	Next                    float64 // share of B's extent transitions A also made
	NextN                   int
}

// Order is how well the first K distinct extents of A match B.
type Order struct {
	K     int
	Reach float64 // share of A's first K that B reads at all
	Same  float64 // share of B's first K found in A's first K
	N     int     // size of A's prefix actually available
}

var orderKs = [...]int{10, 100, 1000, 10000}

func predict(a, b *Run, maps bool) Predict {
	ca, cb := a.coverage(maps), b.coverage(maps)
	var p Predict
	p.FilesB = len(cb)
	for f := range cb {
		if _, ok := ca[f]; ok {
			p.FilesShared++
		}
	}
	p.BytesB, p.BytesShared = cb.size(), ca.overlap(cb)
	pa, pb := ca.align(12), cb.align(12)
	p.PagesB, p.PagesShared = pb.size()/page, pa.overlap(pb)/page
	p.PagesA, p.PagesUsed = pa.size()/page, p.PagesShared
	ea, eb := ca.align(20), cb.align(20)
	p.Ext1B, p.Ext1Shared = eb.size()>>20, ea.overlap(eb)>>20
	ea, eb = ca.align(22), cb.align(22)
	p.Ext4B, p.Ext4Shared = eb.size()>>22, ea.overlap(eb)>>22

	ta := map[int32]bool{}
	for _, f := range a.Touched {
		ta[f] = true
	}
	p.TouchedB = len(b.Touched)
	for _, f := range b.Touched {
		if ta[f] {
			p.TouchedShared++
		}
	}

	sa, sb := firstTouch(a, 20, maps), firstTouch(b, 20, maps)
	inB := set(sb)
	for _, k := range orderKs {
		o := Order{K: k, N: min(k, len(sa))}
		pa, pb := sa[:min(k, len(sa))], sb[:min(k, len(sb))]
		if len(pa) > 0 {
			var reach int
			for _, x := range pa {
				if inB[x] {
					reach++
				}
			}
			o.Reach = float64(reach) / float64(len(pa))
		}
		if len(pb) > 0 {
			prefix := set(pa)
			var same int
			for _, x := range pb {
				if prefix[x] {
					same++
				}
			}
			o.Same = float64(same) / float64(len(pb))
		}
		p.Order = append(p.Order, o)
	}
	pairs := map[[2]uint64]bool{}
	for i := 1; i < len(sa); i++ {
		pairs[[2]uint64{sa[i-1], sa[i]}] = true
	}
	for i := 1; i < len(sb); i++ {
		p.NextN++
		if pairs[[2]uint64{sb[i-1], sb[i]}] {
			p.Next++
		}
	}
	if p.NextN > 0 {
		p.Next /= float64(p.NextN)
	}
	return p
}

func set(v []uint64) map[uint64]bool {
	m := make(map[uint64]bool, len(v))
	for _, x := range v {
		m[x] = true
	}
	return m
}

// firstTouch lists the distinct extents of 1<<shift bytes in the order
// the run first touched them.
func firstTouch(r *Run, shift uint, maps bool) []uint64 {
	seen := map[uint64]bool{}
	var out []uint64
	for _, a := range r.Base {
		if a.Kind == kindMap && !maps {
			continue
		}
		for e := a.Lo >> shift; e <= (a.Hi-1)>>shift; e++ {
			k := uint64(a.File)<<32 | uint64(e)
			if !seen[k] {
				seen[k] = true
				out = append(out, k)
			}
		}
	}
	return out
}

// Reuse is the part of run B's base reads that run A of another project
// also read, by class of path.
type Reuse struct {
	Class  map[string][2]int64 // class -> {bytes B reads, bytes shared}
	Total  [2]int64
	Shared cover // shared ranges, for the warm set
}

func reuse(a, b *Run, maps bool) Reuse {
	ca, cb := a.coverage(maps).align(12), b.coverage(maps).align(12)
	out := Reuse{Class: map[string][2]int64{}, Shared: cover{}}
	for f, sb := range cb {
		class := b.Class[f]
		v := out.Class[class]
		v[0] += sb.size()
		if sa, ok := ca[f]; ok {
			for _, x := range intersect(sa, sb) {
				out.Shared.add(f, x.lo, x.hi)
				v[1] += x.hi - x.lo
			}
		}
		out.Class[class] = v
		out.Total[0] += sb.size()
	}
	for _, v := range out.Class {
		out.Total[1] += v[1]
	}
	return out
}

// WorkingSet is what a run needed from the disk it started with.
type WorkingSet struct {
	Files        int
	Read, Mapped int64 // bytes by 4 KiB page: read calls only, and with maps and execs
	Ext1         int64 // bytes in 1 MiB extents, with maps
	Written      int64
	Local        int64
}

func workingset(r *Run) WorkingSet {
	w := WorkingSet{Files: r.Files, Written: r.WriteSum, Local: r.Local}
	w.Read = r.coverage(false).align(12).size()
	w.Mapped = r.coverage(true).align(12).size()
	for _, e := range extents(r.coverage(true), r, 20, func(int32) bool { return true }) {
		w.Ext1 += e.bytes
	}
	return w
}

// percentile of an unsorted slice.
func percentile(v []int64, p float64) int64 {
	if len(v) == 0 {
		return 0
	}
	s := append([]int64(nil), v...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	return s[int(float64(len(s)-1)*p)]
}
