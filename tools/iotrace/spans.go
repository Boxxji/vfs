package main

import "sort"

// span is the byte range [lo, hi).
type span struct{ lo, hi int64 }

// spans is a sorted set of disjoint, non-touching ranges.
type spans []span

// add inserts [lo, hi) and merges whatever it touches.
func (s *spans) add(lo, hi int64) {
	if hi <= lo {
		return
	}
	v := *s
	// first range whose end reaches lo
	i := sort.Search(len(v), func(i int) bool { return v[i].hi >= lo })
	j := i
	for j < len(v) && v[j].lo <= hi {
		if v[j].lo < lo {
			lo = v[j].lo
		}
		if v[j].hi > hi {
			hi = v[j].hi
		}
		j++
	}
	switch {
	case j == i:
		v = append(v, span{})
		copy(v[i+1:], v[i:])
		v[i] = span{lo, hi}
	default:
		v[i] = span{lo, hi}
		v = append(v[:i+1], v[j:]...)
	}
	*s = v
}

func (s spans) size() int64 {
	var n int64
	for _, r := range s {
		n += r.hi - r.lo
	}
	return n
}

// within is the number of bytes of s inside [lo, hi).
func (s spans) within(lo, hi int64) int64 {
	var n int64
	i := sort.Search(len(s), func(i int) bool { return s[i].hi > lo })
	for ; i < len(s) && s[i].lo < hi; i++ {
		a, b := s[i].lo, s[i].hi
		if a < lo {
			a = lo
		}
		if b > hi {
			b = hi
		}
		n += b - a
	}
	return n
}

// gaps lists the parts of [lo, hi) that s does not cover.
func (s spans) gaps(lo, hi int64) []span {
	var out []span
	at := lo
	i := sort.Search(len(s), func(i int) bool { return s[i].hi > lo })
	for ; i < len(s) && s[i].lo < hi; i++ {
		if s[i].lo > at {
			out = append(out, span{at, s[i].lo})
		}
		at = s[i].hi
	}
	if at < hi {
		out = append(out, span{at, hi})
	}
	return out
}

// overlap is the number of bytes both sets cover.
func overlap(a, b spans) int64 { return intersect(a, b).size() }

// intersect lists the ranges both sets cover.
func intersect(a, b spans) spans {
	var out spans
	for i, j := 0, 0; i < len(a) && j < len(b); {
		lo, hi := max(a[i].lo, b[j].lo), min(a[i].hi, b[j].hi)
		if hi > lo {
			out = append(out, span{lo, hi})
		}
		if a[i].hi < b[j].hi {
			i++
		} else {
			j++
		}
	}
	return out
}

// align widens every range to multiples of 1<<shift.
func (s spans) align(shift uint) spans {
	var out spans
	mask := int64(1)<<shift - 1
	for _, r := range s {
		out.add(r.lo&^mask, (r.hi+mask)&^mask)
	}
	return out
}

// cover is a set of byte ranges per file.
type cover map[int32]spans

func (c cover) add(file int32, lo, hi int64) {
	s := c[file]
	s.add(lo, hi)
	c[file] = s
}

func (c cover) size() int64 {
	var n int64
	for _, s := range c {
		n += s.size()
	}
	return n
}

func (c cover) align(shift uint) cover {
	out := make(cover, len(c))
	for f, s := range c {
		out[f] = s.align(shift)
	}
	return out
}

func (c cover) overlap(o cover) int64 {
	var n int64
	for f, s := range c {
		if t, ok := o[f]; ok {
			n += overlap(s, t)
		}
	}
	return n
}
