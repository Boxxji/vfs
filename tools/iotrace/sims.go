package main

import (
	"fmt"
	"io"
	"slices"
	"sort"
)

type policy struct {
	name  string
	cache int64
	ahead bool
	trace string // "", "same" (run 1 of the project) or "other" (run 1 of the other project)
	node  bool   // run 1 ran on the same node just before
	base  bool   // the base-image warm set is preloaded
}

var policies = []policy{
	{name: "no cache"},
	{name: "LRU 64 MiB", cache: 64 << 20},
	{name: "LRU 512 MiB", cache: 512 << 20},
	{name: "LRU 4 GiB", cache: 4 << 30},
	{name: "LRU 4 GiB + read-ahead", cache: 4 << 30, ahead: true},
	{name: "LRU 512 MiB + trace prefetch", cache: 512 << 20, trace: "same"},
	{name: "LRU 4 GiB + trace prefetch", cache: 4 << 30, trace: "same"},
	{name: "LRU 4 GiB + read-ahead + trace prefetch", cache: 4 << 30, ahead: true, trace: "same"},
	{name: "LRU 4 GiB + trace of the other project", cache: 4 << 30, trace: "other"},
	{name: "LRU 4 GiB + base warm set", cache: 4 << 30, base: true},
	{name: "same node, LRU 64 MiB", cache: 64 << 20, node: true},
	{name: "same node, LRU 512 MiB", cache: 512 << 20, node: true},
	{name: "same node, LRU 4 GiB", cache: 4 << 30, node: true},
}

// Outcome sums a policy's results over the projects of a workflow.
type Outcome struct {
	res    [len(fetches)]Result
	useful int64 // bytes the runs needed, by 4 KiB page
}

func (o *Outcome) add(rs [len(fetches)]Result, useful int64) {
	for i := range rs {
		o.res[i].Reqs += rs[i].Reqs
		o.res[i].Lookups += rs[i].Lookups
		o.res[i].Hit += rs[i].Hit
		o.res[i].Late += rs[i].Late
		o.res[i].Miss += rs[i].Miss
		o.res[i].Stalled += rs[i].Stalled
		o.res[i].Fetched += rs[i].Fetched
		o.res[i].Wasted += rs[i].Wasted
		o.res[i].Stall += rs[i].Stall
		o.res[i].StallMax = max(o.res[i].StallMax, rs[i].StallMax)
	}
	o.useful += useful
}

// extents turns shared byte ranges into the extents that cover them.
func extents(c cover, r *Run, sh uint, keep func(file int32) bool) []ext {
	var out []ext
	var files []int32
	for f := range c {
		files = append(files, f)
	}
	sort.Slice(files, func(i, j int) bool { return files[i] < files[j] })
	for _, f := range files {
		if !keep(f) {
			continue
		}
		for _, s := range c[f].align(sh) {
			for off := s.lo; off < s.hi; off += 1 << sh {
				n := int64(1) << sh
				if fs := r.Sizes[f]; fs > 0 && off+n > fs {
					n = max(fs-off, 1)
				}
				out = append(out, ext{uint64(f)<<32 | uint64(off>>sh), n})
			}
		}
	}
	return out
}

// evaluate runs the named policies (all when none are named) for run 2
// of each project of a workflow.
func evaluate(d *Dataset, wf string, pc int64, sh uint, conc int, only ...string) map[string]*Outcome {
	labels, by := projects(d.Runs[wf])
	out := map[string]*Outcome{}
	var run []policy
	for _, p := range policies {
		if len(only) == 0 || slices.Contains(only, p.name) {
			out[p.name] = &Outcome{}
			run = append(run, p)
		}
	}
	for i, l := range labels {
		if len(by[l]) < 2 {
			continue
		}
		a, b := by[l][0], by[l][1]
		sa, sb := requests(a, pc, sh, true), requests(b, pc, sh, true)
		size := func(f int32) int64 { return b.Sizes[f] }
		useful := b.coverage(true).align(12).size()
		same := order(sa)
		var other []ext
		var warm []ext
		if len(labels) > 1 {
			o := by[labels[(i+1)%len(labels)]][0]
			other = order(requests(o, pc, sh, true))
			u := reuse(o, b, true)
			warm = extents(u.Shared, b, sh, func(f int32) bool {
				c := b.Class[f]
				return c == "toolchain" || c == "system" || c == "cache"
			})
		}
		for _, p := range run {
			var rs [len(fetches)]Result
			for k, c := range fetches {
				cfg := Config{Cache: p.cache, Fetch: c, Conc: conc, Ahead: p.ahead, Shift: sh}
				switch p.trace {
				case "same":
					cfg.Order = same
				case "other":
					cfg.Order = other
				}
				if p.base {
					cfg.Warm = warm
				}
				if p.node {
					rs[k] = follow(sa, sb, size, cfg)
				} else {
					rs[k] = simulate(sb, size, cfg)
				}
			}
			out[p.name].add(rs, useful)
		}
	}
	return out
}

func simTables(w io.Writer, d *Dataset, wfs []string) {
	fmt.Fprintf(w, "### Cache policies on run 2 (cold node unless stated; 1 MiB extents, %d MiB guest page cache, %d prefetches in flight)\n\n", pagecache>>20, conc)
	fmt.Fprintf(w, "hit, late and miss are shares of extent lookups at a 5 ms fetch; amplification is bytes fetched over the bytes the run needed by 4 KiB page; stall is the time reads waited, summed over threads / the busiest thread, for a 1, 5 and 20 ms fetch.\n\n")
	var rows [][]string
	for _, wf := range wfs {
		out := evaluate(d, wf, pagecache, shift, conc)
		for _, p := range policies {
			o := out[p.name]
			r := o.res[1]
			if r.Lookups == 0 && o.useful == 0 {
				continue
			}
			row := []string{wf, p.name, fmt.Sprint(r.Reqs), pct(r.Hit, r.Lookups), pct(r.Late, r.Lookups), pct(r.Miss, r.Lookups),
				mib(r.Fetched), fmt.Sprintf("%.2f", float64(r.Fetched)/float64(max(o.useful, 1))), mib(r.Wasted)}
			for k := range fetches {
				row = append(row, secs(o.res[k].Stall)+" / "+secs(o.res[k].StallMax))
			}
			rows = append(rows, row)
		}
	}
	tabulate(w, []string{"workflow", "policy", "reads to engine (n)", "hit %", "late %", "miss %", "fetched MiB", "amplification", "wasted MiB", "stall s @1 ms", "@5 ms", "@20 ms"}, rows)

	fmt.Fprintf(w, "### Sensitivity (LRU 4 GiB + trace prefetch, 5 ms fetch)\n\n")
	rows = nil
	for _, wf := range wfs {
		for _, v := range []struct {
			pc   int64
			sh   uint
			conc int
		}{{pagecache, 18, conc}, {pagecache, 20, conc}, {pagecache, 22, conc}, {0, 20, conc}, {4 << 30, 20, conc}, {pagecache, 20, 16}, {pagecache, 20, 256}} {
			o := evaluate(d, wf, v.pc, v.sh, v.conc, "LRU 4 GiB + trace prefetch")["LRU 4 GiB + trace prefetch"]
			r := o.res[1]
			rows = append(rows, []string{wf, fmt.Sprintf("%d KiB", 1<<v.sh>>10), fmt.Sprintf("%d MiB", v.pc>>20), fmt.Sprint(v.conc), fmt.Sprint(r.Reqs),
				pct(r.Hit, r.Lookups), pct(r.Late, r.Lookups), pct(r.Miss, r.Lookups), mib(r.Fetched), fmt.Sprintf("%.2f", float64(r.Fetched)/float64(max(o.useful, 1))), secs(r.Stall)})
		}
	}
	tabulate(w, []string{"workflow", "extent", "page cache", "prefetches in flight", "reads to engine (n)", "hit %", "late %", "miss %", "fetched MiB", "amplification", "stall s"}, rows)
}
