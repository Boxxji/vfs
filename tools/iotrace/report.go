package main

import (
	"fmt"
	"io"
	"sort"
	"strings"
)

// Simulation settings, stated in the report.
const (
	pagecache = 1 << 30 // guest page cache in front of the engine
	shift     = 20      // 1 MiB extents
	conc      = 64      // prefetch fetches in flight
	rate      = 750e6   // bytes per second one client moves to or from the store
)

var fetches = [...]int64{1e6, 5e6, 20e6} // ns to fetch a cold extent

func pct(a, b int64) string {
	if b == 0 {
		return "-"
	}
	return fmt.Sprintf("%.1f", 100*float64(a)/float64(b))
}

func mib(b int64) string { return fmt.Sprintf("%.1f", float64(b)/(1<<20)) }

func secs(ns int64) string { return fmt.Sprintf("%.2f", float64(ns)/1e9) }

func tabulate(w io.Writer, head []string, rows [][]string) {
	fmt.Fprintf(w, "| %s |\n|%s\n", strings.Join(head, " | "), strings.Repeat("---|", len(head)))
	for _, r := range rows {
		fmt.Fprintf(w, "| %s |\n", strings.Join(r, " | "))
	}
	fmt.Fprintln(w)
}

// projects groups the runs of a workflow by project, each in run order.
func projects(runs []*Run) (labels []string, by map[string][]*Run) {
	by = map[string][]*Run{}
	for _, r := range runs {
		if _, ok := by[r.M.Project]; !ok {
			labels = append(labels, r.M.Project)
		}
		by[r.M.Project] = append(by[r.M.Project], r)
	}
	sort.Strings(labels)
	for _, l := range labels {
		sort.Slice(by[l], func(i, j int) bool { return by[l][i].M.Run < by[l][j].M.Run })
	}
	return labels, by
}

func report(w io.Writer, dir string) error {
	d, err := load(dir)
	if err != nil {
		return err
	}
	var wfs []string
	for k := range d.Runs {
		wfs = append(wfs, k)
	}
	sort.Strings(wfs)

	fmt.Fprintf(w, "### Dataset\n\n")
	var rows [][]string
	for _, wf := range wfs {
		labels, by := projects(d.Runs[wf])
		for _, l := range labels {
			for _, r := range by[l] {
				rows = append(rows, []string{wf, r.M.Run, fmt.Sprint(r.M.Events), fmt.Sprint(r.Files),
					mib(r.coverage(true).align(12).size()), secs(r.M.Wall), secs(r.M.Plain), ratio(r.M.Wall, r.M.Plain), r.M.Load[0]})
			}
		}
	}
	tabulate(w, []string{"workflow", "run", "events", "files read", "MiB read (pages, with maps)", "traced s", "untraced cold s", "tracing slowdown", "load before"}, rows)

	fmt.Fprintf(w, "### Predictability: what run 2 reads that run 1 of the same workflow read\n\n")
	rows = nil
	var orders [][]string
	for _, wf := range wfs {
		labels, by := projects(d.Runs[wf])
		for _, l := range labels {
			if len(by[l]) < 2 {
				continue
			}
			for _, maps := range []bool{false, true} {
				p := predict(by[l][0], by[l][1], maps)
				view := "reads"
				if maps {
					view = "reads+maps"
				}
				rows = append(rows, []string{wf, l, view, fmt.Sprint(p.FilesB), pct(int64(p.FilesShared), int64(p.FilesB)),
					fmt.Sprint(p.PagesB), pct(p.PagesShared, p.PagesB), fmt.Sprint(p.Ext1B), pct(p.Ext1Shared, p.Ext1B),
					fmt.Sprint(p.Ext4B), pct(p.Ext4Shared, p.Ext4B), pct(p.PagesUsed, p.PagesA),
					fmt.Sprint(p.TouchedB), pct(int64(p.TouchedShared), int64(p.TouchedB))})
				if maps {
					row := []string{wf, l}
					for _, o := range p.Order {
						row = append(row, fmt.Sprintf("%.0f / %.0f", 100*o.Reach, 100*o.Same))
					}
					row = append(row, fmt.Sprintf("%.1f", 100*p.Next), fmt.Sprint(p.NextN))
					orders = append(orders, row)
				}
			}
		}
	}
	tabulate(w, []string{"workflow", "project", "view", "files (n)", "files %", "pages (n)", "pages %", "1 MiB extents (n)", "1 MiB %", "4 MiB extents (n)", "4 MiB %", "run 1 pages used by run 2 %", "paths touched (n)", "paths %"}, rows)

	unseenTable(w, d, wfs)

	fmt.Fprint(w, "### Order: first K distinct 1 MiB extents of run 1 against run 2 (reach % / same-prefix %)\n\n")
	tabulate(w, []string{"workflow", "project", "K=10", "K=100", "K=1000", "K=10000", "run 2 transitions also in run 1 %", "transitions (n)"}, orders)

	sims := simulations(d, wfs)
	summaryTable(w, d, wfs, sims)
	reuseTables(w, d, wfs)
	readTable(w, d, wfs)
	simTables(w, d, wfs, sims)
	setTable(w, d, wfs)
	waitTable(w, d, wfs)
	return nil
}

func ratio(a, b int64) string {
	if b == 0 {
		return "-"
	}
	return fmt.Sprintf("%.2fx", float64(a)/float64(b))
}

// firstRuns returns run 1 of each project of a workflow.
func firstRuns(d *Dataset, wf string) []*Run {
	labels, by := projects(d.Runs[wf])
	var out []*Run
	for _, l := range labels {
		out = append(out, by[l][0])
	}
	return out
}

func reuseTables(w io.Writer, d *Dataset, wfs []string) {
	fmt.Fprintf(w, "### Reuse across projects: bytes (4 KiB pages) run 1 of one project reads that run 1 of the other also reads\n\n")
	var rows [][]string
	warm := cover{}
	var warmBy = map[string]cover{}
	for _, wf := range wfs {
		rs := firstRuns(d, wf)
		if len(rs) < 2 {
			continue
		}
		a, b := rs[0], rs[1]
		for _, dir := range [][2]*Run{{a, b}, {b, a}} {
			u := reuse(dir[0], dir[1], true)
			var classes []string
			for c := range u.Class {
				classes = append(classes, c)
			}
			sort.Strings(classes)
			for _, c := range classes {
				v := u.Class[c]
				rows = append(rows, []string{wf, dir[1].M.Project + " from " + dir[0].M.Project, c, mib(v[0]), mib(v[1]), pct(v[1], v[0]), pct(v[1], u.Total[0])})
			}
			rows = append(rows, []string{wf, dir[1].M.Project + " from " + dir[0].M.Project, "**all**", mib(u.Total[0]), mib(u.Total[1]), pct(u.Total[1], u.Total[0]), "100.0"})
			if dir[1] == b {
				for f, s := range u.Shared {
					if c := b.Class[f]; c == "toolchain" || c == "system" || c == "cache" {
						for _, x := range s {
							warm.add(f, x.lo, x.hi)
						}
						if warmBy[wf] == nil {
							warmBy[wf] = cover{}
						}
						for _, x := range s {
							warmBy[wf].add(f, x.lo, x.hi)
						}
					}
				}
			}
		}
	}
	tabulate(w, []string{"workflow", "project", "class", "MiB read", "MiB shared", "% of class shared", "% of all bytes"}, rows)
	fmt.Fprintf(w, "Candidate base-image warm set (toolchain, system and cache files read by both projects, summed over workflows, deduplicated): %s MiB.\n\n", mib(warm.size()))
	rows = nil
	for _, wf := range wfs {
		if c, ok := warmBy[wf]; ok {
			rows = append(rows, []string{wf, mib(c.size()), fmt.Sprint(len(c))})
		}
	}
	tabulate(w, []string{"workflow", "warm set MiB", "files"}, rows)
}

func readTable(w io.Writer, d *Dataset, wfs []string) {
	fmt.Fprintf(w, "### Read calls: size and sequentiality (all runs of the workflow)\n\n")
	var rows [][]string
	for _, wf := range wfs {
		var n, b [len(buckets)]int64
		var seq, placed, seqb, reads, sum int64
		var chains []int64
		for _, r := range d.Runs[wf] {
			for i := range n {
				n[i] += r.ReadN[i]
				b[i] += r.ReadB[i]
			}
			seq += r.Seq
			placed += r.Placed
			seqb += r.SeqBytes
			reads += r.Reads
			sum += r.ReadSum
			chains = append(chains, r.Chains...)
		}
		row := []string{wf, fmt.Sprint(reads), fmt.Sprintf("%.0f", float64(sum)/float64(max(reads, 1)))}
		for i := range n {
			row = append(row, pct(n[i], reads)+" / "+pct(b[i], sum))
		}
		row = append(row, pct(seq, placed), pct(seqb, sum),
			fmt.Sprintf("%d / %d / %d", percentile(chains, .5)>>10, percentile(chains, .9)>>10, percentile(chains, .99)>>10), fmt.Sprint(len(chains)))
		rows = append(rows, row)
	}
	tabulate(w, []string{"workflow", "reads (n)", "mean B", "<=4K calls / bytes %", "<=16K", "<=64K", "<=128K", "<=1M", ">1M", "sequential calls %", "sequential bytes %", "sequential run KiB p50 / p90 / p99", "runs (n)"}, rows)
}

func setTable(w io.Writer, d *Dataset, wfs []string) {
	fmt.Fprintf(w, "### Working set: what a run needs from the disk it starts with (run 2 of each project)\n\n")
	var rows [][]string
	var tot int64
	for _, wf := range wfs {
		labels, by := projects(d.Runs[wf])
		for _, l := range labels {
			r := by[l][len(by[l])-1]
			s := workingset(r)
			tot += s.Mapped
			rows = append(rows, []string{wf, l, fmt.Sprint(s.Files), mib(s.Read), mib(s.Mapped), mib(s.Ext1), mib(s.Written), mib(s.Local)})
		}
	}
	tabulate(w, []string{"workflow", "project", "files (n)", "MiB read calls", "MiB with maps and execs", "MiB in 1 MiB extents", "MiB written", "MiB read back from own writes"}, rows)
}

func waitTable(w io.Writer, d *Dataset, wfs []string) {
	fmt.Fprintf(w, "### Time waiting for file reads on local NVMe\n\n")
	fmt.Fprintf(w, "Cold and warm are untraced runs: cold after the page cache of every file the workflow reads was dropped, warm straight after. Their difference is the wall time the disk costs. The strace columns sum the time inside read calls with the %d ns strace adds to an empty call removed; strace serialises the stops of all threads, so they overstate the wait of multi-threaded workflows and are shown only as the per-thread view.\n\n", d.Cal.Call)
	var rows [][]string
	for _, wf := range wfs {
		labels, by := projects(d.Runs[wf])
		for _, l := range labels {
			r := by[l][len(by[l])-1]
			rows = append(rows, []string{wf, l, fmt.Sprint(r.Calls), secs(r.M.Wall), spread(r.M.Colds, r.M.Plain), spread(r.M.Warms, r.M.Warm),
				secs(r.M.Plain - r.M.Warm), pct(r.M.Plain-r.M.Warm, r.M.Plain), secs(r.ReadWait), secs(r.ReadUnion), secs(r.MetaWait)})
		}
	}
	tabulate(w, []string{"workflow", "project", "read calls (n)", "traced s", "cold s median (min-max)", "warm s median (min-max)", "cold - warm s", "% of cold wall", "strace read wait s (thread-seconds)", "strace any thread reading s", "strace open/stat/list wait s"}, rows)
}

// summaryTable puts the headline of each workflow in one row.
func summaryTable(w io.Writer, d *Dataset, wfs []string, sims map[string]map[string]*Outcome) {
	fmt.Fprintf(w, "### Summary (run 2 against run 1; 5 ms per cold 1 MiB extent; stall is the time reads waited, summed over threads / the busiest thread's own)\n\n")
	var rows [][]string
	for _, wf := range wfs {
		labels, by := projects(d.Runs[wf])
		var set, setr, pages, shared, plain, warm, n int64
		for _, l := range labels {
			if len(by[l]) < 2 {
				continue
			}
			p := predict(by[l][0], by[l][1], true)
			pages += p.PagesB
			shared += p.PagesShared
			n++
			ws := workingset(by[l][1])
			set += ws.Mapped
			setr += ws.Read
			plain += by[l][1].M.Plain
			warm += by[l][1].M.Warm
		}
		none, lru, trace, node := sims[wf]["no cache"], sims[wf]["LRU 4 GiB"], sims[wf]["LRU 4 GiB + trace prefetch"], sims[wf]["same node, LRU 512 MiB"]
		if none == nil || none.res[1].Lookups == 0 {
			continue
		}
		st := func(o *Outcome) string { return secs(o.res[1].Stall) + " / " + secs(o.res[1].StallMax) }
		rows = append(rows, []string{wf, mib(setr/max(n, 1)) + " / " + mib(set/max(n, 1)), pct(shared, pages), pct(lru.res[1].Hit, lru.res[1].Lookups),
			pct(trace.res[1].Hit, trace.res[1].Lookups), pct(node.res[1].Hit, node.res[1].Lookups),
			st(none), st(trace), pct(plain-warm, plain)})
	}
	tabulate(w, []string{"workflow", "working set MiB per project, reads / with maps", "run 2 pages already read by run 1 %", "cold node LRU hit %", "trace prefetch hit %", "same node 512 MiB hit %",
		"stall s, no cache", "stall s, trace prefetch", "disk share of cold wall %"}, rows)
}

// spread formats a median with the range of the timings behind it.
func spread(all []int64, med int64) string {
	if len(all) == 0 {
		return secs(med)
	}
	lo, hi := all[0], all[0]
	for _, v := range all {
		lo, hi = min(lo, v), max(hi, v)
	}
	return fmt.Sprintf("%s (%s-%s)", secs(med), secs(lo), secs(hi))
}

// unseenTable says where the pages run 2 read and run 1 did not are.
func unseenTable(w io.Writer, d *Dataset, wfs []string) {
	fmt.Fprintf(w, "### What run 2 read that run 1 did not, by class of path (4 KiB pages, with maps)\n\n")
	var rows [][]string
	for _, wf := range wfs {
		labels, by := projects(d.Runs[wf])
		for _, l := range labels {
			if len(by[l]) < 2 {
				continue
			}
			u := reuse(by[l][0], by[l][1], true)
			var classes []string
			for c := range u.Class {
				classes = append(classes, c)
			}
			sort.Strings(classes)
			for _, c := range classes {
				v := u.Class[c]
				if v[0] == v[1] {
					continue
				}
				rows = append(rows, []string{wf, l, c, mib(v[0]), mib(v[0] - v[1]), pct(v[0]-v[1], v[0])})
			}
		}
	}
	tabulate(w, []string{"workflow", "project", "class", "MiB run 2 read", "MiB run 1 did not", "%"}, rows)
}
