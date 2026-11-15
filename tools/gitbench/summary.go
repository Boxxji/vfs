package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"sort"
	"strconv"
)

// summarize reads the CSV that run, probe and net write and prints one row per
// (fs, repo, config, delay, op): how many repetitions, the median and the
// minimum wall time, and the median of each counted system call.
func summarize(in io.Reader, out io.Writer) error {
	r := csv.NewReader(in)
	r.FieldsPerRecord = -1
	rows, err := r.ReadAll()
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		return fmt.Errorf("empty input")
	}
	col := map[string]int{}
	for i, h := range rows[0] {
		col[h] = i
	}
	for _, h := range header {
		if _, ok := col[h]; !ok && h != "calls_us" {
			return fmt.Errorf("missing column %q", h)
		}
	}
	type key struct{ fs, repo, config, delay, calls, op string }
	type group struct {
		wall []float64
		cell map[string][]float64
	}
	counted := []string{"cpu_ms", "fsync", "fdatasync", "syncrange", "rename", "openat", "unlink", "stat", "getdents", "write", "syscalls", "git_kb", "tree_files", "req", "ttfb_ms"}
	groups := map[key]*group{}
	var keys []key
	for _, row := range rows[1:] {
		get := func(h string) string {
			if i, ok := col[h]; ok && i < len(row) {
				return row[i]
			}
			return ""
		}
		if get("exit") != "0" {
			continue
		}
		k := key{get("fs"), get("repo"), get("config"), get("delay_us"), get("calls_us"), get("op")}
		g := groups[k]
		if g == nil {
			g = &group{cell: map[string][]float64{}}
			groups[k] = g
			keys = append(keys, k)
		}
		if v, err := strconv.ParseFloat(get("wall_ms"), 64); err == nil {
			g.wall = append(g.wall, v)
		}
		for _, h := range counted {
			if v, err := strconv.ParseFloat(get(h), 64); err == nil && get(h) != "" {
				g.cell[h] = append(g.cell[h], v)
			}
		}
	}
	sort.SliceStable(keys, func(i, j int) bool {
		a, b := keys[i], keys[j]
		if a.fs != b.fs {
			return a.fs < b.fs
		}
		if a.repo != b.repo {
			return a.repo < b.repo
		}
		if a.config != b.config {
			return a.config < b.config
		}
		if a.delay != b.delay {
			return a.delay < b.delay
		}
		if a.calls != b.calls {
			return a.calls < b.calls
		}
		return rank(a.op) < rank(b.op)
	})
	w := csv.NewWriter(out)
	head := []string{"fs", "repo", "config", "delay_us", "calls_us", "op", "n", "wall_med_ms", "wall_min_ms"}
	head = append(head, counted...)
	if err := w.Write(head); err != nil {
		return err
	}
	for _, k := range keys {
		g := groups[k]
		if len(g.wall) == 0 {
			continue
		}
		sort.Float64s(g.wall)
		line := []string{k.fs, k.repo, k.config, k.delay, k.calls, k.op, itoa(len(g.wall)), num(median(g.wall)), num(g.wall[0])}
		for _, h := range counted {
			v := g.cell[h]
			if len(v) == 0 {
				line = append(line, "")
				continue
			}
			sort.Float64s(v)
			line = append(line, num(median(v)))
		}
		if err := w.Write(line); err != nil {
			return err
		}
	}
	w.Flush()
	return w.Error()
}

// rank orders operations as the sequence performs them.
func rank(op string) int {
	for i, o := range order {
		if o == op {
			return i
		}
	}
	return len(order)
}

// median of a sorted slice.
func median(v []float64) float64 {
	n := len(v)
	if n%2 == 1 {
		return v[n/2]
	}
	return (v[n/2-1] + v[n/2]) / 2
}

func num(v float64) string { return strconv.FormatFloat(v, 'f', 3, 64) }
