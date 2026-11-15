package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const page = 4096

// Kinds of base access: bytes returned by a read call, or a range that
// is mapped or executed and so may be touched at any time.
const (
	kindRead = iota
	kindMap
)

// Access is a read of data that existed before the run began: no write
// of the run covers it. Ranges are [Lo, Hi) in the file named by File.
type Access struct {
	T      int64
	Tid    int32
	File   int32
	Lo, Hi int64
	Kind   uint8
}

// readsize buckets, upper bounds in bytes.
var buckets = [...]int64{4 << 10, 16 << 10, 64 << 10, 128 << 10, 1 << 20, 1 << 62}

// Run is one recorded run reduced to what the analysis needs.
type Run struct {
	M        Manifest
	Base     []Access
	Touched  []int32         // every path touched, in first-touch order
	Sizes    map[int32]int64 // by file
	Class    map[int32]string
	ReadN    [len(buckets)]int64 // read calls by size bucket
	ReadB    [len(buckets)]int64 // bytes by size bucket
	Seq      int64               // read calls that continue the previous one on the file
	Placed   int64               // read calls with a known offset
	SeqBytes int64
	Chains   []int64 // bytes in each uninterrupted sequential run on one file
	Reads    int64
	ReadSum  int64 // bytes returned by read calls
	Local    int64 // bytes read back from data the run wrote
	WriteSum int64
	Unplaced int64 // bytes read through descriptors whose offset was never seen
	Files    int   // distinct paths with base reads
	// wait, in ns, with the cost strace adds to an empty call removed
	ReadWait, ReadRaw, MetaWait, ReadUnion int64
	Calls                                  int64 // read calls, for the wait mean
}

// Dataset is a directory of recorded runs.
type Dataset struct {
	Names []string // id -> canonical path
	Cal   Calibration
	Runs  map[string][]*Run // by workflow, ordered by run name

	ids map[string]int32
}

func (d *Dataset) id(p string) int32 {
	if v, ok := d.ids[p]; ok {
		return v
	}
	v := int32(len(d.Names))
	d.ids[p] = v
	d.Names = append(d.Names, p)
	return v
}

var systemRoots = [...]string{"/usr", "/lib", "/lib64", "/bin", "/sbin", "/etc", "/opt", "/var"}

// classify names the kind of tree a path lives in and gives it a key
// that is the same in every project: caches and toolchains are keyed by
// the part below their root, because each project gets its own copy of
// a package cache while the content is shared.
func classify(path string, roots map[string]string) (class, key string) {
	best, root := "", ""
	for c, r := range roots {
		if r != "" && (path == r || strings.HasPrefix(path, r+"/")) && len(r) > len(root) {
			best, root = c, r
		}
	}
	switch best {
	case "":
		for _, s := range systemRoots {
			if path == s || strings.HasPrefix(path, s+"/") {
				return "system", path
			}
		}
		return "other", path
	case "cache", "toolchain", "build":
		return best, best + ":" + strings.TrimPrefix(path, root)
	}
	return best, path
}

func load(dir string) (*Dataset, error) {
	d := &Dataset{Runs: map[string][]*Run{}, ids: map[string]int32{}}
	if b, err := os.ReadFile(filepath.Join(dir, "calibration.json")); err == nil {
		if err := json.Unmarshal(b, &d.Cal); err != nil {
			return nil, err
		}
	}
	manifests, err := filepath.Glob(filepath.Join(dir, "*", "*", "manifest.json"))
	if err != nil {
		return nil, err
	}
	sort.Strings(manifests)
	for _, mp := range manifests {
		r, err := d.loadRun(filepath.Dir(mp))
		if err != nil {
			return nil, fmt.Errorf("%s: %w", mp, err)
		}
		d.Runs[r.M.Workflow] = append(d.Runs[r.M.Workflow], r)
	}
	if len(d.Runs) == 0 {
		return nil, fmt.Errorf("no recorded runs under %s", dir)
	}
	return d, nil
}

func (d *Dataset) loadRun(dir string) (*Run, error) {
	r := &Run{Sizes: map[int32]int64{}, Class: map[int32]string{}}
	b, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(b, &r.M); err != nil {
		return nil, err
	}
	ef, err := os.Open(filepath.Join(dir, "events.tsv"))
	if err != nil {
		return nil, err
	}
	defer ef.Close()
	_, _, events, err := readEvents(bufio.NewReaderSize(ef, 1<<20))
	if err != nil {
		return nil, err
	}
	sf, err := os.Open(filepath.Join(dir, "sizes.tsv"))
	if err != nil {
		return nil, err
	}
	defer sf.Close()
	sizes, err := readSizes(bufio.NewReaderSize(sf, 1<<20))
	if err != nil {
		return nil, err
	}
	name := func(p string) int32 {
		class, key := classify(p, r.M.Roots)
		id := d.id(key)
		r.Class[id] = class
		return id
	}
	for p, v := range sizes {
		r.Sizes[name(p)] = v
	}
	d.reduce(r, events, name)
	return r, nil
}

const fresh = 1 << 30 // identities at or above this were made by the run

type state struct {
	fresh   bool // created or truncated by the run: nothing in it is base data
	written spans
}

// reduce walks the events of one run in order. It separates reads of
// data that existed before the run (base) from reads of data the run
// wrote itself, follows renames and links so a file keeps its identity,
// and gathers read sizes, sequentiality and wait times.
func (d *Dataset) reduce(r *Run, events []Event, name func(string) int32) {
	ident := map[int32]int32{}
	states := map[int32]*state{}
	next := int32(fresh)
	identity := func(f int32) int32 {
		if v, ok := ident[f]; ok {
			return v
		}
		return f
	}
	st := func(id int32) *state {
		s := states[id]
		if s == nil {
			s = &state{}
			states[id] = s
		}
		return s
	}
	newfresh := func() int32 {
		next++
		states[next] = &state{fresh: true}
		return next
	}
	seen := map[int32]bool{}
	touch := func(f int32) {
		if !seen[f] {
			seen[f] = true
			r.Touched = append(r.Touched, f)
		}
	}
	basefiles := map[int32]bool{}
	lastEnd := map[int32]int64{}
	chain := map[int32]int64{}
	type iv struct{ lo, hi int64 }
	var waits []iv
	call := d.Cal.Call

	for _, e := range events {
		f := name(e.Path)
		touch(f)
		switch e.Op {
		case opOpen, opMiss, opStat, opList:
			r.MetaWait += max(0, e.Dur-call)
		case opMkdir, opSymlink:
		case opWrite:
			r.WriteSum += e.Len
			if e.Off >= 0 {
				st(identity(f)).written.add(e.Off, e.Off+e.Len)
			}
		case opTrunc:
			if e.Off == 0 {
				st(identity(f)).fresh = true
			}
		case opUnlink:
			ident[f] = newfresh()
		case opRename:
			a := name(e.Aux)
			touch(a)
			ident[a] = identity(f)
			ident[f] = newfresh()
		case opLink:
			a := name(e.Aux)
			touch(a)
			ident[a] = identity(f)
		case opRead, opMap, opExec:
			if e.Op == opRead {
				r.read(e, f, lastEnd, chain)
				w := max(0, e.Dur-call)
				r.ReadWait += w
				r.ReadRaw += e.Dur
				r.Calls++
				waits = append(waits, iv{e.Time, e.Time + w})
			}
			id := identity(f)
			lo, hi := e.Off, e.Off+e.Len
			if e.Op == opExec {
				lo, hi = 0, max(r.Sizes[f], r.Sizes[id])
			}
			if e.Op == opMap {
				if size := r.Sizes[f]; size > 0 && hi > size {
					hi = size
				}
			}
			if lo < 0 {
				r.Unplaced += e.Len
				continue
			}
			if hi <= lo {
				continue
			}
			s := states[id]
			if id >= fresh || (s != nil && s.fresh) {
				r.Local += hi - lo
				continue
			}
			gaps := []span{{lo, hi}}
			if s != nil {
				gaps = s.written.gaps(lo, hi)
			}
			var base int64
			kind := uint8(kindRead)
			if e.Op != opRead {
				kind = kindMap
			}
			for _, g := range gaps {
				r.Base = append(r.Base, Access{T: e.Time, Tid: int32(e.Tid), File: id, Lo: g.lo, Hi: g.hi, Kind: kind})
				base += g.hi - g.lo
			}
			r.Local += hi - lo - base
			if base > 0 {
				basefiles[id] = true
			}
		}
	}
	for _, c := range chain {
		if c > 0 {
			r.Chains = append(r.Chains, c)
		}
	}
	r.Files = len(basefiles)
	r.ReadUnion = union(len(waits), func(i int) (int64, int64) { return waits[i].lo, waits[i].hi })
}

// read tallies one read call into the size and sequentiality counters.
func (r *Run) read(e Event, f int32, lastEnd, chain map[int32]int64) {
	r.Reads++
	r.ReadSum += e.Len
	b := 0
	for e.Len > buckets[b] {
		b++
	}
	r.ReadN[b]++
	r.ReadB[b] += e.Len
	if e.Off < 0 {
		return
	}
	r.Placed++
	if end, ok := lastEnd[f]; ok && e.Off == end {
		r.Seq++
		r.SeqBytes += e.Len
		chain[f] += e.Len
	} else {
		if chain[f] > 0 {
			r.Chains = append(r.Chains, chain[f])
		}
		chain[f] = e.Len
	}
	lastEnd[f] = e.Off + e.Len
}

// union is the total length covered by n intervals.
func union(n int, at func(int) (int64, int64)) int64 {
	type iv struct{ lo, hi int64 }
	v := make([]iv, n)
	for i := range v {
		v[i].lo, v[i].hi = at(i)
	}
	sort.Slice(v, func(i, j int) bool { return v[i].lo < v[j].lo })
	var total, end int64
	started := false
	for _, x := range v {
		switch {
		case !started || x.lo > end:
			total += x.hi - x.lo
			end, started = x.hi, true
		case x.hi > end:
			total += x.hi - end
			end = x.hi
		}
	}
	return total
}
