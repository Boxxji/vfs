package main

import (
	"reflect"
	"testing"
)

// reduced runs the reduction over events and returns the run and the
// ids of the paths.
func reduced(sizes map[string]int64, events ...Event) (*Run, map[string]int32) {
	d := &Dataset{ids: map[string]int32{}}
	r := &Run{Sizes: map[int32]int64{}, Class: map[int32]string{}}
	name := func(p string) int32 { return d.id(p) }
	for p, v := range sizes {
		r.Sizes[name(p)] = v
	}
	d.reduce(r, events, name)
	return r, d.ids
}

func read(path string, off, n int64) Event {
	return Event{Op: opRead, Path: path, Off: off, Len: n, Tid: 1}
}
func write(path string, off, n int64) Event {
	return Event{Op: opWrite, Path: path, Off: off, Len: n, Tid: 1}
}

func ranges(r *Run) []span {
	var out []span
	for _, a := range r.Base {
		out = append(out, span{a.Lo, a.Hi})
	}
	return out
}

func TestUntouchedFileIsBase(t *testing.T) {
	r, ids := reduced(nil, read("/f", 100, 50))
	if len(r.Base) != 1 || r.Base[0].File != ids["/f"] || r.Base[0].Lo != 100 || r.Base[0].Hi != 150 || r.Base[0].Kind != kindRead {
		t.Errorf("%+v", r.Base)
	}
}

func TestReadingOwnWritesIsLocal(t *testing.T) {
	r, _ := reduced(nil, write("/f", 0, 100), read("/f", 0, 100), read("/f", 50, 100))
	if got := ranges(r); !reflect.DeepEqual(got, []span{{100, 150}}) {
		t.Errorf("base %v: only the part the run did not write", got)
	}
	if r.Local != 100+50 {
		t.Errorf("local %d", r.Local)
	}
}

func TestTruncationMakesTheFileLocal(t *testing.T) {
	r, _ := reduced(nil, Event{Op: opTrunc, Path: "/f", Off: 0}, write("/f", 0, 10), read("/f", 0, 4096))
	if len(r.Base) != 0 {
		t.Errorf("a truncated file has no base data: %+v", r.Base)
	}
	r, _ = reduced(nil, Event{Op: opTrunc, Path: "/f", Off: 100}, read("/f", 0, 50))
	if len(r.Base) != 1 {
		t.Errorf("truncating to a length keeps the head: %+v", r.Base)
	}
}

func TestUnlinkedPathStartsOver(t *testing.T) {
	r, _ := reduced(nil, Event{Op: opUnlink, Path: "/f"}, write("/f", 0, 10), read("/f", 0, 4096))
	if len(r.Base) != 0 {
		t.Errorf("a recreated file has no base data: %+v", r.Base)
	}
}

func TestRenameKeepsIdentity(t *testing.T) {
	r, ids := reduced(nil,
		Event{Op: opRename, Path: "/old", Aux: "/new"},
		read("/new", 0, 10),
		read("/old", 0, 10))
	if len(r.Base) != 1 || r.Base[0].File != ids["/old"] {
		t.Errorf("a renamed file reads the data it had: %+v", r.Base)
	}
	if r.Local != 10 {
		t.Errorf("the old name is gone: local %d", r.Local)
	}
}

func TestLinkSharesData(t *testing.T) {
	r, ids := reduced(nil, Event{Op: opLink, Path: "/store/x", Aux: "/w/x"}, read("/w/x", 0, 10))
	if len(r.Base) != 1 || r.Base[0].File != ids["/store/x"] {
		t.Errorf("%+v", r.Base)
	}
}

func TestExecAndMapUseTheFileSize(t *testing.T) {
	r, _ := reduced(map[string]int64{"/bin/x": 1000, "/lib/y": 500},
		Event{Op: opExec, Path: "/bin/x", Off: 0, Len: -1},
		Event{Op: opMap, Path: "/lib/y", Off: 0, Len: 8192})
	if got := ranges(r); !reflect.DeepEqual(got, []span{{0, 1000}, {0, 500}}) {
		t.Errorf("%v", got)
	}
	if r.Base[0].Kind != kindMap || r.Base[1].Kind != kindMap {
		t.Errorf("kinds %+v", r.Base)
	}
}

func TestUnplacedReadsAreCountedNotGuessed(t *testing.T) {
	r, _ := reduced(nil, read("/f", -1, 100))
	if len(r.Base) != 0 || r.Unplaced != 100 {
		t.Errorf("%+v unplaced %d", r.Base, r.Unplaced)
	}
}

func TestSequentialityAndSizeBuckets(t *testing.T) {
	r, _ := reduced(nil, read("/a", 0, 100), read("/a", 100, 100), read("/a", 500, 100), read("/b", 0, 1<<20), read("/b", 1<<20, 2<<20))
	if r.Reads != 5 || r.Placed != 5 || r.Seq != 2 {
		t.Errorf("reads %d placed %d seq %d", r.Reads, r.Placed, r.Seq)
	}
	if r.SeqBytes != 100+2<<20 {
		t.Errorf("seq bytes %d", r.SeqBytes)
	}
	want := map[int64]bool{200: true, 100: true, 3 << 20: true}
	if len(r.Chains) != 3 {
		t.Fatalf("chains %v", r.Chains)
	}
	for _, c := range r.Chains {
		if !want[c] {
			t.Errorf("chains %v", r.Chains)
		}
	}
	if r.ReadN[0] != 3 || r.ReadN[4] != 1 || r.ReadN[5] != 1 {
		t.Errorf("buckets %v", r.ReadN)
	}
}

func TestUnionOfIntervals(t *testing.T) {
	iv := [][2]int64{{0, 10}, {5, 15}, {12, 20}, {30, 40}, {32, 35}}
	got := union(len(iv), func(i int) (int64, int64) { return iv[i][0], iv[i][1] })
	if got != 20+10 {
		t.Errorf("union %d", got)
	}
}

func TestClassify(t *testing.T) {
	roots := map[string]string{"workspace": "/w/a", "cache": "/c/a", "toolchain": "/usr/local/go"}
	cases := []struct{ path, class, key string }{
		{"/w/a/src/x.go", "workspace", "/w/a/src/x.go"},
		{"/c/a/ab/cd", "cache", "cache:/ab/cd"},
		{"/usr/local/go/bin/go", "toolchain", "toolchain:/bin/go"},
		{"/usr/lib/libc.so.6", "system", "/usr/lib/libc.so.6"},
		{"/home/x/y", "other", "/home/x/y"},
		{"/w/ab", "other", "/w/ab"}, // a sibling of a root is not under it
	}
	for _, c := range cases {
		if class, key := classify(c.path, roots); class != c.class || key != c.key {
			t.Errorf("%s: %s %s, want %s %s", c.path, class, key, c.class, c.key)
		}
	}
}

func runOf(accesses ...Access) *Run {
	r := &Run{Base: accesses, Sizes: map[int32]int64{}, Class: map[int32]string{}}
	for _, a := range accesses {
		r.Touched = append(r.Touched, a.File)
	}
	return r
}

func TestPredictability(t *testing.T) {
	a := runOf(Access{File: 1, Lo: 0, Hi: 8192}, Access{File: 2, Lo: 0, Hi: 100})
	b := runOf(Access{File: 1, Lo: 4096, Hi: 12288}, Access{File: 3, Lo: 0, Hi: 100})
	p := predict(a, b, true)
	if p.FilesB != 2 || p.FilesShared != 1 {
		t.Errorf("files %d/%d", p.FilesShared, p.FilesB)
	}
	if p.BytesB != 4096*2+100 || p.BytesShared != 4096 {
		t.Errorf("bytes %d/%d", p.BytesShared, p.BytesB)
	}
	if p.PagesB != 3 || p.PagesShared != 1 || p.PagesA != 3 {
		t.Errorf("pages %d/%d of %d", p.PagesShared, p.PagesB, p.PagesA)
	}
	if p.Ext1B != 2 || p.Ext1Shared != 1 {
		t.Errorf("extents %d/%d", p.Ext1Shared, p.Ext1B)
	}
	if p.TouchedB != 2 || p.TouchedShared != 1 {
		t.Errorf("touched %d/%d", p.TouchedShared, p.TouchedB)
	}
}

func TestFourMebibyteExtents(t *testing.T) {
	a := runOf(Access{File: 1, Lo: 0, Hi: 8192})
	b := runOf(Access{File: 1, Lo: 3 << 20, Hi: 3<<20 + 10})
	p := predict(a, b, true)
	if p.Ext1Shared != 0 || p.Ext4Shared != 1 || p.Ext4B != 1 {
		t.Errorf("1 MiB %d, 4 MiB %d of %d", p.Ext1Shared, p.Ext4Shared, p.Ext4B)
	}
}

func TestOrderMetrics(t *testing.T) {
	mk := func(files ...int32) *Run {
		var acc []Access
		for _, f := range files {
			acc = append(acc, Access{File: f, Lo: 0, Hi: 10})
		}
		return runOf(acc...)
	}
	a, b := mk(1, 2, 3, 4), mk(1, 2, 9, 4)
	p := predict(a, b, true)
	// B's transitions: 1>2 (in A), 2>9 (not), 9>4 (not)
	if p.NextN != 3 || p.Next < 0.333 || p.Next > 0.334 {
		t.Errorf("next %v of %d", p.Next, p.NextN)
	}
	o := p.Order[1] // K=100: A's four extents against B's four
	if o.Reach != 0.75 || o.Same != 0.75 {
		t.Errorf("order %+v", o)
	}
	small := p.Order[0] // K=10 behaves as K=4 here
	if small.N != 4 {
		t.Errorf("prefix size %d", small.N)
	}
}

func TestReuseByClass(t *testing.T) {
	a := runOf(Access{File: 1, Lo: 0, Hi: 8192}, Access{File: 2, Lo: 0, Hi: 4096})
	b := runOf(Access{File: 1, Lo: 4096, Hi: 12288}, Access{File: 3, Lo: 0, Hi: 4096})
	b.Class = map[int32]string{1: "toolchain", 3: "workspace"}
	u := reuse(a, b, true)
	if v := u.Class["toolchain"]; v != [2]int64{8192, 4096} {
		t.Errorf("toolchain %v", v)
	}
	if v := u.Class["workspace"]; v != [2]int64{4096, 0} {
		t.Errorf("workspace %v", v)
	}
	if u.Total != [2]int64{12288, 4096} {
		t.Errorf("total %v", u.Total)
	}
	if u.Shared.size() != 4096 {
		t.Errorf("shared %d", u.Shared.size())
	}
}

func TestExtentsAreClippedToTheFile(t *testing.T) {
	r := &Run{Sizes: map[int32]int64{1: 1<<20 + 10}}
	c := cover{1: {{0, 1<<20 + 5}}}
	got := extents(c, r, 20, func(int32) bool { return true })
	if len(got) != 2 || got[0].bytes != 1<<20 || got[1].bytes != 10 {
		t.Errorf("%+v", got)
	}
	if none := extents(c, r, 20, func(int32) bool { return false }); len(none) != 0 {
		t.Errorf("%+v", none)
	}
}

func TestPercentile(t *testing.T) {
	v := []int64{5, 1, 9, 3, 7}
	if percentile(v, 0.5) != 5 || percentile(v, 0) != 1 || percentile(v, 1) != 9 || percentile(nil, 0.5) != 0 {
		t.Error("percentile")
	}
}
