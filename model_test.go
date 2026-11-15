package vfs_test

import (
	"bytes"
	"fmt"
	"io"
	"math/rand/v2"
	"testing"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/internal/storetest"
)

// TestModelFuzz drives a Volume with random POSIX-shaped file operations and
// checks every byte against a plain []byte model, across flushes, reopen and
// takeover. Tiny Dirty/Overlay/Cache bounds force settle, spill and eviction in
// the middle of write streams.
var fuzzSeeds = 0

func TestModelFuzzMany(t *testing.T) {
	if testing.Short() {
		t.Skip("TestModelFuzz runs the same model; the fuzz step runs more seeds")
	}
	fuzzSeeds = 120
	defer func() { fuzzSeeds = 0 }()
	TestModelFuzz(t)
}

func TestModelFuzz(t *testing.T) {
	seeds := 24
	if fuzzSeeds > 0 {
		seeds = fuzzSeeds
	}
	if testing.Short() {
		seeds = 6
	}
	for seed := 1; seed <= seeds; seed++ {
		t.Run(fmt.Sprint("seed", seed), func(t *testing.T) { modelRun(t, uint64(seed)) })
	}
}

func modelRun(t *testing.T, seed uint64) {
	r := rand.New(rand.NewPCG(seed, 99))
	be := storetest.NewMem()
	c := vfsAge(t)
	if err := vfs.Create(ctx, cfg(be, c, 0)); err != nil {
		t.Fatal(err)
	}
	mk := func(round uint64) *vfs.Volume {
		cf := cfg(be, c, round)
		switch seed % 3 {
		case 0:
			cf.Dirty, cf.Overlay, cf.Cache = 1<<20, 40, 2<<20
		case 1:
			cf.Dirty, cf.Overlay, cf.Cache = 200<<10, 7, 1<<20
		}
		v, err := vfs.Attach(ctx, cf)
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	round := uint64(1)
	v := mk(round)
	type file struct {
		ino  vfs.InodeID
		data []byte
	}
	files := map[string]*file{}
	names := []string{}
	var pend []string // names created or changed since last flush

	rnd := func(n int) []byte {
		b := make([]byte, n)
		for i := range b {
			b[i] = byte(r.Uint32())
		}
		return b
	}
	pickSize := func() int {
		switch r.IntN(8) {
		case 0:
			return r.IntN(64)
		case 1, 2:
			return 16<<10 - 40 + r.IntN(80) // straddle the inline threshold
		case 3:
			return r.IntN(70 << 10)
		case 4:
			return 64<<10 - 3 + r.IntN(6)
		case 5:
			return r.IntN(1 << 20)
		case 6:
			return r.IntN(3 << 20)
		}
		return r.IntN(200 << 10)
	}
	check := func(what string, f *file, vol *vfs.Volume) {
		t.Helper()
		in, err := vol.Stat(ctx, f.ino)
		if err != nil {
			t.Fatalf("%s: stat: %v", what, err)
		}
		if in.Size != uint64(len(f.data)) {
			t.Fatalf("%s: size %d, model %d", what, in.Size, len(f.data))
		}
		fh, err := vol.Open(ctx, f.ino)
		if err != nil {
			t.Fatal(err)
		}
		defer fh.Close()
		buf := bytes.Repeat([]byte{0xA5}, len(f.data)+10)
		n, err := fh.ReadAt(ctx, buf, 0)
		if err != nil && err != io.EOF {
			t.Fatalf("%s: read: %v", what, err)
		}
		if n != len(f.data) {
			t.Fatalf("%s: read %d, model %d", what, n, len(f.data))
		}
		if !bytes.Equal(buf[:n], f.data) {
			i := 0
			for buf[i] == f.data[i] {
				i++
			}
			t.Fatalf("%s: content differs at %d of %d (got %x want %x)", what, i, len(f.data), buf[i], f.data[i])
		}
	}
	bound := func(what string, vol *vfs.Volume) {
		t.Helper()
		// The decoded-shard and chunk caches stay within their bound, plus the one
		// item a cache always keeps.
		if st := vol.Stats(); st.CacheBytes > st.CacheMax+(8<<20) {
			t.Fatalf("%s: caches hold %d bytes, bound %d", what, st.CacheBytes, st.CacheMax)
		}
	}
	checkAll := func(what string, vol *vfs.Volume) {
		t.Helper()
		bound(what, vol)
		for name, f := range files {
			check(what+" "+name, f, vol)
		}
		ents, err := vol.ReadDir(ctx, vfs.RootInode)
		if err != nil {
			t.Fatal(err)
		}
		if len(ents) != len(files) {
			var got []string
			for _, e := range ents {
				if _, ok := files[e.Name]; !ok {
					got = append(got, e.Name)
				}
			}
			t.Fatalf("%s: %d entries, model %d; extra %v", what, len(ents), len(files), got)
		}
	}
	var removed []string
	for step := 0; step < 160; step++ {
		for _, name := range removed {
			if _, err := v.Lookup(ctx, vfs.RootInode, name); err == nil {
				t.Fatalf("step %d: removed %s is back (live)", step, name)
			}
		}
		op := r.IntN(100)
		switch {
		case op < 12 || len(names) == 0: // create
			name := fmt.Sprintf("f%d", step)
			in, err := v.Create(ctx, vfs.RootInode, name, 0o644)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("step %d create %s ino %d", step, name, in.ID)
			files[name] = &file{ino: in.ID}
			names = append(names, name)
			pend = append(pend, name)
			sz := pickSize()
			d := rnd(sz)
			fh, _ := v.Open(ctx, in.ID)
			if _, err := fh.WriteAt(ctx, d, 0); err != nil && sz > 0 {
				t.Fatal(err)
			}
			fh.Close()
			files[name].data = d
		case op < 45: // write at random offset (maybe past EOF)
			name := names[r.IntN(len(names))]
			f := files[name]
			var off int
			switch r.IntN(4) {
			case 0:
				off = len(f.data) + r.IntN(300<<10) // sparse extension
			case 1:
				off = len(f.data) // append
			default:
				if len(f.data) > 0 {
					off = r.IntN(len(f.data))
				}
			}
			n := 1 + r.IntN(min(pickSize()+1, 1<<20))
			d := rnd(n)
			fh, _ := v.Open(ctx, f.ino)
			if _, err := fh.WriteAt(ctx, d, int64(off)); err != nil {
				t.Fatal(err)
			}
			fh.Close()
			if off+n > len(f.data) {
				f.data = append(f.data, make([]byte, off+n-len(f.data))...)
			}
			copy(f.data[off:], d)
			pend = append(pend, name)
		case op < 58: // truncate (shrink or extend)
			name := names[r.IntN(len(names))]
			f := files[name]
			var sz int
			if r.IntN(2) == 0 {
				sz = r.IntN(len(f.data) + 1)
			} else {
				sz = len(f.data) + r.IntN(400<<10)
			}
			if err := v.Truncate(ctx, f.ino, uint64(sz)); err != nil {
				t.Fatal(err)
			}
			if sz < len(f.data) {
				f.data = f.data[:sz]
			} else {
				f.data = append(f.data, make([]byte, sz-len(f.data))...)
			}
			pend = append(pend, name)
		case op < 63: // remove
			i := r.IntN(len(names))
			name := names[i]
			if err := v.Remove(ctx, vfs.RootInode, name); err != nil {
				t.Fatal(err)
			}
			t.Logf("step %d remove %s ino %d size %d", step, name, files[name].ino, len(files[name].data))
			delete(files, name)
			removed = append(removed, name)
			pend = append(pend, "rm "+name)
			names = append(names[:i], names[i+1:]...)
		case op < 80: // random read check on one file
			name := names[r.IntN(len(names))]
			check("live "+name, files[name], v)
		case op < 90: // flush and compare with a cold read-only view
			t.Logf("step %d flush", step)
			flush(t, v)
			checkAll(fmt.Sprintf("seed %d step %d cold", seed, step), reopen(t, be, c))
			pend = nil
		case op < 95: // takeover by a new round: the old Volume is dropped
			t.Logf("step %d flush+takeover", step)
			flush(t, v)
			round++
			v = mk(round)
			checkAll(fmt.Sprintf("seed %d step %d takeover", seed, step), v)
		default: // drop unflushed work by taking over WITHOUT a flush: model reverts
			// Only do it when nothing is pending, so the model stays right.
			if len(pend) == 0 {
				round++
				v = mk(round)
			}
		}
	}
	flush(t, v)
	if st := v.Stats(); st.Dirty != 0 || st.Pending != 0 {
		t.Fatalf("after Flush: Dirty=%d Pending=%d (accounting drift)", st.Dirty, st.Pending)
	}
	checkAll("final cold", reopen(t, be, c))
}
