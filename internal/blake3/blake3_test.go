package blake3

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/bits"
	"math/rand"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	zeebo "github.com/zeebo/blake3"
)

// A path is one way to get the digests. Every test holds all of them to the same
// expectations: the exported functions (which on amd64 are the library the engine
// already links), the tree with the kernel of this architecture, and the tree with
// the portable kernel.
type path struct {
	name     string
	sum      func([]byte) [Size]byte
	hasher   func(key *[32]byte) *Hasher
	parallel func(p []byte, workers int) [Size]byte
}

func eachPath(t *testing.T, f func(t *testing.T, p path)) {
	t.Helper()
	hashers := func(key *[32]byte) *Hasher {
		h := &Hasher{h: open(key), keyed: key != nil}
		if key != nil {
			h.key = *key
		}
		return h
	}
	fans := func(p []byte, workers int) [Size]byte {
		n := (len(p) + grain - 1) / grain
		if workers < 2 || n < 2 {
			return canon(p)
		}
		return fan(p, workers, n)
	}
	api := path{"api", Sum, func(key *[32]byte) *Hasher {
		if key == nil {
			return New()
		}
		return Keyed(key)
	}, Parallel}
	tr := path{"tree", canon, hashers, fans}
	t.Run(api.name, func(t *testing.T) { f(t, api) })
	t.Run(tr.name, func(t *testing.T) { f(t, tr) })
	native := kernel
	kernel = portable
	defer func() { kernel = native }()
	pt := path{"portable", canon, hashers, fans}
	t.Run(pt.name, func(t *testing.T) { f(t, pt) })
}

// pattern is the input of the official test vectors: 0, 1, ... 250, 0, 1, ...
func pattern(n int) []byte {
	p := make([]byte, n)
	for i := range p {
		p[i] = byte(i % 251)
	}
	return p
}

func random(r *rand.Rand, n int) []byte {
	p := make([]byte, n)
	r.Read(p)
	return p
}

type vectorFile struct {
	Key     string `json:"key"`
	Context string `json:"context_string"`
	Cases   []struct {
		Len    int    `json:"input_len"`
		Hash   string `json:"hash"`
		Keyed  string `json:"keyed_hash"`
		Derive string `json:"derive_key"`
	} `json:"cases"`
}

func loadVectors(t *testing.T) vectorFile {
	t.Helper()
	raw, err := os.ReadFile("testdata/test_vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	var v vectorFile
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatal(err)
	}
	return v
}

// TestVectors checks the official BLAKE3 test vectors: the digest and its
// extension to 131 bytes, plain, keyed and derived, through every entry point.
func TestVectors(t *testing.T) {
	v := loadVectors(t)
	var key [32]byte
	copy(key[:], v.Key)
	eachPath(t, func(t *testing.T, p path) {
		for _, c := range v.Cases {
			in := pattern(c.Len)
			want, _ := hex.DecodeString(c.Hash)
			wantKeyed, _ := hex.DecodeString(c.Keyed)
			wantDerived, _ := hex.DecodeString(c.Derive)

			if got := p.sum(in); !bytes.Equal(got[:], want[:Size]) {
				t.Errorf("sum(%d) = %x, want %x", c.Len, got, want[:Size])
			}
			for _, workers := range []int{1, 3} {
				if got := p.parallel(in, workers); !bytes.Equal(got[:], want[:Size]) {
					t.Errorf("parallel(%d, %d) = %x, want %x", c.Len, workers, got, want[:Size])
				}
			}

			h := p.hasher(nil)
			h.Write(in)
			if got := h.Sum(nil); !bytes.Equal(got, want[:Size]) {
				t.Errorf("Hasher(%d).Sum = %x, want %x", c.Len, got, want[:Size])
			}
			long := make([]byte, len(want))
			h.Expand(long)
			if !bytes.Equal(long, want) {
				t.Errorf("Hasher(%d).Expand = %x, want %x", c.Len, long, want)
			}

			k := p.hasher(&key)
			k.Write(in)
			klong := make([]byte, len(wantKeyed))
			k.Expand(klong)
			if !bytes.Equal(klong, wantKeyed) {
				t.Errorf("keyed Hasher(%d).Expand = %x, want %x", c.Len, klong, wantKeyed)
			}

			derived := make([]byte, len(wantDerived))
			DeriveKey(v.Context, in, derived)
			if !bytes.Equal(derived, wantDerived) {
				t.Errorf("DeriveKey(%d) = %x, want %x", c.Len, derived, wantDerived)
			}
		}
	})
}

// TestDeriveKey compares DeriveKey with the library the engine links, for contexts
// and materials of every awkward length and for every output length around a block.
func TestDeriveKey(t *testing.T) {
	r := rand.New(rand.NewSource(12))
	buf := random(r, 70000)
	eachPath(t, func(t *testing.T, p path) {
		for _, ctxLen := range []int{0, 1, 31, 32, 33, 63, 64, 65, 1023, 1024, 1025, 5000} {
			ctx := string(random(r, ctxLen))
			for _, n := range []int{0, 1, 63, 64, 65, 1023, 1024, 1025, 2048, 16384, 16385, 70000} {
				for _, outLen := range []int{0, 1, 31, 32, 33, 63, 64, 65, 128, 131, 1000} {
					got := make([]byte, outLen)
					DeriveKey(ctx, buf[:n], got)
					want := make([]byte, outLen)
					zeebo.DeriveKey(ctx, buf[:n], want)
					if !bytes.Equal(got, want) {
						t.Fatalf("DeriveKey(context %d bytes, material %d bytes, out %d): %x, want %x", ctxLen, n, outLen, got, want)
					}
				}
			}
		}
	})
}

// lengths are the input sizes where a tree hash goes wrong: around every block,
// chunk and kernel boundary, and around the power-of-two subtree sizes. Past a few
// dozen chunks the chunk counts are sampled: the powers of two and their
// neighbours, and every 31st.
func lengths(limit int) []int {
	seen := map[int]bool{}
	var out []int
	add := func(n int) {
		if n >= 0 && n <= limit && !seen[n] {
			seen[n] = true
			out = append(out, n)
		}
	}
	for n := 0; n <= 4*chunkSize+2*blockSize; n++ {
		add(n)
	}
	for chunks := 5; chunks*chunkSize <= limit+chunkSize; chunks++ {
		pow := bits.OnesCount(uint(chunks)) == 1 || bits.OnesCount(uint(chunks-1)) == 1 || bits.OnesCount(uint(chunks+1)) == 1
		if chunks > 70 && !pow && chunks%31 != 0 {
			continue
		}
		for _, d := range []int{-blockSize - 1, -blockSize, -1, 0, 1, blockSize - 1, blockSize, blockSize + 1} {
			add(chunks*chunkSize + d)
		}
	}
	for size := reach; size <= limit+2*blockSize; size *= 2 {
		for _, d := range []int{-129, -128, -65, -64, -63, -2, -1, 0, 1, 2, 63, 64, 65, 127, 128, 129} {
			add(size + d)
		}
	}
	return out
}

// TestAgainstReference compares Sum with an independent implementation for
// random contents at every boundary length.
func TestAgainstReference(t *testing.T) {
	limit := 1 << 18
	if testing.Short() {
		limit = 1 << 16
	}
	r := rand.New(rand.NewSource(1))
	buf := random(r, limit+1)
	eachPath(t, func(t *testing.T, p path) {
		for _, n := range lengths(limit) {
			want := zeebo.Sum256(buf[:n])
			if got := p.sum(buf[:n]); got != want {
				t.Fatalf("sum(%d) = %x, want %x", n, got, want)
			}
		}
	})
}

// TestLarge hashes a few sizes of several megabytes, where the tree is many kernel
// calls deep, on the paths that use the native kernel.
func TestLarge(t *testing.T) {
	if testing.Short() {
		t.Skip("large inputs")
	}
	r := rand.New(rand.NewSource(11))
	buf := random(r, 5<<20)
	eachPath(t, func(t *testing.T, p path) {
		if p.name == "portable" {
			return
		}
		for _, n := range []int{1<<20 + 1, 4<<20 + 12345, 5 << 20} {
			want := zeebo.Sum256(buf[:n])
			if got := p.sum(buf[:n]); got != want {
				t.Fatalf("sum(%d) = %x, want %x", n, got, want)
			}
			if got := p.parallel(buf[:n], 5); got != want {
				t.Fatalf("parallel(%d) = %x, want %x", n, got, want)
			}
		}
	})
}

// TestKernel compares the kernel of this architecture with the portable one on
// the node it returns, over every length it accepts, at chunk counters that
// carry into the high word, and for keyed hashing.
func TestKernel(t *testing.T) {
	r := rand.New(rand.NewSource(7))
	buf := random(r, reach)
	var key [8]uint32
	for i := range key {
		key[i] = r.Uint32()
	}
	for _, counter := range []uint64{0, 1, 5, 1<<32 - 3, 1<<32 - 1, 1 << 32, 1<<40 + 3, 1<<64 - 20} {
		for n := 0; n <= reach; n += 1 + r.Intn(300) {
			for _, k := range []*[8]uint32{&iv, &key} {
				flags := uint32(0)
				if k == &key {
					flags = flagKeyed
				}
				want := portable(buf[:n], counter, k, flags)
				if got := kernel(buf[:n], counter, k, flags); got != want {
					t.Fatalf("kernel(%d bytes, counter %#x, flags %d) = %+v, want %+v", n, counter, flags, got, want)
				}
			}
		}
	}
}

// TestRandomLengths covers lengths that are not near a boundary.
func TestRandomLengths(t *testing.T) {
	r := rand.New(rand.NewSource(2))
	buf := random(r, 1<<20)
	if testing.Short() {
		buf = buf[:1<<18]
	}
	eachPath(t, func(t *testing.T, p path) {
		for range 24 {
			n := r.Intn(len(buf) + 1)
			want := zeebo.Sum256(buf[:n])
			if got := p.sum(buf[:n]); got != want {
				t.Fatalf("sum(%d) = %x, want %x", n, got, want)
			}
		}
	})
}

// TestStreaming writes the input in pieces of every awkward size, one byte up
// to several chunks, and in random pieces, and expects the digest of the whole.
func TestStreaming(t *testing.T) {
	r := rand.New(rand.NewSource(3))
	buf := random(r, 70000)
	want := zeebo.Sum256(buf)
	eachPath(t, func(t *testing.T, p path) {
		for _, step := range []int{1, 2, 63, 64, 65, 1023, 1024, 1025, 2048, 4097, 16384, 16385, 33000} {
			h := p.hasher(nil)
			for q := buf; len(q) > 0; {
				n := min(step, len(q))
				if w, err := h.Write(q[:n]); w != n || err != nil {
					t.Fatalf("Write(%d bytes) = %d, %v", n, w, err)
				}
				q = q[n:]
			}
			if got := h.Sum(nil); !bytes.Equal(got, want[:]) {
				t.Errorf("pieces of %d: %x, want %x", step, got, want)
			}
		}
		h := p.hasher(nil)
		for _, c := range buf[:3000] {
			if err := h.WriteByte(c); err != nil {
				t.Fatal(err)
			}
		}
		h.Write(buf[3000:])
		if got := h.Sum(nil); !bytes.Equal(got, want[:]) {
			t.Errorf("WriteByte for the first 3000 bytes: %x, want %x", got, want)
		}
		for range 200 {
			h := p.hasher(nil)
			for q := buf; len(q) > 0; {
				n := min(r.Intn(40000)+r.Intn(2), len(q))
				h.Write(q[:n])
				q = q[n:]
			}
			if got := h.Sum(nil); !bytes.Equal(got, want[:]) {
				t.Fatalf("random pieces: %x, want %x", got, want)
			}
		}
	})
}

// TestSumKeepsState checks that Sum and Expand leave the hasher usable, and that
// Reset returns it to the state of a new one, key included.
func TestSumKeepsState(t *testing.T) {
	r := rand.New(rand.NewSource(4))
	buf := random(r, 5000)
	var key [32]byte
	copy(key[:], "an arbitrary thirty-two byte key")
	eachPath(t, func(t *testing.T, p path) {
		for _, k := range []*[32]byte{nil, &key} {
			ref := func(b []byte) []byte {
				if k == nil {
					d := zeebo.Sum256(b)
					return d[:]
				}
				h, _ := zeebo.NewKeyed(k[:])
				h.Write(b)
				return h.Sum(nil)
			}
			h := p.hasher(k)
			for i := 0; i < len(buf); i += 777 {
				end := min(i+777, len(buf))
				h.Write(buf[i:end])
				first := h.Sum(nil)
				h.Expand(make([]byte, 100))
				if !bytes.Equal(first, ref(buf[:end])) || !bytes.Equal(h.Sum(nil), first) {
					t.Fatalf("prefix %d: %x, want %x", end, first, ref(buf[:end]))
				}
			}
			h.Reset()
			if got := h.Sum(nil); !bytes.Equal(got, ref(nil)) {
				t.Fatalf("after Reset: %x, want %x", got, ref(nil))
			}
			h.Write(buf)
			if got := h.Sum(nil); !bytes.Equal(got, ref(buf)) {
				t.Fatalf("after Reset and a write: %x, want %x", got, ref(buf))
			}
		}
	})
}

// TestZeroValue checks that a Hasher that was never made by New is an unkeyed
// Hasher, not a keyed one with a zero key, and that Reset keeps it so.
func TestZeroValue(t *testing.T) {
	want := zeebo.Sum256([]byte("abc"))
	var h Hasher
	h.Write([]byte("abc"))
	if got := h.Sum(nil); !bytes.Equal(got, want[:]) {
		t.Fatalf("zero value: %x, want %x", got, want)
	}
	h.Reset()
	h.WriteString("abc")
	if got := h.Sum(nil); !bytes.Equal(got, want[:]) {
		t.Fatalf("zero value after Reset: %x, want %x", got, want)
	}
	var e Hasher
	if got := e.Sum(nil); !bytes.Equal(got, func() []byte { d := zeebo.Sum256(nil); return d[:] }()) {
		t.Fatalf("zero value, nothing written: %x", got)
	}
	var s Hasher
	s.Reset()
	s.Write([]byte("abc"))
	if got := s.Sum(nil); !bytes.Equal(got, want[:]) {
		t.Fatalf("Reset on a zero value: %x, want %x", got, want)
	}
}

// TestExpandLengths checks every output length around the end of one and two
// blocks of extended output, one byte included.
func TestExpandLengths(t *testing.T) {
	eachPath(t, func(t *testing.T, p path) {
		h := p.hasher(nil)
		h.Write([]byte("abc"))
		full := make([]byte, 200)
		h.Expand(full)
		ref := zeebo.New()
		ref.Write([]byte("abc"))
		want := make([]byte, 200)
		ref.Digest().Read(want)
		if !bytes.Equal(full, want) {
			t.Fatalf("Expand(200) = %x, want %x", full, want)
		}
		for n := 0; n <= 130; n++ {
			out := make([]byte, n+2)
			for i := range out {
				out[i] = 0xEE // sentinel
			}
			h.Expand(out[:n])
			if !bytes.Equal(out[:n], want[:n]) {
				t.Fatalf("Expand(%d) = %x, want %x", n, out[:n], want[:n])
			}
			if out[n] != 0xEE || out[n+1] != 0xEE {
				t.Fatalf("Expand(%d) wrote past the end", n)
			}
		}
	})
}

type gear [256]uint64

// gearEngine is newGear of the engine (chunk.go on main), verbatim except for the package name.
func gearEngine(seed [32]byte) *gear {
	h := zeebo.New()
	_, _ = h.WriteString("hanzoai/vfs chunker gear table")
	_, _ = h.Write(seed[:])
	var raw [256 * 8]byte
	_, _ = h.Digest().Read(raw[:])
	var g gear
	for i := range g {
		g[i] = binary.LittleEndian.Uint64(raw[i*8:])
	}
	return &g
}

// The docs/hashing.md recipe, through the package, for many seeds and every path:
// the table is part of the on-disk layout, so it must be the engine's today. The same
// derivation is held to the layout's text in spec_test.go.
func TestGearRecipe(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	eachPath(t, func(t *testing.T, p path) {
		for i := 0; i < 12; i++ {
			var seed [32]byte
			if i > 0 {
				r.Read(seed[:])
			} // seed 0 is all zero
			h := p.hasher(nil)
			h.WriteString("hanzoai/vfs chunker gear table")
			h.Write(seed[:])
			var raw [256 * 8]byte
			h.Expand(raw[:])
			var g gear
			for j := range g {
				g[j] = binary.LittleEndian.Uint64(raw[j*8:])
			}
			if g != *gearEngine(seed) {
				t.Fatalf("seed %d: not the engine's table", i)
			}
			// the trap the docs name: Expand over the seed instead of Write
			bad := p.hasher(nil)
			bad.WriteString("hanzoai/vfs chunker gear table")
			wrong := seed
			bad.Expand(wrong[:])
			if bytes.Equal(wrong[:], seed[:]) && i > 0 {
				t.Fatal("Expand did not overwrite")
			}
		}
	})
}

// The zero value and the literal digest of "abc".
func TestZeroAbc(t *testing.T) {
	const abc = "6437b3ac38465133ffb63b75273a8db548c558465d79db03fd359c6cd5bd9d85"
	var h Hasher
	h.Write([]byte("abc"))
	if got := h.Sum(nil); !bytes.Equal(got, mustHex(abc)) {
		t.Fatalf("zero value: %x", got)
	}
	h.Reset()
	h.WriteString("abc")
	if got := h.Sum(nil); !bytes.Equal(got, mustHex(abc)) {
		t.Fatalf("zero value after Reset: %x", got)
	}
	var k Hasher // zero value then Expand with nothing written
	out := make([]byte, 32)
	k.Expand(out)
	want := Sum(nil)
	if !bytes.Equal(out, want[:]) {
		t.Fatalf("zero value Expand: %x", out)
	}
	if s := Sum([]byte("abc")); !bytes.Equal(s[:], mustHex(abc)) {
		t.Fatalf("Sum: %x", s)
	}
	if s := Parallel([]byte("abc"), 8); !bytes.Equal(s[:], mustHex(abc)) {
		t.Fatalf("Parallel: %x", s)
	}
}

func mustHex(s string) []byte {
	b := make([]byte, len(s)/2)
	for i := range b {
		var v byte
		for _, c := range s[2*i : 2*i+2] {
			v <<= 4
			switch {
			case c >= 'a':
				v |= byte(c-'a') + 10
			default:
				v |= byte(c - '0')
			}
		}
		b[i] = v
	}
	return b
}

// TestConcurrentSums has several goroutines hash one read-only buffer at once,
// through every path; under the race detector this checks that the calls share
// nothing.
func TestConcurrentSums(t *testing.T) {
	r := rand.New(rand.NewSource(10))
	buf := random(r, 300<<10)
	want := zeebo.Sum256(buf)
	eachPath(t, func(t *testing.T, p path) {
		var wg sync.WaitGroup
		for range 6 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if got := p.sum(buf); got != want {
					t.Errorf("sum = %x, want %x", got, want)
				}
				if got := p.parallel(buf, 3); got != want {
					t.Errorf("parallel = %x, want %x", got, want)
				}
			}()
		}
		wg.Wait()
	})
}

// TestParallel compares Parallel with Sum around the job boundaries, across worker counts.
func TestParallel(t *testing.T) {
	r := rand.New(rand.NewSource(5))
	buf := random(r, 12*grain+5000)
	eachPath(t, func(t *testing.T, p path) {
		for _, n := range []int{grain - 1, grain, grain + 1, 2*grain - 1, 2 * grain, 2*grain + 1, 3 * grain, 5 * grain, 5*grain + 1, 8 * grain, 9*grain - 1, 9 * grain, 11*grain + 1023, 12 * grain, len(buf)} {
			want := zeebo.Sum256(buf[:n])
			for _, workers := range []int{0, 1, 2, 3, 8, 100} {
				if got := p.parallel(buf[:n], workers); got != want {
					t.Fatalf("parallel(%d, %d) = %x, want %x", n, workers, got, want)
				}
			}
		}
	})
}

// TestParallelBounded checks that no more than workers jobs are ever in the
// kernel at once, however many jobs there are. The kernel is wrapped to count the
// calls in flight; a deterministic upper bound that a worker too many can break.
func TestParallelBounded(t *testing.T) {
	r := rand.New(rand.NewSource(8))
	buf := random(r, 64*grain)
	want := zeebo.Sum256(buf)
	const workers = 3
	native := kernel
	var inflight, peak atomic.Int64
	kernel = func(in []byte, counter uint64, key *[8]uint32, flags uint32) node {
		n := inflight.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		runtime.Gosched()
		o := native(in, counter, key, flags)
		inflight.Add(-1)
		return o
	}
	defer func() { kernel = native }()
	for range 20 {
		if got := fan(buf, workers, 64); got != want {
			t.Fatalf("fan = %x, want %x", got, want)
		}
	}
	if got := peak.Load(); got < 1 || got > workers {
		t.Fatalf("%d jobs in the kernel at once, want 1 to %d", got, workers)
	}
	// A huge worker count is held to half the jobs: four jobs, two in the kernel.
	peak.Store(0)
	small := buf[:4*grain]
	for range 20 {
		if got := Parallel(small, 200000); got != zeebo.Sum256(small) {
			t.Fatal("Parallel with a huge worker count gave the wrong digest")
		}
	}
	if got := peak.Load(); got < 1 || got > 2 {
		t.Fatalf("%d jobs in the kernel at once for four jobs, want 1 or 2", got)
	}
}

// TestParallelStartsNoMoreThanJobs checks that a caller that passes a huge worker
// count for a buffer of eight jobs does not pay for that many goroutines: at most
// four workers, three of them started.
func TestParallelStartsNoMoreThanJobs(t *testing.T) {
	buf := make([]byte, 8*grain)
	var peak atomic.Int64
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
				if n := int64(runtime.NumGoroutine()); n > peak.Load() {
					peak.Store(n)
				}
				runtime.Gosched()
			}
		}
	}()
	base := runtime.NumGoroutine()
	if Parallel(buf, 200000) != Sum(buf) {
		t.Fatal("Parallel and Sum differ")
	}
	close(stop)
	<-done
	// Three helpers and the sampler, with room for goroutines of earlier tests
	// that are still on their way out.
	if got := int(peak.Load()) - base; got > 10 {
		t.Fatalf("%d goroutines started for 8 jobs", got)
	}
}

// FuzzHasher feeds arbitrary data in arbitrary pieces to every entry point.
func FuzzHasher(f *testing.F) {
	f.Add([]byte{}, uint16(1))
	f.Add(pattern(1025), uint16(64))
	f.Add(pattern(reach+1), uint16(1000))
	f.Add(pattern(3*reach+77), uint16(4097))
	f.Fuzz(func(t *testing.T, data []byte, step uint16) {
		want := zeebo.Sum256(data)
		var key [32]byte
		copy(key[:], "an arbitrary thirty-two byte key")
		wantKeyed, err := zeebo.NewKeyed(key[:])
		if err != nil {
			t.Fatal(err)
		}
		wantKeyed.Write(data)
		eachPath(t, func(t *testing.T, p path) {
			if got := p.sum(data); got != want {
				t.Fatalf("sum(%d) = %x, want %x", len(data), got, want)
			}
			h := p.hasher(nil)
			s := int(step) + 1
			for q := data; len(q) > 0; {
				n := min(s, len(q))
				h.Write(q[:n])
				q = q[n:]
			}
			if got := h.Sum(nil); !bytes.Equal(got, want[:]) {
				t.Fatalf("pieces of %d over %d bytes: %x, want %x", s, len(data), got, want)
			}
			k := p.hasher(&key)
			k.Write(data)
			if got := k.Sum(nil); !bytes.Equal(got, wantKeyed.Sum(nil)) {
				t.Fatalf("keyed over %d bytes: %x, want %x", len(data), got, wantKeyed.Sum(nil))
			}
		})
	})
}

func ExampleSum() {
	d := Sum([]byte("hello"))
	fmt.Printf("%x\n", d)
	// Output: ea8f163db38682925e4491c5e58d4bb3506ef8c14eb78a86e908c5624a67200f
}
