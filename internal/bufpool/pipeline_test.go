package bufpool_test

import (
	"bytes"
	"io"
	"math/rand/v2"
	"runtime"
	"sort"
	"syscall"
	"testing"
	"time"

	"github.com/luxfi/age"

	"github.com/hanzoai/vfs/internal/blake3"
	"github.com/hanzoai/vfs/internal/bufpool"
)

// The two pipelines below carry bytes the way the engine's write path does, once
// with the buffers it allocates today and once with the buffers the pool would
// hand out: file writes land in a dirty span; settling cuts the span into
// chunks, hashes each, seals it with a hybrid post-quantum recipient into a
// frame, appends the frame to a pack that is uploaded at 16 MiB, and keeps a
// copy of the chunk in a bounded cache.
//
// What is the same in both: the cut points, the hash, the cipher and the upload.
// What differs is only where the bytes live and how often they are copied. The
// alloc columns of the benchmark are the point; the time column shows what the
// copies and the collector cost.
const (
	writeSize = 128 << 10 // what the mount delivers per write
	maxChunk  = 4 << 20
)

// shape is the sizes the pipelines run with. The engine's are in production; the
// test uses smaller ones so that it runs in seconds under the race detector.
type shape struct {
	dirty int // dirty bytes before a settle
	pack  int // pack size at which it is uploaded
	cache int // bytes of chunk cache
}

var (
	production = shape{dirty: 64 << 20, pack: 16 << 20, cache: 32 << 20}
	small      = shape{dirty: 6 << 20, pack: 2 << 20, cache: 3 << 20}
)

type frameKey = string

type pipeline interface {
	// write adds p to the dirty span.
	write(p []byte)
	// settle chunks, seals and packs the dirty span, uploading full packs.
	settle()
	// flush uploads the pack in progress.
	flush()
}

// upload receives packs; the buffer is valid only during the call.
type upload func(pack []byte)

// cuts returns chunk lengths that sum to n: pseudo-random, between 64 KiB and
// 4 MiB, averaging about 580 KiB as the engine's chunker does.
func cuts(r *rand.Rand, n int) []int {
	var out []int
	for n > 0 {
		c := 64<<10 + int(r.ExpFloat64()*(520<<10))
		c = min(c, maxChunk, n)
		out = append(out, c)
		n -= c
	}
	return out
}

func header(id string) []byte { return append([]byte{byte(len(id))}, id...) }

const keyID = "0123456789abcdef"

// baseline allocates as the engine does.
type baseline struct {
	shape shape
	rcpt  age.Recipient
	up    upload
	span  []byte
	pack  []byte
	cache [][]byte
	held  int
	r     *rand.Rand
	ids   [][32]byte
	sizes []int // length of each frame, as the engine's chunk records keep it
}

func (b *baseline) write(p []byte) { b.span = append(b.span, p...) }

func (b *baseline) settle() {
	window := make([]byte, min(len(b.span), maxChunk))
	pos := 0
	for _, n := range cuts(b.r, len(b.span)) {
		w := window[:n]
		copy(w, b.span[pos:pos+n]) // Volume.read into the recut window
		pos += n
		b.ids = append(b.ids, blake3.Sum(w))

		var buf bytes.Buffer // ageCipher.Seal
		enc, err := age.Encrypt(&buf, b.rcpt)
		if err != nil {
			panic(err)
		}
		enc.Write(w)
		enc.Close()
		frame := make([]byte, 0, len(keyID)+1+buf.Len()) // seal: the frame
		frame = append(frame, header(keyID)...)
		frame = append(frame, buf.Bytes()...)

		b.sizes = append(b.sizes, len(frame))
		b.pack = append(b.pack, frame...) // pack.add
		if len(b.pack) >= b.shape.pack {
			b.flush()
		}
		b.cache = append(b.cache, append([]byte(nil), w...)) // chunks.add
		b.held += n
		for b.held > b.shape.cache {
			b.held -= len(b.cache[0])
			b.cache = b.cache[1:]
		}
	}
	b.span = nil
}

func (b *baseline) flush() {
	if len(b.pack) > 0 {
		b.up(b.pack)
		b.pack = nil
	}
}

// pooled keeps every large buffer in a Pool.
type pooled struct {
	shape  shape
	rcpt   age.Recipient
	up     upload
	plain  *bufpool.Pool // plaintext: cleared on return
	sealed *bufpool.Pool // ciphertext
	span   *bufpool.Buffer
	pack   *bufpool.Buffer
	cache  [][]byte
	held   int
	r      *rand.Rand
	ids    [][32]byte
	sizes  []int
}

func newPooled(s shape, rcpt age.Recipient, up upload, r *rand.Rand) *pooled {
	budget := bufpool.NewBudget(384<<20, time.Minute)
	p := &pooled{shape: s, rcpt: rcpt, up: up, r: r,
		plain:  budget.Pool(true),
		sealed: budget.Pool(false),
	}
	p.span = p.plain.Buffer(s.dirty + writeSize)
	p.pack = p.sealed.Buffer(s.pack + maxChunk + 64<<10)
	return p
}

func (p *pooled) write(b []byte) { p.span.Write(b) }

func (p *pooled) settle() {
	span := p.span.Bytes()
	pos := 0
	for _, n := range cuts(p.r, len(span)) {
		w := span[pos : pos+n] // the chunk is a view of the span
		pos += n
		p.ids = append(p.ids, blake3.Sum(w))

		start := p.pack.Len()
		p.pack.Write(header(keyID)) // the cipher writes the frame in place
		enc, err := age.Encrypt(p.pack, p.rcpt)
		if err != nil {
			panic(err)
		}
		enc.Write(w)
		enc.Close()
		p.sizes = append(p.sizes, p.pack.Len()-start)
		if p.pack.Len() >= p.shape.pack {
			p.flush()
		}
		c := p.plain.Get(n) // the cache owns this copy until it evicts it
		copy(c, w)
		p.cache = append(p.cache, c)
		p.held += n
		for p.held > p.shape.cache {
			p.held -= len(p.cache[0])
			p.plain.Put(p.cache[0])
			p.cache = p.cache[1:]
		}
	}
	p.span.Reset()
}

func (p *pooled) flush() {
	if p.pack.Len() > 0 {
		p.up(p.pack.Bytes())
		p.pack.Reset()
	}
}

// run writes total bytes through a pipeline in mount-sized writes, settling at
// the dirty limit, and flushes.
func run(p pipeline, s shape, data []byte, total int) {
	dirty := 0
	for off := 0; off < total; off += writeSize {
		p.write(data[off : off+writeSize])
		dirty += writeSize
		if dirty >= s.dirty {
			p.settle()
			dirty = 0
		}
	}
	if dirty > 0 {
		p.settle()
	}
	p.flush()
}

func input(total int) []byte {
	data := make([]byte, total)
	r := rand.New(rand.NewPCG(1, 2))
	for i := 0; i+8 <= total; i += 8 {
		x := r.Uint64()
		for j := range 8 {
			data[i+j] = byte(x >> (8 * j))
		}
	}
	return data
}

func recipient(tb testing.TB) (age.Recipient, age.Identity) {
	k, err := age.GenerateHybridIdentity()
	if err != nil {
		tb.Fatal(err)
	}
	return k.Recipient(), k
}

// TestPipelinesAgree runs both pipelines over the same bytes and requires the
// same chunks: equal ids in equal order, and every frame of both packs opening
// to the plaintext whose hash its id is.
func TestPipelinesAgree(t *testing.T) {
	const total = 16 << 20
	data := input(total)
	rcpt, id := recipient(t)

	var packsA, packsB [][]byte
	a := &baseline{shape: small, rcpt: rcpt, r: rand.New(rand.NewPCG(9, 9)), up: func(p []byte) { packsA = append(packsA, bytes.Clone(p)) }}
	b := newPooled(small, rcpt, func(p []byte) { packsB = append(packsB, bytes.Clone(p)) }, rand.New(rand.NewPCG(9, 9)))
	run(a, small, data, total)
	run(b, small, data, total)

	if len(a.ids) == 0 || len(a.ids) != len(b.ids) {
		t.Fatalf("%d chunks against %d", len(a.ids), len(b.ids))
	}
	for i := range a.ids {
		if a.ids[i] != b.ids[i] {
			t.Fatalf("chunk %d: id %x against %x", i, a.ids[i], b.ids[i])
		}
	}
	for name, c := range map[string]struct {
		packs [][]byte
		sizes []int
	}{"baseline": {packsA, a.sizes}, "pooled": {packsB, b.sizes}} {
		var got [][32]byte
		sizes := c.sizes
		for _, pack := range c.packs {
			for len(pack) > 0 {
				frame := pack[:sizes[0]]
				pack, sizes = pack[sizes[0]:], sizes[1:]
				hn := 1 + int(frame[0])
				if string(frame[1:hn]) != keyID {
					t.Fatalf("%s: bad frame header", name)
				}
				dec, err := age.Decrypt(bytes.NewReader(frame[hn:]), id)
				if err != nil {
					t.Fatalf("%s: %v", name, err)
				}
				plain, err := io.ReadAll(dec)
				if err != nil {
					t.Fatalf("%s: %v", name, err)
				}
				got = append(got, blake3.Sum(plain))
			}
		}
		if len(got) != len(a.ids) {
			t.Fatalf("%s: %d frames, want %d", name, len(got), len(a.ids))
		}
		for i := range got {
			if got[i] != a.ids[i] {
				t.Fatalf("%s: frame %d opens to %x, want %x", name, i, got[i], a.ids[i])
			}
		}
	}
}

func BenchmarkPipeline(b *testing.B) {
	const total = 256 << 20
	data := input(total)
	rcpt, _ := recipient(b)
	sink := func([]byte) {}
	b.Run("baseline", func(b *testing.B) {
		b.SetBytes(total)
		b.ReportAllocs()
		for range b.N {
			run(&baseline{shape: production, rcpt: rcpt, up: sink, r: rand.New(rand.NewPCG(9, 9))}, production, data, total)
		}
	})
	b.Run("pooled", func(b *testing.B) {
		b.SetBytes(total)
		b.ReportAllocs()
		p := newPooled(production, rcpt, sink, rand.New(rand.NewPCG(9, 9)))
		for range b.N {
			run(p, production, data, total)
			p.ids = p.ids[:0]
		}
	})
}

func processCPU() time.Duration {
	var ru syscall.Rusage
	syscall.Getrusage(0, &ru)
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}

// BenchmarkPipelineInterleaved runs the two pipelines in turn, the order flipped
// every round, so that load on the machine falls on both, and reports the median
// wall time, process CPU time and allocation of each (b.N rounds). Use
// -benchtime 12x.
func BenchmarkPipelineInterleaved(b *testing.B) {
	const total = 256 << 20
	data := input(total)
	rcpt, _ := recipient(b)
	sink := func([]byte) {}
	pooled := newPooled(production, rcpt, sink, rand.New(rand.NewPCG(9, 9)))
	one := func(name string) (wall, cpu time.Duration, alloc uint64) {
		var m0, m1 runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&m0)
		c0, t0 := processCPU(), time.Now()
		if name == "baseline" {
			run(&baseline{shape: production, rcpt: rcpt, up: sink, r: rand.New(rand.NewPCG(9, 9))}, production, data, total)
		} else {
			run(pooled, production, data, total)
			pooled.ids, pooled.sizes = pooled.ids[:0], pooled.sizes[:0]
		}
		wall, cpu = time.Since(t0), processCPU()-c0
		runtime.ReadMemStats(&m1)
		return wall, cpu, m1.TotalAlloc - m0.TotalAlloc
	}
	one("pooled") // the pool warm, as in a long-lived Volume
	res := map[string][][3]float64{}
	b.ResetTimer()
	for r := range b.N {
		order := []string{"baseline", "pooled"}
		if r%2 == 1 {
			order = []string{"pooled", "baseline"}
		}
		for _, name := range order {
			w, c, a := one(name)
			res[name] = append(res[name], [3]float64{w.Seconds(), c.Seconds(), float64(a) / 1e6})
		}
	}
	b.StopTimer()
	med := func(v [][3]float64, k int) float64 {
		s := make([]float64, len(v))
		for i := range v {
			s[i] = v[i][k]
		}
		sort.Float64s(s)
		return s[len(s)/2]
	}
	for _, name := range []string{"baseline", "pooled"} {
		b.ReportMetric(med(res[name], 0), name+"-wall-s")
		b.ReportMetric(med(res[name], 1), name+"-cpu-s")
		b.ReportMetric(med(res[name], 2), name+"-alloc-MB")
	}
	b.ReportMetric(med(res["pooled"], 0)/med(res["baseline"], 0), "wall-ratio")
	b.ReportMetric(med(res["pooled"], 1)/med(res["baseline"], 1), "cpu-ratio")
}
