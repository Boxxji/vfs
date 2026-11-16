package blake3

import (
	"fmt"
	"math/rand"
	"runtime"
	"testing"

	zeebo "github.com/zeebo/blake3"
	luke "lukechampine.com/blake3"
)

// The benchmarks compare this package with the two implementations it can
// replace. Run them on the machine that matters:
//
//	go test -run '^$' -bench . -count 10 ./internal/blake3
//
// The sizes are the ones the engine hashes: a small file, a typical chunk, a
// large chunk and a shard.
var sizes = []int{4 << 10, 64 << 10, 1 << 20, 16 << 20}

func sizeName(n int) string {
	if n >= 1<<20 {
		return fmt.Sprintf("%dMiB", n>>20)
	}
	return fmt.Sprintf("%dKiB", n>>10)
}

func benchBuf(n int) []byte {
	p := make([]byte, n)
	rand.New(rand.NewSource(1)).Read(p)
	return p
}

var sinkSum [Size]byte

func BenchmarkSum(b *testing.B) {
	impls := []struct {
		name string
		sum  func([]byte) [Size]byte
	}{
		{"this", Sum},
		{"zeebo", zeebo.Sum256},
		{"luke", luke.Sum256},
	}
	for _, n := range sizes {
		p := benchBuf(n)
		for _, im := range impls {
			b.Run(fmt.Sprintf("%s/%s", im.name, sizeName(n)), func(b *testing.B) {
				b.SetBytes(int64(n))
				for range b.N {
					sinkSum = im.sum(p)
				}
			})
		}
	}
}

// BenchmarkGeneric is the portable kernel alone, the floor on any architecture.
func BenchmarkGeneric(b *testing.B) {
	native := kernel
	kernel = portable
	defer func() { kernel = native }()
	for _, n := range sizes[:3] {
		p := benchBuf(n)
		b.Run(sizeName(n), func(b *testing.B) {
			b.SetBytes(int64(n))
			for range b.N {
				sinkSum = Sum(p)
			}
		})
	}
}

// BenchmarkStream writes 32 KiB at a time, the shape of an io.Copy.
func BenchmarkStream(b *testing.B) {
	for _, n := range sizes[1:] {
		p := benchBuf(n)
		b.Run(fmt.Sprintf("this/%s", sizeName(n)), func(b *testing.B) {
			b.SetBytes(int64(n))
			for range b.N {
				h := New()
				for q := p; len(q) > 0; q = q[min(32<<10, len(q)):] {
					h.Write(q[:min(32<<10, len(q))])
				}
				h.Sum(sinkSum[:0])
			}
		})
		b.Run(fmt.Sprintf("zeebo/%s", sizeName(n)), func(b *testing.B) {
			b.SetBytes(int64(n))
			for range b.N {
				h := zeebo.New()
				for q := p; len(q) > 0; q = q[min(32<<10, len(q)):] {
					h.Write(q[:min(32<<10, len(q))])
				}
				h.Sum(sinkSum[:0])
			}
		})
	}
}

// BenchmarkParallel hashes one buffer on several workers.
func BenchmarkParallel(b *testing.B) {
	for _, n := range sizes[2:] {
		p := benchBuf(n)
		for _, w := range []int{1, 2, 4, 8, runtime.NumCPU()} {
			b.Run(fmt.Sprintf("%s/%dworkers", sizeName(n), w), func(b *testing.B) {
				b.SetBytes(int64(n))
				for range b.N {
					sinkSum = Parallel(p, w)
				}
			})
		}
	}
}

// BenchmarkConcurrent has every CPU hash its own buffer, the shape of a
// pipeline of independent chunks.
func BenchmarkConcurrent(b *testing.B) {
	for _, n := range sizes[:3] {
		b.Run(sizeName(n), func(b *testing.B) {
			b.SetBytes(int64(n))
			b.RunParallel(func(pb *testing.PB) {
				p := benchBuf(n)
				var d [Size]byte
				for pb.Next() {
					d = Sum(p)
				}
				sinkSum = d
			})
		})
	}
}

// BenchmarkBoundary is the hash the metadata tree evaluates for every entry it
// writes (SPEC 6.3): a pooled keyed Hasher reset, written a level byte and a short
// key, and summed to 32 bytes.
func BenchmarkBoundary(b *testing.B) {
	var key [32]byte
	rand.New(rand.NewSource(1)).Read(key[:])
	entry := benchBuf(40)
	b.Run("this", func(b *testing.B) {
		h := Keyed(&key)
		var sum [32]byte
		for range b.N {
			h.Reset()
			h.WriteByte(2)
			h.Write(entry)
			h.Sum(sum[:0])
		}
		sinkSum = sum
	})
	b.Run("zeebo", func(b *testing.B) {
		h, _ := zeebo.NewKeyed(key[:])
		var sum [32]byte
		for range b.N {
			h.Reset()
			h.Write([]byte{2})
			h.Write(entry)
			h.Sum(sum[:0])
		}
		sinkSum = sum
	})
}
