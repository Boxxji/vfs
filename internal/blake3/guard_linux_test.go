// No kernel, glue or streamer reads outside the slice it is given: each input sits against
// an inaccessible page, with capacity equal to length, so a read past the end faults and
// kills the test binary. A kernel that pads its lanes with a pointer past the last
// whole chunk, instead of repeating the last, is caught here and nowhere else.
package blake3

import (
	"math/rand"
	"os"
	"syscall"
	"testing"

	zeebo "github.com/zeebo/blake3"
)

// guardBuf is the data the guarded slices are cut from: random, fixed.
var guardBuf = func() []byte {
	b := make([]byte, 5<<20)
	rand.New(rand.NewSource(0x5eed)).Read(b)
	return b
}()

// fan0 is Parallel without the fallbacks, so that the tree is entered with the
// workers asked for.
func fan0(p []byte, workers int) [Size]byte {
	return fan(p, workers, (len(p)+grain-1)/grain)
}

// guarded returns n bytes of guardBuf placed so that the byte after the slice (and, with end=false, the byte before)
// is a PROT_NONE page; with cap == len (3-index) any read past len faults.
func guarded(tb testing.TB, n int, end bool) (data []byte, free func()) {
	pg := os.Getpagesize()
	body := (n + pg - 1) / pg * pg
	region, err := syscall.Mmap(-1, 0, body+2*pg, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_ANON|syscall.MAP_PRIVATE)
	if err != nil {
		tb.Fatal(err)
	}
	if err := syscall.Mprotect(region[:pg], syscall.PROT_NONE); err != nil {
		tb.Fatal(err)
	}
	if err := syscall.Mprotect(region[pg+body:], syscall.PROT_NONE); err != nil {
		tb.Fatal(err)
	}
	start := pg
	if end {
		start = pg + body - n
	}
	copy(region[start:start+n], guardBuf[:n])
	return region[start : start+n : start+n], func() { syscall.Munmap(region) }
}

// No kernel, glue or streamer reads outside the slice it is given (a fault on the guard page kills the test binary).
func TestGuardPages(t *testing.T) {
	max := 3*reach + 2000
	step := 1
	for n := 0; n <= max; n += step {
		if n > 1200 {
			step = 13
		}
		want := zeebo.Sum256(guardBuf[:n])
		for _, end := range []bool{false, true} {
			d, free := guarded(t, n, end)
			if got := Sum(d); got != want {
				t.Fatalf("Sum n=%d end=%v wrong", n, end)
			}
			if got := canon(d); got != want {
				t.Fatalf("canon n=%d end=%v wrong", n, end)
			}
			h := New()
			h.Write(d)
			if got := h.Sum(nil); string(got) != string(want[:]) {
				t.Fatalf("Hasher n=%d end=%v wrong", n, end)
			}
			s := open(nil)
			s.Write(d)
			if got := s.Sum(nil); string(got) != string(want[:]) {
				t.Fatalf("streamer n=%d end=%v wrong", n, end)
			}
			if n > grain && n%3 == 0 {
				if got := fan0(d, 4); got != want {
					t.Fatalf("fan n=%d wrong", n)
				}
			}
			free()
		}
	}
	for _, n := range []int{1 << 20, 1<<20 + 1, 4<<20 + 12345, 2*grain + 1, 3*grain - 1} {
		want := zeebo.Sum256(guardBuf[:n])
		for _, end := range []bool{false, true} {
			d, free := guarded(t, n, end)
			if got := Parallel(d, 8); got != want {
				t.Fatalf("Parallel n=%d wrong", n)
			}
			if got := fan0(d, 8); got != want {
				t.Fatalf("fan n=%d wrong", n)
			}
			free()
		}
	}
}
