package blake3

import (
	"math/rand"
	"testing"
	"unsafe"
)

// TestLane4Layout pins the offsets kernel_arm64.s reads and writes.
func TestLane4Layout(t *testing.T) {
	var l lanes
	for _, f := range []struct {
		name string
		got  uintptr
		want uintptr
	}{
		{"in", unsafe.Offsetof(l.in), 0},
		{"ctr", unsafe.Offsetof(l.ctr), 32},
		{"key", unsafe.Offsetof(l.key), 64},
		{"out", unsafe.Offsetof(l.out), 96},
		{"blocks", unsafe.Offsetof(l.blocks), 224},
		{"flags", unsafe.Offsetof(l.flags), 228},
		{"start", unsafe.Offsetof(l.start), 232},
		{"end", unsafe.Offsetof(l.end), 236},
		{"ahead", unsafe.Offsetof(l.ahead), 240},
		{"m", unsafe.Offsetof(l.m), 256},
		{"size", unsafe.Sizeof(l), 512},
	} {
		if f.got != f.want {
			t.Errorf("lanes.%s at %d, want %d", f.name, f.got, f.want)
		}
	}
}

// chain is hash4 for one lane, written with the portable compression.
func chain(in []byte, blocks int, key [8]uint32, counter uint64, flags, start, end uint32) [8]uint32 {
	cv := key
	for b := 0; b < blocks; b++ {
		f := flags
		if b == 0 {
			f |= start
		}
		if b == blocks-1 {
			f |= end
		}
		m := load(in[b*blockSize:])
		w := compress(&cv, &m, counter, blockSize, f)
		cv = [8]uint32(w[:8])
	}
	return cv
}

// TestHash4 checks the kernel lane by lane against the portable compression,
// with the counters, flags, keys and block counts the tree never produces:
// counters across the 32-bit boundary, one block and many, every flag set.
func TestHash4(t *testing.T) {
	r := rand.New(rand.NewSource(6))
	data := random(r, 4*40*blockSize)
	counters := []uint64{0, 1, 1<<32 - 3, 1<<32 - 1, 1 << 32, 1<<32 + 5, 1<<63 - 2, 1<<64 - 4}
	for _, blocks := range []int{1, 2, 3, 16, 40} {
		for _, counter := range counters {
			var key [8]uint32
			for i := range key {
				key[i] = r.Uint32()
			}
			flags, start, end := uint32(r.Intn(32))&^3, uint32(1), uint32(2)
			stride := blocks * blockSize
			l := lanes{key: key, blocks: uint32(blocks), flags: flags, start: start, end: end}
			for j := range l.in {
				l.in[j] = unsafe.Pointer(&data[j*stride])
				c := counter + uint64(j)
				l.ctr[j], l.ctr[4+j] = uint32(c), uint32(c>>32)
			}
			hash4(&l)
			for j := range l.out {
				want := chain(data[j*stride:], blocks, key, counter+uint64(j), flags, start, end)
				if l.out[j] != want {
					t.Fatalf("blocks %d counter %#x lane %d: %x, want %x", blocks, counter, j, l.out[j], want)
				}
			}
		}
	}
}
