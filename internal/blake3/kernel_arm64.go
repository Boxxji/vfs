package blake3

import "unsafe"

// lanes is the argument block of hash4. Its layout is the contract with
// kernel_arm64.s; kernel_arm64_test.go checks the offsets.
type lanes struct {
	in     [4]unsafe.Pointer // the four inputs
	ctr    [8]uint32         // chunk counters: four low words, then four high words
	key    [8]uint32         // chaining value every lane starts from
	out    [4][8]uint32      // chaining value of each lane at the end
	blocks uint32            // blocks per input
	flags  uint32            // flags of every block
	start  uint32            // extra flags of the first block
	end    uint32            // extra flags of the last block
	ahead  uint32            // bytes ahead of each input to prefetch, 0 for none
	_      [3]uint32         // pads m to a multiple of 16
	m      [16][4]uint32     // scratch: the transposed message block
}

// hash4 compresses blocks blocks of each of the four inputs in lockstep, chaining
// the blocks of each input, and stores the four chaining values in l.out. The
// counter of a lane does not change between its blocks.
//
//go:noescape
func hash4(l *lanes)

func init() { kernel = neon }

// neon is portable with the whole chunks hashed four at a time and
// the parents four at a time.
func neon(in []byte, counter uint64, key *[8]uint32, flags uint32) node {
	n := (len(in) + chunkSize - 1) / chunkSize
	if n <= 1 {
		return leaf(in, counter, key, flags)
	}
	var cvs [width][8]uint32
	whole := len(in) / chunkSize
	l := lanes{key: *key, blocks: chunkSize / blockSize, flags: flags, start: flagChunkStart, end: flagChunkEnd}
	for i := 0; i < whole; i += 4 {
		for j := range l.in {
			// Lanes past the last whole chunk repeat it; their output is dropped.
			l.in[j] = unsafe.Pointer(&in[min(i+j, whole-1)*chunkSize])
			c := counter + uint64(i+j)
			l.ctr[j], l.ctr[4+j] = uint32(c), uint32(c>>32)
		}
		// The next group is the next 4 KiB of the buffer the caller owns, if the
		// slice has room for it; past the end of the buffer there is nothing to fetch.
		l.ahead = 0
		if (i+4+4)*chunkSize <= cap(in) {
			l.ahead = 4 * chunkSize
		}
		hash4(&l)
		copy(cvs[i:], l.out[:])
	}
	if whole < n {
		o := leaf(in[whole*chunkSize:], counter+uint64(whole), key, flags)
		cvs[whole] = o.value()
	}
	return fold4(&cvs, n, key, flags)
}

// fold4 is merge with the parents of each level four at a time.
func fold4(cvs *[width][8]uint32, n int, key *[8]uint32, flags uint32) node {
	l := lanes{key: *key, blocks: 1, flags: flags | flagParent}
	for n > 2 {
		pairs, m := n/2, 0
		for ; m+4 <= pairs; m += 4 {
			for j := range l.in {
				l.in[j] = unsafe.Pointer(&cvs[2*(m+j)])
			}
			hash4(&l)
			copy(cvs[m:], l.out[:])
		}
		for ; m < pairs; m++ {
			o := parent(cvs[2*m], cvs[2*m+1], key, flags)
			cvs[m] = o.value()
		}
		if n&1 == 1 {
			cvs[m] = cvs[n-1]
			m++
		}
		n = m
	}
	return parent(cvs[0], cvs[1], key, flags)
}
