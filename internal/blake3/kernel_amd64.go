package blake3

import "lukechampine.com/blake3/guts"

func init() { kernel = avx }

// avx hands the chunks to the AVX2 and AVX-512 kernel of
// lukechampine.com/blake3/guts (MIT), which picks the widest the CPU has.
func avx(in []byte, counter uint64, key *[8]uint32, flags uint32) node {
	n := (len(in) + chunkSize - 1) / chunkSize
	if n <= 1 {
		return leaf(in, counter, key, flags)
	}
	var o guts.Node
	if len(in) == reach {
		o = guts.CompressBuffer((*[reach]byte)(in), len(in), key, counter, flags)
	} else {
		o = pad(in, counter, key, flags)
	}
	return node{cv: o.CV, block: o.Block, counter: o.Counter, blockLen: o.BlockLen, flags: o.Flags}
}

// pad copies in into a full-size buffer, which the kernel requires because it
// loads whole vectors of chunks past the end of the data.
//
//go:noinline
func pad(in []byte, counter uint64, key *[8]uint32, flags uint32) guts.Node {
	var buf [reach]byte
	copy(buf[:], in)
	return guts.CompressBuffer(&buf, len(in), key, counter, flags)
}
