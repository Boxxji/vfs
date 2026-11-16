package blake3

import (
	"encoding/binary"
	"math/bits"
)

// iv is the initialization vector, the fractional parts of the square roots of
// the first eight primes, as in SHA-256.
var iv = [8]uint32{
	0x6A09E667, 0xBB67AE85, 0x3C6EF372, 0xA54FF53A,
	0x510E527F, 0x9B05688C, 0x1F83D9AB, 0x5BE0CD19,
}

// g is the quarter round of the BLAKE3 compression function: it mixes two
// message words into four state words.
func g(a, b, c, d, x, y uint32) (uint32, uint32, uint32, uint32) {
	a += b + x
	d = bits.RotateLeft32(d^a, -16)
	c += d
	b = bits.RotateLeft32(b^c, -12)
	a += b + y
	d = bits.RotateLeft32(d^a, -8)
	c += d
	b = bits.RotateLeft32(b^c, -7)
	return a, b, c, d
}

// load reads a 64 byte block as sixteen little-endian words.
func load(p []byte) (m [16]uint32) {
	p = p[:blockSize]
	for i := range m {
		m[i] = binary.LittleEndian.Uint32(p[4*i:])
	}
	return m
}

// compress runs the BLAKE3 compression function once, over one block with
// chaining value cv, and returns the sixteen output words: the first eight are
// the next chaining value, all sixteen are the extendable output of the root.
//
// The rounds are written out with the message schedule folded into the word
// indices, so that every state word stays in a register.
func compress(cv *[8]uint32, block *[16]uint32, counter uint64, blockLen, flags uint32) [16]uint32 {
	m0, m1, m2, m3, m4, m5, m6, m7 := block[0], block[1], block[2], block[3], block[4], block[5], block[6], block[7]
	m8, m9, m10, m11, m12, m13, m14, m15 := block[8], block[9], block[10], block[11], block[12], block[13], block[14], block[15]
	v0, v1, v2, v3, v4, v5, v6, v7 := cv[0], cv[1], cv[2], cv[3], cv[4], cv[5], cv[6], cv[7]
	v8, v9, v10, v11 := iv[0], iv[1], iv[2], iv[3]
	v12, v13, v14, v15 := uint32(counter), uint32(counter>>32), blockLen, flags

	v0, v4, v8, v12 = g(v0, v4, v8, v12, m0, m1)
	v1, v5, v9, v13 = g(v1, v5, v9, v13, m2, m3)
	v2, v6, v10, v14 = g(v2, v6, v10, v14, m4, m5)
	v3, v7, v11, v15 = g(v3, v7, v11, v15, m6, m7)
	v0, v5, v10, v15 = g(v0, v5, v10, v15, m8, m9)
	v1, v6, v11, v12 = g(v1, v6, v11, v12, m10, m11)
	v2, v7, v8, v13 = g(v2, v7, v8, v13, m12, m13)
	v3, v4, v9, v14 = g(v3, v4, v9, v14, m14, m15)

	v0, v4, v8, v12 = g(v0, v4, v8, v12, m2, m6)
	v1, v5, v9, v13 = g(v1, v5, v9, v13, m3, m10)
	v2, v6, v10, v14 = g(v2, v6, v10, v14, m7, m0)
	v3, v7, v11, v15 = g(v3, v7, v11, v15, m4, m13)
	v0, v5, v10, v15 = g(v0, v5, v10, v15, m1, m11)
	v1, v6, v11, v12 = g(v1, v6, v11, v12, m12, m5)
	v2, v7, v8, v13 = g(v2, v7, v8, v13, m9, m14)
	v3, v4, v9, v14 = g(v3, v4, v9, v14, m15, m8)

	v0, v4, v8, v12 = g(v0, v4, v8, v12, m3, m4)
	v1, v5, v9, v13 = g(v1, v5, v9, v13, m10, m12)
	v2, v6, v10, v14 = g(v2, v6, v10, v14, m13, m2)
	v3, v7, v11, v15 = g(v3, v7, v11, v15, m7, m14)
	v0, v5, v10, v15 = g(v0, v5, v10, v15, m6, m5)
	v1, v6, v11, v12 = g(v1, v6, v11, v12, m9, m0)
	v2, v7, v8, v13 = g(v2, v7, v8, v13, m11, m15)
	v3, v4, v9, v14 = g(v3, v4, v9, v14, m8, m1)

	v0, v4, v8, v12 = g(v0, v4, v8, v12, m10, m7)
	v1, v5, v9, v13 = g(v1, v5, v9, v13, m12, m9)
	v2, v6, v10, v14 = g(v2, v6, v10, v14, m14, m3)
	v3, v7, v11, v15 = g(v3, v7, v11, v15, m13, m15)
	v0, v5, v10, v15 = g(v0, v5, v10, v15, m4, m0)
	v1, v6, v11, v12 = g(v1, v6, v11, v12, m11, m2)
	v2, v7, v8, v13 = g(v2, v7, v8, v13, m5, m8)
	v3, v4, v9, v14 = g(v3, v4, v9, v14, m1, m6)

	v0, v4, v8, v12 = g(v0, v4, v8, v12, m12, m13)
	v1, v5, v9, v13 = g(v1, v5, v9, v13, m9, m11)
	v2, v6, v10, v14 = g(v2, v6, v10, v14, m15, m10)
	v3, v7, v11, v15 = g(v3, v7, v11, v15, m14, m8)
	v0, v5, v10, v15 = g(v0, v5, v10, v15, m7, m2)
	v1, v6, v11, v12 = g(v1, v6, v11, v12, m5, m3)
	v2, v7, v8, v13 = g(v2, v7, v8, v13, m0, m1)
	v3, v4, v9, v14 = g(v3, v4, v9, v14, m6, m4)

	v0, v4, v8, v12 = g(v0, v4, v8, v12, m9, m14)
	v1, v5, v9, v13 = g(v1, v5, v9, v13, m11, m5)
	v2, v6, v10, v14 = g(v2, v6, v10, v14, m8, m12)
	v3, v7, v11, v15 = g(v3, v7, v11, v15, m15, m1)
	v0, v5, v10, v15 = g(v0, v5, v10, v15, m13, m3)
	v1, v6, v11, v12 = g(v1, v6, v11, v12, m0, m10)
	v2, v7, v8, v13 = g(v2, v7, v8, v13, m2, m6)
	v3, v4, v9, v14 = g(v3, v4, v9, v14, m4, m7)

	v0, v4, v8, v12 = g(v0, v4, v8, v12, m11, m15)
	v1, v5, v9, v13 = g(v1, v5, v9, v13, m5, m0)
	v2, v6, v10, v14 = g(v2, v6, v10, v14, m1, m9)
	v3, v7, v11, v15 = g(v3, v7, v11, v15, m8, m6)
	v0, v5, v10, v15 = g(v0, v5, v10, v15, m14, m10)
	v1, v6, v11, v12 = g(v1, v6, v11, v12, m2, m12)
	v2, v7, v8, v13 = g(v2, v7, v8, v13, m3, m4)
	v3, v4, v9, v14 = g(v3, v4, v9, v14, m7, m13)
	return [16]uint32{
		v0 ^ v8, v1 ^ v9, v2 ^ v10, v3 ^ v11, v4 ^ v12, v5 ^ v13, v6 ^ v14, v7 ^ v15,
		v8 ^ cv[0], v9 ^ cv[1], v10 ^ cv[2], v11 ^ cv[3], v12 ^ cv[4], v13 ^ cv[5], v14 ^ cv[6], v15 ^ cv[7],
	}
}
