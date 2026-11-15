package vfs

import (
	"encoding/binary"

	"github.com/zeebo/blake3"
)

// Chunk sizes. A file's bytes are cut into content-defined chunks between
// minChunk and maxChunk, averaging near avgChunk.
//
// The cut is FastCDC (Xia et al., USENIX ATC 2016): a gear rolling hash decides,
// byte by byte, whether the hash of the last 64 bytes matches a mask. Two masks
// give "normalized chunking": a stricter one up to avgChunk, a looser one after,
// which pulls the size distribution toward the average and keeps most chunks
// large. Why this one: a cut depends only on the bytes since the previous cut, so
// after an edit the cuts re-synchronize within a few chunks and everything past
// them keeps its ids. An edit therefore rewrites only the chunks it touched,
// where fixed-size blocks would rewrite every block after an insertion. FastCDC
// skips the first minChunk bytes unhashed, so it is also several times cheaper
// than a plain rolling hash.
//
// The gear table below and the masks are part of the on-disk format: changing
// either changes every cut and so every chunk id.
const (
	minChunk = 64 << 10
	avgChunk = 512 << 10
	maxChunk = 4 << 20

	// avgBits is log2(avgChunk). The strict mask has two more bits, the loose
	// mask two fewer. Masks use the top bits of the hash, which depend on all 64
	// bytes of the window.
	avgBits = 19
	maskS   = (1<<(avgBits+2) - 1) << (64 - (avgBits + 2))
	maskL   = (1<<(avgBits-2) - 1) << (64 - (avgBits - 2))
)

// gear maps a byte to 64 random bits, drawn from blake3's extendable output
// over a fixed domain string so the table is reproducible without a literal.
var gear [256]uint64

func init() {
	h := blake3.New()
	_, _ = h.WriteString("hanzoai/vfs chunker gear table, format 2")
	var seed [256 * 8]byte
	_, _ = h.Digest().Read(seed[:])
	for i := range gear {
		gear[i] = binary.LittleEndian.Uint64(seed[i*8:])
	}
}

// cut returns the length of the first chunk of b. b is the next min(maxChunk,
// remaining) bytes of the file, so a result shorter than len(b) is a content
// cut and a result equal to len(b) is the window's end (the file's end, or
// maxChunk). The result depends only on b[:result].
func cut(b []byte) int {
	n := len(b)
	if n <= minChunk {
		return n
	}
	mid := min(avgChunk, n)
	var fp uint64
	i := minChunk
	for ; i < mid; i++ {
		fp = fp<<1 + gear[b[i]]
		if fp&maskS == 0 {
			return i + 1
		}
	}
	for ; i < n; i++ {
		fp = fp<<1 + gear[b[i]]
		if fp&maskL == 0 {
			return i + 1
		}
	}
	return n
}

// chunkID names a chunk by the blake3 digest of its plaintext. Ids appear only
// inside sealed shards, so the backend never sees them; within one Volume equal
// ids mean equal bytes, which is what dedupe uses. Volumes do not share chunks.
type chunkID [32]byte

func sumChunk(p []byte) chunkID { return blake3.Sum256(p) }
