package blake3

import (
	"encoding/binary"
	"math/bits"
)

// The hash is a binary tree over 1 KiB chunks. A chunk is a chain of 64 byte
// blocks; a parent compresses the chaining values of its two children; the
// root is the node that is finalized with the root flag. A tree over n chunks
// puts the largest power of two strictly below n on the left, so every left
// subtree is complete and an appended chunk never changes a complete subtree.
// O'Connor, Aumasson, Neves and Wilcox-O'Hearn, "BLAKE3: one function, fast
// everywhere", https://github.com/BLAKE3-team/BLAKE3-specs.
const (
	blockSize = 64
	chunkSize = 1024

	// width is how many chunks the kernel hashes in one call.
	width = 16
	reach = width * chunkSize

	// depth is the deepest the tree can be: 2^54 chunks of 1 KiB is 2^64 bytes.
	depth = 54
)

// Domain flags, mixed into every compression.
const (
	flagChunkStart uint32 = 1 << iota
	flagChunkEnd
	flagParent
	flagRoot
	flagKeyed
	flagDeriveContext
	flagDeriveMaterial
)

// node is a node of the tree that is not yet compressed: the inputs of its
// one compression. It is compressed as an interior node to a chaining value,
// or, if it is the root, with the root flag to any number of output bytes.
type node struct {
	cv       [8]uint32
	block    [16]uint32
	counter  uint64
	blockLen uint32
	flags    uint32
}

// value compresses o as an interior node.
func (o *node) value() [8]uint32 {
	w := compress(&o.cv, &o.block, o.counter, o.blockLen, o.flags)
	return [8]uint32(w[:8])
}

// root fills out with the extendable output of the root node o.
func (o *node) root(out []byte) {
	var b [blockSize]byte
	if len(out) <= Size {
		// The digest: the first eight words of the first block of output.
		w := compress(&o.cv, &o.block, 0, o.blockLen, o.flags|flagRoot)
		for i := range 8 {
			binary.LittleEndian.PutUint32(b[4*i:], w[i])
		}
		copy(out, b[:Size])
		return
	}
	for counter := uint64(0); len(out) > 0; counter++ {
		w := compress(&o.cv, &o.block, counter, o.blockLen, o.flags|flagRoot)
		for i, x := range w {
			binary.LittleEndian.PutUint32(b[4*i:], x)
		}
		out = out[copy(out, b[:]):]
	}
}

// parent is the node whose children have the chaining values left and right.
func parent(left, right [8]uint32, key *[8]uint32, flags uint32) node {
	o := node{cv: *key, blockLen: blockSize, flags: flags | flagParent}
	copy(o.block[:8], left[:])
	copy(o.block[8:], right[:])
	return o
}

// leaf is the last block of the chunk in, with the chain of its earlier
// blocks already compressed into its chaining value. in holds at most chunkSize
// bytes; counter is the chunk's index.
func leaf(in []byte, counter uint64, key *[8]uint32, flags uint32) node {
	cv := *key
	f := flags | flagChunkStart
	for len(in) > blockSize {
		m := load(in)
		w := compress(&cv, &m, counter, blockSize, f)
		cv = [8]uint32(w[:8])
		in = in[blockSize:]
		f = flags
	}
	var last [blockSize]byte
	copy(last[:], in)
	return node{cv: cv, block: load(last[:]), counter: counter, blockLen: uint32(len(in)), flags: f | flagChunkEnd}
}

// split is the length of the left subtree of a tree over n > chunkSize bytes.
func split(n int) int {
	return chunkSize << (bits.Len(uint((n-1)/chunkSize)) - 1)
}

// kernel returns the node over in, which holds between zero and reach bytes and
// starts at chunk number counter: the chunk's last block for one chunk,
// otherwise the topmost parent, whose block is the chaining values of its two
// children. It is the one function an architecture replaces with a faster one
// that returns the same node.
var kernel = portable

// portable hashes the chunks one after another.
func portable(in []byte, counter uint64, key *[8]uint32, flags uint32) node {
	n := (len(in) + chunkSize - 1) / chunkSize
	if n <= 1 {
		return leaf(in, counter, key, flags)
	}
	var cvs [width][8]uint32
	for i := range n {
		o := leaf(in[i*chunkSize:min((i+1)*chunkSize, len(in))], counter+uint64(i), key, flags)
		cvs[i] = o.value()
	}
	return merge(&cvs, n, key, flags)
}

// merge folds the n >= 2 chaining values of adjacent subtrees into the topmost
// parent. Adjacent values are paired level by level and an odd one out moves up
// unpaired, which builds the same tree as splitting at the largest power of two.
func merge(cvs *[width][8]uint32, n int, key *[8]uint32, flags uint32) node {
	for n > 2 {
		m := 0
		for i := 0; i+1 < n; i += 2 {
			o := parent(cvs[i], cvs[i+1], key, flags)
			cvs[m] = o.value()
			m++
		}
		if n&1 == 1 {
			cvs[m] = cvs[n-1]
			m++
		}
		n = m
	}
	return parent(cvs[0], cvs[1], key, flags)
}

// tree returns the node over in, which starts at chunk number counter.
func tree(in []byte, counter uint64, key *[8]uint32, flags uint32) node {
	if len(in) <= reach {
		return kernel(in, counter, key, flags)
	}
	l := split(len(in))
	left := tree(in[:l], counter, key, flags)
	right := tree(in[l:], counter+uint64(l/chunkSize), key, flags)
	return parent(left.value(), right.value(), key, flags)
}

// chunk is a chunk being filled a few bytes at a time. Its last block is
// held back until more input arrives, because that block carries the chunk-end
// flag, and possibly the root flag, so it cannot be compressed early.
type chunk struct {
	cv       [8]uint32
	counter  uint64
	block    [blockSize]byte
	blockLen int
	blocks   int // blocks already compressed into cv
}

func (c *chunk) reset(key *[8]uint32, counter uint64) {
	*c = chunk{cv: *key, counter: counter}
}

func (c *chunk) len() int { return c.blocks*blockSize + c.blockLen }

func (c *chunk) start() uint32 {
	if c.blocks == 0 {
		return flagChunkStart
	}
	return 0
}

func (c *chunk) update(p []byte, flags uint32) {
	for len(p) > 0 {
		if c.blockLen == blockSize {
			m := load(c.block[:])
			w := compress(&c.cv, &m, c.counter, blockSize, flags|c.start())
			c.cv = [8]uint32(w[:8])
			c.blocks++
			c.blockLen = 0
		}
		n := copy(c.block[c.blockLen:], p)
		c.blockLen += n
		p = p[n:]
	}
}

// last is the node of the chunk's last block.
func (c *chunk) last(flags uint32) node {
	var last [blockSize]byte
	copy(last[:], c.block[:c.blockLen])
	return node{cv: c.cv, block: load(last[:]), counter: c.counter, blockLen: uint32(c.blockLen), flags: flags | c.start() | flagChunkEnd}
}
