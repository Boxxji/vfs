// Package blake3 is the BLAKE3 hash function with SIMD kernels.
//
// Sum hashes a buffer, Hasher hashes a stream (plain or keyed, with extendable
// output), DeriveKey derives keys, and Parallel hashes a large buffer on several
// cores. All of them give the digests of the BLAKE3
// specification, bit for bit; the tests check them against the official test
// vectors and against an independent implementation.
//
// The tree hash (node.go) is portable Go. It hands every run of up to sixteen
// chunks to one kernel function, which is where the speed is: on arm64 it is
// NEON assembly that hashes four chunks per instruction stream (kernel_arm64.s,
// derived from the BLAKE3 reference C implementation blake3_neon.c); on amd64 it
// is the AVX2 and AVX-512 kernel of lukechampine.com/blake3/guts; elsewhere it
// is plain Go.
//
// On amd64 the one-shot and streaming hashes are the library the engine already
// links, because it is faster there than the tree over that kernel (see
// docs/hashing.md); Parallel uses the tree. The tree hash itself is tested on
// every architecture, so the routing can change without changing the tests.
//
// BLAKE3 reference implementations: Copyright the BLAKE3 team, Apache-2.0 or
// CC0-1.0, https://github.com/BLAKE3-team/BLAKE3.
package blake3

import (
	"encoding/binary"
	"math/bits"
	"sync"
	"sync/atomic"
)

// Size is the length of a digest in bytes.
const Size = 32

// stream makes the Hasher's state. An architecture may replace it in init with
// a faster implementation that gives the same results.
var stream = open

// hasher is what a Hasher drives.
type hasher interface {
	Write(p []byte) (int, error)
	WriteByte(c byte) error
	Sum(b []byte) []byte
	Expand(out []byte)
	Reset()
}

// canon is Sum by the tree.
func canon(p []byte) (digest [Size]byte) {
	o := tree(p, 0, &iv, 0)
	o.root(digest[:])
	return digest
}

// grain is the unit of parallel work: a complete subtree of 128 chunks. Large
// enough that handing it out is noise, small enough to balance the load.
const grain = 128 * chunkSize

// Parallel returns the same digest as Sum(p), computed by up to workers
// goroutines, the caller's included. It splits p into fixed jobs that are
// complete subtrees of the hash tree, so the work is independent and the join
// is a handful of parent nodes. It never uses more workers than half the number
// of jobs, because a worker that gets one job loses what it costs to start; with
// fewer than two it is Sum. When it returns, no goroutine it started is still
// reading p.
func Parallel(p []byte, workers int) [Size]byte {
	n := (len(p) + grain - 1) / grain
	workers = min(workers, n/2)
	if workers < 2 {
		return Sum(p)
	}
	return fan(p, workers, n)
}

func fan(p []byte, workers, n int) (digest [Size]byte) {
	cvs := make([][8]uint32, n)
	var next atomic.Int64
	run := func() {
		for {
			i := int(next.Add(1)) - 1
			if i >= n {
				return
			}
			o := tree(p[i*grain:min((i+1)*grain, len(p))], uint64(i)*(grain/chunkSize), &iv, 0)
			cvs[i] = o.value()
		}
	}
	var wg sync.WaitGroup
	for range workers - 1 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			run()
		}()
	}
	run()
	wg.Wait()
	o := join(cvs)
	o.root(digest[:])
	return digest
}

// join folds the chaining values of consecutive jobs, the last of which may be
// short, into the root, splitting at the largest power of two below the count as
// the tree does for chunks.
func join(cvs [][8]uint32) node {
	if len(cvs) == 2 {
		return parent(cvs[0], cvs[1], &iv, 0)
	}
	l := 1 << (bits.Len(uint(len(cvs)-1)) - 1)
	return parent(top(cvs[:l]), top(cvs[l:]), &iv, 0)
}

// top is the chaining value of the subtree over cvs.
func top(cvs [][8]uint32) [8]uint32 {
	if len(cvs) == 1 {
		return cvs[0]
	}
	o := join(cvs)
	return o.value()
}

// Hasher hashes a stream. It satisfies hash.Hash. The zero value is an unkeyed
// Hasher ready for use. A Hasher is not safe for concurrent use.
type Hasher struct {
	h     hasher
	key   [32]byte
	keyed bool
}

// New returns a Hasher for the unkeyed hash.
func New() *Hasher {
	h := new(Hasher)
	h.impl()
	return h
}

// Keyed returns a Hasher for the keyed hash (a MAC) under key.
func Keyed(key *[32]byte) *Hasher {
	h := &Hasher{key: *key, keyed: true}
	h.impl()
	return h
}

func (h *Hasher) impl() hasher {
	if h.h == nil {
		var key *[32]byte
		if h.keyed {
			key = &h.key
		}
		h.h = stream(key)
	}
	return h.h
}

// Reset discards the data written so far; the key is kept.
func (h *Hasher) Reset() { h.impl().Reset() }

// Size is the length of a digest.
func (h *Hasher) Size() int { return Size }

// BlockSize is the length of a compression block.
func (h *Hasher) BlockSize() int { return blockSize }

// Write adds p to the hash. It never fails.
func (h *Hasher) Write(p []byte) (int, error) { return h.impl().Write(p) }

// WriteByte adds one byte to the hash, without a slice to allocate for it. It never
// fails.
func (h *Hasher) WriteByte(c byte) error { return h.impl().WriteByte(c) }

// WriteString is Write for a string.
func (h *Hasher) WriteString(s string) (int, error) { return h.impl().Write([]byte(s)) }

// Sum appends the digest of the data written so far to b. It does not change
// the state of h.
func (h *Hasher) Sum(b []byte) []byte { return h.impl().Sum(b) }

// Expand fills out with the extendable output of the data written so far: the
// first Size bytes are the digest and any number may follow. It does not change
// the state of h.
func (h *Hasher) Expand(out []byte) { h.impl().Expand(out) }

// streamer is the Hasher built on the tree.
type streamer struct {
	key   [8]uint32
	flags uint32
	chunk chunk
	// stack holds the chaining values of the complete subtrees to the left of
	// the chunk in progress, largest first. Merging is lazy, as in the reference
	// implementation: a value is merged only when the next one arrives, so the
	// root is never merged early.
	stack [depth + 1][8]uint32
	n     int
}

// newStreamer returns a streamer for the chaining value key and the flags that every
// node of its tree carries.
func newStreamer(key [8]uint32, flags uint32) *streamer {
	s := &streamer{key: key, flags: flags}
	s.Reset()
	return s
}

// open returns a streamer for the unkeyed hash, or for the keyed hash under key.
func open(key *[32]byte) hasher {
	if key == nil {
		return newStreamer(iv, 0)
	}
	var words [8]uint32
	for i := range words {
		words[i] = binary.LittleEndian.Uint32(key[4*i:])
	}
	return newStreamer(words, flagKeyed)
}

// DeriveKey fills out with key material derived from material under the context
// string, as the key derivation mode of the BLAKE3 specification defines: the
// context is hashed into a key, and the material is hashed under that key. out may
// be any length; its first 32 bytes are the derived key. The context should be a
// fixed string, different for every purpose. It is the same function, with the same
// signature, as the one in the library the engine links today.
func DeriveKey(context string, material, out []byte) {
	c := newStreamer(iv, flagDeriveContext)
	c.Write([]byte(context))
	var ck [32]byte
	c.Expand(ck[:])
	var words [8]uint32
	for i := range words {
		words[i] = binary.LittleEndian.Uint32(ck[4*i:])
	}
	m := newStreamer(words, flagDeriveMaterial)
	m.Write(material)
	m.Expand(out)
}

func (h *streamer) Reset() {
	h.chunk.reset(&h.key, 0)
	h.n = 0
}

// push adds the chaining value of a subtree that starts at chunk number counter.
func (h *streamer) push(cv [8]uint32, counter uint64) {
	h.merge(counter)
	h.stack[h.n] = cv
	h.n++
}

// merge joins the values on the stack that complete a subtree once total chunks
// have been hashed. The stack holds one value per set bit of total.
func (h *streamer) merge(total uint64) {
	for h.n > bits.OnesCount64(total) {
		o := parent(h.stack[h.n-2], h.stack[h.n-1], &h.key, h.flags)
		h.stack[h.n-2] = o.value()
		h.n--
	}
}

func (h *streamer) WriteByte(c byte) error {
	var one [1]byte
	one[0] = c
	h.Write(one[:])
	return nil
}

func (h *streamer) Write(p []byte) (int, error) {
	total := len(p)
	if h.chunk.len() > 0 {
		take := min(chunkSize-h.chunk.len(), len(p))
		h.chunk.update(p[:take], h.flags)
		p = p[take:]
		if len(p) == 0 {
			return total, nil
		}
		// The chunk is full and more input follows, so it is not the root.
		o := h.chunk.last(h.flags)
		h.push(o.value(), h.chunk.counter)
		h.chunk.reset(&h.key, h.chunk.counter+1)
	}
	// While more than one chunk remains, hash the largest complete subtree that
	// starts where the hash stands: a power of two chunks, aligned to its size.
	for len(p) > chunkSize {
		size := 1 << (bits.Len(uint(len(p))) - 1)
		for uint64(size-1)&(h.chunk.counter*chunkSize) != 0 {
			size >>= 1
		}
		chunks := uint64(size / chunkSize)
		if size <= chunkSize {
			var c chunk
			c.reset(&h.key, h.chunk.counter)
			c.update(p[:size], h.flags)
			o := c.last(h.flags)
			h.push(o.value(), c.counter)
		} else {
			// Push both children of the subtree's top node, not their parent, so
			// that the top node can still become the root.
			o := tree(p[:size], h.chunk.counter, &h.key, h.flags)
			h.push([8]uint32(o.block[:8]), h.chunk.counter)
			h.push([8]uint32(o.block[8:]), h.chunk.counter+chunks/2)
		}
		h.chunk.counter += chunks
		p = p[size:]
	}
	if len(p) > 0 {
		h.chunk.update(p, h.flags)
		h.merge(h.chunk.counter)
	}
	return total, nil
}

// root is the root node of the data written so far.
func (h *streamer) root() node {
	if h.n == 0 {
		return h.chunk.last(h.flags)
	}
	var o node
	rest := h.n
	if h.chunk.len() > 0 {
		o = h.chunk.last(h.flags)
	} else {
		// The chunk is empty, so the top two values on the stack are siblings.
		rest -= 2
		o = parent(h.stack[rest], h.stack[rest+1], &h.key, h.flags)
	}
	for rest > 0 {
		rest--
		o = parent(h.stack[rest], o.value(), &h.key, h.flags)
	}
	return o
}

func (h *streamer) Sum(b []byte) []byte {
	var d [Size]byte
	o := h.root()
	o.root(d[:])
	return append(b, d[:]...)
}

func (h *streamer) Expand(out []byte) {
	o := h.root()
	o.root(out)
}
