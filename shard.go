package vfs

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/zeebo/blake3"

	"github.com/hanzoai/vfs/pkg/backend"
)

// ref names a shard: the blake3 digest of its sealed bytes. The shard is
// stored at meta/<2 hex>/<hex>, immutable, so a ref is both its address and its
// integrity check: a shard whose bytes do not hash to its ref is corrupt.
type ref [32]byte

// none is the ref of the empty tree.
var none ref

func shardKey(r ref) string {
	h := hex.EncodeToString(r[:])
	return "meta/" + h[:2] + "/" + h
}

// node is one decoded shard. A leaf (level 0) holds records; a higher node
// holds, per child, the child's first key and its ref. Nodes are immutable once
// decoded, so a cached node is shared freely.
type node struct {
	level int
	keys  [][]byte
	vals  [][]byte
	size  int64 // plaintext bytes, for cache accounting
}

func (n *node) encode() []byte {
	var w writer
	w.uvarint(uint64(n.level))
	w.uvarint(uint64(len(n.keys)))
	for i := range n.keys {
		w.bytes(n.keys[i])
		w.bytes(n.vals[i])
	}
	return w.b
}

func decodeNode(b []byte) (*node, error) {
	r := reader{b: b}
	n := &node{level: int(r.uvarint()), size: int64(len(b))}
	count := r.uvarint()
	if count == 0 || count > uint64(len(b)) {
		return nil, fmt.Errorf("%w: node of %d entries", ErrCorrupt, count)
	}
	n.keys, n.vals = make([][]byte, count), make([][]byte, count)
	for i := range n.keys {
		n.keys[i], n.vals[i] = r.bytes(), r.bytes()
		if n.level > 0 && len(n.vals[i]) != len(ref{}) {
			return nil, fmt.Errorf("%w: child ref of %d bytes", ErrCorrupt, len(n.vals[i]))
		}
	}
	return n, r.done()
}

// shards reads and writes sealed shards through the backend, with a bounded
// cache of decoded ones.
type shards struct {
	be    backend.Backend
	c     Cipher
	aad   []byte // what every shard is sealed to: the Volume
	cache *lru[ref, *node]
}

// maxShard bounds what a read will accept for a shard: a node ends at nodeMax, by
// at most one record more.
const maxShard = 2 * nodeMax

func (s *shards) load(ctx context.Context, r ref) (*node, error) {
	if n, ok := s.cache.get(r); ok {
		return n, nil
	}
	sealed, _, err := s.be.Get(ctx, shardKey(r), maxShard)
	if err != nil {
		if errors.Is(err, backend.ErrNotFound) {
			return nil, fmt.Errorf("%w: shard %x is missing", ErrCorrupt, r[:6])
		}
		return nil, fmt.Errorf("vfs: read shard %x: %w", r[:6], err)
	}
	if blake3.Sum256(sealed) != r {
		return nil, fmt.Errorf("%w: shard %x does not hash to its name", ErrCorrupt, r[:6])
	}
	plain, err := unseal(s.c, s.aad, sealed)
	if err != nil {
		return nil, fmt.Errorf("vfs: open shard %x: %w", r[:6], err)
	}
	n, err := decodeNode(plain)
	if err != nil {
		return nil, fmt.Errorf("vfs: decode shard %x: %w", r[:6], err)
	}
	s.cache.add(r, n, n.size+int64(48*len(n.keys)))
	return n, nil
}

// store seals and writes a node and returns its ref. The write is idempotent
// (the name is the hash of the bytes), so a retry after an error is safe.
func (s *shards) store(ctx context.Context, n *node) (ref, error) {
	plain := n.encode()
	n.size = int64(len(plain))
	sealed, err := seal(s.c, s.aad, plain)
	if err != nil {
		return none, err
	}
	r := ref(blake3.Sum256(sealed))
	if err := s.be.Put(ctx, shardKey(r), sealed); err != nil {
		return none, fmt.Errorf("vfs: write shard %x: %w", r[:6], err)
	}
	s.cache.add(r, n, n.size+int64(48*len(n.keys)))
	return r, nil
}
