package vfs

import "bytes"

// overlay is the ordered set of changes not yet applied to the tree: a skip
// list keyed by record key. Ordered, so a directory listing or a file's chunk
// list merges with the tree by walking both in key order, and applying it to the
// tree needs no sort.
type overlay struct {
	head  *onode
	level int
	n     int
	seed  uint64
}

const maxLevel = 24

type onode struct {
	key, val []byte
	del      bool
	next     []*onode
}

func newOverlay() *overlay {
	return &overlay{head: &onode{next: make([]*onode, maxLevel)}, level: 1, seed: 0x9e3779b97f4a7c15}
}

// height draws a level with probability 1/4 of each step up.
func (o *overlay) height() int {
	o.seed ^= o.seed << 13
	o.seed ^= o.seed >> 7
	o.seed ^= o.seed << 17
	h := 1
	for s := o.seed; s&3 == 0 && h < maxLevel; s >>= 2 {
		h++
	}
	return h
}

// find returns the first node whose key is at least k, and fills prev with the
// last node before it on each level.
func (o *overlay) find(k []byte, prev *[maxLevel]*onode) *onode {
	x := o.head
	for l := o.level - 1; l >= 0; l-- {
		for x.next[l] != nil && bytes.Compare(x.next[l].key, k) < 0 {
			x = x.next[l]
		}
		if prev != nil {
			prev[l] = x
		}
	}
	return x.next[0]
}

func (o *overlay) set(k, v []byte, del bool) {
	var prev [maxLevel]*onode
	if x := o.find(k, &prev); x != nil && bytes.Equal(x.key, k) {
		x.val, x.del = v, del
		return
	}
	h := o.height()
	for l := o.level; l < h; l++ {
		prev[l] = o.head
	}
	o.level = max(o.level, h)
	x := &onode{key: k, val: v, del: del, next: make([]*onode, h)}
	for l := range h {
		x.next[l] = prev[l].next[l]
		prev[l].next[l] = x
	}
	o.n++
}

func (o *overlay) put(k, v []byte) { o.set(k, v, false) }
func (o *overlay) del(k []byte)    { o.set(k, nil, true) }

// get returns the pending change to k, if any.
func (o *overlay) get(k []byte) (n *onode, ok bool) {
	if x := o.find(k, nil); x != nil && bytes.Equal(x.key, k) {
		return x, true
	}
	return nil, false
}

// seek returns the first pending change at or after k.
func (o *overlay) seek(k []byte) *onode { return o.find(k, nil) }

// drain returns every pending change in key order.
func (o *overlay) drain() []mut {
	ms := make([]mut, 0, o.n)
	for x := o.head.next[0]; x != nil; x = x.next[0] {
		ms = append(ms, mut{key: x.key, val: x.val, del: x.del})
	}
	return ms
}

func (o *overlay) reset() { *o = *newOverlay() }
