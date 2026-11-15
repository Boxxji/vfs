// Package vfs is a filesystem on an object store: a Volume.
//
// # Format (version 2)
//
// A Volume is a prefix of a bucket holding three kinds of object, all sealed by
// the Volume's Cipher:
//
//	head                    the one mutable object: names the root shard
//	meta/<2 hex>/<hash>     metadata shards, immutable, named by the hash of their bytes
//	pack/<2 hex>/<id>       packs of file chunks, immutable, named by a random id
//
// File bytes are cut into content-defined chunks of 64 KiB to 4 MiB (chunk.go).
// Each chunk is sealed on its own, and sealed chunks are packed together into
// objects of about 16 MiB, so a tree of 150 GiB is a few thousand PUTs rather
// than one per chunk. Files of at most 16 KiB are not chunked at all: their bytes
// are a record in the metadata shard, beside their inode.
//
// All metadata (inodes, directory entries, chunk references, the table saying
// where each chunk lives) is records in one ordered Merkle tree (tree.go), cut
// into shards. The Head holds the tree's root ref and the next inode number.
//
// # Commit
//
// Flush settles every written file into chunks, uploads the open pack, writes
// the shards the changes touched (and no others), and then replaces the Head with
// one conditional write. Until that write lands the Head names the previous
// tree, which was not touched: a crash anywhere before it leaves the Volume
// exactly as the last Flush left it. Chunks, packs and shards a crashed or
// failed Flush uploaded are unreferenced garbage, never corruption.
//
// # Flush and Fsync
//
// Flush returns only after the Head naming everything written so far has
// advanced in the store. Fsync is Flush: there is no node-local journal yet, so
// no cheaper durable point exists. Neither runs on a timer here: a bound on how
// stale the store may be is a deadline timer owned by the caller of Volume,
// which calls Flush when it fires.
//
// # Fence
//
// Attach claims the Volume at a Round, a number that only grows. The claim is a
// conditional write of the Head that also carries forward the previous owner's
// last committed Head, so no acknowledged commit is lost, and from then on a
// commit by any older Round is refused with ErrStaleRound.
package vfs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"sync"
	"time"

	"github.com/hanzoai/vfs/pkg/backend"
	"github.com/hanzoai/vfs/pkg/fence"
)

var (
	// ErrStaleRound is returned when a newer Round holds the Volume: this
	// writer was fenced and its changes were not, and will never be, committed.
	ErrStaleRound = fence.ErrStaleRound

	// ErrReadOnly is returned by a write on a Volume that was Opened, not Attached.
	ErrReadOnly = errors.New("vfs: volume is read-only")

	ErrNotDir   = errors.New("vfs: not a directory")
	ErrIsDir    = errors.New("vfs: is a directory")
	ErrNotEmpty = errors.New("vfs: directory not empty")
)

// Config says where a Volume is, how it is sealed, and how much memory it may use.
type Config struct {
	Backend backend.Backend // the Volume's prefix; the Volume owns it and Close closes it
	Cipher  Cipher          // seals and opens every object
	Round   uint64          // Attach: the Round to claim; must exceed the recorded one, and be above 0

	Cache   int64 // bytes of shards and chunks kept decoded; default 256 MiB
	Dirty   int64 // bytes of unsettled writes before they are chunked and uploaded; default 64 MiB
	Overlay int   // metadata changes held before they are written into shards; default 262144
}

func (c Config) valid() (Config, error) {
	if c.Backend == nil || c.Cipher == nil {
		return c, errors.New("vfs: config needs a Backend and a Cipher")
	}
	if c.Cache <= 0 {
		c.Cache = 256 << 20
	}
	if c.Dirty <= 0 {
		c.Dirty = 64 << 20
	}
	if c.Overlay <= 0 {
		c.Overlay = 1 << 18
	}
	return c, nil
}

// Volume is a mounted filesystem. All methods are safe for concurrent use.
// A Volume that was Attached may write; one that was Opened may only read.
type Volume struct {
	be    backend.Backend
	c     Cipher
	fence *fence.Store
	cfg   Config
	round uint64 // 0 when read-only
	t     *tree

	chunks *lru[chunkID, []byte]

	// mu guards everything below. Flush holds it throughout: writers wait for a
	// commit, readers too. The upload pipeline that relaxes this comes with the
	// node cache.
	mu     sync.RWMutex
	head   Head // the committed Head
	root   ref  // the tree with every applied change, committed or not
	over   *overlay
	next   InodeID
	files  map[InodeID]*fileState
	dirty  int64
	pack   pack
	fenced bool
}

func newVolume(cfg Config) *Volume {
	sh := &shards{be: cfg.Backend, c: cfg.Cipher, cache: newLRU[ref, *node](cfg.Cache / 2)}
	return &Volume{
		be:     cfg.Backend,
		c:      cfg.Cipher,
		fence:  fence.New(cfg.Backend),
		cfg:    cfg,
		t:      newTree(sh),
		chunks: newLRU[chunkID, []byte](cfg.Cache / 2),
		over:   newOverlay(),
		files:  map[InodeID]*fileState{},
	}
}

// Create makes an empty Volume at the Backend's prefix: a tree holding the root
// directory, and a Head naming it, written only if no Head exists. It returns an
// error wrapping fs.ErrExist if the prefix already holds a Volume.
func Create(ctx context.Context, cfg Config) error {
	cfg, err := cfg.valid()
	if err != nil {
		return err
	}
	v := newVolume(cfg)
	now := time.Now()
	root := Inode{ID: RootInode, Mode: fs.ModeDir | 0o755, Mtime: now, Atime: now, Ctime: now}
	r, err := v.t.apply(ctx, none, []mut{{key: keyInode(RootInode), val: encodeInode(root)}})
	if err != nil {
		return err
	}
	sealed, err := seal(v.c, Head{Root: r, Next: uint64(RootInode) + 1}.marshal())
	if err != nil {
		return err
	}
	if err := v.fence.Create(ctx, headKey, sealed); err != nil {
		if errors.Is(err, backend.ErrConflict) {
			return fmt.Errorf("vfs: %s already holds a volume: %w", cfg.Backend, fs.ErrExist)
		}
		return err
	}
	return nil
}

// Open returns a read-only Volume at the committed Head. It claims nothing and
// fences no one.
func Open(ctx context.Context, cfg Config) (*Volume, error) {
	cfg, err := cfg.valid()
	if err != nil {
		return nil, err
	}
	v := newVolume(cfg)
	payload, _, err := v.fence.Get(ctx, headKey)
	if err != nil {
		if errors.Is(err, backend.ErrNotFound) {
			return nil, fmt.Errorf("vfs: no volume at %s: %w", cfg.Backend, fs.ErrNotExist)
		}
		return nil, err
	}
	if v.head, err = v.readHead(payload); err != nil {
		return nil, err
	}
	v.root, v.next = v.head.Root, InodeID(v.head.Next)
	return v, nil
}

// Attach claims the Volume at cfg.Round and returns it ready to write. The claim
// is refused with ErrStaleRound unless cfg.Round exceeds the Round recorded; it
// seals the predecessor's last committed Head at the new Round, atomically, and
// opens the Volume at that Head, so nothing the predecessor committed is lost and
// nothing it tries to commit afterwards lands.
func Attach(ctx context.Context, cfg Config) (*Volume, error) {
	cfg, err := cfg.valid()
	if err != nil {
		return nil, err
	}
	if cfg.Round == 0 {
		return nil, errors.New("vfs: Attach needs a Round above 0")
	}
	v := newVolume(cfg)
	err = v.fence.CarryForward(ctx, headKey, cfg.Round, func(payload []byte, recorded uint64) error {
		if payload == nil {
			return fmt.Errorf("vfs: no volume at %s: %w", cfg.Backend, fs.ErrNotExist)
		}
		if cfg.Round <= recorded {
			return fmt.Errorf("%w: round %d does not exceed the recorded %d", ErrStaleRound, cfg.Round, recorded)
		}
		var err error
		v.head, err = v.readHead(payload)
		return err
	})
	if err != nil {
		return nil, err
	}
	v.round, v.root, v.next = cfg.Round, v.head.Root, InodeID(v.head.Next)
	return v, nil
}

// Stats is a snapshot of a Volume for observability.
type Stats struct {
	Backend    string
	Round      uint64 // 0 when read-only
	Seq        uint64 // commits since Create
	Inodes     uint64 // inode numbers handed out
	CacheBytes int64
	CacheMax   int64
	Pending    int // metadata changes not yet written into shards
	Dirty      int64
}

func (v *Volume) Stats() Stats {
	v.mu.RLock()
	defer v.mu.RUnlock()
	cs, cm := v.chunks.stats()
	ss, sm := v.t.sh.cache.stats()
	return Stats{
		Backend: v.be.String(), Round: v.round, Seq: v.head.Seq, Inodes: uint64(v.next) - 1,
		CacheBytes: cs + ss, CacheMax: cm + sm, Pending: v.over.n, Dirty: v.dirty,
	}
}

// Close releases the backend. It does not Flush: changes not flushed are lost.
func (v *Volume) Close() error { return v.be.Close() }

// writable reports why v cannot write, if it cannot. mu must be held.
func (v *Volume) writable() error {
	switch {
	case v.round == 0:
		return ErrReadOnly
	case v.fenced:
		return ErrStaleRound
	}
	return nil
}

// Flush makes everything written so far durable: it returns only after the Head
// naming it has advanced in the store. If it returns an error the Volume is
// unchanged in the store, and the changes are kept in memory for the next Flush;
// ErrStaleRound means they never will land.
func (v *Volume) Flush(ctx context.Context) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if err := v.writable(); err != nil {
		return err
	}
	if err := v.settleAll(ctx); err != nil {
		return err
	}
	if v.over.n == 0 && v.pack.empty() && v.root == v.head.Root {
		return nil // the Head already names everything
	}
	if err := v.pack.upload(ctx, v.be); err != nil {
		return err
	}
	if err := v.spill(ctx); err != nil {
		return err
	}
	h := Head{Seq: v.head.Seq + 1, Root: v.root, Next: uint64(v.next)}
	if err := v.writeHead(ctx, h); err != nil {
		if errors.Is(err, ErrStaleRound) {
			v.fenced = true
		}
		return fmt.Errorf("vfs: commit: %w", err)
	}
	v.head = h
	return nil
}

// spill writes the pending metadata changes into shards, advancing the working
// root. The Head is untouched, so the shards stay garbage until a commit names them.
func (v *Volume) spill(ctx context.Context) error {
	if v.over.n == 0 {
		return nil
	}
	r, err := v.t.apply(ctx, v.root, v.over.drain())
	if err != nil {
		return err
	}
	v.root = r
	v.over.reset()
	return nil
}

// relieve bounds memory after a change: it chunks and uploads written file
// bytes, and writes pending metadata into shards, once either passes its limit.
func (v *Volume) relieve(ctx context.Context) error {
	if v.dirty >= v.cfg.Dirty {
		if err := v.settleAll(ctx); err != nil {
			return err
		}
	}
	if v.over.n >= v.cfg.Overlay {
		return v.spill(ctx)
	}
	return nil
}

// get returns the current value of a record: a pending change, else the tree's.
func (v *Volume) get(ctx context.Context, k []byte) ([]byte, bool, error) {
	if n, ok := v.over.get(k); ok {
		return n.val, !n.del, nil
	}
	return v.t.get(ctx, v.root, k)
}

// scan returns the current records whose key starts with prefix, in key order.
func (v *Volume) scan(ctx context.Context, prefix []byte) ([]entry, error) {
	var base []entry
	if err := v.t.scan(ctx, v.root, prefix, func(k, val []byte) bool {
		base = append(base, entry{k, val})
		return true
	}); err != nil {
		return nil, err
	}
	out := make([]entry, 0, len(base))
	i := 0
	for n := v.over.seek(prefix); n != nil && bytes.HasPrefix(n.key, prefix); n = n.next[0] {
		for i < len(base) && bytes.Compare(base[i].key, n.key) < 0 {
			out = append(out, base[i])
			i++
		}
		if i < len(base) && bytes.Equal(base[i].key, n.key) {
			i++
		}
		if !n.del {
			out = append(out, entry{n.key, n.val})
		}
	}
	return append(out, base[i:]...), nil
}
