package bufpool_test

import (
	"bytes"
	"container/list"
	"sync"
	"testing"

	"github.com/hanzoai/vfs/internal/blake3"
	"github.com/hanzoai/vfs/internal/bufpool"
)

// The cache below has the shape of the engine's chunk cache: get returns the
// shared slice and drops the cache's lock, readers run at once under a read lock,
// and a reader's miss adds an entry, which evicts the oldest. With plain slices
// the evicted entry goes back to the pool while a reader is still copying from
// it. With Shared it does not: the reader pins the entry, and the eviction only
// ends the cache's hold.
type cache struct {
	mu   sync.Mutex
	max  int64
	used int64
	ll   *list.List
	m    map[int]*list.Element
}

type item struct {
	k int
	s *bufpool.Shared
	n int
}

func newCache(max int64) *cache {
	return &cache{max: max, ll: list.New(), m: map[int]*list.Element{}}
}

// pin returns the entry's bytes held until unpin, or false on a miss.
func (c *cache) pin(k int) ([]byte, *bufpool.Shared, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.m[k]
	if !ok {
		return nil, nil, false
	}
	c.ll.MoveToFront(el)
	s := el.Value.(*item).s
	b, ok := s.Pin()
	return b, s, ok
}

func (c *cache) add(k int, s *bufpool.Shared, n int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.m[k] = c.ll.PushFront(&item{k, s, n})
	c.used += int64(n)
	for c.used > c.max && c.ll.Len() > 1 {
		el := c.ll.Back()
		it := el.Value.(*item)
		c.ll.Remove(el)
		delete(c.m, it.k)
		c.used -= int64(it.n)
		it.s.Release()
	}
}

// TestCacheEvictionUnderReader: reader A pins chunk 1; reader B's miss evicts it;
// another writer of plaintext asks the pool for a slice. A reads chunk 1 whole,
// and the writer is given other memory until A has unpinned.
func TestCacheEvictionUnderReader(t *testing.T) {
	plain := bufpool.NewBudget(256<<20, 0).Pool(true)
	const n = 1 << 20
	c := newCache(n + n/2) // room for one chunk and a half
	one := plain.Get(n)
	copy(one, bytes.Repeat([]byte("A"), n))
	c.add(1, plain.Share(one), n)

	got, pin, _ := c.pin(1) // reader A, before it copies

	two := plain.Get(n) // reader B: a miss, decrypt, add: evicts chunk 1
	copy(two, bytes.Repeat([]byte("B"), n))
	c.add(2, plain.Share(two), n)

	three := plain.Get(n) // any other writer of plaintext
	copy(three, bytes.Repeat([]byte("C"), n))
	if &three[0] == &got[0] {
		t.Fatal("the pool handed out a chunk that a reader has pinned")
	}

	out := make([]byte, n)
	copy(out, got) // reader A finally copies
	if !bytes.Equal(out, bytes.Repeat([]byte("A"), n)) {
		t.Fatalf("reader A copied %q... from an entry that was evicted under it", out[:8])
	}
	pin.Unpin()

	// Now the slice is free to be reused, cleared.
	four := plain.Get(n)
	if &four[0] != &got[0] {
		t.Fatal("the chunk did not go back to the pool when its reader unpinned")
	}
	if bytes.Contains(four, []byte("A")) {
		t.Fatal("a recycled chunk kept its plaintext")
	}
}

// TestCacheConcurrent: many readers and adders at once; every reader sees whole
// chunks of the value it asked for, and every slice ends up back in the pool.
func TestCacheConcurrent(t *testing.T) {
	bud := bufpool.NewBudget(64<<20, 0)
	plain := bud.Pool(true)
	const n = 64 << 10
	c := newCache(8 * n)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				k := (g*31 + i*7) % 24
				v, pin, ok := c.pin(k)
				if !ok {
					b := plain.Get(n)
					for j := range b {
						b[j] = byte(k + 1)
					}
					c.add(k, plain.Share(b), n)
					continue
				}
				for _, x := range v {
					if x != byte(k+1) {
						t.Errorf("reader of %d saw %d", k, x)
						break
					}
				}
				pin.Unpin()
			}
		}()
	}
	wg.Wait()
	if st := plain.Stats(); st.Refused != 0 {
		t.Fatalf("slices were put back wrongly: %+v", st)
	}
}

// TestHashOfPinnedSlice: a hash of a pinned slice, whatever kernel does it, reads
// the bytes it was given while the owner lets go and the pool is asked for the
// same memory. The race detector does not see the assembly kernels read, so this
// ordering is what checks it.
func TestHashOfPinnedSlice(t *testing.T) {
	plain := bufpool.NewBudget(64<<20, 0).Pool(true)
	const n = 1 << 20
	b := plain.Get(n)
	for i := range b {
		b[i] = byte(i % 251)
	}
	want := blake3.Sum(b)
	s := plain.Share(b)

	view, ok := s.Pin()
	if !ok {
		t.Fatal("Pin failed")
	}
	released := make(chan struct{})
	go func() {
		defer close(released)
		s.Release() // the cache lets go
		next := plain.Get(n)
		if &next[0] == &view[0] {
			t.Error("the pool reused a slice that is pinned")
			return
		}
		for i := range next {
			next[i] = 0xFF // another owner fills its own memory
		}
	}()
	<-released
	if got := blake3.Sum(view); got != want {
		t.Fatal("the digest of a pinned slice changed while the owner let go")
	}
	s.Unpin()
}
