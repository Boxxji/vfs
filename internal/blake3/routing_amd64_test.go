package blake3

import (
	"reflect"
	"sync/atomic"
	"testing"
)

// On amd64 the one-shot and streaming hashes are the library and never enter the tree; Parallel uses the tree
// with the guts kernel. The digests are the same either way, so only the kernel can tell the routes apart.
func TestAMD64Routing(t *testing.T) {
	buf := pattern(300 << 10)
	native := kernel
	var calls atomic.Int64
	kernel = func(in []byte, counter uint64, key *[8]uint32, flags uint32) node {
		calls.Add(1)
		return native(in, counter, key, flags)
	}
	defer func() { kernel = native }()

	Sum(buf)
	h := New()
	h.Write(buf)
	h.Sum(nil)
	k := Keyed(&[32]byte{1})
	k.Write(buf)
	k.Expand(make([]byte, 100))
	var z Hasher
	z.Write(buf)
	z.Sum(nil)
	if n := calls.Load(); n != 0 {
		t.Fatalf("Sum or Hasher entered the tree %d times on amd64", n)
	}
	for _, hh := range []*Hasher{h, k, &z} {
		if _, ok := hh.impl().(library); !ok {
			t.Fatalf("Hasher state is %T, want the library", hh.impl())
		}
	}
	Parallel(pattern(1<<20), 4)
	if calls.Load() == 0 {
		t.Fatal("Parallel did not use the tree")
	}
	if reflect.ValueOf(native).Pointer() != reflect.ValueOf(avx).Pointer() {
		t.Fatal("the tree's kernel on amd64 is not the guts kernel")
	}
}
