package vfs_test

import (
	"testing"

	"github.com/zeebo/blake3"
)

// The name of an extension object in the store is a hash keyed from the Volume's Seed (SPEC
// 3.1). The store sees every name, so an unkeyed one would make the same workflow key the same
// object name in two Volumes, confirm a guess at a low-entropy key by hashing it, and say which
// Volumes share a key. Every other name the store sees is a hash of random sealed bytes or a
// random id.
func TestExtNameRevealsTheKey(t *testing.T) {
	be1, _, v1 := newMem(t)
	be2, _, v2 := newMem(t)
	if err := v1.PutExt(ctx, "trace", []byte("ci-main"), []byte("a")); err != nil {
		t.Fatal(err)
	}
	if err := v2.PutExt(ctx, "trace", []byte("ci-main"), []byte("b")); err != nil {
		t.Fatal(err)
	}
	var k1, k2 string
	for k := range be1.Dump() {
		if len(k) > 4 && k[:4] == "ext/" {
			k1 = k
		}
	}
	for k := range be2.Dump() {
		if len(k) > 4 && k[:4] == "ext/" {
			k2 = k
		}
	}
	sum := blake3.Sum256([]byte("ci-main"))
	guess := "ext/trace/" + hexs(sum[:])
	if k1 == k2 {
		t.Errorf("two Volumes name the object of the same key identically (%s)", k1)
	}
	if k1 == guess {
		t.Errorf("a store that guesses the key \"ci-main\" confirms it: BLAKE3(\"ci-main\") is the object's name")
	}
}

func hexs(b []byte) string {
	const d = "0123456789abcdef"
	out := make([]byte, 0, 2*len(b))
	for _, c := range b {
		out = append(out, d[c>>4], d[c&15])
	}
	return string(out)
}
