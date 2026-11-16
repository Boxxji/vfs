package vfs_test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/zeebo/blake3"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/internal/storetest"
)

// identity is a Cipher that seals nothing: the shape of an adapter to a key service
// whose 'encrypt' call was wired to a no-op.
type identity struct{}

func (identity) Seal(p []byte) ([]byte, string, error)   { return append([]byte(nil), p...), "k", nil }
func (identity) Open(s []byte, _ string) ([]byte, error) { return append([]byte(nil), s...), nil }

// macOnly authenticates and hides nothing: the plaintext travels in the clear with a
// tag on the end.
type macOnly struct{}

func (macOnly) Seal(p []byte) ([]byte, string, error) {
	tag := blake3.Sum256(p)
	return append(append([]byte(nil), p...), tag[:16]...), "m", nil
}

func (macOnly) Open(s []byte, _ string) ([]byte, error) {
	if len(s) < 16 {
		return nil, errors.New("short")
	}
	p, tag := s[:len(s)-16], s[len(s)-16:]
	if sum := blake3.Sum256(p); !bytes.Equal(sum[:16], tag) {
		return nil, errors.New("bad tag")
	}
	return append([]byte(nil), p...), nil
}

// xorOnly hides its input and authenticates nothing.
type xorOnly struct{}

func (xorOnly) Seal(p []byte) ([]byte, string, error) {
	out := make([]byte, len(p))
	for i := range p {
		out[i] = p[i] ^ 0x5a
	}
	return out, "x", nil
}

func (xorOnly) Open(s []byte, _ string) ([]byte, error) {
	out := make([]byte, len(s))
	for i := range s {
		out[i] = s[i] ^ 0x5a
	}
	return out, nil
}

// TestIdentityCipherIsRefused: the Cipher is the only door, so a Cipher that
// returns its input stores every file in the clear and every tamper goes
// unnoticed by the AEAD layer. A Volume checks its Cipher whenever it is
// configured: a probe must not appear in its sealed form, must open, and must
// not open once altered.
func TestIdentityCipherIsRefused(t *testing.T) {
	mem := storetest.NewMem()
	if err := vfs.Create(ctx, cfg(mem, identity{}, 0)); !errors.Is(err, vfs.ErrBadCipher) {
		t.Fatalf("Create with a Cipher that seals nothing = %v, want ErrBadCipher", err)
	}
	if mem.Count() != 0 {
		t.Fatalf("a refused Create left %d objects", mem.Count())
	}
	// And a Cipher that seals but does not authenticate: it opens altered bytes.
	if err := vfs.Create(ctx, cfg(mem, xorOnly{}, 0)); !errors.Is(err, vfs.ErrBadCipher) {
		t.Fatalf("Create with an unauthenticated Cipher = %v, want ErrBadCipher", err)
	}
	// And one that authenticates but hides nothing: the only fault is the plaintext in
	// its output.
	if err := vfs.Create(ctx, cfg(mem, macOnly{}, 0)); !errors.Is(err, vfs.ErrBadCipher) {
		t.Fatalf("Create with a Cipher that hides nothing = %v, want ErrBadCipher", err)
	}
	// Configuring is where it is refused, before any object is read or written: Open
	// has nothing to seal, and finds no Volume, yet says the Cipher is bad.
	if _, err := vfs.Open(ctx, cfg(mem, macOnly{}, 0)); !errors.Is(err, vfs.ErrBadCipher) {
		t.Fatalf("Open with a Cipher that hides nothing = %v, want ErrBadCipher", err)
	}
	// A volume made with a real Cipher refuses to be attached through a bad one.
	c := vfsAge(t)
	if err := vfs.Create(ctx, cfg(mem, c, 0)); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []vfs.Cipher{identity{}, xorOnly{}} {
		if _, err := vfs.Attach(ctx, cfg(mem, bad, 1)); !errors.Is(err, vfs.ErrBadCipher) {
			t.Fatalf("Attach through a bad Cipher = %v, want ErrBadCipher", err)
		}
		if _, err := vfs.Open(ctx, cfg(mem, bad, 0)); !errors.Is(err, vfs.ErrBadCipher) {
			t.Fatalf("Open through a bad Cipher = %v, want ErrBadCipher", err)
		}
	}
}
