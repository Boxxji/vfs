package vfs_test

import (
	"bytes"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/internal/storetest"
	"github.com/hanzoai/vfs/pkg/backend"
)

// TestWrongKeyFailsClosed: a Cipher that does not hold the Volume's key opens
// nothing: not the Head, so neither Open nor Attach succeeds, and no byte of
// content or metadata is returned.
func TestWrongKeyFailsClosed(t *testing.T) {
	be, c, v := newMem(t)
	write(t, v, vfs.RootInode, "secret", []byte("classified"))
	flush(t, v)

	other := vfsAge(t)
	if _, err := vfs.Open(ctx, cfg(be, other, 0)); !errors.Is(err, vfs.ErrUnknownKey) {
		t.Fatalf("Open with the wrong key = %v, want ErrUnknownKey", err)
	}
	if _, err := vfs.Attach(ctx, cfg(be, other, 9)); !errors.Is(err, vfs.ErrUnknownKey) {
		t.Fatalf("Attach with the wrong key = %v, want ErrUnknownKey", err)
	}
	if round, err := roundOf(be); err != nil || round != 1 {
		t.Fatalf("a refused Attach moved the recorded round to %d (%v)", round, err)
	}
	if _, err := vfs.Open(ctx, cfg(be, c, 0)); err != nil {
		t.Fatalf("the right key stopped working: %v", err)
	}
}

func roundOf(be backend.Backend) (uint64, error) {
	b, _, err := be.Get(ctx, "head", backend.Unbounded)
	if err != nil {
		return 0, err
	}
	var round uint64
	for _, x := range b[6:14] { // the fence header: magic, then the round big-endian
		round = round<<8 | uint64(x)
	}
	return round, nil
}

// TestTamperedObjectsAreRefused: flipping a bit in a stored pack, shard or Head
// is an error on read, never silently different data.
func TestTamperedObjectsAreRefused(t *testing.T) {
	for _, class := range []string{"pack/", "meta/", "head"} {
		t.Run(class, func(t *testing.T) {
			be, c, v := newMem(t)
			ino := write(t, v, vfs.RootInode, "f", content(60, 300_000)).ID
			write(t, v, vfs.RootInode, "g", []byte("small"))
			flush(t, v)
			for k, b := range be.Dump() {
				if strings.HasPrefix(k, class) {
					b[len(b)/2] ^= 1
					if err := be.Put(ctx, k, b); err != nil {
						t.Fatal(err)
					}
				}
			}
			ro, err := vfs.Open(ctx, cfg(be, c, 0))
			if err == nil {
				_, err = ro.Resolve(ctx, "/g")
			}
			if err == nil && class != "pack/" {
				t.Fatal("tampered metadata was read without error")
			}
			if class == "pack/" {
				f, _ := ro.Open(ctx, ino)
				if _, err = f.ReadAt(ctx, make([]byte, 300_000), 0); err == nil || !errors.Is(err, vfs.ErrCorrupt) {
					t.Fatalf("tampered pack read = %v, want ErrCorrupt", err)
				}
			}
		})
	}
}

// TestSwappedChunkIsRefused: two chunks that both open under the Volume's key,
// exchanged in the pack, are caught by the chunk's own hash: the right key is not
// enough to be believed.
func TestSwappedChunkIsRefused(t *testing.T) {
	be, c, v := newMem(t)
	x := write(t, v, vfs.RootInode, "x", content(70, 50_000)).ID
	write(t, v, vfs.RootInode, "y", content(71, 50_000))
	flush(t, v)
	var key string
	for k := range be.Dump() {
		if strings.HasPrefix(k, "pack/") {
			key = k
		}
	}
	pack, _, _ := be.Get(ctx, key, backend.Unbounded)
	half := len(pack) / 2
	swapped := append(append([]byte(nil), pack[half:]...), pack[:half]...)
	if err := be.Put(ctx, key, swapped); err != nil {
		t.Fatal(err)
	}
	ro := reopen(t, be, c)
	f, _ := ro.Open(ctx, x)
	if _, err := f.ReadAt(ctx, make([]byte, 50_000), 0); !errors.Is(err, vfs.ErrCorrupt) {
		t.Fatalf("reading a swapped chunk = %v, want ErrCorrupt", err)
	}
}

// TestRotation: a Cipher holding a new key and the old one reads everything the
// old key sealed and seals new objects under the new key. Once the old key is
// dropped, what it sealed no longer opens; what the new key sealed does.
func TestRotation(t *testing.T) {
	be := storetest.NewMem()
	oldKey, newKey_ := newKey(t), newKey(t)
	oldC := vfs.Hybrid(oldKey)
	if err := vfs.Create(ctx, cfg(be, oldC, 0)); err != nil {
		t.Fatal(err)
	}
	v, err := vfs.Attach(ctx, cfg(be, oldC, 1))
	if err != nil {
		t.Fatal(err)
	}
	old := write(t, v, vfs.RootInode, "old", content(80, 100_000)).ID
	flush(t, v)

	both := vfs.Hybrid(newKey_, oldKey)
	v2, err := vfs.Attach(ctx, cfg(be, both, 2))
	if err != nil {
		t.Fatalf("attach under the rotating cipher: %v", err)
	}
	mustEqual(t, "old file under the new cipher", readAll(t, v2, old), content(80, 100_000))
	fresh := write(t, v2, vfs.RootInode, "fresh", content(81, 100_000)).ID
	flush(t, v2)
	mustEqual(t, "fresh file", readAll(t, reopen(t, be, both), fresh), content(81, 100_000))

	// The old key alone cannot open the Head any more: it was sealed under the new one.
	if _, err := vfs.Open(ctx, cfg(be, oldC, 0)); !errors.Is(err, vfs.ErrUnknownKey) {
		t.Fatalf("old key alone after rotation = %v, want ErrUnknownKey", err)
	}
	// The new key alone opens the Head and what was rewritten since, but not what
	// only the old key sealed: nothing was bulk re-encrypted, and nothing was weakened.
	only := reopen(t, be, vfs.Hybrid(newKey_))
	f, _ := only.Open(ctx, fresh)
	if _, err := f.ReadAt(ctx, make([]byte, 100_000), 0); err != nil && !errors.Is(err, vfs.ErrUnknownKey) {
		t.Fatalf("fresh chunk under the new key alone: %v", err)
	}
	g, err := only.Open(ctx, old)
	if err == nil {
		_, err = g.ReadAt(ctx, make([]byte, 100_000), 0)
	}
	if !errors.Is(err, vfs.ErrUnknownKey) {
		t.Fatalf("a chunk sealed under the dropped key = %v, want ErrUnknownKey", err)
	}
}

// TestNothingKeyShapedOnDisk: after a Volume has been created, written, flushed
// and reopened on the file backend, no file under the store contains the secret key,
// a plaintext marker from the content, or a file name.
func TestNothingKeyShapedOnDisk(t *testing.T) {
	dir := t.TempDir()
	be := reopenBackend(t, dir)
	key := newKey(t)
	c := vfs.Hybrid(key)
	if err := vfs.Create(ctx, cfg(be, c, 0)); err != nil {
		t.Fatal(err)
	}
	v, err := vfs.Attach(ctx, cfg(be, c, 1))
	if err != nil {
		t.Fatal(err)
	}
	marker := bytes.Repeat([]byte("PLAINTEXT-MARKER-0123456789abcdef"), 5000)
	write(t, v, vfs.RootInode, "a-distinctive-file-name.txt", marker)
	write(t, v, vfs.RootInode, "tiny", []byte("PLAINTEXT-MARKER-inline"))
	flush(t, v)

	secret := key.String()
	needles := [][]byte{[]byte(secret), []byte(secret[len(secret)-24:]), []byte("AGE-SECRET-KEY"), []byte("PLAINTEXT-MARKER"), []byte("a-distinctive-file-name"), []byte("tiny")}
	files := 0
	err = filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		files++
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		for _, n := range needles {
			if bytes.Contains(b, n) {
				t.Errorf("%s contains %q", p, n)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if files < 3 {
		t.Fatalf("only %d files on disk; the scan proved little", files)
	}
}

// countingCipher counts every use of the door.
type countingCipher struct {
	vfs.Cipher
	seals, opens atomic.Int64
}

func (c *countingCipher) Seal(p []byte) ([]byte, string, error) {
	c.seals.Add(1)
	return c.Cipher.Seal(p)
}

func (c *countingCipher) Open(s []byte, id string) ([]byte, error) {
	c.opens.Add(1)
	return c.Cipher.Open(s, id)
}

// TestCipherIsTheOnlyDoor: every object in the store went through Seal, and a
// read of it goes through Open; nothing reaches the backend except as the Cipher
// returned it.
func TestCipherIsTheOnlyDoor(t *testing.T) {
	be := storetest.NewMem()
	cc := &countingCipher{Cipher: vfsAge(t)}
	if err := vfs.Create(ctx, cfg(be, cc, 0)); err != nil {
		t.Fatal(err)
	}
	v, err := vfs.Attach(ctx, cfg(be, cc, 1))
	if err != nil {
		t.Fatal(err)
	}
	ino := write(t, v, vfs.RootInode, "f", content(90, 400_000)).ID
	write(t, v, vfs.RootInode, "g", []byte("inline"))
	flush(t, v)
	objects := be.Count()
	// Every metadata shard and the Head is one Seal; every chunk is one Seal.
	if seals := cc.seals.Load(); seals < int64(objects) {
		t.Fatalf("%d objects stored but only %d Seals", objects, seals)
	}
	before := cc.opens.Load()
	mustEqual(t, "read through a cold volume", readAll(t, reopen(t, be, cc), ino), content(90, 400_000))
	if cc.opens.Load() == before {
		t.Fatal("a cold read never called Open")
	}
}
