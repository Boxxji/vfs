package vfs

import (
	"context"
	"errors"
	"testing"

	"github.com/luxfi/age"

	"github.com/hanzoai/vfs/internal/storetest"
)

// An extension object is sealed to its Volume, its extension and its key: copied to another
// Volume, another extension or another key it does not open.
func TestExtensionObjectIsSealedToItsVolumeNameAndKey(t *testing.T) {
	ctx := context.Background()
	k, _ := age.GenerateHybridIdentity()
	c := Hybrid(k)
	vol := func() (*storetest.Mem, *Volume) {
		be := storetest.NewMem()
		if err := Create(ctx, Config{Backend: be, Cipher: c}); err != nil {
			t.Fatal(err)
		}
		v, err := Attach(ctx, Config{Backend: be, Cipher: c, Round: 1})
		if err != nil {
			t.Fatal(err)
		}
		return be, v
	}
	beA, a := vol()
	beB, b := vol()
	if err := a.PutExt(ctx, "one", []byte("k1"), []byte("secret")); err != nil {
		t.Fatal(err)
	}
	src := extObjectKey("one", extRest([]byte("k1")))
	sealed := beA.Dump()[src]
	for name, dst := range map[string]struct {
		be   *storetest.Mem
		v    *Volume
		ext  string
		key  string
		desc string
	}{
		"another Volume":    {beB, b, "one", "k1", "same name and key"},
		"another extension": {beA, a, "two", "k1", "same Volume and key"},
		"another key":       {beA, a, "one", "k2", "same Volume and name"},
	} {
		if err := dst.be.Put(ctx, extObjectKey(dst.ext, extRest([]byte(dst.key))), sealed); err != nil {
			t.Fatal(err)
		}
		if _, err := dst.v.GetExt(ctx, dst.ext, []byte(dst.key)); !errors.Is(err, ErrCorrupt) {
			t.Errorf("an object copied to %s (%s) opened: %v", name, dst.desc, err)
		}
	}
}
