package vfs_test

import (
	"bytes"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/internal/storetest"
	"github.com/hanzoai/vfs/pkg/backend"
	_ "github.com/hanzoai/vfs/pkg/backend/s3"
)

// TestBackendOpenParseErrorHidesUserinfo: a URL with a credential in its userinfo that
// fails to parse is not copied into the error.
func TestBackendOpenParseErrorHidesUserinfo(t *testing.T) {
	for _, raw := range []string{
		"s3://AKIAEXAMPLE:sEcReTsEcReT@bucket/pre\x7ffix?endpoint=x",
		"s3://AKIAEXAMPLE:sEcReTsEcReT@bucket/a b%zz",
		"s3://AKIAEXAMPLE:sEcReTsEcReT@[::1/x",
		"AKIAEXAMPLE:sEcReTsEcReT@bucket/x",
		"s3://AKIAEXAMPLE:sEcReTsEcReT@bucket/x",
	} {
		_, err := backend.Open(ctx, raw)
		if err == nil {
			t.Errorf("%q opened", raw)
			continue
		}
		if strings.Contains(err.Error(), "sEcReTsEcReT") || strings.Contains(err.Error(), "AKIAEXAMPLE") {
			t.Errorf("the error for a credentialed URL prints the credential: %v", err)
		}
	}
}

// stopsSealing is a Cipher that seals correctly until its key service is gone, then
// returns its input: the usual shape of a fallback "to keep the volume available".
type stopsSealing struct {
	inner vfs.Cipher
	down  atomic.Bool
}

func (f *stopsSealing) Seal(p []byte) ([]byte, string, error) {
	if f.down.Load() {
		return append([]byte(nil), p...), "k", nil
	}
	return f.inner.Seal(p)
}

func (f *stopsSealing) Open(s []byte, id string) ([]byte, error) {
	if id == "k" {
		return append([]byte(nil), s...), nil
	}
	return f.inner.Open(s, id)
}

// TestCipherThatStopsSealingIsCaughtAtEverySeal: the Cipher is checked when the Volume
// is configured, and a Cipher that falls back to the identity afterwards would write
// plaintext. Every Seal checks that its output does not contain its input.
func TestCipherThatStopsSealingIsCaughtAtEverySeal(t *testing.T) {
	be := storetest.NewMem()
	c := &stopsSealing{inner: vfs.Hybrid(newKey(t))}
	if err := vfs.Create(ctx, cfg(be, c, 0)); err != nil {
		t.Fatal(err)
	}
	v, err := vfs.Attach(ctx, cfg(be, c, 1))
	if err != nil {
		t.Fatal(err)
	}
	c.down.Store(true)
	write(t, v, vfs.RootInode, "s", []byte("TOP-SECRET-PLAINTEXT-MARKER-"+strings.Repeat("x", 100)))
	write(t, v, vfs.RootInode, "b", bytes.Repeat([]byte("PLAINTEXT-BIG-MARKER-"), 20000))
	if err := v.Flush(ctx); err == nil {
		t.Fatal("a Flush through a Cipher that stopped sealing succeeded")
	}
	for k, b := range be.Dump() {
		if bytes.Contains(b, []byte("TOP-SECRET-PLAINTEXT-MARKER")) || bytes.Contains(b, []byte("PLAINTEXT-BIG-MARKER")) {
			t.Fatalf("object %s holds the plaintext", k)
		}
	}
}
