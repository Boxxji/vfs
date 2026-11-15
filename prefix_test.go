package vfs_test

import (
	"strings"
	"testing"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/internal/storetest"
	"github.com/hanzoai/vfs/pkg/backend"
	"github.com/hanzoai/vfs/pkg/backend/s3"
)

func prefixed(t *testing.T, srv *storetest.Real, prefix string) backend.Backend {
	be, err := s3.New(s3.Config{Endpoint: srv.Endpoint, Insecure: true, Region: "us-east-1", Bucket: srv.Bucket,
		Prefix: prefix, AccessKey: srv.AccessKey, SecretKey: srv.SecretKey})
	if err != nil {
		t.Fatal(err)
	}
	return be
}

// TestPrefixConfinement: a Volume's Config names a prefix; no spelling of a prefix
// may reach another Volume's objects. Keys are built as prefix + "/" + key, and a
// store may treat "a//b" or "a/./b" as another spelling of "a/b", so the backend
// refuses a prefix with an empty, "." or ".." segment, a control character, or too
// many bytes, rather than trust the store to take it literally.
func TestPrefixConfinement(t *testing.T) {
	srv := storetest.RealS3(t)
	base := srv.Prefix("pfx")
	c := vfsAge(t)
	// Volume B holds a file.
	b := prefixed(t, srv, base+"/b")
	if err := vfs.Create(ctx, cfg(b, c, 0)); err != nil {
		t.Fatal(err)
	}
	bv, err := vfs.Attach(ctx, cfg(b, c, 1))
	if err != nil {
		t.Fatal(err)
	}
	write(t, bv, vfs.RootInode, "b-secret", []byte("only volume b"))
	flush(t, bv)

	for _, spelling := range []string{
		base + "/a/../b",       // dot-dot
		base + "//b",           // empty segment
		base + "/./b",          // dot
		base + "/b/../b",       // wanders and returns
		base + "/b/x/../../b",  // deeper
		base + "/b/head/../..", // reaches the parent as a prefix
		base + "/b\x00",        // NUL
		base + "/b\n",          // control character
		base + "/" + strings.Repeat("x", 600),
	} {
		t.Run(spelling, func(t *testing.T) {
			_, err := s3.New(s3.Config{Endpoint: srv.Endpoint, Insecure: true, Region: "us-east-1", Bucket: srv.Bucket,
				Prefix: spelling, AccessKey: srv.AccessKey, SecretKey: srv.SecretKey})
			if err == nil {
				t.Errorf("prefix %q was accepted", spelling)
			}
		})
	}
	// A URL is decoded before the backend sees its path, so an encoded dot-dot is a dot-dot.
	for _, u := range []string{"/a/%2e%2e/b", "/a/%2E%2E/b", "/x//b", "/a%00b"} {
		if _, err := backend.Open(ctx, "s3://"+srv.Bucket+"/"+base+u+"?endpoint="+srv.Endpoint+"&insecure=1"); err == nil {
			t.Errorf("URL path %q was accepted", base+u)
		}
	}
	// Plain spellings, with a leading or trailing slash, are the same prefix.
	for _, ok := range []string{base + "/b", "/" + base + "/b", base + "/b/"} {
		be := prefixed(t, srv, ok)
		if _, err := vfs.Open(ctx, cfg(be, c, 0)); err != nil {
			t.Errorf("prefix %q: %v", ok, err)
		}
	}
}
