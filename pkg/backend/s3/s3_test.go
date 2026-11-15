package s3

import (
	"context"
	"testing"

	"github.com/hanzoai/vfs/internal/storetest"
	"github.com/hanzoai/vfs/pkg/backend"
)

// TestS3Conforms runs the conformance suite through the real S3 backend and its
// client, over HTTP, against a server that implements S3's conditional writes.
func TestS3Conforms(t *testing.T) {
	storetest.Run(t, func(t *testing.T) backend.Backend {
		srv := storetest.NewS3(t, "disk")
		be, err := New(Config{Endpoint: srv.Endpoint(), Insecure: true, Region: "us-east-1", Bucket: "disk", Prefix: "org/vol", AccessKey: "k", SecretKey: "s"})
		if err != nil {
			t.Fatal(err)
		}
		return be
	})
}

func TestOpenURL(t *testing.T) {
	srv := storetest.NewS3(t, "disk")
	be, err := backend.Open(context.Background(), "s3://disk/org/vol?endpoint="+srv.Endpoint()+"&insecure=1&region=us-east-1")
	if err != nil {
		t.Fatal(err)
	}
	if got := be.String(); got == "" {
		t.Fatal("empty description")
	}
}

// TestGetIsOneRequest: a Get is a single GET, so the bytes and the version it
// returns come from one response and a writer changing the object cannot split
// them (a HEAD for the version followed by a conditional GET for the body would fail
// with 412 whenever the object changed between the two).
func TestGetIsOneRequest(t *testing.T) {
	srv := storetest.NewS3(t, "disk")
	be, err := New(Config{Endpoint: srv.Endpoint(), Insecure: true, Region: "us-east-1", Bucket: "disk", AccessKey: "k", SecretKey: "s"})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := be.Put(ctx, "k", []byte("body")); err != nil {
		t.Fatal(err)
	}
	before := srv.Requests()
	b, v, err := be.Get(ctx, "k")
	if err != nil || string(b) != "body" || v == "" {
		t.Fatalf("Get = %q %q %v", b, v, err)
	}
	if n := srv.Requests() - before; n != 1 {
		t.Fatalf("Get made %d requests, want 1", n)
	}
}
