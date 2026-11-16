package s3

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
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
	b, v, err := be.Get(ctx, "k", backend.Unbounded)
	if err != nil || string(b) != "body" || v == "" {
		t.Fatalf("Get = %q %q %v", b, v, err)
	}
	if n := srv.Requests() - before; n != 1 {
		t.Fatalf("Get made %d requests, want 1", n)
	}
}

// TestLargeConditional: a conditional write too large for the single request that can
// carry its condition is refused, not made unconditionally, and nothing is sent.
func TestLargeConditional(t *testing.T) {
	srv := storetest.NewS3(t, "disk")
	be, err := New(Config{Endpoint: srv.Endpoint(), Insecure: true, Region: "us-east-1", Bucket: "disk", AccessKey: "k", SecretKey: "s"})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	big := make([]byte, partSize+1)
	before := srv.Requests()
	if _, err := be.PutIfAbsent(ctx, "obj", big); !errors.Is(err, backend.ErrTooLarge) {
		t.Fatalf("PutIfAbsent over the limit = %v, want ErrTooLarge", err)
	}
	if _, err := be.PutIfVersion(ctx, "obj", big, "any"); !errors.Is(err, backend.ErrTooLarge) {
		t.Fatalf("PutIfVersion over the limit = %v, want ErrTooLarge", err)
	}
	if n := srv.Requests() - before; n != 0 {
		t.Fatalf("a refused conditional write sent %d requests", n)
	}
	// At the limit it goes, conditionally.
	atLimit := make([]byte, partSize)
	if _, err := be.PutIfAbsent(ctx, "obj", atLimit); err != nil {
		t.Fatal(err)
	}
	if _, err := be.PutIfAbsent(ctx, "obj", atLimit); !errors.Is(err, backend.ErrConflict) {
		t.Fatalf("PutIfAbsent at the limit over an existing object = %v, want ErrConflict", err)
	}
}

func TestURLCredentialRefused(t *testing.T) {
	_, err := backend.Open(context.Background(), "s3://AKIAEXAMPLE:SuperSecretKey123@bucket/prefix?endpoint=127.0.0.1:1&insecure=1")
	if err == nil {
		t.Fatal("a URL with a credential in it was accepted")
	}
	if strings.Contains(err.Error(), "SuperSecretKey123") || strings.Contains(err.Error(), "AKIAEXAMPLE") {
		t.Fatalf("the refusal prints the credential: %v", err)
	}
}

func TestURLPrefixValidated(t *testing.T) {
	for _, path := range []string{"/a/../b", "/a/%2e%2e/b", "/a/%2E%2E/b", "/a//b", "/a/./b", "/a%00b", "/a%0ab"} {
		if _, err := backend.Open(context.Background(), "s3://bucket"+path+"?endpoint=127.0.0.1:1&insecure=1&region=r"); err == nil {
			t.Errorf("URL path %q was accepted", path)
		}
	}
	if _, err := backend.Open(context.Background(), "s3://bucket/org/vol/?endpoint=127.0.0.1:1&insecure=1&region=r"); err == nil {
		t.Error("a prefix with a trailing slash was cleaned instead of refused")
	}
	if _, err := backend.Open(context.Background(), "s3://bucket/org/vol?endpoint=127.0.0.1:1&insecure=1&region=r"); err != nil {
		t.Errorf("a plain prefix: %v", err)
	}
}

func TestURLStringNamesNoQuery(t *testing.T) {
	be, err := backend.Open(context.Background(), "s3://bucket/vol/one?endpoint=127.0.0.1:1&insecure=1&region=r")
	if err != nil {
		t.Fatal(err)
	}
	if got := be.String(); got != "s3://bucket/vol/one" {
		t.Fatalf("String() = %q, want s3://bucket/vol/one", got)
	}
}

// TestAbsentKeyOnAConditionalWriteIsAConflict: some stores answer a conditional write
// to an absent key with 404 NoSuchKey instead of 412. It is a lost precondition all
// the same, and nothing was written.
func TestAbsentKeyOnAConditionalWriteIsAConflict(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><Error><Code>NoSuchKey</Code><Message>no such key</Message><Resource>/x</Resource></Error>`))
	}))
	defer srv.Close()
	be, err := New(Config{Endpoint: strings.TrimPrefix(srv.URL, "http://"), Insecure: true, Region: "us-east-1", Bucket: "disk", AccessKey: "k", SecretKey: "s"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := be.PutIfVersion(context.Background(), "head", []byte("x"), "nothing"); !errors.Is(err, backend.ErrConflict) {
		t.Fatalf("PutIfVersion answered NoSuchKey = %v, want ErrConflict", err)
	}
}
