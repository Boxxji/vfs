// Package backend defines the object-store interface that a Volume stores its
// sealed objects in. Implementations are an S3-compatible store and a `file://`
// adapter for local dev and tests. The interface is intentionally minimal: the
// Volume does its own chunking, addressing and encryption above this layer; the
// backend is a keyed byte store with ranged reads and two conditional writes.
package backend

import (
	"context"
	"errors"
	"fmt"
	"net/url"
)

var (
	// ErrNotFound is returned by Get when the requested key is absent.
	ErrNotFound = errors.New("backend: key not found")

	// ErrConflict is returned by PutIfAbsent and PutIfVersion when the
	// precondition failed: the key exists (PutIfAbsent), or its stored version
	// is not the expected one, or it is absent (PutIfVersion). Nothing was written.
	ErrConflict = errors.New("backend: conditional write lost")
)

// Version is an opaque token naming one stored state of one key: the S3 ETag,
// or for the file backend a digest of the content. Two Gets of an unchanged key
// return the same Version; a write by anyone changes it. Callers compare it for
// equality and hand it back to PutIfVersion; they never interpret it.
type Version string

// Backend is the contract every object-store adapter implements.
//
// Keys are arbitrary `/`-delimited strings; a Volume uses `head`,
// `meta/<2hex>/<hash>` and `pack/<2hex>/<id>` under the backend's prefix.
//
// Implementations MUST be safe for concurrent use. Get/Put/Delete are
// idempotent — repeating the same operation is allowed and SHOULD NOT
// surface as an error.
type Backend interface {
	// Get returns the bytes stored at key and the Version naming exactly
	// those bytes. If the key does not exist, the returned error wraps
	// ErrNotFound (errors.Is(err, ErrNotFound)).
	Get(ctx context.Context, key string) ([]byte, Version, error)

	// GetRange returns n bytes of key starting at off. A range that runs
	// past the end of the object is an error, never a short read.
	GetRange(ctx context.Context, key string, off, n int64) ([]byte, error)

	// Put writes data at key. Overwriting is allowed. Objects larger than
	// the implementation's single-request limit are uploaded in parts, and
	// an upload that fails is aborted so no incomplete parts remain.
	Put(ctx context.Context, key string, data []byte) error

	// PutIfAbsent writes data at key only if key does not exist (S3
	// If-None-Match: *), atomically with the check, and returns the new
	// Version. If key exists the error wraps ErrConflict and nothing is written.
	PutIfAbsent(ctx context.Context, key string, data []byte) (Version, error)

	// PutIfVersion writes data at key only if key exists and its current
	// Version is v (S3 If-Match), atomically with the check, and returns the
	// new Version. Otherwise the error wraps ErrConflict and nothing is
	// written. Exactly one of several concurrent PutIfVersion calls naming
	// the same v succeeds.
	PutIfVersion(ctx context.Context, key string, data []byte, v Version) (Version, error)

	// Delete removes the key. Removing a non-existent key MUST NOT error.
	Delete(ctx context.Context, key string) error

	// List returns keys with the given prefix. The returned channel is
	// closed when the listing completes; on error the channel is closed
	// after the error is sent on errCh.
	List(ctx context.Context, prefix string) (keys <-chan string, errCh <-chan error)

	// Stat returns the size in bytes for key without fetching the body.
	// ErrNotFound is returned if the key is absent.
	Stat(ctx context.Context, key string) (size int64, err error)

	// Close releases any resources (HTTP connections, file handles).
	// Safe to call multiple times.
	Close() error

	// String returns a stable, human-readable description of the
	// backend (e.g. "s3://bucket/prefix").
	String() string
}

// Open dispatches a backend URL to the right implementation. Recognised
// schemes:
//
//	file://path           — local filesystem (dev and tests)
//	s3://bucket/prefix    — any S3-compatible store (Hanzo S3 first)
//
// The opener function is registered by each backend package's init().
type Opener func(ctx context.Context, u *url.URL) (Backend, error)

var openers = map[string]Opener{}

// Register attaches an Opener for a URL scheme. Called from backend
// implementations' init().
func Register(scheme string, opener Opener) {
	if scheme == "" || opener == nil {
		panic("backend.Register: scheme and opener are required")
	}
	openers[scheme] = opener
}

// Open parses the backend URL and dispatches to the registered opener.
func Open(ctx context.Context, raw string) (Backend, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("backend.Open: parse %q: %w", raw, err)
	}
	if u.Scheme == "" {
		return nil, fmt.Errorf("backend.Open: missing scheme in %q", raw)
	}
	opener, ok := openers[u.Scheme]
	if !ok {
		return nil, fmt.Errorf("backend.Open: no opener registered for %q (have: %v)", u.Scheme, registeredSchemes())
	}
	return opener(ctx, u)
}

func registeredSchemes() []string {
	out := make([]string, 0, len(openers))
	for k := range openers {
		out = append(out, k)
	}
	return out
}
