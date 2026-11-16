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
	"math"
	"net/url"
	"strings"
	"time"
)

var (
	// ErrNotFound is returned by Get when the requested key is absent.
	ErrNotFound = errors.New("backend: key not found")

	// ErrTooLarge is returned by Get when an object is larger than the bound the
	// caller named, and by PutIfAbsent and PutIfVersion when the data is larger
	// than the backend can write atomically. Nothing is written or returned.
	ErrTooLarge = errors.New("backend: object too large")

	// ErrConflict is returned by PutIfAbsent and PutIfVersion when the
	// precondition failed: the key exists (PutIfAbsent), or its stored version
	// is not the expected one, or it is absent (PutIfVersion). Nothing was written.
	ErrConflict = errors.New("backend: conditional write lost")

	// ErrKey is returned by an operation that is given a key the Backend cannot keep: a
	// directory-backed one refuses a key that would leave its directory, and keeps no two
	// keys of which one is a directory of the other. Nothing was read or written.
	ErrKey = errors.New("backend: invalid key")
)

// MaxKey is the longest key, in bytes. A prefix is at most 512 bytes and is joined to a
// key by one "/", so the whole is at most 1024, the longest key an S3 store keeps.
const MaxKey = 511

// ValidKey reports whether key is of the grammar of every key a Volume writes, which every
// Backend MUST keep exactly as given: 1 to MaxKey bytes of "a" to "z", "0" to "9" and "-",
// in segments of 1 to 255 bytes separated by "/". The alphabet is the one every store keeps
// without folding it, normalising it or giving it another meaning (a case-insensitive file
// system, a path segment of "." or ".."), and a segment is as long as a file name may be.
// The keys a Volume writes (SPECIFICATION 3) are all of this grammar and every key of one
// kind has the same number of segments, so no key is a prefix of another at a "/", which
// a store that keeps keys as files could not hold. A Backend MAY keep other keys (the
// replica package's "orgs/acme/kv.db"); nothing is promised for them.
func ValidKey(key string) error {
	if key == "" || len(key) > MaxKey {
		return fmt.Errorf("%w: %d bytes, want 1 to %d", ErrKey, len(key), MaxKey)
	}
	if err := validAlphabet(key); err != nil {
		return err
	}
	for _, seg := range strings.Split(key, "/") {
		if seg == "" || len(seg) > 255 {
			return fmt.Errorf("%w: %q has an empty segment or one over 255 bytes", ErrKey, key)
		}
	}
	return nil
}

// ValidKeyPrefix reports whether p can be the prefix of a listing: the alphabet of a key,
// ending anywhere, in the middle of a segment or at a "/", or empty.
func ValidKeyPrefix(p string) error {
	if len(p) > MaxKey {
		return fmt.Errorf("%w: a prefix of %d bytes", ErrKey, len(p))
	}
	if err := validAlphabet(p); err != nil {
		return err
	}
	if strings.Contains(p, "//") || strings.HasPrefix(p, "/") {
		return fmt.Errorf("%w: the prefix %q has an empty segment", ErrKey, p)
	}
	return nil
}

func validAlphabet(s string) error {
	for i := 0; i < len(s); i++ {
		switch c := s[i]; {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9', c == '-', c == '/':
		default:
			return fmt.Errorf("%w: byte %#x of %q is not a-z, 0-9, - or /", ErrKey, c, s)
		}
	}
	return nil
}

// Object is one entry of a listing: the key, its size, and when the store last modified
// it, by the store's own clock. A collector decides what is old by this time and never
// by a writer's.
type Object struct {
	Key      string
	Size     int64
	Modified time.Time
}

// Unbounded is the max argument of Get for a caller that accepts any size.
const Unbounded int64 = math.MaxInt64

// Past returns max+1, the number of bytes to read to learn that an object is longer
// than max, without overflowing when max is Unbounded.
func Past(max int64) int64 {
	if max == Unbounded {
		return Unbounded
	}
	return max + 1
}

// redact removes what a URL carries before its last "@", which is where a credential
// would be, so that a URL can be named in an error. It is applied to text that did not
// parse, so it does not look for the authority: whatever the scheme was spelled like,
// everything before the last "@" is dropped except a well-formed "scheme://".
func redact(raw string) string {
	at := strings.LastIndex(raw, "@")
	if at < 0 {
		return raw
	}
	head := ""
	if i := strings.Index(raw, "://"); i > 0 && isScheme(raw[:i]) {
		head = raw[:i+3]
	}
	return head + "***@" + raw[at+1:]
}

func isScheme(s string) bool {
	for i, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z':
		case i > 0 && (r >= '0' && r <= '9' || r == '+' || r == '-' || r == '.'):
		default:
			return false
		}
	}
	return s != ""
}

// ValidPrefix reports whether p can name a Volume's place in a bucket: slash
// separated segments, none empty, none "." or "..", no NUL or other control
// character, short enough to leave room for a key. A prefix is joined to keys by
// a plain slash, and a store is free to treat "a//b" or "a/./b" as another spelling
// of "a/b", so a spelling that could reach beyond the prefix is refused rather
// than cleaned.
func ValidPrefix(p string) error {
	if len(p) > 512 {
		return fmt.Errorf("backend: prefix of %d bytes, at most 512", len(p))
	}
	if p == "" {
		return nil
	}
	for _, seg := range strings.Split(p, "/") {
		switch seg {
		case "", ".", "..":
			return fmt.Errorf("backend: prefix %q has an empty, \".\" or \"..\" segment", redact(p))
		}
		for _, r := range seg {
			if r < 0x20 || r == 0x7f {
				return fmt.Errorf("backend: prefix %q has a control character", redact(p))
			}
		}
	}
	return nil
}

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
	// ErrNotFound (errors.Is(err, ErrNotFound)). An object larger than max bytes
	// is not read: the error wraps ErrTooLarge. A caller knows how large the
	// object it asks for can be, so a store that answers with more is wrong.
	Get(ctx context.Context, key string, max int64) ([]byte, Version, error)

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
	// Data too large to write atomically is refused with ErrTooLarge, never
	// written unconditionally.
	PutIfAbsent(ctx context.Context, key string, data []byte) (Version, error)

	// PutIfVersion writes data at key only if key exists and its current
	// Version is v (S3 If-Match), atomically with the check, and returns the
	// new Version. Otherwise the error wraps ErrConflict and nothing is
	// written. Exactly one of several concurrent PutIfVersion calls naming
	// the same v succeeds.
	PutIfVersion(ctx context.Context, key string, data []byte, v Version) (Version, error)

	// Delete removes the key. Removing a non-existent key MUST NOT error.
	Delete(ctx context.Context, key string) error

	// List returns the objects whose keys begin with prefix, as a string and not as a
	// directory: List("ext/y/a") returns "ext/y/ab" as well as "ext/y/a/c". It hides
	// none. The returned channel is closed when the listing completes; on error the
	// channel is closed after the error is sent on errCh.
	List(ctx context.Context, prefix string) (objects <-chan Object, errCh <-chan error)

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
//	file:///dir           — local filesystem (dev and tests); file://./dir is relative
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
		// The parser's own message repeats the URL, userinfo included.
		return nil, fmt.Errorf("backend.Open: %q is not a URL", redact(raw))
	}
	if u.Scheme == "" {
		return nil, fmt.Errorf("backend.Open: missing scheme in %q", redact(raw))
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
