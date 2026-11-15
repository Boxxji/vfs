// Package storetest holds the test doubles and the conformance suite for
// backend.Backend: an in-memory store with S3's conditional-write semantics and
// counters, an S3 protocol server for the real S3 backend to talk to, and the
// suite every backend must pass.
package storetest

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/hanzoai/vfs/pkg/backend"
)

// Mem is an in-memory backend. Versions are the MD5 of the content, as an S3
// ETag is, and conditional writes decide and write under one lock, so exactly one
// of several racing writers wins.
type Mem struct {
	mu   sync.Mutex
	objs map[string][]byte
	puts map[string]int
}

func NewMem() *Mem { return &Mem{objs: map[string][]byte{}, puts: map[string]int{}} }

func etag(b []byte) backend.Version {
	sum := md5.Sum(b)
	return backend.Version(hex.EncodeToString(sum[:]))
}

// class is the first path segment: head, meta or pack.
func class(key string) string {
	c, _, _ := strings.Cut(key, "/")
	return c
}

// Puts returns how many objects of each class (head, meta, pack) have been
// written since the last Reset.
func (m *Mem) Puts() map[string]int {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := map[string]int{}
	for k, v := range m.puts {
		out[k] = v
	}
	return out
}

// Total returns the number of objects written since the last Reset.
func (m *Mem) Total() int {
	n := 0
	for _, v := range m.Puts() {
		n += v
	}
	return n
}

// Reset zeroes the write counters.
func (m *Mem) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.puts = map[string]int{}
}

// Count returns how many objects are stored.
func (m *Mem) Count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.objs)
}

func (m *Mem) Get(_ context.Context, key string) ([]byte, backend.Version, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.objs[key]
	if !ok {
		return nil, "", fmt.Errorf("%w: %s", backend.ErrNotFound, key)
	}
	return append([]byte(nil), b...), etag(b), nil
}

func (m *Mem) GetRange(_ context.Context, key string, off, n int64) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.objs[key]
	if !ok {
		return nil, fmt.Errorf("%w: %s", backend.ErrNotFound, key)
	}
	if off < 0 || n < 0 || off+n > int64(len(b)) {
		return nil, fmt.Errorf("storetest: range %d+%d outside %s (%d bytes)", off, n, key, len(b))
	}
	return append([]byte(nil), b[off:off+n]...), nil
}

func (m *Mem) store(key string, data []byte, cond func(cur []byte, ok bool) bool) (backend.Version, error) {
	m.mu.Lock()
	cur, ok := m.objs[key]
	if cond != nil && !cond(cur, ok) {
		m.mu.Unlock()
		return "", fmt.Errorf("%w: %s", backend.ErrConflict, key)
	}
	m.objs[key] = append([]byte(nil), data...)
	m.puts[class(key)]++
	m.mu.Unlock()
	return etag(data), nil
}

func (m *Mem) Put(_ context.Context, key string, data []byte) error {
	_, err := m.store(key, data, nil)
	return err
}

func (m *Mem) PutIfAbsent(_ context.Context, key string, data []byte) (backend.Version, error) {
	return m.store(key, data, func(_ []byte, ok bool) bool { return !ok })
}

func (m *Mem) PutIfVersion(_ context.Context, key string, data []byte, v backend.Version) (backend.Version, error) {
	return m.store(key, data, func(cur []byte, ok bool) bool { return ok && etag(cur) == v })
}

func (m *Mem) Delete(_ context.Context, key string) error {
	m.mu.Lock()
	delete(m.objs, key)
	m.mu.Unlock()
	return nil
}

func (m *Mem) List(_ context.Context, prefix string) (<-chan string, <-chan error) {
	m.mu.Lock()
	var keys []string
	for k := range m.objs {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	m.mu.Unlock()
	sort.Strings(keys)
	ch, errCh := make(chan string, len(keys)), make(chan error)
	for _, k := range keys {
		ch <- k
	}
	close(ch)
	close(errCh)
	return ch, errCh
}

func (m *Mem) Stat(_ context.Context, key string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.objs[key]
	if !ok {
		return 0, fmt.Errorf("%w: %s", backend.ErrNotFound, key)
	}
	return int64(len(b)), nil
}

// Keys returns every stored key, sorted.
func (m *Mem) Keys() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	keys := make([]string, 0, len(m.objs))
	for k := range m.objs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// Dump returns a copy of every stored object, for tests that search the bytes.
func (m *Mem) Dump() map[string][]byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string][]byte, len(m.objs))
	for k, v := range m.objs {
		out[k] = append([]byte(nil), v...)
	}
	return out
}

func (m *Mem) Close() error   { return nil }
func (m *Mem) String() string { return "mem://" }

var _ backend.Backend = (*Mem)(nil)
