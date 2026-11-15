// Package file is a filesystem-backed Backend for local dev and tests. NOT
// suitable for production: writes are atomic (temp file + rename) but not
// durable against power loss.
//
// Conditional writes are atomic: every mutation runs under one flock on the
// store directory, so a PutIfAbsent or PutIfVersion decides and writes with no
// other writer in between, across goroutines and across processes. A key's
// Version is the blake3 digest of its content, the same property an S3 ETag has.
package file

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/zeebo/blake3"

	"github.com/hanzoai/vfs/pkg/backend"
)

func init() {
	backend.Register("file", openFile)
}

type fileBackend struct {
	root string

	closeOnce sync.Once
	closed    bool
	mu        sync.Mutex
}

func openFile(_ context.Context, u *url.URL) (backend.Backend, error) {
	// file:///abs/path  → u.Path = "/abs/path"
	// file://./relpath  → u.Host = ".", u.Path = "/relpath"
	root := u.Path
	if u.Host != "" && u.Host != "localhost" {
		root = filepath.Join(u.Host, u.Path)
	}
	if root == "" {
		return nil, fmt.Errorf("file backend: missing path in %q", u.String())
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, fmt.Errorf("file backend: mkdir %q: %w", root, err)
	}
	return &fileBackend{root: root}, nil
}

func (f *fileBackend) keyPath(key string) string {
	// Disallow "..". Backend keys are simple `/`-delimited names; never user paths.
	if strings.Contains(key, "..") {
		return ""
	}
	return filepath.Join(f.root, filepath.FromSlash(key))
}

func version(data []byte) backend.Version {
	sum := blake3.Sum256(data)
	return backend.Version(hex.EncodeToString(sum[:]))
}

func (f *fileBackend) Get(_ context.Context, key string, max int64) ([]byte, backend.Version, error) {
	p := f.keyPath(key)
	if p == "" {
		return nil, "", fmt.Errorf("file backend: invalid key %q", key)
	}
	fd, err := os.Open(p)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, "", fmt.Errorf("%w: %s", backend.ErrNotFound, key)
		}
		return nil, "", err
	}
	defer fd.Close()
	b, err := io.ReadAll(io.LimitReader(fd, backend.Past(max)))
	if err != nil {
		return nil, "", err
	}
	if int64(len(b)) > max {
		return nil, "", fmt.Errorf("%w: %s is larger than %d bytes", backend.ErrTooLarge, key, max)
	}
	return b, version(b), nil
}

func (f *fileBackend) GetRange(_ context.Context, key string, off, n int64) ([]byte, error) {
	p := f.keyPath(key)
	if p == "" {
		return nil, fmt.Errorf("file backend: invalid key %q", key)
	}
	fd, err := os.Open(p)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("%w: %s", backend.ErrNotFound, key)
		}
		return nil, err
	}
	defer fd.Close()
	buf := make([]byte, n)
	if _, err := fd.ReadAt(buf, off); err != nil {
		return nil, fmt.Errorf("file backend: range %d+%d of %s: %w", off, n, key, err)
	}
	return buf, nil
}

func (f *fileBackend) Put(_ context.Context, key string, data []byte) error {
	p := f.keyPath(key)
	if p == "" {
		return fmt.Errorf("file backend: invalid key %q", key)
	}
	unlock, err := f.lock()
	if err != nil {
		return err
	}
	defer unlock()
	return replace(p, data)
}

func (f *fileBackend) PutIfAbsent(_ context.Context, key string, data []byte) (backend.Version, error) {
	p := f.keyPath(key)
	if p == "" {
		return "", fmt.Errorf("file backend: invalid key %q", key)
	}
	unlock, err := f.lock()
	if err != nil {
		return "", err
	}
	defer unlock()
	if _, err := os.Stat(p); err == nil {
		return "", fmt.Errorf("%w: %s exists", backend.ErrConflict, key)
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	if err := replace(p, data); err != nil {
		return "", err
	}
	return version(data), nil
}

func (f *fileBackend) PutIfVersion(_ context.Context, key string, data []byte, v backend.Version) (backend.Version, error) {
	p := f.keyPath(key)
	if p == "" {
		return "", fmt.Errorf("file backend: invalid key %q", key)
	}
	unlock, err := f.lock()
	if err != nil {
		return "", err
	}
	defer unlock()
	cur, err := os.ReadFile(p)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", fmt.Errorf("%w: %s is absent", backend.ErrConflict, key)
		}
		return "", err
	}
	if have := version(cur); have != v {
		return "", fmt.Errorf("%w: %s is at %s, not %s", backend.ErrConflict, key, have, v)
	}
	if err := replace(p, data); err != nil {
		return "", err
	}
	return version(data), nil
}

// replace writes data at p atomically: a temp file in the same directory, synced,
// then renamed over p, so a reader sees the old bytes or the new, never a mix.
func replace(p string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(p), ".vfs-"+filepath.Base(p)+"-*")
	if err != nil {
		return err
	}
	defer func() {
		_ = os.Remove(tmp.Name())
	}()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), p)
}

func (f *fileBackend) Delete(_ context.Context, key string) error {
	p := f.keyPath(key)
	if p == "" {
		return fmt.Errorf("file backend: invalid key %q", key)
	}
	unlock, err := f.lock()
	if err != nil {
		return err
	}
	defer unlock()
	if err := os.Remove(p); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func (f *fileBackend) List(ctx context.Context, prefix string) (<-chan string, <-chan error) {
	keys := make(chan string, 64)
	errCh := make(chan error, 1)
	go func() {
		defer close(keys)
		defer close(errCh)
		base := f.root
		walkRoot := base
		if prefix != "" {
			walkRoot = filepath.Join(base, filepath.FromSlash(prefix))
		}
		err := filepath.WalkDir(walkRoot, func(path string, d os.DirEntry, err error) error {
			if err != nil {
				if errors.Is(err, os.ErrNotExist) {
					return nil // empty prefix
				}
				return err
			}
			if d.IsDir() {
				return nil
			}
			rel, rerr := filepath.Rel(base, path)
			if rerr != nil {
				return rerr
			}
			rel = filepath.ToSlash(rel)
			if rel == ".lock" || strings.HasPrefix(filepath.Base(rel), ".vfs-") {
				return nil // store bookkeeping and in-flight temp files are not objects
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case keys <- rel:
			}
			return nil
		})
		if err != nil && !errors.Is(err, context.Canceled) {
			errCh <- err
		}
	}()
	return keys, errCh
}

func (f *fileBackend) Stat(_ context.Context, key string) (int64, error) {
	p := f.keyPath(key)
	if p == "" {
		return 0, fmt.Errorf("file backend: invalid key %q", key)
	}
	st, err := os.Stat(p)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, fmt.Errorf("%w: %s", backend.ErrNotFound, key)
		}
		return 0, err
	}
	return st.Size(), nil
}

func (f *fileBackend) Close() error {
	f.closeOnce.Do(func() { f.closed = true })
	return nil
}

func (f *fileBackend) String() string { return "file://" + f.root }

// Compile-time interface check.
var _ backend.Backend = (*fileBackend)(nil)
