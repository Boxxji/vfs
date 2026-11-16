package storetest

import (
	"context"

	"github.com/hanzoai/vfs/pkg/backend"
)

// Strict wraps a backend, refuses a key outside the grammar of a Volume's keys
// (backend.ValidKey), so a path that writes another is found by the tests that use it,
// and panics when a call arrives with a nil context, as the
// client of every real store does ("cannot create context from nil parent"). The
// directory backend and the in-memory store ignore their context, so a path that
// reaches the store on a nil one passes every test that uses them; a test that wants
// to catch it wraps its store in Strict.
type Strict struct{ backend.Backend }

func need(ctx context.Context) {
	if ctx == nil {
		panic("storetest: the store was called with a nil context")
	}
	_ = ctx.Done()
}

func (s Strict) Get(ctx context.Context, key string, max int64) ([]byte, backend.Version, error) {
	need(ctx)
	if err := backend.ValidKey(key); err != nil {
		return nil, "", err
	}
	return s.Backend.Get(ctx, key, max)
}

func (s Strict) GetRange(ctx context.Context, key string, off, n int64) ([]byte, error) {
	need(ctx)
	if err := backend.ValidKey(key); err != nil {
		return nil, err
	}
	return s.Backend.GetRange(ctx, key, off, n)
}

func (s Strict) Put(ctx context.Context, key string, data []byte) error {
	need(ctx)
	if err := backend.ValidKey(key); err != nil {
		return err
	}
	return s.Backend.Put(ctx, key, data)
}

func (s Strict) PutIfAbsent(ctx context.Context, key string, data []byte) (backend.Version, error) {
	need(ctx)
	if err := backend.ValidKey(key); err != nil {
		return "", err
	}
	return s.Backend.PutIfAbsent(ctx, key, data)
}

func (s Strict) PutIfVersion(ctx context.Context, key string, data []byte, v backend.Version) (backend.Version, error) {
	need(ctx)
	if err := backend.ValidKey(key); err != nil {
		return "", err
	}
	return s.Backend.PutIfVersion(ctx, key, data, v)
}

func (s Strict) Delete(ctx context.Context, key string) error {
	need(ctx)
	if err := backend.ValidKey(key); err != nil {
		return err
	}
	return s.Backend.Delete(ctx, key)
}

func (s Strict) List(ctx context.Context, prefix string) (<-chan backend.Object, <-chan error) {
	need(ctx)
	return s.Backend.List(ctx, prefix)
}

func (s Strict) Stat(ctx context.Context, key string) (int64, error) {
	need(ctx)
	if err := backend.ValidKey(key); err != nil {
		return 0, err
	}
	return s.Backend.Stat(ctx, key)
}
