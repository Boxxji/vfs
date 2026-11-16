package storetest

import (
	"context"

	"github.com/hanzoai/vfs/pkg/backend"
)

// Hook wraps a backend to inject failures or stop a process at a chosen write.
// Before runs ahead of every mutating call (op is "put", "putabsent", "putver" or
// "delete"); an error it returns fails the call with nothing written. After runs
// once the call has been applied; an error it returns is returned by the call
// although the write landed, the shape of a lost acknowledgement.
type Hook struct {
	backend.Backend
	Before func(op, key string) error
	After  func(op, key string) error
}

func (h *Hook) before(op, key string) error {
	if h.Before != nil {
		return h.Before(op, key)
	}
	return nil
}

func (h *Hook) after(op, key string) error {
	if h.After != nil {
		return h.After(op, key)
	}
	return nil
}

func (h *Hook) Put(ctx context.Context, key string, data []byte) error {
	if err := h.before("put", key); err != nil {
		return err
	}
	if err := h.Backend.Put(ctx, key, data); err != nil {
		return err
	}
	return h.after("put", key)
}

func (h *Hook) PutIfAbsent(ctx context.Context, key string, data []byte) (backend.Version, error) {
	if err := h.before("putabsent", key); err != nil {
		return "", err
	}
	v, err := h.Backend.PutIfAbsent(ctx, key, data)
	if err != nil {
		return "", err
	}
	return v, h.after("putabsent", key)
}

func (h *Hook) PutIfVersion(ctx context.Context, key string, data []byte, v backend.Version) (backend.Version, error) {
	if err := h.before("putver", key); err != nil {
		return "", err
	}
	nv, err := h.Backend.PutIfVersion(ctx, key, data, v)
	if err != nil {
		return "", err
	}
	return nv, h.after("putver", key)
}

func (h *Hook) Delete(ctx context.Context, key string) error {
	if err := h.before("delete", key); err != nil {
		return err
	}
	if err := h.Backend.Delete(ctx, key); err != nil {
		return err
	}
	return h.after("delete", key)
}
