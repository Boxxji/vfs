// Copyright 2026 Hanzo AI, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package most

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/hanzoai/vfs/pkg/backend"
)

// NVMeTier models and executes local NVMe (L2) I/O over a real backend.Backend
// (typically pkg/backend/file on local disk). Concurrent operations share a
// bounded channel pool so demotion writebacks and concurrent agent reads
// contend realistically for NVMe queues.
type NVMeTier struct {
	store        backend.Backend
	sem          chan struct{}
	readLatency  time.Duration
	writeLatency time.Duration

	readIOPS       atomic.Uint64
	readBytes      atomic.Uint64
	writeIOPS      atomic.Uint64
	writeBytes     atomic.Uint64
	deleteOps      atomic.Uint64
	demotionWrites atomic.Uint64
	demotionBytes  atomic.Uint64
	promoWrites    atomic.Uint64
}

// NewNVMeTier creates an NVMeTier wrapping cfg.Store.
func NewNVMeTier(cfg NVMeConfig) (*NVMeTier, error) {
	if cfg.Store == nil {
		return nil, ErrInvalidConfig
	}
	ch := cfg.Channels
	if ch <= 0 {
		ch = 4
	}
	return &NVMeTier{
		store:        cfg.Store,
		sem:          make(chan struct{}, ch),
		readLatency:  cfg.ReadLatency,
		writeLatency: cfg.WriteLatency,
	}, nil
}

func (n *NVMeTier) acquire(ctx context.Context) error {
	select {
	case n.sem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (n *NVMeTier) release() {
	<-n.sem
}

func sleepWithContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Read fetches key from local NVMe under the bounded channel semaphore.
func (n *NVMeTier) Read(ctx context.Context, key string) ([]byte, error) {
	if err := n.acquire(ctx); err != nil {
		return nil, err
	}
	defer n.release()

	if err := sleepWithContext(ctx, n.readLatency); err != nil {
		return nil, err
	}
	data, _, err := n.store.Get(ctx, key, backend.Unbounded)
	if err != nil {
		if errors.Is(err, backend.ErrNotFound) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	n.readIOPS.Add(1)
	n.readBytes.Add(uint64(len(data)))
	return data, nil
}

// WritePromotion writes a block fetched from L3 (Hanzo S3) into local NVMe.
func (n *NVMeTier) WritePromotion(ctx context.Context, key string, data []byte) error {
	if err := n.acquire(ctx); err != nil {
		return err
	}
	defer n.release()

	if err := sleepWithContext(ctx, n.writeLatency); err != nil {
		return err
	}
	if err := n.store.Put(ctx, key, data); err != nil {
		return err
	}
	n.promoWrites.Add(1)
	return nil
}

// WriteDemotion writes an evicted RAM block back to local NVMe (used by
// exclusive_tier_migration on eviction and write-through/invalidation churn).
func (n *NVMeTier) WriteDemotion(ctx context.Context, key string, data []byte) error {
	if err := n.acquire(ctx); err != nil {
		return err
	}
	defer n.release()

	if err := sleepWithContext(ctx, n.writeLatency); err != nil {
		return err
	}
	if err := n.store.Put(ctx, key, data); err != nil {
		return err
	}
	n.writeIOPS.Add(1)
	n.writeBytes.Add(uint64(len(data)))
	n.demotionWrites.Add(1)
	n.demotionBytes.Add(uint64(len(data)))
	return nil
}

// WriteSync persists a mutable update to local NVMe and counts it as writeback I/O.
func (n *NVMeTier) WriteSync(ctx context.Context, key string, data []byte) error {
	if err := n.acquire(ctx); err != nil {
		return err
	}
	defer n.release()

	if err := sleepWithContext(ctx, n.writeLatency); err != nil {
		return err
	}
	if err := n.store.Put(ctx, key, data); err != nil {
		return err
	}
	n.writeIOPS.Add(1)
	n.writeBytes.Add(uint64(len(data)))
	return nil
}

// Delete removes key from local NVMe (used in exclusive_tier_migration after
// promoting a block exclusively to RAM so the NVMe slot is freed).
func (n *NVMeTier) Delete(ctx context.Context, key string) error {
	if err := n.acquire(ctx); err != nil {
		return err
	}
	defer n.release()

	if err := n.store.Delete(ctx, key); err != nil {
		return err
	}
	n.deleteOps.Add(1)
	return nil
}

// SeedDirectly writes data directly to the underlying NVMe store without
// charging steady-state read/writeback counters (used during fixture preload).
func (n *NVMeTier) SeedDirectly(ctx context.Context, key string, data []byte) error {
	return n.store.Put(ctx, key, data)
}

// Close closes the underlying NVMe store.
func (n *NVMeTier) Close() error {
	return n.store.Close()
}
