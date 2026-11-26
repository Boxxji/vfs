// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package aiturbo

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hanzoai/vfs/pkg/backend"
)

const bytesPerGiB = 1024.0 * 1024.0 * 1024.0

// MemBackend is a concurrency-safe in-memory implementation of backend.Backend
// honoring S3 ETag conditional writes (PutIfAbsent, PutIfVersion), exact range
// bounds checking, and backend.ValidKey enforcement.
type MemBackend struct {
	mu     sync.RWMutex
	objs   map[string][]byte
	mod    map[string]time.Time
	closed bool
}

// NewMemoryBackend returns an empty MemBackend ready for concurrent use.
func NewMemoryBackend() *MemBackend {
	return &MemBackend{
		objs: make(map[string][]byte),
		mod:  make(map[string]time.Time),
	}
}

func memETag(data []byte) backend.Version {
	sum := md5.Sum(data)
	return backend.Version(hex.EncodeToString(sum[:]))
}

func (m *MemBackend) Get(ctx context.Context, key string, maxBytes int64) ([]byte, backend.Version, error) {
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	if err := backend.ValidKey(key); err != nil {
		return nil, "", err
	}
	m.mu.RLock()
	b, ok := m.objs[key]
	if !ok {
		m.mu.RUnlock()
		return nil, "", fmt.Errorf("%w: %s", backend.ErrNotFound, key)
	}
	if int64(len(b)) > maxBytes {
		m.mu.RUnlock()
		return nil, "", fmt.Errorf("%w: %s (%d > %d)", backend.ErrTooLarge, key, len(b), maxBytes)
	}
	out := make([]byte, len(b))
	copy(out, b)
	m.mu.RUnlock()
	return out, memETag(out), nil
}

func (m *MemBackend) GetRange(ctx context.Context, key string, off, n int64) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := backend.ValidKey(key); err != nil {
		return nil, err
	}
	m.mu.RLock()
	b, ok := m.objs[key]
	if !ok {
		m.mu.RUnlock()
		return nil, fmt.Errorf("%w: %s", backend.ErrNotFound, key)
	}
	if off < 0 || n < 0 || off+n > int64(len(b)) {
		m.mu.RUnlock()
		return nil, fmt.Errorf("aiturbo membackend: range [%d,%d) outside %s (len=%d)", off, off+n, key, len(b))
	}
	out := make([]byte, n)
	copy(out, b[off:off+n])
	m.mu.RUnlock()
	return out, nil
}

func (m *MemBackend) Put(ctx context.Context, key string, data []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := backend.ValidKey(key); err != nil {
		return err
	}
	cp := make([]byte, len(data))
	copy(cp, data)
	m.mu.Lock()
	m.objs[key] = cp
	m.mod[key] = time.Now().UTC()
	m.mu.Unlock()
	return nil
}

func (m *MemBackend) PutIfAbsent(ctx context.Context, key string, data []byte) (backend.Version, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if err := backend.ValidKey(key); err != nil {
		return "", err
	}
	if int64(len(data)) > MaxTargetGroupBytes {
		return "", fmt.Errorf("%w: %s (%d bytes)", backend.ErrTooLarge, key, len(data))
	}
	cp := make([]byte, len(data))
	copy(cp, data)
	m.mu.Lock()
	if _, exists := m.objs[key]; exists {
		m.mu.Unlock()
		return "", fmt.Errorf("%w: %s already exists", backend.ErrConflict, key)
	}
	m.objs[key] = cp
	m.mod[key] = time.Now().UTC()
	m.mu.Unlock()
	return memETag(cp), nil
}

func (m *MemBackend) PutIfVersion(ctx context.Context, key string, data []byte, v backend.Version) (backend.Version, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if err := backend.ValidKey(key); err != nil {
		return "", err
	}
	if int64(len(data)) > MaxTargetGroupBytes {
		return "", fmt.Errorf("%w: %s (%d bytes)", backend.ErrTooLarge, key, len(data))
	}
	cp := make([]byte, len(data))
	copy(cp, data)
	m.mu.Lock()
	cur, exists := m.objs[key]
	if !exists || memETag(cur) != v {
		m.mu.Unlock()
		return "", fmt.Errorf("%w: %s version mismatch", backend.ErrConflict, key)
	}
	m.objs[key] = cp
	m.mod[key] = time.Now().UTC()
	m.mu.Unlock()
	return memETag(cp), nil
}

func (m *MemBackend) Delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := backend.ValidKey(key); err != nil {
		return err
	}
	m.mu.Lock()
	delete(m.objs, key)
	delete(m.mod, key)
	m.mu.Unlock()
	return nil
}

func (m *MemBackend) List(ctx context.Context, prefix string) (<-chan backend.Object, <-chan error) {
	out := make(chan backend.Object)
	errCh := make(chan error, 1)
	go func() {
		defer close(out)
		defer close(errCh)
		if err := backend.ValidKeyPrefix(prefix); err != nil {
			errCh <- err
			return
		}
		m.mu.RLock()
		items := make([]backend.Object, 0)
		for k, b := range m.objs {
			if strings.HasPrefix(k, prefix) {
				items = append(items, backend.Object{
					Key:      k,
					Size:     int64(len(b)),
					Modified: m.mod[k],
				})
			}
		}
		m.mu.RUnlock()
		sort.Slice(items, func(i, j int) bool { return items[i].Key < items[j].Key })
		for _, item := range items {
			select {
			case <-ctx.Done():
				errCh <- ctx.Err()
				return
			case out <- item:
			}
		}
	}()
	return out, errCh
}

func (m *MemBackend) Stat(ctx context.Context, key string) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if err := backend.ValidKey(key); err != nil {
		return 0, err
	}
	m.mu.RLock()
	b, ok := m.objs[key]
	m.mu.RUnlock()
	if !ok {
		return 0, fmt.Errorf("%w: %s", backend.ErrNotFound, key)
	}
	return int64(len(b)), nil
}

func (m *MemBackend) Close() error {
	m.mu.Lock()
	m.closed = true
	m.mu.Unlock()
	return nil
}

func (m *MemBackend) String() string { return "mem://aiturbo" }

// ObjectCount returns the current number of stored objects in MemBackend.
func (m *MemBackend) ObjectCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.objs)
}

// S3ModelConfig configures latency, link bandwidth, and fault/corruption
// injection on InstrumentedBackend. Zero values disable simulated delays and
// fault injection, passing calls directly through to the wrapped backend.
type S3ModelConfig struct {
	// PutRTT is the per-request S3 PUT / conditional-PUT first-byte round-trip time.
	PutRTT time.Duration `json:"put_rtt_ns"`

	// GetRTT is the per-request S3 GET / Range-GET first-byte round-trip time.
	GetRTT time.Duration `json:"get_rtt_ns"`

	// DeleteRTT is the per-request S3 DELETE round-trip time.
	DeleteRTT time.Duration `json:"delete_rtt_ns"`

	// PerStreamBytesPerSec models single HTTP/TLS connection throughput cap
	// (0 disables per-stream throttling).
	PerStreamBytesPerSec float64 `json:"per_stream_bytes_per_sec"`

	// AggregateBytesPerSec models shared NIC/bucket link bandwidth cap across
	// all concurrent requests (0 disables aggregate link pacing).
	AggregateBytesPerSec float64 `json:"aggregate_bytes_per_sec"`

	// FailOnPutCall, if > 0, injects ErrInjectedFailure on the exact N-th Put call
	// (1-indexed since construction or ResetMetrics) to test fail-closed S3 atomicity.
	FailOnPutCall int64 `json:"fail_on_put_call,omitempty"`

	// FailOnHeadPut, if true, injects ErrInjectedFailure on PutIfAbsent / PutIfVersion.
	FailOnHeadPut bool `json:"fail_on_head_put,omitempty"`

	// CorruptOnReadKey, if non-empty, flips one bit of the returned bytes whenever
	// that key is read via Get or GetRange, testing BLAKE3 corruption rejection.
	CorruptOnReadKey string `json:"corrupt_on_read_key,omitempty"`
}

// BackendMetrics captures exact request and byte accounting across all S3 calls.
type BackendMetrics struct {
	PutCalls          int64 `json:"put_calls"`
	PutIfAbsentCalls  int64 `json:"put_if_absent_calls"`
	PutIfVersionCalls int64 `json:"put_if_version_calls"`
	GetCalls          int64 `json:"get_calls"`
	GetRangeCalls     int64 `json:"get_range_calls"`
	DeleteCalls       int64 `json:"delete_calls"`
	StatCalls         int64 `json:"stat_calls"`
	ListCalls         int64 `json:"list_calls"`
	WriteErrors       int64 `json:"write_errors"`
	ReadErrors        int64 `json:"read_errors"`
	BytesUploaded     int64 `json:"bytes_uploaded"`
	BytesDownloaded   int64 `json:"bytes_downloaded"`
}

// TotalWriteRequests returns the sum of Put, PutIfAbsent, and PutIfVersion calls.
func (m BackendMetrics) TotalWriteRequests() int64 {
	return m.PutCalls + m.PutIfAbsentCalls + m.PutIfVersionCalls
}

// TotalReadRequests returns the sum of Get and GetRange calls.
func (m BackendMetrics) TotalReadRequests() int64 {
	return m.GetCalls + m.GetRangeCalls
}

// TotalRequests returns all object-store requests issued.
func (m BackendMetrics) TotalRequests() int64 {
	return m.TotalWriteRequests() + m.TotalReadRequests() + m.DeleteCalls + m.StatCalls + m.ListCalls
}

// WriteRequestsPerGiB normalizes write requests per GiB of logical checkpoint payload.
func (m BackendMetrics) WriteRequestsPerGiB(logicalBytes int64) float64 {
	if logicalBytes <= 0 {
		return 0
	}
	return float64(m.TotalWriteRequests()) * (bytesPerGiB / float64(logicalBytes))
}

// ReadRequestsPerGiB normalizes read requests per GiB of logical checkpoint payload.
func (m BackendMetrics) ReadRequestsPerGiB(logicalBytes int64) float64 {
	if logicalBytes <= 0 {
		return 0
	}
	return float64(m.TotalReadRequests()) * (bytesPerGiB / float64(logicalBytes))
}

// bandwidthPacer serializes airtime on a shared link without holding a lock
// during sleep, accurately enforcing AggregateBytesPerSec across concurrent workers.
type bandwidthPacer struct {
	mu       sync.Mutex
	nextFree time.Time
}

func (p *bandwidthPacer) reserve(bytes int64, aggBPS float64) time.Duration {
	if aggBPS <= 0 || bytes <= 0 {
		return 0
	}
	dur := time.Duration(float64(bytes) / aggBPS * float64(time.Second))
	now := time.Now()
	p.mu.Lock()
	start := now
	if p.nextFree.After(start) {
		start = p.nextFree
	}
	end := start.Add(dur)
	p.nextFree = end
	p.mu.Unlock()
	return end.Sub(now)
}

// InstrumentedBackend wraps any canonical vfs backend.Backend (s3, file, mem)
// with exact S3 request/byte counters, configurable RTT/bandwidth modeling,
// and deterministic fault/corruption injection.
type InstrumentedBackend struct {
	inner backend.Backend

	cfgMu sync.RWMutex
	cfg   S3ModelConfig

	upPacer   bandwidthPacer
	downPacer bandwidthPacer

	putCalls          atomic.Int64
	putIfAbsentCalls  atomic.Int64
	putIfVersionCalls atomic.Int64
	getCalls          atomic.Int64
	getRangeCalls     atomic.Int64
	deleteCalls       atomic.Int64
	statCalls         atomic.Int64
	listCalls         atomic.Int64
	writeErrors       atomic.Int64
	readErrors        atomic.Int64
	bytesUploaded     atomic.Int64
	bytesDownloaded   atomic.Int64
}

// NewInstrumentedBackend wraps inner with the supplied S3ModelConfig.
func NewInstrumentedBackend(inner backend.Backend, cfg S3ModelConfig) *InstrumentedBackend {
	return &InstrumentedBackend{
		inner: inner,
		cfg:   cfg,
	}
}

// SetModelConfig updates the S3ModelConfig safely at runtime.
func (b *InstrumentedBackend) SetModelConfig(cfg S3ModelConfig) {
	b.cfgMu.Lock()
	b.cfg = cfg
	b.cfgMu.Unlock()
}

func (b *InstrumentedBackend) modelConfig() S3ModelConfig {
	b.cfgMu.RLock()
	c := b.cfg
	b.cfgMu.RUnlock()
	return c
}

// ResetMetrics zeroes all request and byte counters and clears bandwidth pacer state.
func (b *InstrumentedBackend) ResetMetrics() {
	b.putCalls.Store(0)
	b.putIfAbsentCalls.Store(0)
	b.putIfVersionCalls.Store(0)
	b.getCalls.Store(0)
	b.getRangeCalls.Store(0)
	b.deleteCalls.Store(0)
	b.statCalls.Store(0)
	b.listCalls.Store(0)
	b.writeErrors.Store(0)
	b.readErrors.Store(0)
	b.bytesUploaded.Store(0)
	b.bytesDownloaded.Store(0)

	b.upPacer.mu.Lock()
	b.upPacer.nextFree = time.Time{}
	b.upPacer.mu.Unlock()
	b.downPacer.mu.Lock()
	b.downPacer.nextFree = time.Time{}
	b.downPacer.mu.Unlock()
}

// Metrics returns an atomic snapshot of all request and byte counters.
func (b *InstrumentedBackend) Metrics() BackendMetrics {
	return BackendMetrics{
		PutCalls:          b.putCalls.Load(),
		PutIfAbsentCalls:  b.putIfAbsentCalls.Load(),
		PutIfVersionCalls: b.putIfVersionCalls.Load(),
		GetCalls:          b.getCalls.Load(),
		GetRangeCalls:     b.getRangeCalls.Load(),
		DeleteCalls:       b.deleteCalls.Load(),
		StatCalls:         b.statCalls.Load(),
		ListCalls:         b.listCalls.Load(),
		WriteErrors:       b.writeErrors.Load(),
		ReadErrors:        b.readErrors.Load(),
		BytesUploaded:     b.bytesUploaded.Load(),
		BytesDownloaded:   b.bytesDownloaded.Load(),
	}
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *InstrumentedBackend) paceTransfer(ctx context.Context, rtt time.Duration, n int64, pacer *bandwidthPacer, cfg S3ModelConfig) error {
	var streamDur time.Duration
	if cfg.PerStreamBytesPerSec > 0 && n > 0 {
		streamDur = time.Duration(float64(n) / cfg.PerStreamBytesPerSec * float64(time.Second))
	}
	aggDur := pacer.reserve(n, cfg.AggregateBytesPerSec)
	xfer := streamDur
	if aggDur > xfer {
		xfer = aggDur
	}
	return sleepCtx(ctx, rtt+xfer)
}

func (b *InstrumentedBackend) Get(ctx context.Context, key string, maxBytes int64) ([]byte, backend.Version, error) {
	cfg := b.modelConfig()
	b.getCalls.Add(1)
	data, ver, err := b.inner.Get(ctx, key, maxBytes)
	if err != nil {
		b.readErrors.Add(1)
		_ = sleepCtx(ctx, cfg.GetRTT)
		return nil, "", err
	}
	if err := b.paceTransfer(ctx, cfg.GetRTT, int64(len(data)), &b.downPacer, cfg); err != nil {
		b.readErrors.Add(1)
		return nil, "", err
	}
	b.bytesDownloaded.Add(int64(len(data)))
	if cfg.CorruptOnReadKey != "" && cfg.CorruptOnReadKey == key && len(data) > 0 {
		corrupt := make([]byte, len(data))
		copy(corrupt, data)
		corrupt[len(corrupt)/2] ^= 0x01
		return corrupt, ver, nil
	}
	return data, ver, nil
}

func (b *InstrumentedBackend) GetRange(ctx context.Context, key string, off, n int64) ([]byte, error) {
	cfg := b.modelConfig()
	b.getRangeCalls.Add(1)
	data, err := b.inner.GetRange(ctx, key, off, n)
	if err != nil {
		b.readErrors.Add(1)
		_ = sleepCtx(ctx, cfg.GetRTT)
		return nil, err
	}
	if err := b.paceTransfer(ctx, cfg.GetRTT, int64(len(data)), &b.downPacer, cfg); err != nil {
		b.readErrors.Add(1)
		return nil, err
	}
	b.bytesDownloaded.Add(int64(len(data)))
	if cfg.CorruptOnReadKey != "" && cfg.CorruptOnReadKey == key && len(data) > 0 {
		corrupt := make([]byte, len(data))
		copy(corrupt, data)
		corrupt[0] ^= 0x80
		return corrupt, nil
	}
	return data, nil
}

func (b *InstrumentedBackend) Put(ctx context.Context, key string, data []byte) error {
	cfg := b.modelConfig()
	callNum := b.putCalls.Add(1)
	if cfg.FailOnPutCall > 0 && callNum == cfg.FailOnPutCall {
		b.writeErrors.Add(1)
		_ = sleepCtx(ctx, cfg.PutRTT)
		return fmt.Errorf("%w: put call #%d key %s", ErrInjectedFailure, callNum, key)
	}
	if err := b.paceTransfer(ctx, cfg.PutRTT, int64(len(data)), &b.upPacer, cfg); err != nil {
		b.writeErrors.Add(1)
		return err
	}
	if err := b.inner.Put(ctx, key, data); err != nil {
		b.writeErrors.Add(1)
		return err
	}
	b.bytesUploaded.Add(int64(len(data)))
	return nil
}

func (b *InstrumentedBackend) PutIfAbsent(ctx context.Context, key string, data []byte) (backend.Version, error) {
	cfg := b.modelConfig()
	b.putIfAbsentCalls.Add(1)
	if cfg.FailOnHeadPut {
		b.writeErrors.Add(1)
		_ = sleepCtx(ctx, cfg.PutRTT)
		return "", fmt.Errorf("%w: put-if-absent key %s", ErrInjectedFailure, key)
	}
	if err := b.paceTransfer(ctx, cfg.PutRTT, int64(len(data)), &b.upPacer, cfg); err != nil {
		b.writeErrors.Add(1)
		return "", err
	}
	ver, err := b.inner.PutIfAbsent(ctx, key, data)
	if err != nil {
		b.writeErrors.Add(1)
		return "", err
	}
	b.bytesUploaded.Add(int64(len(data)))
	return ver, nil
}

func (b *InstrumentedBackend) PutIfVersion(ctx context.Context, key string, data []byte, v backend.Version) (backend.Version, error) {
	cfg := b.modelConfig()
	b.putIfVersionCalls.Add(1)
	if cfg.FailOnHeadPut {
		b.writeErrors.Add(1)
		_ = sleepCtx(ctx, cfg.PutRTT)
		return "", fmt.Errorf("%w: put-if-version key %s", ErrInjectedFailure, key)
	}
	if err := b.paceTransfer(ctx, cfg.PutRTT, int64(len(data)), &b.upPacer, cfg); err != nil {
		b.writeErrors.Add(1)
		return "", err
	}
	ver, err := b.inner.PutIfVersion(ctx, key, data, v)
	if err != nil {
		b.writeErrors.Add(1)
		return "", err
	}
	b.bytesUploaded.Add(int64(len(data)))
	return ver, nil
}

func (b *InstrumentedBackend) Delete(ctx context.Context, key string) error {
	cfg := b.modelConfig()
	b.deleteCalls.Add(1)
	if err := sleepCtx(ctx, cfg.DeleteRTT); err != nil {
		return err
	}
	return b.inner.Delete(ctx, key)
}

func (b *InstrumentedBackend) List(ctx context.Context, prefix string) (<-chan backend.Object, <-chan error) {
	b.listCalls.Add(1)
	return b.inner.List(ctx, prefix)
}

func (b *InstrumentedBackend) Stat(ctx context.Context, key string) (int64, error) {
	cfg := b.modelConfig()
	b.statCalls.Add(1)
	if err := sleepCtx(ctx, cfg.GetRTT); err != nil {
		return 0, err
	}
	return b.inner.Stat(ctx, key)
}

func (b *InstrumentedBackend) Close() error   { return b.inner.Close() }
func (b *InstrumentedBackend) String() string { return "instrumented(" + b.inner.String() + ")" }
