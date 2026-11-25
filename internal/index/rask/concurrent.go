// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package rask

import "sync"

// ConcurrentIndex wraps any RangeIndex with a read-write mutex so multiple
// readers can execute Lookup and RangeScan concurrently while writers hold
// exclusive access.
type ConcurrentIndex struct {
	mu  sync.RWMutex
	idx RangeIndex
}

// NewConcurrentIndex wraps idx with read-write synchronization. If idx is nil,
// a new RASKIndex is created.
func NewConcurrentIndex(idx RangeIndex) *ConcurrentIndex {
	if idx == nil {
		idx = NewRASKIndex()
	}
	return &ConcurrentIndex{idx: idx}
}

func (c *ConcurrentIndex) Insert(start, end, val uint64) {
	c.mu.Lock()
	c.idx.Insert(start, end, val)
	c.mu.Unlock()
}

func (c *ConcurrentIndex) DeleteRange(start, end uint64) {
	c.mu.Lock()
	c.idx.DeleteRange(start, end)
	c.mu.Unlock()
}

func (c *ConcurrentIndex) Lookup(offset uint64) (uint64, bool) {
	c.mu.RLock()
	val, ok := c.idx.Lookup(offset)
	c.mu.RUnlock()
	return val, ok
}

func (c *ConcurrentIndex) RangeScan(start, end uint64, fn func(Extent) bool) {
	c.mu.RLock()
	c.idx.RangeScan(start, end, fn)
	c.mu.RUnlock()
}

func (c *ConcurrentIndex) Len() int {
	c.mu.RLock()
	n := c.idx.Len()
	c.mu.RUnlock()
	return n
}

func (c *ConcurrentIndex) MemoryBytes() int64 {
	c.mu.RLock()
	b := c.idx.MemoryBytes()
	c.mu.RUnlock()
	return b
}

func (c *ConcurrentIndex) PointerCount() int {
	c.mu.RLock()
	p := c.idx.PointerCount()
	c.mu.RUnlock()
	return p
}

// TenantRegistry manages per-tenant / per-inode RASKIndex instances with
// independent per-tenant read-write locking, mirroring hanzoai/vfs open-file
// extent tables across isolated tenants.
type TenantRegistry struct {
	mu      sync.RWMutex
	tenants map[uint64]*ConcurrentIndex
}

// NewTenantRegistry constructs an empty multi-tenant RASK registry.
func NewTenantRegistry() *TenantRegistry {
	return &TenantRegistry{
		tenants: make(map[uint64]*ConcurrentIndex),
	}
}

// GetOrCreate returns the isolated ConcurrentIndex for tenantID, creating a
// fresh RASKIndex if it does not exist yet.
func (t *TenantRegistry) GetOrCreate(tenantID uint64) *ConcurrentIndex {
	t.mu.RLock()
	ci := t.tenants[tenantID]
	t.mu.RUnlock()
	if ci != nil {
		return ci
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if ci = t.tenants[tenantID]; ci != nil {
		return ci
	}
	ci = NewConcurrentIndex(NewRASKIndex())
	t.tenants[tenantID] = ci
	return ci
}

// Delete removes tenantID from the registry.
func (t *TenantRegistry) Delete(tenantID uint64) {
	t.mu.Lock()
	delete(t.tenants, tenantID)
	t.mu.Unlock()
}

// TotalMemoryBytes returns the aggregate structural memory across all tenants.
func (t *TenantRegistry) TotalMemoryBytes() int64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	var total int64
	for _, ci := range t.tenants {
		total += ci.MemoryBytes()
	}
	return total
}
