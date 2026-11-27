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
	"container/list"
	"sync"
	"sync/atomic"
)

type ramEntry struct {
	key     string
	keyHash uint64
	data    []byte // immutable once stored in ramEntry
	size    int64
	dirty   bool
	ref     atomic.Bool
	elem    *list.Element
}

type evictedEntry struct {
	key   string
	data  []byte
	dirty bool
}

// ramPool is a concurrency-safe CLOCK-LRU RAM tier supporting parallel RLock
// reads and O(1) amortized eviction/instant-drop.
type ramPool struct {
	mu       sync.RWMutex
	budget   int64
	used     int64
	peak     int64
	entries  map[string]*ramEntry
	order    *list.List // front = newest, back = eviction candidate
	demoting map[string][]byte
}

func newRAMPool(budget int64) *ramPool {
	return &ramPool{
		budget:   budget,
		entries:  make(map[string]*ramEntry),
		order:    list.New(),
		demoting: make(map[string][]byte),
	}
}

// Get returns a defensive copy of key's bytes if present in RAM (or currently
// being demoted to NVMe), using only an RLock so concurrent agent reads scale
// without write-lock contention.
func (p *ramPool) Get(key string) ([]byte, bool) {
	p.mu.RLock()
	if e, ok := p.entries[key]; ok {
		e.ref.Store(true)
		out := make([]byte, len(e.data))
		copy(out, e.data)
		p.mu.RUnlock()
		return out, true
	}
	if b, ok := p.demoting[key]; ok {
		out := make([]byte, len(b))
		copy(out, b)
		p.mu.RUnlock()
		return out, true
	}
	p.mu.RUnlock()
	return nil, false
}

// GetMeta returns current occupancy and the eviction candidate's keyHash.
func (p *ramPool) peekVictimLocked() (*ramEntry, bool) {
	if p.order.Len() == 0 {
		return nil, false
	}
	// Advance CLOCK hand at most once around the tail to find a cold candidate.
	steps := p.order.Len()
	for i := 0; i < steps; i++ {
		back := p.order.Back()
		if back == nil {
			return nil, false
		}
		e := back.Value.(*ramEntry)
		if e.ref.Swap(false) {
			p.order.MoveToFront(back)
			continue
		}
		return e, true
	}
	back := p.order.Back()
	if back == nil {
		return nil, false
	}
	return back.Value.(*ramEntry), true
}

// PutExclusive inserts key into RAM under exclusive-tier semantics, evicting
// cold entries as needed and placing them into the demoting map until their
// NVMe demotion writes finish.
func (p *ramPool) PutExclusive(key string, keyHash uint64, data []byte, dirty bool) []evictedEntry {
	cp := make([]byte, len(data))
	copy(cp, data)
	sz := int64(len(cp))

	p.mu.Lock()
	defer p.mu.Unlock()

	if existing, ok := p.entries[key]; ok {
		p.used -= existing.size
		existing.data = cp
		existing.size = sz
		if dirty {
			existing.dirty = true
		}
		existing.ref.Store(true)
		p.used += sz
		if p.used > p.peak {
			p.peak = p.used
		}
		p.order.MoveToFront(existing.elem)
		return nil
	}

	if p.budget > 0 && sz > p.budget {
		// Single block exceeds total RAM budget; cannot fit.
		return nil
	}

	var evicted []evictedEntry
	for p.budget > 0 && p.used+sz > p.budget {
		victim, ok := p.peekVictimLocked()
		if !ok {
			break
		}
		p.removeLocked(victim)
		p.demoting[victim.key] = victim.data
		evicted = append(evicted, evictedEntry{
			key:   victim.key,
			data:  victim.data,
			dirty: victim.dirty,
		})
	}

	e := &ramEntry{
		key:     key,
		keyHash: keyHash,
		data:    cp,
		size:    sz,
		dirty:   dirty,
	}
	e.ref.Store(true)
	e.elem = p.order.PushFront(e)
	p.entries[key] = e
	p.used += sz
	if p.used > p.peak {
		p.peak = p.used
	}
	return evicted
}

// FinishDemotion removes key from the in-flight demoting visibility map once
// its NVMe write has completed.
func (p *ramPool) FinishDemotion(key string) {
	p.mu.Lock()
	delete(p.demoting, key)
	p.mu.Unlock()
}

// TryAdmitMirror attempts to selectively mirror key into the shared read-only
// RAM mirror using MOST's HotnessTracker admission gate. Any evicted victim is
// instantly dropped in O(1) with zero NVMe writeback IOPS because NVMe already
// retains the canonical copy.
func (p *ramPool) TryAdmitMirror(key string, keyHash uint64, data []byte, candReads uint32, readRatio float64, tracker *HotnessTracker) (admitted bool, dropped int) {
	sz := int64(len(data))
	if p.budget <= 0 || sz > p.budget {
		return false, 0
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if existing, ok := p.entries[key]; ok {
		existing.ref.Store(true)
		return false, 0
	}

	full := p.used+sz > p.budget
	fillRatio := float64(p.used) / float64(p.budget)
	var victimHash uint64
	if full {
		if v, ok := p.peekVictimLocked(); ok {
			victimHash = v.keyHash
		}
	}

	if !tracker.ShouldAdmit(candReads, readRatio, victimHash, full, fillRatio) {
		return false, 0
	}

	for p.used+sz > p.budget {
		victim, ok := p.peekVictimLocked()
		if !ok {
			return false, dropped
		}
		// Clean O(1) instant drop — NVMe holds the canonical copy!
		p.removeLocked(victim)
		dropped++
	}

	cp := make([]byte, len(data))
	copy(cp, data)
	e := &ramEntry{
		key:     key,
		keyHash: keyHash,
		data:    cp,
		size:    sz,
	}
	e.ref.Store(true)
	e.elem = p.order.PushFront(e)
	p.entries[key] = e
	p.used += sz
	if p.used > p.peak {
		p.peak = p.used
	}
	return true, dropped
}

// PinUnbounded inserts key into RAM without eviction (used by naive_full_ram_pin).
// If enforceCap is true and inserting would exceed p.budget, returns ErrRAMExceeded.
func (p *ramPool) PinUnbounded(key string, keyHash uint64, data []byte, enforceCap bool) error {
	cp := make([]byte, len(data))
	copy(cp, data)
	sz := int64(len(cp))

	p.mu.Lock()
	defer p.mu.Unlock()

	if existing, ok := p.entries[key]; ok {
		existing.ref.Store(true)
		return nil
	}
	if enforceCap && p.budget > 0 && p.used+sz > p.budget {
		return ErrRAMExceeded
	}
	e := &ramEntry{
		key:     key,
		keyHash: keyHash,
		data:    cp,
		size:    sz,
	}
	e.ref.Store(true)
	e.elem = p.order.PushFront(e)
	p.entries[key] = e
	p.used += sz
	if p.used > p.peak {
		p.peak = p.used
	}
	return nil
}

// Delete removes key from RAM if present (used on mirror invalidation).
func (p *ramPool) Delete(key string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	e, ok := p.entries[key]
	if !ok {
		return false
	}
	p.removeLocked(e)
	return true
}

// Contains reports whether key is currently in RAM (for tests/diagnostics).
func (p *ramPool) Contains(key string) bool {
	p.mu.RLock()
	_, ok := p.entries[key]
	p.mu.RUnlock()
	return ok
}

func (p *ramPool) removeLocked(e *ramEntry) {
	delete(p.entries, e.key)
	if e.elem != nil {
		p.order.Remove(e.elem)
		e.elem = nil
	}
	p.used -= e.size
}

func (p *ramPool) Stats() (used, peak int64) {
	p.mu.RLock()
	used = p.used
	peak = p.peak
	p.mu.RUnlock()
	return used, peak
}
