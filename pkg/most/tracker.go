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
	"math"
	"math/bits"
	"sync"
)

const (
	defaultTrackerWidth         = 2048
	defaultTrackerDepth         = 4
	defaultTrackerDecayInterval = 8192
	defaultTrackerMinHits       = 2
	defaultMinReadDominance     = 0.85
)

// HotnessTracker implements a bounded-memory Count-Min Sketch with periodic
// counter aging, read/write dominance tracking, and an adaptive admission gate.
// It decides whether a shared toolchain block is hot and read-dominant enough
// to enter the selective RAM mirror without polluting the hot set during scans.
type HotnessTracker struct {
	mu sync.Mutex

	width            int
	mask             uint64
	depth            int
	decayInterval    uint64
	minHits          uint32
	minReadDominance float64

	reads      []uint16 // depth * width
	writes     []uint16 // depth * width
	seeds      []uint64
	ops        uint64
	ewmaVictim float64
}

// NewHotnessTracker constructs a HotnessTracker with bounded memory footprint.
func NewHotnessTracker(cfg TrackerConfig) *HotnessTracker {
	w := cfg.Width
	if w <= 0 {
		w = defaultTrackerWidth
	}
	w = nextPow2(w)
	d := cfg.Depth
	if d <= 0 {
		d = defaultTrackerDepth
	}
	decay := cfg.DecayInterval
	if decay == 0 {
		decay = defaultTrackerDecayInterval
	}
	minHits := cfg.MinHits
	if minHits == 0 {
		minHits = defaultTrackerMinHits
	}
	minDom := cfg.MinReadDominance
	if minDom <= 0 || minDom > 1 {
		minDom = defaultMinReadDominance
	}

	seeds := make([]uint64, d)
	for i := 0; i < d; i++ {
		// Distinct odd SplitMix64 mixing constants per row.
		seeds[i] = splitMix64(0x9e3779b97f4a7c15 ^ uint64(i+1)*0xbf58476d1ce4e5b9)
	}

	return &HotnessTracker{
		width:            w,
		mask:             uint64(w - 1),
		depth:            d,
		decayInterval:    decay,
		minHits:          minHits,
		minReadDominance: minDom,
		reads:            make([]uint16, d*w),
		writes:           make([]uint16, d*w),
		seeds:            seeds,
		ewmaVictim:       float64(minHits),
	}
}

// MemoryBytes returns the exact heap byte footprint of the tracker's sketches
// and metadata so benchmark memory accounting includes tracking overhead.
func (t *HotnessTracker) MemoryBytes() int64 {
	// reads ([]uint16) + writes ([]uint16) + seeds ([]uint64) + fixed struct overhead.
	return int64(len(t.reads)*2 + len(t.writes)*2 + len(t.seeds)*8 + 96)
}

// HashKey computes a fast deterministic 64-bit hash of a block key for indexing
// the Count-Min Sketch.
func HashKey(key string) uint64 {
	var h uint64 = 14695981039346656037
	for i := 0; i < len(key); i++ {
		h ^= uint64(key[i])
		h *= 1099511628211
	}
	return splitMix64(h)
}

// ObserveRead increments the read sketch for keyHash and returns the estimated
// decayed read frequency and read-dominance ratio.
func (t *HotnessTracker) ObserveRead(keyHash uint64) (estReads uint32, readRatio float64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.maybeDecayLocked()
	rMin := t.incAndEstimateLocked(t.reads, keyHash)
	wMin := t.estimateLocked(t.writes, keyHash)
	total := uint32(rMin) + uint32(wMin)
	if total == 0 {
		return uint32(rMin), 1.0
	}
	return uint32(rMin), float64(rMin) / float64(total)
}

// ObserveWrite increments the write sketch for keyHash.
func (t *HotnessTracker) ObserveWrite(keyHash uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.maybeDecayLocked()
	t.incAndEstimateLocked(t.writes, keyHash)
}

// Estimate returns the current decayed read count and read-dominance ratio for
// keyHash without mutating counters.
func (t *HotnessTracker) Estimate(keyHash uint64) (estReads uint32, readRatio float64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	rMin := t.estimateLocked(t.reads, keyHash)
	wMin := t.estimateLocked(t.writes, keyHash)
	total := uint32(rMin) + uint32(wMin)
	if total == 0 {
		return 0, 1.0
	}
	return uint32(rMin), float64(rMin) / float64(total)
}

// ShouldAdmit applies MOST's adaptive hotness + read-dominance gate to decide
// whether a candidate block should be mirrored into RAM.
//
// When the RAM mirror is not yet full (mirrorFillRatio < 0.75), admission
// requires only estReads >= minHits and readRatio >= minReadDominance.
// Under eviction pressure, the candidate must strictly outscore the LRU victim
// (or tie/exceed the adaptive EWMA victim threshold) so uniform cold scans
// cannot thrash established hot toolchain blocks out of RAM.
func (t *HotnessTracker) ShouldAdmit(candReads uint32, readRatio float64, victimKeyHash uint64, mirrorFull bool, fillRatio float64) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	if readRatio < t.minReadDominance {
		return false
	}
	if candReads < t.minHits {
		return false
	}
	if !mirrorFull && fillRatio < 0.75 {
		return true
	}

	victimReads := uint32(0)
	if mirrorFull {
		victimReads = uint32(t.estimateLocked(t.reads, victimKeyHash))
		// Smooth EWMA of victim hotness (alpha = 0.125).
		t.ewmaVictim = 0.875*t.ewmaVictim + 0.125*float64(victimReads)
		// Candidate must be strictly hotter than the victim to justify instant drop.
		return candReads > victimReads
	}

	// Between 75% and 100% occupancy: admit if at least as hot as adaptive floor.
	dynFloor := uint32(math.Ceil(t.ewmaVictim))
	if dynFloor < t.minHits {
		dynFloor = t.minHits
	}
	return candReads >= dynFloor
}

// DynamicThreshold returns the current adaptive admission floor (for diagnostics).
func (t *HotnessTracker) DynamicThreshold() float64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.ewmaVictim < float64(t.minHits) {
		return float64(t.minHits)
	}
	return t.ewmaVictim
}

func (t *HotnessTracker) maybeDecayLocked() {
	t.ops++
	if t.ops%t.decayInterval == 0 {
		for i := range t.reads {
			t.reads[i] >>= 1
		}
		for i := range t.writes {
			t.writes[i] >>= 1
		}
		t.ewmaVictim *= 0.5
		if t.ewmaVictim < float64(t.minHits) {
			t.ewmaVictim = float64(t.minHits)
		}
	}
}

func (t *HotnessTracker) incAndEstimateLocked(table []uint16, keyHash uint64) uint16 {
	var minVal uint16 = math.MaxUint16
	for row := 0; row < t.depth; row++ {
		idx := row*t.width + int(splitMix64(keyHash^t.seeds[row])&t.mask)
		v := table[idx]
		if v < math.MaxUint16 {
			v++
			table[idx] = v
		}
		if v < minVal {
			minVal = v
		}
	}
	return minVal
}

func (t *HotnessTracker) estimateLocked(table []uint16, keyHash uint64) uint16 {
	var minVal uint16 = math.MaxUint16
	for row := 0; row < t.depth; row++ {
		idx := row*t.width + int(splitMix64(keyHash^t.seeds[row])&t.mask)
		if v := table[idx]; v < minVal {
			minVal = v
		}
	}
	return minVal
}

func nextPow2(n int) int {
	if n <= 1 {
		return 1
	}
	return 1 << bits.Len(uint(n-1))
}

func splitMix64(x uint64) uint64 {
	x += 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}
