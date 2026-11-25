package most

import "math/bits"

const (
	sketchRows = 4
	maxNibble  = 15
)

// freqSketch is a compact 4-bit Count-Min Sketch with periodic halving
// (TinyLFU-style frequency estimator). It tracks recent access hotness in O(1)
// with zero per-operation heap allocations so MOST selective mirroring admits
// genuinely hot toolchain blocks while filtering cold and one-hit scan blocks.
//
// Callers synchronize access under their shard mutex.
type freqSketch struct {
	rows       [sketchRows][]uint64
	mask       uint64
	samples    uint64
	resetAfter uint64
}

var sketchSeeds = [sketchRows]uint64{
	0xc3a5c85c97cb3127,
	0xb492b66fbe98f273,
	0x9ae16a3b2f90404f,
	0x94d049bb133111eb,
}

// newFreqSketch allocates a 4-row 4-bit sketch sized for capacity items.
func newFreqSketch(capacity int) *freqSketch {
	if capacity < 64 {
		capacity = 64
	}
	// Round width up to power of two counters (16 4-bit counters per uint64).
	width := uint64(1) << bits.Len(uint(capacity-1))
	if width < 64 {
		width = 64
	}
	words := width / 16
	s := &freqSketch{
		mask:       width - 1,
		resetAfter: width * 10,
	}
	for r := 0; r < sketchRows; r++ {
		s.rows[r] = make([]uint64, words)
	}
	return s
}

func mix64(h, seed uint64) uint64 {
	x := h ^ seed
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	return x ^ (x >> 31)
}

// Increment increments the 4-bit counters for h and returns the minimum
// estimated frequency across the 4 rows (in [1, 15]).
func (s *freqSketch) Increment(h uint64) uint8 {
	minVal := uint8(maxNibble)
	for r := 0; r < sketchRows; r++ {
		idx := mix64(h, sketchSeeds[r]) & s.mask
		word := idx >> 4
		shift := (idx & 15) * 4
		v := uint8((s.rows[r][word] >> shift) & 0x0f)
		if v < maxNibble {
			v++
			s.rows[r][word] = (s.rows[r][word] &^ (uint64(0x0f) << shift)) | (uint64(v) << shift)
		}
		if v < minVal {
			minVal = v
		}
	}
	s.samples++
	if s.samples >= s.resetAfter {
		s.halve()
	}
	return minVal
}

// Estimate returns the current minimum estimated frequency across all 4 rows.
func (s *freqSketch) Estimate(h uint64) uint8 {
	minVal := uint8(maxNibble)
	for r := 0; r < sketchRows; r++ {
		idx := mix64(h, sketchSeeds[r]) & s.mask
		word := idx >> 4
		shift := (idx & 15) * 4
		v := uint8((s.rows[r][word] >> shift) & 0x0f)
		if v < minVal {
			minVal = v
		}
	}
	return minVal
}

// halve ages all 4-bit counters by right-shifting each nibble by 1 bit.
func (s *freqSketch) halve() {
	const mask = 0x7777777777777777
	for r := 0; r < sketchRows; r++ {
		row := s.rows[r]
		for i := range row {
			row[i] = (row[i] >> 1) & mask
		}
	}
	s.samples >>= 1
}
