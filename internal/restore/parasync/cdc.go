package parasync

import (
	"encoding/binary"
	"math/bits"

	"github.com/hanzoai/vfs/internal/blake3"
)

// ChunkingMode selects how file contents are partitioned into chunks before
// per-tenant AEAD sealing.
type ChunkingMode string

const (
	// ChunkModeFixed64K partitions files into fixed 64 KiB blocks. It is simple
	// but loses chunk alignment on mid-file byte insertions or deletions.
	ChunkModeFixed64K ChunkingMode = "fixed_64k"

	// ChunkModeFastCDC partitions files using per-tenant keyed FastCDC
	// (normalized Gear rolling hash, Xia et al., USENIX ATC '16) matching the
	// security model of hanzoai/vfs/chunk.go: each tenant and key epoch uses an
	// independent secret gear table so chunk boundaries cannot be correlated
	// across tenants.
	ChunkModeFastCDC ChunkingMode = "fastcdc"
)

// CDCConfig configures FastCDC and fixed-size chunking parameters.
type CDCConfig struct {
	MinSize   int // minimum FastCDC chunk size (default 8 KiB)
	AvgSize   int // target average FastCDC chunk size (default 32 KiB, power of 2)
	MaxSize   int // maximum FastCDC chunk size (default 128 KiB)
	FixedSize int // fixed chunk size for ChunkModeFixed64K (default 64 KiB)
}

// DefaultCDCConfig returns FastCDC parameters (8 KiB min / 32 KiB target avg /
// 128 KiB max) tuned for fast re-synchronization after mid-file byte insertions
// while comparing against the 64 KiB fixed-block sequential delta baseline.
func DefaultCDCConfig() CDCConfig {
	return CDCConfig{
		MinSize:   8 << 10,
		AvgSize:   32 << 10,
		MaxSize:   128 << 10,
		FixedSize: 64 << 10,
	}
}

func (c CDCConfig) normalized() CDCConfig {
	def := DefaultCDCConfig()
	if c.MinSize <= 0 {
		c.MinSize = def.MinSize
	}
	if c.AvgSize <= 0 {
		c.AvgSize = def.AvgSize
	}
	if c.MaxSize <= 0 {
		c.MaxSize = def.MaxSize
	}
	if c.FixedSize <= 0 {
		c.FixedSize = def.FixedSize
	}
	if c.MinSize >= c.AvgSize {
		c.MinSize = c.AvgSize / 4
		if c.MinSize < 1024 {
			c.MinSize = 1024
		}
	}
	if c.MaxSize < c.AvgSize {
		c.MaxSize = c.AvgSize * 4
	}
	return c
}

// GearTable maps each byte value to 64 pseudorandom bits derived from a
// per-tenant, per-epoch secret seed.
type GearTable [256]uint64

// NewGearTable expands seed via BLAKE3 XOF into a 256-entry Gear rolling-hash
// lookup table, following hanzoai/vfs/chunk.go's per-volume unpredictability model.
func NewGearTable(seed [32]byte) *GearTable {
	h := blake3.New()
	_, _ = h.WriteString("hanzoai/vfs/parasync/gear-table/v1")
	_, _ = h.Write(seed[:])
	var raw [256 * 8]byte
	h.Expand(raw[:])
	var g GearTable
	for i := range g {
		g[i] = binary.LittleEndian.Uint64(raw[i*8:])
	}
	return &g
}

// ChunkSpan describes a contiguous byte range [Offset, Offset+Length) inside a file.
type ChunkSpan struct {
	Offset int64
	Length int
}

// fastCDCMasks computes normalized chunking masks (strict before AvgSize, loose
// after AvgSize) using top bits of the 64-bit Gear rolling fingerprint.
func fastCDCMasks(avgSize int) (maskS, maskL uint64) {
	avgBits := uint(bits.Len(uint(avgSize)) - 1)
	if avgBits < 4 {
		avgBits = 4
	}
	if avgBits > 28 {
		avgBits = 28
	}
	strictBits := avgBits + 2
	looseBits := avgBits - 2
	maskS = (uint64(1)<<strictBits - 1) << (64 - strictBits)
	maskL = (uint64(1)<<looseBits - 1) << (64 - looseBits)
	return maskS, maskL
}

// CutFastCDC returns the length of the next content-defined chunk at the start
// of data using normalized FastCDC.
func CutFastCDC(g *GearTable, data []byte, cfg CDCConfig) int {
	cfg = cfg.normalized()
	n := len(data)
	if n > cfg.MaxSize {
		n = cfg.MaxSize
	}
	if n <= cfg.MinSize {
		return n
	}
	maskS, maskL := fastCDCMasks(cfg.AvgSize)
	mid := cfg.AvgSize
	if mid > n {
		mid = n
	}

	var fp uint64
	i := cfg.MinSize
	for ; i < mid; i++ {
		fp = (fp << 1) + g[data[i]]
		if fp&maskS == 0 {
			return i + 1
		}
	}
	for ; i < n; i++ {
		fp = (fp << 1) + g[data[i]]
		if fp&maskL == 0 {
			return i + 1
		}
	}
	return n
}

// ChunkBytes partitions data into contiguous ChunkSpans according to mode.
// Empty inputs produce an empty slice (representing a 0-byte file).
func ChunkBytes(mode ChunkingMode, g *GearTable, data []byte, cfg CDCConfig) []ChunkSpan {
	cfg = cfg.normalized()
	if len(data) == 0 {
		return nil
	}
	switch mode {
	case ChunkModeFixed64K:
		step := cfg.FixedSize
		spans := make([]ChunkSpan, 0, (len(data)+step-1)/step)
		for off := 0; off < len(data); off += step {
			end := off + step
			if end > len(data) {
				end = len(data)
			}
			spans = append(spans, ChunkSpan{Offset: int64(off), Length: end - off})
		}
		return spans
	default:
		spans := make([]ChunkSpan, 0, (len(data)/cfg.AvgSize)+2)
		off := 0
		for off < len(data) {
			n := CutFastCDC(g, data[off:], cfg)
			spans = append(spans, ChunkSpan{Offset: int64(off), Length: n})
			off += n
		}
		return spans
	}
}
