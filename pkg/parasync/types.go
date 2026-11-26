// Copyright 2026 Hanzo AI Inc.
// SPDX-License-Identifier: Apache-2.0

package parasync

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"path"
	"strings"

	"github.com/zeebo/blake3"
)

// Sentinel errors returned by the parasync planner and restore pipeline.
var (
	// ErrTenantIsolation is returned when a delta plan or restore attempts to
	// reuse chunks across different tenants or volumes.
	ErrTenantIsolation = errors.New("parasync: cross-tenant or cross-volume delta forbidden")

	// ErrIncompatibleCheckpoint is returned when source and target checkpoints
	// do not share compatible cipher suites, chunking parameters, or valid metadata.
	ErrIncompatibleCheckpoint = errors.New("parasync: incompatible checkpoints")

	// ErrCorruptChunk is returned when a reused local chunk or fetched remote
	// chunk fails BLAKE3-256 verification against the manifest.
	ErrCorruptChunk = errors.New("parasync: chunk digest mismatch")

	// ErrCorruptCheckpoint is returned when an assembled file or checkpoint
	// root digest fails BLAKE3-256 verification.
	ErrCorruptCheckpoint = errors.New("parasync: checkpoint integrity mismatch")

	// ErrInvalidPath is returned when a checkpoint manifest contains an unsafe
	// or malformed file path (absolute, traversal, NUL byte, or duplicate).
	ErrInvalidPath = errors.New("parasync: invalid manifest file path")
)

// Digest is a 32-byte BLAKE3-256 content digest.
type Digest [32]byte

// Hex returns the lowercase hexadecimal representation of the digest.
func (d Digest) Hex() string {
	return hex.EncodeToString(d[:])
}

// String implements fmt.Stringer.
func (d Digest) String() string {
	return d.Hex()
}

// MarshalText encodes the digest as lowercase hex for JSON serialization.
func (d Digest) MarshalText() ([]byte, error) {
	dst := make([]byte, hex.EncodedLen(len(d)))
	hex.Encode(dst, d[:])
	return dst, nil
}

// UnmarshalText decodes a 64-character hexadecimal digest.
func (d *Digest) UnmarshalText(text []byte) error {
	if len(text) != hex.EncodedLen(len(d)) {
		return fmt.Errorf("parasync: invalid digest length %d", len(text))
	}
	if _, err := hex.Decode(d[:], text); err != nil {
		return fmt.Errorf("parasync: invalid digest hex: %w", err)
	}
	return nil
}

// Cipher matches vfs.Cipher so any Volume cipher (AEAD / post-quantum Hybrid)
// can seal and open remote pack chunks without introducing a package cycle.
type Cipher interface {
	Seal(plaintext []byte) (sealed []byte, keyID string, err error)
	Open(sealed []byte, keyID string) (plaintext []byte, err error)
}

// ChunkingMode selects between content-defined chunking (FastCDC) and
// fixed-size block chunking.
type ChunkingMode string

const (
	// ChunkingFastCDC uses a gear-hash rolling fingerprint with normalized
	// chunking (Xia et al., USENIX ATC 2016) to preserve deduplication across
	// mid-file insertions and deletions.
	ChunkingFastCDC ChunkingMode = "fastcdc"

	// ChunkingFixed partitions files into uniform fixed-size blocks.
	ChunkingFixed ChunkingMode = "fixed"
)

// Default FastCDC and fixed-block parameters tuned for workspace and volume
// checkpoint delta restore.
const (
	DefaultMinChunkSize   = 4 << 10  // 4 KiB
	DefaultAvgChunkSize   = 16 << 10 // 16 KiB
	DefaultMaxChunkSize   = 64 << 10 // 64 KiB
	DefaultFixedChunkSize = 16 << 10 // 16 KiB
)

// ChunkConfig fully specifies how checkpoint files are partitioned into chunks.
// Two checkpoints are delta-compatible only if their ChunkConfig values match
// identically (including the per-volume GearSeed for FastCDC).
type ChunkConfig struct {
	Mode      ChunkingMode `json:"mode"`
	MinSize   int          `json:"min_size,omitempty"`
	AvgSize   int          `json:"avg_size,omitempty"`
	MaxSize   int          `json:"max_size,omitempty"`
	FixedSize int          `json:"fixed_size,omitempty"`
	GearSeed  [32]byte     `json:"gear_seed,omitempty"`
}

// DefaultFastCDCConfig returns the standard normalized FastCDC configuration
// (4 KiB min / 16 KiB avg / 64 KiB max) with the provided per-volume gear seed.
func DefaultFastCDCConfig(seed [32]byte) ChunkConfig {
	return ChunkConfig{
		Mode:     ChunkingFastCDC,
		MinSize:  DefaultMinChunkSize,
		AvgSize:  DefaultAvgChunkSize,
		MaxSize:  DefaultMaxChunkSize,
		GearSeed: seed,
	}
}

// DefaultFixedConfig returns the standard 16 KiB fixed-block configuration.
func DefaultFixedConfig() ChunkConfig {
	return ChunkConfig{
		Mode:      ChunkingFixed,
		FixedSize: DefaultFixedChunkSize,
	}
}

// Validate checks that the chunking configuration is well-formed.
func (c ChunkConfig) Validate() error {
	switch c.Mode {
	case ChunkingFixed:
		if c.FixedSize <= 0 {
			return fmt.Errorf("%w: fixed chunk size must be > 0", ErrIncompatibleCheckpoint)
		}
		return nil
	case ChunkingFastCDC:
		if c.MinSize <= 0 || c.AvgSize <= 0 || c.MaxSize <= 0 {
			return fmt.Errorf("%w: fastcdc chunk sizes must be > 0", ErrIncompatibleCheckpoint)
		}
		if c.MinSize >= c.AvgSize || c.AvgSize >= c.MaxSize {
			return fmt.Errorf("%w: fastcdc requires MinSize < AvgSize < MaxSize (got %d/%d/%d)",
				ErrIncompatibleCheckpoint, c.MinSize, c.AvgSize, c.MaxSize)
		}
		if c.AvgSize&(c.AvgSize-1) != 0 {
			return fmt.Errorf("%w: fastcdc AvgSize must be a power of two (got %d)",
				ErrIncompatibleCheckpoint, c.AvgSize)
		}
		if c.AvgSize < 64 {
			return fmt.Errorf("%w: fastcdc AvgSize too small (%d)", ErrIncompatibleCheckpoint, c.AvgSize)
		}
		return nil
	default:
		return fmt.Errorf("%w: unknown chunking mode %q", ErrIncompatibleCheckpoint, c.Mode)
	}
}

// Equal reports whether two chunking configurations produce identical cuts.
func (c ChunkConfig) Equal(other ChunkConfig) bool {
	if c.Mode != other.Mode {
		return false
	}
	switch c.Mode {
	case ChunkingFixed:
		return c.FixedSize == other.FixedSize
	case ChunkingFastCDC:
		return c.MinSize == other.MinSize &&
			c.AvgSize == other.AvgSize &&
			c.MaxSize == other.MaxSize &&
			c.GearSeed == other.GearSeed
	default:
		return false
	}
}

// ChunkRef locates a single content-addressed chunk within both a logical file
// and a remote backend pack object.
type ChunkRef struct {
	Offset     int64  `json:"offset"`
	Length     int64  `json:"length"`
	Digest     Digest `json:"digest"`
	PackKey    string `json:"pack_key"`
	PackOffset int64  `json:"pack_offset"`
	PackLength int64  `json:"pack_length"`
}

// FileEntry describes a regular file in a checkpoint manifest.
type FileEntry struct {
	Path       string     `json:"path"`
	Size       int64      `json:"size"`
	Mode       uint32     `json:"mode"`
	FileDigest Digest     `json:"file_digest"`
	Chunks     []ChunkRef `json:"chunks"`
}

// CheckpointMeta identifies the security and storage scope of a checkpoint.
type CheckpointMeta struct {
	ID            string `json:"id"`
	TenantID      string `json:"tenant_id"`
	VolumeID      string `json:"volume_id"`
	CipherSuiteID string `json:"cipher_suite_id"`
}

// Checkpoint is an immutable manifest describing all files and chunks of a
// volume checkpoint within a single (TenantID, VolumeID) namespace.
type Checkpoint struct {
	ID            string      `json:"id"`
	TenantID      string      `json:"tenant_id"`
	VolumeID      string      `json:"volume_id"`
	CipherSuiteID string      `json:"cipher_suite_id"`
	Chunking      ChunkConfig `json:"chunking"`
	Files         []FileEntry `json:"files"`
	RootDigest    Digest      `json:"root_digest"`
}

// TotalBytes returns the total logical payload size across all files in the
// checkpoint.
func (c *Checkpoint) TotalBytes() int64 {
	var total int64
	for i := range c.Files {
		total += c.Files[i].Size
	}
	return total
}

// TotalChunks returns the total number of chunk references in the checkpoint.
func (c *Checkpoint) TotalChunks() int {
	total := 0
	for i := range c.Files {
		total += len(c.Files[i].Chunks)
	}
	return total
}

// ValidatePath verifies that a manifest file path is a safe, canonical,
// relative slash-delimited path that cannot escape the destination directory.
func ValidatePath(p string) error {
	if p == "" {
		return fmt.Errorf("%w: empty path", ErrInvalidPath)
	}
	if strings.ContainsRune(p, 0) || strings.ContainsRune(p, '\\') {
		return fmt.Errorf("%w: illegal character in %q", ErrInvalidPath, p)
	}
	if strings.HasPrefix(p, "/") || strings.HasSuffix(p, "/") {
		return fmt.Errorf("%w: leading or trailing slash in %q", ErrInvalidPath, p)
	}
	if path.Clean(p) != p {
		return fmt.Errorf("%w: non-canonical path %q", ErrInvalidPath, p)
	}
	for _, seg := range strings.Split(p, "/") {
		if seg == "" || seg == "." || seg == ".." {
			return fmt.Errorf("%w: unsafe path segment in %q", ErrInvalidPath, p)
		}
	}
	return nil
}

// ValidateStructure verifies metadata invariants, file path safety, chunk
// contiguity, and root digest integrity on a single checkpoint manifest.
func (c *Checkpoint) ValidateStructure() error {
	if c == nil {
		return fmt.Errorf("%w: nil checkpoint", ErrIncompatibleCheckpoint)
	}
	if c.TenantID == "" || c.VolumeID == "" || c.CipherSuiteID == "" {
		return fmt.Errorf("%w: tenant_id, volume_id, and cipher_suite_id are required", ErrIncompatibleCheckpoint)
	}
	if err := c.Chunking.Validate(); err != nil {
		return err
	}
	seen := make(map[string]struct{}, len(c.Files))
	for i := range c.Files {
		f := &c.Files[i]
		if err := ValidatePath(f.Path); err != nil {
			return err
		}
		if _, dup := seen[f.Path]; dup {
			return fmt.Errorf("%w: duplicate path %q", ErrInvalidPath, f.Path)
		}
		seen[f.Path] = struct{}{}
		if f.Size < 0 {
			return fmt.Errorf("%w: negative size for %q", ErrCorruptCheckpoint, f.Path)
		}
		var off int64
		for j := range f.Chunks {
			ch := &f.Chunks[j]
			if ch.Offset != off || ch.Length <= 0 || ch.PackOffset < 0 || ch.PackLength < 0 {
				return fmt.Errorf("%w: invalid chunk %d in %q (off=%d want=%d len=%d)",
					ErrCorruptCheckpoint, j, f.Path, ch.Offset, off, ch.Length)
			}
			off += ch.Length
		}
		if off != f.Size {
			return fmt.Errorf("%w: chunk coverage %d != file size %d for %q",
				ErrCorruptCheckpoint, off, f.Size, f.Path)
		}
	}
	wantRoot := ComputeRootDigest(c)
	if c.RootDigest != wantRoot {
		return fmt.Errorf("%w: root digest mismatch (got %s, want %s)",
			ErrCorruptCheckpoint, c.RootDigest.Hex(), wantRoot.Hex())
	}
	return nil
}

// ComputeRootDigest computes the domain-separated BLAKE3-256 digest committing
// to the checkpoint's security scope, chunking parameters, and complete file
// and chunk manifest.
func ComputeRootDigest(c *Checkpoint) Digest {
	h := blake3.New()
	writeLenStr(h, "hanzoai/vfs/parasync/root-v1")
	writeLenStr(h, c.TenantID)
	writeLenStr(h, c.VolumeID)
	writeLenStr(h, c.CipherSuiteID)
	writeLenStr(h, string(c.Chunking.Mode))
	writeU64(h, uint64(c.Chunking.MinSize))
	writeU64(h, uint64(c.Chunking.AvgSize))
	writeU64(h, uint64(c.Chunking.MaxSize))
	writeU64(h, uint64(c.Chunking.FixedSize))
	_, _ = h.Write(c.Chunking.GearSeed[:])
	writeU64(h, uint64(len(c.Files)))
	for i := range c.Files {
		f := &c.Files[i]
		writeLenStr(h, f.Path)
		writeU64(h, uint64(f.Size))
		writeU64(h, uint64(f.Mode))
		_, _ = h.Write(f.FileDigest[:])
		writeU64(h, uint64(len(f.Chunks)))
		for j := range f.Chunks {
			ch := &f.Chunks[j]
			writeU64(h, uint64(ch.Offset))
			writeU64(h, uint64(ch.Length))
			_, _ = h.Write(ch.Digest[:])
		}
	}
	var out Digest
	copy(out[:], h.Sum(nil))
	return out
}

func writeLenStr(h *blake3.Hasher, s string) {
	writeU64(h, uint64(len(s)))
	_, _ = h.WriteString(s)
}

func writeU64(h *blake3.Hasher, v uint64) {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], v)
	_, _ = h.Write(buf[:])
}

// SumDigest computes the BLAKE3-256 digest of data.
func SumDigest(data []byte) Digest {
	return Digest(blake3.Sum256(data))
}
