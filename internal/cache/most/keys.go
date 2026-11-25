// Package most implements Mirror-Optimized Storage Tiering (MOST) selective
// hot-data RAM/NVMe mirroring for immutable shared toolchains alongside
// strictly isolated per-tenant private workspace blocks.
//
// Design reference: Kaiwei Tu, Kan Wu, Andrea C. Arpaci-Dusseau, and
// Remzi H. Arpaci-Dusseau, "Getting the MOST out of your Storage Hierarchy
// with Mirror-Optimized Storage Tiering", USENIX FAST '26
// (https://www.usenix.org/conference/fast26/presentation/tu).
//
// This package is an independent clean-room Apache-2.0 Go implementation
// tailored to hanzoai/vfs cache composition (internal/bufpool and
// internal/blake3) with a hard cryptographic tenant-isolation gate:
//
//   - Only explicitly attested immutable shared toolchain blocks addressed by
//     (ToolchainID, ContentHash) and verified against their cryptographic
//     digest may enter the host-shared RAM/NVMe mirror pool.
//   - Mutable or tenant-private workspace blocks are keyed by
//     (TenantID, VolumeID, BlockID) in strictly isolated per-tenant partitions
//     and never enter the host-shared mirror tier. Cross-tenant lookups fail
//     closed with ErrTenantAccessDenied before touching any backing index.
package most

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"github.com/hanzoai/vfs/internal/blake3"
)

// Sentinel errors returned by the cache engine and isolation gate.
var (
	// ErrTenantAccessDenied is returned when a caller requests a private block
	// belonging to another tenant or with missing tenant/volume identity.
	// Checked before touching any tenant cache map or NVMe index so cross-tenant
	// probes have zero presence or timing oracle.
	ErrTenantAccessDenied = errors.New("most: tenant access denied")

	// ErrUnattestedBlock is returned when a shared toolchain key is not
	// registered in the immutable attestation manifest.
	ErrUnattestedBlock = errors.New("most: shared block not attested as immutable toolchain")

	// ErrDigestMismatch is returned when payload bytes fail cryptographic
	// digest verification against the attested ContentHash.
	ErrDigestMismatch = errors.New("most: content digest mismatch")

	// ErrBlockNotFound is returned when an authorized block is absent from
	// both RAM and NVMe tiers.
	ErrBlockNotFound = errors.New("most: block not found")

	// ErrBlockSizeMismatch is returned when a payload length does not equal
	// the configured BlockSize.
	ErrBlockSizeMismatch = errors.New("most: block size mismatch")

	// ErrInvalidConfig is returned when Config fails validation.
	ErrInvalidConfig = errors.New("most: invalid configuration")

	// ErrClosed is returned when operating on a closed Engine.
	ErrClosed = errors.New("most: engine closed")

	// ErrPrivateBlockInSharedTier is a defensive sentinel ensuring mutable or
	// private tenant data can never be registered into the shared mirror tier.
	ErrPrivateBlockInSharedTier = errors.New("most: private or mutable block barred from shared tier")
)

// HashAlgo selects the cryptographic digest used to attest immutable shared
// toolchain blocks.
type HashAlgo uint8

const (
	// AlgoBLAKE3 uses hanzoai/vfs's native BLAKE3-256 digest (default).
	AlgoBLAKE3 HashAlgo = iota
	// AlgoSHA256 uses standard FIPS 180-4 SHA-256.
	AlgoSHA256
)

// String returns the canonical algorithm name.
func (a HashAlgo) String() string {
	switch a {
	case AlgoBLAKE3:
		return "blake3"
	case AlgoSHA256:
		return "sha256"
	default:
		return fmt.Sprintf("unknown(%d)", a)
	}
}

// Digest is a 32-byte cryptographic content digest (BLAKE3 or SHA-256).
type Digest [32]byte

// ComputeDigest computes the 32-byte digest of data under algo.
func ComputeDigest(algo HashAlgo, data []byte) Digest {
	switch algo {
	case AlgoSHA256:
		return sha256.Sum256(data)
	default:
		return blake3.Sum(data)
	}
}

// VerifyDigest checks data against expected in constant time.
func VerifyDigest(algo HashAlgo, data []byte, expected Digest) bool {
	got := ComputeDigest(algo, data)
	return subtle.ConstantTimeCompare(got[:], expected[:]) == 1
}

// SharedToolchainKey uniquely addresses an immutable shared toolchain block.
// Shared host-tier entries are strictly content-addressed by (ToolchainID, ContentHash)
// and carry no tenant state.
type SharedToolchainKey struct {
	ToolchainID string
	ContentHash Digest
}

// shardHash returns a deterministic 64-bit mixing hash for sharding and
// Count-Min Sketch indexing.
func (k SharedToolchainKey) shardHash() uint64 {
	h := binary.LittleEndian.Uint64(k.ContentHash[0:8]) ^
		binary.LittleEndian.Uint64(k.ContentHash[8:16])
	for i := 0; i < len(k.ToolchainID); i++ {
		h ^= uint64(k.ToolchainID[i])
		h *= 1099511628211
	}
	return h
}

// PrivateBlockKey uniquely addresses a mutable or tenant-private workspace block.
// PrivateBlockKey is structurally disjoint from SharedToolchainKey so private
// pages can never be placed into the host-shared mirror pool.
type PrivateBlockKey struct {
	TenantID string
	VolumeID string
	BlockID  uint64
}

func (k PrivateBlockKey) sketchHash() uint64 {
	h := k.BlockID*0x9e3779b97f4a7c15 ^ 0xcbf29ce484222325
	for i := 0; i < len(k.TenantID); i++ {
		h ^= uint64(k.TenantID[i])
		h *= 1099511628211
	}
	for i := 0; i < len(k.VolumeID); i++ {
		h ^= uint64(k.VolumeID[i])
		h *= 1099511628211
	}
	return h
}

// AgentContext identifies the calling sandbox agent and its tenant boundary.
type AgentContext struct {
	AgentID  string
	TenantID string
	VolumeID string
}

// authorizePrivate enforces the hard tenant-isolation gate before any cache
// lookup or NVMe probe. Returns ErrTenantAccessDenied on mismatch or missing
// credentials so cross-tenant probes cannot distinguish hit vs miss.
func authorizePrivate(ctx AgentContext, key PrivateBlockKey) error {
	if ctx.TenantID == "" || key.TenantID == "" || ctx.VolumeID == "" || key.VolumeID == "" {
		return ErrTenantAccessDenied
	}
	if subtle.ConstantTimeCompare([]byte(ctx.TenantID), []byte(key.TenantID)) != 1 {
		return ErrTenantAccessDenied
	}
	if subtle.ConstantTimeCompare([]byte(ctx.VolumeID), []byte(key.VolumeID)) != 1 {
		return ErrTenantAccessDenied
	}
	return nil
}

// AttestedMeta records the verified digest metadata for an immutable shared
// toolchain block.
type AttestedMeta struct {
	Algo      HashAlgo
	Size      int
	Immutable bool
}

// Manifest is the immutable attestation registry for host-shared toolchain
// blocks. Only blocks attested with a verified cryptographic digest and marked
// Immutable=true are eligible for host-shared RAM/NVMe mirroring.
type Manifest struct {
	mu      sync.RWMutex
	entries map[SharedToolchainKey]AttestedMeta
}

// NewManifest creates an empty attestation manifest.
func NewManifest() *Manifest {
	return &Manifest{
		entries: make(map[SharedToolchainKey]AttestedMeta),
	}
}

// Attest verifies data against key.ContentHash under algo and registers the
// block as an immutable shared toolchain block.
func (m *Manifest) Attest(key SharedToolchainKey, data []byte, algo HashAlgo) error {
	if key.ToolchainID == "" {
		return ErrUnattestedBlock
	}
	if !VerifyDigest(algo, data, key.ContentHash) {
		return ErrDigestMismatch
	}
	m.mu.Lock()
	m.entries[key] = AttestedMeta{
		Algo:      algo,
		Size:      len(data),
		Immutable: true,
	}
	m.mu.Unlock()
	return nil
}

// Lookup returns the attestation metadata for key if registered as immutable.
func (m *Manifest) Lookup(key SharedToolchainKey) (AttestedMeta, bool) {
	m.mu.RLock()
	meta, ok := m.entries[key]
	m.mu.RUnlock()
	if !ok || !meta.Immutable {
		return AttestedMeta{}, false
	}
	return meta, true
}

// Len returns the number of attested immutable shared blocks.
func (m *Manifest) Len() int {
	m.mu.RLock()
	n := len(m.entries)
	m.mu.RUnlock()
	return n
}
