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

// Package most implements a clean-room Mirror-Optimized Storage Tiering (MOST)
// selective hot-data mirroring engine for multi-agent shared immutable toolchains
// in hanzoai/vfs (HIP-1345).
//
// Canonical architecture stays fixed: vfs -> Hanzo S3 -> Visor.
// Package most models and implements node-local RAM (L1) + NVMe (L2) tiering
// in front of a backend.Backend remote object store (L3, Hanzo S3), comparing
// selective hot-data mirroring against ordinary exclusive cache-tier migration,
// naive full RAM pinning, and unshared per-agent private caches.
package most

import (
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/zeebo/blake3"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/pkg/backend"
)

var (
	// ErrNotFound is returned when a requested block does not exist in any tier.
	ErrNotFound = errors.New("most: block not found")

	// ErrTenantIsolation is returned on any cross-tenant access or attempt to
	// admit or read a private workspace block through the shared toolchain mirror.
	ErrTenantIsolation = errors.New("most: tenant isolation boundary violation")

	// ErrNamespaceDenied is returned when a shared toolchain block names a
	// ToolchainNamespace outside the configured allowlist.
	ErrNamespaceDenied = errors.New("most: toolchain namespace not allowlisted")

	// ErrCorruptBlock is returned when a fetched shared block fails BLAKE3-256
	// content-address verification.
	ErrCorruptBlock = errors.New("most: block failed BLAKE3-256 digest verification")

	// ErrImmutableMutation is returned when a write attempts to overwrite an
	// immutable shared toolchain block.
	ErrImmutableMutation = errors.New("most: immutable shared toolchain block cannot be mutated")

	// ErrRAMExceeded is returned when strict RAM cap enforcement rejects a pin
	// or allocation that exceeds the configured RAM budget.
	ErrRAMExceeded = errors.New("most: RAM budget exceeded")

	// ErrInvalidConfig is returned when Engine configuration parameters are invalid.
	ErrInvalidConfig = errors.New("most: invalid configuration")

	// ErrInvalidBlock is returned when a BlockID, Scope, or key fails validation.
	ErrInvalidBlock = errors.New("most: invalid block identifier")
)

// Regime selects the tiering / caching strategy executed by Engine.
type Regime string

const (
	// RegimeExclusiveMigration is ordinary exclusive LRU cache-tier migration:
	// promoting hot blocks from NVMe to RAM and demoting evicted RAM victims
	// back to NVMe (causing promotion/demotion write amplification and NVMe
	// contention under multi-agent bursts).
	RegimeExclusiveMigration Regime = "exclusive_tier_migration"

	// RegimeNaiveFullRAMPin pins every accessed toolchain block in RAM.
	// It yields minimal warm read latency but explodes memory per agent and
	// exceeds tight RAM caps.
	RegimeNaiveFullRAMPin Regime = "naive_full_ram_pin"

	// RegimePerAgentPrivateCache gives each agent its own unshared private RAM
	// LRU cache. Total RAM scales linearly O(M) with agent count M (or, if
	// capped at a fixed node total, partitions RAM to B/M per agent).
	RegimePerAgentPrivateCache Regime = "per_agent_private_cache"

	// RegimeMOSTSelectiveMirror keeps the canonical copy of every immutable
	// shared toolchain block on NVMe and selectively mirrors only top-hotness,
	// read-dominant blocks into a single deduplicated read-only RAM mirror
	// shared across all M agents. Eviction from the RAM mirror is an O(1)
	// instant drop with zero NVMe writeback IOPS.
	RegimeMOSTSelectiveMirror Regime = "most_selective_mirror"
)

// Valid reports whether r is a recognized tiering regime.
func (r Regime) Valid() bool {
	switch r {
	case RegimeExclusiveMigration,
		RegimeNaiveFullRAMPin,
		RegimePerAgentPrivateCache,
		RegimeMOSTSelectiveMirror:
		return true
	default:
		return false
	}
}

// BlockClass distinguishes immutable shared toolchain content from mutable
// tenant/agent private workspace data.
type BlockClass uint8

const (
	// ClassSharedToolchain identifies content-addressed, read-only shared
	// toolchain blocks eligible for deduplicated cross-agent RAM mirroring.
	ClassSharedToolchain BlockClass = iota + 1

	// ClassPrivateWorkspace identifies mutable per-tenant/per-volume workspace
	// blocks that are strictly excluded from the shared mirror and encrypted
	// per volume via vfs.Cipher.
	ClassPrivateWorkspace
)

// Scope identifies a tenant and volume boundary for private workspace blocks.
type Scope struct {
	TenantID string
	VolumeID string
}

// Valid reports whether Scope carries valid, backend-safe tenant and volume IDs.
func (s Scope) Valid() bool {
	return validSegment(s.TenantID) && validSegment(s.VolumeID)
}

// Equal reports whether two scopes denote the exact same tenant and volume.
func (s Scope) Equal(other Scope) bool {
	return s.TenantID == other.TenantID && s.VolumeID == other.VolumeID
}

// IsEmpty reports whether both TenantID and VolumeID are empty.
func (s Scope) IsEmpty() bool {
	return s.TenantID == "" && s.VolumeID == ""
}

// BlockID uniquely addresses either a content-addressed shared toolchain block
// or a tenant-scoped private workspace block.
type BlockID struct {
	Class     BlockClass
	Namespace string   // Required for ClassSharedToolchain (allowlisted ToolchainNamespace).
	Digest    [32]byte // Canonical BLAKE3-256 digest for ClassSharedToolchain.
	Scope     Scope    // Required for ClassPrivateWorkspace; must be empty for shared blocks.
	Key       string   // Canonical backend object key (valid under backend.ValidKey).
}

// NewSharedBlockID constructs a content-addressed BlockID for an immutable
// shared toolchain block by hashing data with BLAKE3-256.
func NewSharedBlockID(namespace string, data []byte) (BlockID, error) {
	if !validSegment(namespace) {
		return BlockID{}, fmt.Errorf("%w: invalid toolchain namespace %q", ErrInvalidBlock, namespace)
	}
	sum := blake3.Sum256(data)
	return NewSharedBlockIDWithDigest(namespace, sum)
}

// NewSharedBlockIDWithDigest constructs a content-addressed BlockID from an
// existing BLAKE3-256 digest.
func NewSharedBlockIDWithDigest(namespace string, digest [32]byte) (BlockID, error) {
	if !validSegment(namespace) {
		return BlockID{}, fmt.Errorf("%w: invalid toolchain namespace %q", ErrInvalidBlock, namespace)
	}
	hexDigest := hex.EncodeToString(digest[:])
	key := fmt.Sprintf("tc/%s/%s/%s", namespace, hexDigest[:2], hexDigest)
	if err := backend.ValidKey(key); err != nil {
		return BlockID{}, fmt.Errorf("%w: %v", ErrInvalidBlock, err)
	}
	return BlockID{
		Class:     ClassSharedToolchain,
		Namespace: namespace,
		Digest:    digest,
		Key:       key,
	}, nil
}

// NewPrivateBlockID constructs a tenant- and volume-scoped BlockID for a
// private mutable workspace block.
func NewPrivateBlockID(scope Scope, logicalKey string) (BlockID, error) {
	if !scope.Valid() {
		return BlockID{}, fmt.Errorf("%w: invalid private scope %+v", ErrInvalidBlock, scope)
	}
	if !validSegment(logicalKey) {
		return BlockID{}, fmt.Errorf("%w: invalid logical key %q", ErrInvalidBlock, logicalKey)
	}
	key := fmt.Sprintf("priv/%s/%s/%s", scope.TenantID, scope.VolumeID, logicalKey)
	if err := backend.ValidKey(key); err != nil {
		return BlockID{}, fmt.Errorf("%w: %v", ErrInvalidBlock, err)
	}
	return BlockID{
		Class: ClassPrivateWorkspace,
		Scope: scope,
		Key:   key,
	}, nil
}

// Validate checks internal structural invariants of BlockID.
func (id BlockID) Validate() error {
	if err := backend.ValidKey(id.Key); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidBlock, err)
	}
	switch id.Class {
	case ClassSharedToolchain:
		if !validSegment(id.Namespace) {
			return fmt.Errorf("%w: shared block requires valid namespace", ErrInvalidBlock)
		}
		if !id.Scope.IsEmpty() {
			return fmt.Errorf("%w: shared block must not carry private tenant scope", ErrTenantIsolation)
		}
		hexDigest := hex.EncodeToString(id.Digest[:])
		want := fmt.Sprintf("tc/%s/%s/%s", id.Namespace, hexDigest[:2], hexDigest)
		if id.Key != want {
			return fmt.Errorf("%w: shared block key does not match canonical digest path", ErrInvalidBlock)
		}
		return nil
	case ClassPrivateWorkspace:
		if !id.Scope.Valid() {
			return fmt.Errorf("%w: private block requires valid tenant/volume scope", ErrTenantIsolation)
		}
		if id.Namespace != "" {
			return fmt.Errorf("%w: private block must not carry shared toolchain namespace", ErrTenantIsolation)
		}
		prefix := fmt.Sprintf("priv/%s/%s/", id.Scope.TenantID, id.Scope.VolumeID)
		if len(id.Key) <= len(prefix) || id.Key[:len(prefix)] != prefix {
			return fmt.Errorf("%w: private block key escaped scope prefix", ErrTenantIsolation)
		}
		return nil
	default:
		return fmt.Errorf("%w: unknown block class %d", ErrInvalidBlock, id.Class)
	}
}

// validSegment checks that s is 1..64 bytes of [a-z0-9-], safe as a single
// backend key path segment without traversal or case folding.
func validSegment(s string) bool {
	if len(s) == 0 || len(s) > 64 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-') {
			return false
		}
	}
	return true
}

// NVMeConfig configures the local NVMe tier (L2) layered over a real
// backend.Backend (such as pkg/backend/file).
type NVMeConfig struct {
	// Store is the backing backend.Backend for local NVMe persistence.
	Store backend.Backend

	// Channels bounds concurrent NVMe I/O operations to model real NVMe queue
	// contention between concurrent reads and demotion/writeback writes.
	// Defaults to 4 when <= 0.
	Channels int

	// ReadLatency is an optional calibrated NVMe read service time applied
	// inside the channel semaphore (0 in fast unit tests).
	ReadLatency time.Duration

	// WriteLatency is an optional calibrated NVMe write/program service time
	// applied inside the channel semaphore (0 in fast unit tests).
	WriteLatency time.Duration
}

// TrackerConfig configures the bounded Count-Min Sketch HotnessTracker.
type TrackerConfig struct {
	// Width is the number of 16-bit counters per sketch row (power of 2).
	// Defaults to 2048.
	Width int

	// Depth is the number of independent hash rows. Defaults to 4.
	Depth int

	// DecayInterval halves all counters every DecayInterval observations to
	// adapt to shifting hot sets. Defaults to 8192.
	DecayInterval uint64

	// MinHits is the initial read count threshold required before a shared
	// block is eligible for selective RAM mirroring. Defaults to 2.
	MinHits uint32

	// MinReadDominance is the minimum reads/(reads+writes) ratio required to
	// admit or retain a block in the mirror. Defaults to 0.85.
	MinReadDominance float64
}

// Config configures a MOST Engine instance.
type Config struct {
	// Regime selects the tiering strategy to run.
	Regime Regime

	// RAMBudgetBytes is the shared RAM capacity for RegimeMOSTSelectiveMirror
	// and RegimeExclusiveMigration, and the per-agent cache budget for
	// RegimePerAgentPrivateCache (unless PerAgentTotalBudgetCap is true).
	RAMBudgetBytes int64

	// EnforceRAMCap causes RegimeNaiveFullRAMPin to fail with ErrRAMExceeded
	// if pinning a block would push RAM usage above RAMBudgetBytes. When false,
	// RegimeNaiveFullRAMPin pins unboundedly and reports PeakRAMBytes > RAMBudgetBytes.
	EnforceRAMCap bool

	// PerAgentTotalBudgetCap, when true for RegimePerAgentPrivateCache, splits
	// RAMBudgetBytes evenly across AgentCount instead of allocating
	// RAMBudgetBytes per agent.
	PerAgentTotalBudgetCap bool

	// AgentCount is the number of concurrent agents sharing the node (used for
	// per-agent RAM accounting and per-agent budget partitioning). Defaults to 1.
	AgentCount int

	// AllowedNamespaces is the explicit allowlist of immutable shared toolchain
	// namespaces permitted to enter the shared read-only RAM mirror.
	AllowedNamespaces []string

	// NVMe configures the local NVMe tier (L2).
	NVMe NVMeConfig

	// Remote is the remote Hanzo S3 tier (L3, e.g. pkg/backend/s3).
	Remote backend.Backend

	// RemoteLatency is an optional simulated RTT added on L3 fetches (0 in unit tests).
	RemoteLatency time.Duration

	// Tracker configures the MOST HotnessTracker.
	Tracker TrackerConfig

	// VolumeCiphers maps a private Scope to its per-volume vfs.Cipher (e.g.
	// vfs.Hybrid post-quantum X25519 + ML-KEM-768).
	VolumeCiphers map[Scope]vfs.Cipher

	// FetchTimeout bounds detached single-flight fetches. Defaults to 30s.
	FetchTimeout time.Duration

	// AllowMutableMirrorForStudy enables an explicit ablation mode ONLY for
	// measuring the write-heavy negative regime (dual RAM+NVMe invalidation
	// churn on mutable blocks). Production configs leave this false.
	AllowMutableMirrorForStudy bool
}

// Metrics is a point-in-time snapshot of Engine performance, memory, and
// security telemetry.
type Metrics struct {
	Regime                Regime  `json:"regime"`
	AgentCount            int     `json:"agent_count"`
	Reads                 uint64  `json:"reads"`
	Writes                uint64  `json:"writes"`
	RAMHits               uint64  `json:"ram_hits"`
	NVMeHits              uint64  `json:"nvme_hits"`
	S3Fetches             uint64  `json:"s3_fetches"`
	SingleFlightCoalesced uint64  `json:"single_flight_coalesced"`
	MirrorAdmissions      uint64  `json:"mirror_admissions"`
	MirrorRejections      uint64  `json:"mirror_rejections"`
	MirrorInstantDrops    uint64  `json:"mirror_instant_drops"`
	MirrorInvalidations   uint64  `json:"mirror_invalidations"`
	NVMeReadIOPS          uint64  `json:"nvme_read_iops"`
	NVMeReadBytes         uint64  `json:"nvme_read_bytes"`
	NVMePromotionWrites   uint64  `json:"nvme_promotion_writes"`
	NVMeDemotionWrites    uint64  `json:"nvme_demotion_writes"`
	NVMeDemotionBytes     uint64  `json:"nvme_demotion_bytes"`
	NVMeWritebackIOPS     uint64  `json:"nvme_writeback_iops"`
	NVMeWritebackBytes    uint64  `json:"nvme_writeback_bytes"`
	CurrentRAMBytes       int64   `json:"current_ram_bytes"`
	PeakRAMBytes          int64   `json:"peak_ram_bytes"`
	RAMBudgetBytes        int64   `json:"ram_budget_bytes"`
	TrackerMetaBytes      int64   `json:"tracker_meta_bytes"`
	RAMPerAgentBytes      float64 `json:"ram_per_agent_bytes"`
	RAMHitRate            float64 `json:"ram_hit_rate"`
	OverBudget            bool    `json:"over_budget"`
	CorruptionRejects     uint64  `json:"corruption_rejects"`
	IsolationRejects      uint64  `json:"isolation_rejects"`
}
