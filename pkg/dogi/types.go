// Copyright 2026 Hanzo AI Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package dogi

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
)

// Sentinel errors returned by package dogi.
var (
	// ErrTenantIsolation is returned when a chunk or operation attempts to cross
	// a (TenantID, VolumeID) namespace boundary. Cross-tenant co-packing is
	// strictly forbidden.
	ErrTenantIsolation = errors.New("dogi: cross-tenant or cross-volume packing forbidden")

	// ErrInvalidNamespace is returned when TenantID or VolumeID is empty or malformed.
	ErrInvalidNamespace = errors.New("dogi: invalid tenant or volume namespace")

	// ErrInvalidConfig is returned when Packer configuration parameters are out of range.
	ErrInvalidConfig = errors.New("dogi: invalid configuration")

	// ErrInvalidChunk is returned for empty paths or oversized/nil chunk inputs.
	ErrInvalidChunk = errors.New("dogi: invalid chunk metadata or payload")

	// ErrChunkNotFound is returned when a requested path is absent from the live index.
	ErrChunkNotFound = errors.New("dogi: chunk not found")

	// ErrCorruptPack is returned when pack framing, AAD binding, or BLAKE3 digest fails.
	ErrCorruptPack = errors.New("dogi: corrupt pack or checksum mismatch")

	// ErrCipherFailed is returned when sealing or opening an encrypted chunk fails.
	ErrCipherFailed = errors.New("dogi: cipher seal/open failed")
)

// LifetimeClass partitions objects into discrete pack streams by expected
// invalidation-time range (analogous to DOGI/NoDaP Block Invalidation-time Ranges).
type LifetimeClass uint8

const (
	// ClassShortLived holds ephemeral build outputs, caches, and scratch files
	// expected to be invalidated within 1-2 epochs.
	ClassShortLived LifetimeClass = iota
	// ClassMediumLived holds actively edited workspace files expected to survive
	// several epochs before overwrite or deletion.
	ClassMediumLived
	// ClassLongLived holds stable source trees, vendored dependencies, and lockfiles.
	ClassLongLived
	// ClassPinnedImmutable holds immutable content such as .git/objects, snapshots,
	// and model weights that are rarely or never deleted.
	ClassPinnedImmutable

	// NumClasses is the total number of discrete lifetime streams.
	NumClasses = 4
)

// String returns a canonical lowercase label for the lifetime class.
func (c LifetimeClass) String() string {
	switch c {
	case ClassShortLived:
		return "short_lived"
	case ClassMediumLived:
		return "medium_lived"
	case ClassLongLived:
		return "long_lived"
	case ClassPinnedImmutable:
		return "pinned_immutable"
	default:
		return fmt.Sprintf("unknown(%d)", uint8(c))
	}
}

// Policy selects which packing strategy the Packer uses.
type Policy string

const (
	// PolicyFIFOMixed packs all incoming chunks into one arrival-order stream.
	PolicyFIFOMixed Policy = "fifo_mixed_baseline"
	// PolicyPathGrouped packs chunks using only static path/extension priors.
	PolicyPathGrouped Policy = "path_grouped_baseline"
	// PolicyDOGILifetime packs chunks using DOGI's multi-signal online predictor
	// (static priors + per-directory EWMA death-rate/lifetime tracker + burst/size
	// signals + GC-relocation re-prediction).
	PolicyDOGILifetime Policy = "dogi_lifetime_packed"
	// PolicyOracleUpperBound packs chunks using hindsight ground-truth remaining
	// lifetime (NoDaP-style upper bound for benchmarking only).
	PolicyOracleUpperBound Policy = "oracle_lifetime_upper_bound"
)

// Valid reports whether p is a known packing policy.
func (p Policy) Valid() bool {
	switch p {
	case PolicyFIFOMixed, PolicyPathGrouped, PolicyDOGILifetime, PolicyOracleUpperBound:
		return true
	default:
		return false
	}
}

// Namespace strictly scopes every pack and chunk to a single (TenantID, VolumeID).
type Namespace struct {
	TenantID string `json:"tenant_id"`
	VolumeID string `json:"volume_id"`
}

// Validate verifies that TenantID and VolumeID are non-empty and safe.
func (ns Namespace) Validate() error {
	if err := validateNSComponent("tenant_id", ns.TenantID); err != nil {
		return err
	}
	if err := validateNSComponent("volume_id", ns.VolumeID); err != nil {
		return err
	}
	return nil
}

func validateNSComponent(name, val string) error {
	if val == "" || len(val) > 128 {
		return fmt.Errorf("%w: %s must be 1..128 bytes", ErrInvalidNamespace, name)
	}
	if strings.ContainsAny(val, "/\\\x00") || val == "." || val == ".." {
		return fmt.Errorf("%w: %s contains disallowed characters", ErrInvalidNamespace, name)
	}
	return nil
}

// KeyPrefix returns a backend.ValidKey-compliant, collision-resistant key prefix
// isolated to this (TenantID, VolumeID) pair.
func (ns Namespace) KeyPrefix() string {
	th := sha256.Sum256([]byte("tenant:" + ns.TenantID))
	vh := sha256.Sum256([]byte("volume:" + ns.VolumeID))
	return "dogi/" + hex.EncodeToString(th[:8]) + "/" + hex.EncodeToString(vh[:8]) + "/pack"
}

// Sealer is structurally identical to vfs.Cipher so any per-volume VFS cipher
// (including post-quantum vfs.Hybrid) can seal pack chunks without creating a
// circular dependency on package vfs.
type Sealer interface {
	Seal(plaintext []byte) (sealed []byte, keyID string, err error)
	Open(sealed []byte, keyID string) (plaintext []byte, err error)
}

// ChunkMeta carries observable metadata for an incoming chunk write.
type ChunkMeta struct {
	// Namespace must match the Packer's configured Namespace or ErrTenantIsolation is returned.
	Namespace Namespace
	// Path is the workspace-relative POSIX path (e.g. "target/debug/deps/foo.o").
	Path string
	// Epoch is the logical write epoch (monotonically non-decreasing).
	Epoch uint64
	// OracleDeathEpoch is optional hindsight metadata (epoch at which this chunk
	// will be deleted or overwritten; 0 means never deleted). Strictly ignored by
	// PolicyFIFOMixed, PolicyPathGrouped, and PolicyDOGILifetime.
	OracleDeathEpoch uint64
}

// Config controls pack sizing, GC thresholds, and policy behavior.
type Config struct {
	// Namespace isolates all packs to one (TenantID, VolumeID). Required.
	Namespace Namespace
	// Policy selects the packing policy. Defaults to PolicyDOGILifetime.
	Policy Policy
	// PackTargetBytes is the flush threshold per open lifetime stream.
	// Defaults to 1 MiB (1 << 20).
	PackTargetBytes int
	// GCDeadRatioThreshold is the minimum dead-byte fraction [0.0, 1.0] required
	// to compact a partially-dead pack during Collect. 100%-dead packs are always
	// reclaimed with zero live-data copying regardless of threshold. Defaults to 0.50.
	GCDeadRatioThreshold float64
	// Sealer optionally encrypts every chunk frame inside packs (e.g. vfs.Hybrid).
	Sealer Sealer
	// AdaptiveGroupGuard enables DOGI's misprediction/entropy guard when true.
	// When the online tracker detects high cross-directory unpredictability,
	// it can damp stream splitting. Defaults to false so multi-stream overhead
	// in negative regimes is transparently measurable unless explicitly enabled.
	AdaptiveGroupGuard bool
}

// DefaultPackTargetBytes is the default 1 MiB immutable pack flush threshold.
const DefaultPackTargetBytes = 1 << 20

// DefaultGCDeadRatioThreshold is the default 50% dead-byte compaction threshold.
const DefaultGCDeadRatioThreshold = 0.50

func (c *Config) normalize() error {
	if err := c.Namespace.Validate(); err != nil {
		return err
	}
	if c.Policy == "" {
		c.Policy = PolicyDOGILifetime
	}
	if !c.Policy.Valid() {
		return fmt.Errorf("%w: unknown policy %q", ErrInvalidConfig, c.Policy)
	}
	if c.PackTargetBytes == 0 {
		c.PackTargetBytes = DefaultPackTargetBytes
	}
	if c.PackTargetBytes < 1024 || c.PackTargetBytes > 64<<20 {
		return fmt.Errorf("%w: PackTargetBytes=%d must be in [1024, 64MiB]", ErrInvalidConfig, c.PackTargetBytes)
	}
	if c.GCDeadRatioThreshold == 0 {
		c.GCDeadRatioThreshold = DefaultGCDeadRatioThreshold
	}
	if c.GCDeadRatioThreshold <= 0 || c.GCDeadRatioThreshold > 1.0 {
		return fmt.Errorf("%w: GCDeadRatioThreshold=%f must be in (0, 1]", ErrInvalidConfig, c.GCDeadRatioThreshold)
	}
	return nil
}

// GCStats records the exact outcome of a single Collect pass.
type GCStats struct {
	Epoch                  uint64 `json:"epoch"`
	PacksScanned           int    `json:"packs_scanned"`
	ZeroCopyReclaimedPacks int    `json:"zero_copy_reclaimed_packs"`
	CompactedPacks         int    `json:"compacted_packs"`
	ReclaimedBytes         int64  `json:"reclaimed_bytes"`
	LiveCopyBytes          int64  `json:"live_copy_bytes"`
	LiveCopyChunks         int64  `json:"live_copy_chunks"`
	GetRangeOps            int64  `json:"get_range_ops"`
}

// EpochMetric captures end-of-epoch storage and compaction state for time-series analysis.
type EpochMetric struct {
	Epoch                  uint64  `json:"epoch"`
	LiveBytes              int64   `json:"live_bytes"`
	StoredBytes            int64   `json:"stored_bytes"`
	StorageAmp             float64 `json:"storage_amp"`
	EpochLiveCopyBytes     int64   `json:"epoch_live_copy_bytes"`
	CumulativeLiveCopy     int64   `json:"cumulative_live_copy_bytes"`
	CumulativeIngest       int64   `json:"cumulative_ingest_bytes"`
	CumulativeWAF          float64 `json:"cumulative_waf"`
	ZeroCopyReclaimedPacks int     `json:"zero_copy_reclaimed_packs"`
	CompactedPacks         int     `json:"compacted_packs"`
	ActivePacks            int     `json:"active_packs"`
}

// Metrics summarizes cumulative packing, GC compaction, and storage-amplification telemetry.
type Metrics struct {
	Policy                 Policy            `json:"policy"`
	PackTargetBytes        int               `json:"pack_target_bytes"`
	GCDeadRatioThreshold   float64           `json:"gc_dead_ratio_threshold"`
	TotalIngestBytes       int64             `json:"total_ingest_bytes"`
	TotalIngestChunks      int64             `json:"total_ingest_chunks"`
	LiveBytes              int64             `json:"live_bytes"`
	LiveChunks             int64             `json:"live_chunks"`
	StoredBytes            int64             `json:"stored_bytes"`
	ActivePacks            int               `json:"active_packs"`
	PeakStoredBytes        int64             `json:"peak_stored_bytes"`
	PeakStorageAmp         float64           `json:"peak_storage_amp"`
	MeanStorageAmp         float64           `json:"mean_storage_amp"`
	FinalStorageAmp        float64           `json:"final_storage_amp"`
	LiveCopyBytes          int64             `json:"live_copy_bytes"`
	LiveCopyChunks         int64             `json:"live_copy_chunks"`
	WriteAmplification     float64           `json:"write_amplification"`
	PacksUploaded          int64             `json:"packs_uploaded"`
	PacksDeleted           int64             `json:"packs_deleted"`
	ZeroCopyReclaimedPacks int64             `json:"zero_copy_reclaimed_packs"`
	CompactedPacks         int64             `json:"compacted_packs"`
	GCGetRangeOps          int64             `json:"gc_get_range_ops"`
	GCCycles               int64             `json:"gc_cycles"`
	ClassCounts            [NumClasses]int64 `json:"class_counts"`
	EpochSeries            []EpochMetric     `json:"epoch_series,omitempty"`
}
