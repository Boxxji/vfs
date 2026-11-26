// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package aiturbo

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"strings"

	"github.com/zeebo/blake3"

	"github.com/hanzoai/vfs/pkg/backend"
)

const (
	// SchemaVersion identifies the canonical on-store checkpoint manifest format.
	SchemaVersion uint32 = 1

	// MinTargetGroupBytes is the lower bound for grouped pack target size (64 KiB).
	MinTargetGroupBytes int64 = 64 << 10

	// MaxTargetGroupBytes matches Hanzo S3's single-request part size (64 MiB).
	MaxTargetGroupBytes int64 = 64 << 20

	// DefaultTargetGroupBytes is the default grouped pack target size (8 MiB).
	DefaultTargetGroupBytes int64 = 8 << 20

	// DefaultMaxGapBytes is the default Range-GET gap-merge threshold (64 KiB).
	DefaultMaxGapBytes int64 = 64 << 10

	// DefaultMaxCoalescedRangeBytes bounds a single merged GetRange request (16 MiB).
	DefaultMaxCoalescedRangeBytes int64 = 16 << 20

	// DefaultConcurrency is the default worker pool width for S3 PUT/GET operations.
	DefaultConcurrency = 8

	// DefaultMaxManifestBytes caps manifest reads per backend.Backend.Get contract.
	DefaultMaxManifestBytes int64 = 16 << 20
)

var (
	// ErrInvalidConfig is returned when Config parameters violate bounds.
	ErrInvalidConfig = errors.New("aiturbo: invalid config")

	// ErrInvalidNamespace is returned when TenantID or VolumeID is invalid.
	ErrInvalidNamespace = errors.New("aiturbo: invalid namespace")

	// ErrInvalidCheckpointID is returned when a checkpoint ID is empty or malformed.
	ErrInvalidCheckpointID = errors.New("aiturbo: invalid checkpoint id")

	// ErrInvalidEntry is returned when an Entry has a malformed path, size, or digest.
	ErrInvalidEntry = errors.New("aiturbo: invalid entry")

	// ErrDuplicatePath is returned when two entries in the same namespace share a path.
	ErrDuplicatePath = errors.New("aiturbo: duplicate entry path")

	// ErrTenantIsolation is returned when an operation attempts to mix or reference
	// objects across distinct (TenantID, VolumeID) namespaces.
	ErrTenantIsolation = errors.New("aiturbo: cross-tenant namespace violation")

	// ErrEntryNotFound is returned when a selective download requests a path
	// absent from the published checkpoint manifest.
	ErrEntryNotFound = errors.New("aiturbo: entry not found in manifest")

	// ErrCorruptManifest is returned when a loaded manifest fails decoding or
	// BLAKE3 integrity verification.
	ErrCorruptManifest = errors.New("aiturbo: corrupt manifest")

	// ErrCorruptData is returned when downloaded bytes fail BLAKE3 verification.
	ErrCorruptData = errors.New("aiturbo: corrupt data")

	// ErrInjectedFailure is returned by InstrumentedBackend when fault injection fires.
	ErrInjectedFailure = errors.New("aiturbo: injected backend failure")
)

// Namespace scopes all grouped objects and checkpoint manifests to a single
// (TenantID, VolumeID) isolation boundary.
type Namespace struct {
	TenantID string `json:"tenant_id"`
	VolumeID string `json:"volume_id"`
}

// Validate checks that TenantID and VolumeID are non-empty and free of control bytes.
func (n Namespace) Validate() error {
	if err := validateIdentifier("tenant_id", n.TenantID); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidNamespace, err)
	}
	if err := validateIdentifier("volume_id", n.VolumeID); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidNamespace, err)
	}
	return nil
}

func validateIdentifier(field, v string) error {
	if v == "" || len(v) > 255 {
		return fmt.Errorf("%s must be 1..255 bytes", field)
	}
	if strings.TrimSpace(v) != v {
		return fmt.Errorf("%s must not have leading or trailing whitespace", field)
	}
	for i := 0; i < len(v); i++ {
		c := v[i]
		if c < 0x20 || c == 0x7f {
			return fmt.Errorf("%s contains control byte %#x", field, c)
		}
	}
	return nil
}

// KeyPrefix returns a collision-free, backend.ValidKey-compliant key prefix
// uniquely identifying this (TenantID, VolumeID) namespace.
func (n Namespace) KeyPrefix() string {
	h := blake3.New()
	_, _ = h.Write([]byte("aiturbo:ns:v1\x00"))
	var lenBuf [8]byte
	binary.LittleEndian.PutUint64(lenBuf[:], uint64(len(n.TenantID)))
	_, _ = h.Write(lenBuf[:])
	_, _ = h.Write([]byte(n.TenantID))
	binary.LittleEndian.PutUint64(lenBuf[:], uint64(len(n.VolumeID)))
	_, _ = h.Write(lenBuf[:])
	_, _ = h.Write([]byte(n.VolumeID))
	sum := h.Sum(nil)
	return "aiturbo/ns-" + hex.EncodeToString(sum[:16])
}

// PackKey returns the immutable content-addressed key for a grouped pack object.
func (n Namespace) PackKey(digest [32]byte) string {
	h := hex.EncodeToString(digest[:])
	return n.KeyPrefix() + "/pack/" + h[:2] + "/" + h
}

// DirectKey returns the immutable content-addressed key for a direct-bypass object.
func (n Namespace) DirectKey(digest [32]byte) string {
	h := hex.EncodeToString(digest[:])
	return n.KeyPrefix() + "/direct/" + h[:2] + "/" + h
}

// HeadPrefix returns the key prefix containing all published checkpoint
// manifests within namespace n.
func (n Namespace) HeadPrefix() string {
	return n.KeyPrefix() + "/head/"
}

// HeadKey returns the atomic checkpoint manifest key for checkpointID within n.
func (n Namespace) HeadKey(checkpointID string) string {
	h := blake3.New()
	_, _ = h.Write([]byte("aiturbo:ckpt:v1\x00"))
	_, _ = h.Write([]byte(checkpointID))
	sum := h.Sum(nil)
	return n.HeadPrefix() + hex.EncodeToString(sum[:16])
}

// OwnsKey reports whether key lies strictly inside this namespace's key prefix
// and satisfies backend.ValidKey.
func (n Namespace) OwnsKey(key string) bool {
	if err := backend.ValidKey(key); err != nil {
		return false
	}
	return strings.HasPrefix(key, n.KeyPrefix()+"/")
}

// ValidateCheckpointID validates that checkpointID is safe and non-empty.
func ValidateCheckpointID(checkpointID string) error {
	if err := validateIdentifier("checkpoint_id", checkpointID); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidCheckpointID, err)
	}
	return nil
}

// Config configures upload grouping, direct bypass, Range-GET coalescing, and
// concurrency behavior for the AITURBO grouped IO planner.
type Config struct {
	// TargetGroupBytes is the target size ceiling for packed group objects
	// (recommended: 4 MiB, 8 MiB, or 16 MiB; max 64 MiB).
	TargetGroupBytes int64 `json:"target_group_bytes"`

	// LargeObjectThreshold is the minimum size at which an entry bypasses
	// group packing and is uploaded directly as a standalone object to avoid
	// redundant copying. Defaults to TargetGroupBytes when <= 0. Entries
	// exceeding TargetGroupBytes always take direct bypass so packed groups
	// never exceed TargetGroupBytes.
	LargeObjectThreshold int64 `json:"large_object_threshold"`

	// MaxGapBytes is the largest unrequested byte gap between two slices inside
	// the same group object that PlanDownload may coalesce into a single
	// GetRange request. Setting 0 merges only strictly contiguous slices.
	MaxGapBytes int64 `json:"max_gap_bytes"`

	// MaxCoalescedRangeBytes bounds the total size of a single coalesced
	// GetRange request so gap merging cannot create unbounded reads.
	MaxCoalescedRangeBytes int64 `json:"max_coalesced_range_bytes"`

	// Concurrency is the worker pool size for parallel S3 PUT/GET operations.
	Concurrency int `json:"concurrency"`

	// MaxManifestBytes caps the manifest object size accepted by backend.Get
	// and published by UploadCheckpoint.
	MaxManifestBytes int64 `json:"max_manifest_bytes"`

	// RequireConditionalHead enforces S3 conditional writes (PutIfAbsent for
	// new checkpoints, PutIfVersion for updates) when publishing the manifest.
	RequireConditionalHead bool `json:"require_conditional_head"`

	// CleanupOnAbort, when opted into by a single-writer caller, deletes
	// uploaded objects from an aborted upload ONLY after proving they are not
	// referenced by any published checkpoint manifest in the namespace.
	// Defaults to false so shared content-addressed objects are preserved
	// safely under concurrent writers until GC.
	CleanupOnAbort bool `json:"cleanup_on_abort"`
}

// DefaultConfig returns production-safe defaults tuned for bulk AI checkpoints
// over Hanzo S3.
func DefaultConfig() Config {
	return Config{
		TargetGroupBytes:       DefaultTargetGroupBytes,
		LargeObjectThreshold:   DefaultTargetGroupBytes,
		MaxGapBytes:            DefaultMaxGapBytes,
		MaxCoalescedRangeBytes: DefaultMaxCoalescedRangeBytes,
		Concurrency:            DefaultConcurrency,
		MaxManifestBytes:       DefaultMaxManifestBytes,
		RequireConditionalHead: true,
		CleanupOnAbort:         false,
	}
}

// Normalized fills zero-valued optional fields with defaults and validates all bounds.
func (c Config) Normalized() (Config, error) {
	if c.TargetGroupBytes == 0 {
		c.TargetGroupBytes = DefaultTargetGroupBytes
	}
	if c.TargetGroupBytes < MinTargetGroupBytes || c.TargetGroupBytes > MaxTargetGroupBytes {
		return Config{}, fmt.Errorf("%w: target_group_bytes=%d outside [%d, %d]",
			ErrInvalidConfig, c.TargetGroupBytes, MinTargetGroupBytes, MaxTargetGroupBytes)
	}
	if c.LargeObjectThreshold == 0 {
		c.LargeObjectThreshold = c.TargetGroupBytes
	}
	if c.LargeObjectThreshold < 0 || c.LargeObjectThreshold > MaxTargetGroupBytes {
		return Config{}, fmt.Errorf("%w: large_object_threshold=%d outside [0, %d]",
			ErrInvalidConfig, c.LargeObjectThreshold, MaxTargetGroupBytes)
	}
	if c.MaxGapBytes < 0 {
		return Config{}, fmt.Errorf("%w: max_gap_bytes=%d must be >= 0", ErrInvalidConfig, c.MaxGapBytes)
	}
	if c.MaxCoalescedRangeBytes == 0 {
		c.MaxCoalescedRangeBytes = max(c.TargetGroupBytes, DefaultMaxCoalescedRangeBytes)
	}
	if c.MaxCoalescedRangeBytes < c.TargetGroupBytes || c.MaxCoalescedRangeBytes > MaxTargetGroupBytes {
		return Config{}, fmt.Errorf("%w: max_coalesced_range_bytes=%d outside [%d, %d]",
			ErrInvalidConfig, c.MaxCoalescedRangeBytes, c.TargetGroupBytes, MaxTargetGroupBytes)
	}
	if c.Concurrency == 0 {
		c.Concurrency = DefaultConcurrency
	}
	if c.Concurrency < 1 || c.Concurrency > 256 {
		return Config{}, fmt.Errorf("%w: concurrency=%d outside [1, 256]", ErrInvalidConfig, c.Concurrency)
	}
	if c.MaxManifestBytes == 0 {
		c.MaxManifestBytes = DefaultMaxManifestBytes
	}
	if c.MaxManifestBytes < 4096 || c.MaxManifestBytes > MaxTargetGroupBytes {
		return Config{}, fmt.Errorf("%w: max_manifest_bytes=%d outside [4096, %d]",
			ErrInvalidConfig, c.MaxManifestBytes, MaxTargetGroupBytes)
	}
	return c, nil
}

// Entry represents a single logical file, tensor, or chunk in a bulk checkpoint.
type Entry struct {
	TenantID string
	VolumeID string
	Path     string
	Data     []byte
	Size     int64
	BLAKE3   string
}

// ValidateEntryPath ensures a checkpoint entry path is canonical, relative,
// non-empty, and free of traversal segments or control characters.
func ValidateEntryPath(p string) (string, error) {
	if p == "" || len(p) > 1024 {
		return "", fmt.Errorf("%w: path length %d outside 1..1024", ErrInvalidEntry, len(p))
	}
	for i := 0; i < len(p); i++ {
		if p[i] < 0x20 || p[i] == 0x7f {
			return "", fmt.Errorf("%w: path %q contains control byte", ErrInvalidEntry, p)
		}
	}
	if strings.HasPrefix(p, "/") || strings.Contains(p, "\\") {
		return "", fmt.Errorf("%w: path %q must be slash-delimited relative path", ErrInvalidEntry, p)
	}
	clean := path.Clean(p)
	if clean == "." || clean == ".." || strings.HasPrefix(clean, "../") || clean != p {
		return "", fmt.Errorf("%w: path %q is not canonical relative path", ErrInvalidEntry, p)
	}
	return clean, nil
}

// GroupObjectMeta describes one immutable S3 object produced by an upload plan.
type GroupObjectMeta struct {
	ObjectKey    string `json:"object_key"`
	Size         int64  `json:"size"`
	BLAKE3       string `json:"blake3"`
	ItemCount    int    `json:"item_count"`
	DirectBypass bool   `json:"direct_bypass"`
}

// ManifestEntry maps a logical checkpoint path to its exact verified slice
// inside an immutable grouped pack or direct object.
type ManifestEntry struct {
	Path         string `json:"path"`
	Size         int64  `json:"size"`
	BLAKE3       string `json:"blake3"`
	ObjectKey    string `json:"object_key"`
	Offset       int64  `json:"offset"`
	Length       int64  `json:"length"`
	DirectBypass bool   `json:"direct_bypass"`
}

// Manifest is the atomically published checkpoint head document.
// Readers discover a checkpoint strictly through its Manifest; no partial data
// upload is ever visible until Manifest publication succeeds.
type Manifest struct {
	SchemaVersion     uint32            `json:"schema_version"`
	CheckpointID      string            `json:"checkpoint_id"`
	TenantID          string            `json:"tenant_id"`
	VolumeID          string            `json:"volume_id"`
	Strategy          string            `json:"strategy"`
	CreatedUnixNano   int64             `json:"created_unix_nano"`
	TotalLogicalBytes int64             `json:"total_logical_bytes"`
	TotalStoredBytes  int64             `json:"total_stored_bytes"`
	GroupObjects      []GroupObjectMeta `json:"group_objects"`
	Entries           []ManifestEntry   `json:"entries"`
	ManifestDigest    string            `json:"manifest_digest"`
}

// ComputeDigest computes the canonical BLAKE3 digest of m with ManifestDigest cleared.
func (m *Manifest) ComputeDigest() (string, error) {
	clone := *m
	clone.ManifestDigest = ""
	raw, err := json.Marshal(clone)
	if err != nil {
		return "", fmt.Errorf("aiturbo: marshal manifest for digest: %w", err)
	}
	sum := blake3.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

// MarshalSigned computes ManifestDigest and serializes m to canonical JSON bytes.
func (m *Manifest) MarshalSigned() ([]byte, error) {
	digest, err := m.ComputeDigest()
	if err != nil {
		return nil, err
	}
	m.ManifestDigest = digest
	return json.Marshal(m)
}

// UnmarshalManifest decodes and verifies a checkpoint manifest against expectedNS.
func UnmarshalManifest(raw []byte, expectedNS Namespace) (*Manifest, error) {
	var m Manifest
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, fmt.Errorf("%w: json decode: %v", ErrCorruptManifest, err)
	}
	if m.SchemaVersion != SchemaVersion {
		return nil, fmt.Errorf("%w: unsupported schema version %d", ErrCorruptManifest, m.SchemaVersion)
	}
	if m.ManifestDigest == "" {
		return nil, fmt.Errorf("%w: missing manifest_digest", ErrCorruptManifest)
	}
	wantDigest, err := m.ComputeDigest()
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrCorruptManifest, err)
	}
	if m.ManifestDigest != wantDigest {
		return nil, fmt.Errorf("%w: digest mismatch got %s want %s", ErrCorruptManifest, m.ManifestDigest, wantDigest)
	}
	if err := expectedNS.Validate(); err != nil {
		return nil, err
	}
	if m.TenantID != expectedNS.TenantID || m.VolumeID != expectedNS.VolumeID {
		return nil, fmt.Errorf("%w: manifest belongs to (%s,%s), requested (%s,%s)",
			ErrTenantIsolation, m.TenantID, m.VolumeID, expectedNS.TenantID, expectedNS.VolumeID)
	}
	// Verify every referenced object stays strictly inside expectedNS.
	objSizes := make(map[string]int64, len(m.GroupObjects))
	for _, g := range m.GroupObjects {
		if !expectedNS.OwnsKey(g.ObjectKey) {
			return nil, fmt.Errorf("%w: group object %q outside namespace %s",
				ErrTenantIsolation, g.ObjectKey, expectedNS.KeyPrefix())
		}
		if g.Size < 0 {
			return nil, fmt.Errorf("%w: negative object size for %s", ErrCorruptManifest, g.ObjectKey)
		}
		objSizes[g.ObjectKey] = g.Size
	}
	for _, e := range m.Entries {
		if _, err := ValidateEntryPath(e.Path); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrCorruptManifest, err)
		}
		if e.Size == 0 && e.Length == 0 && e.ObjectKey == "" {
			continue
		}
		if !expectedNS.OwnsKey(e.ObjectKey) {
			return nil, fmt.Errorf("%w: entry %q references key %q outside namespace %s",
				ErrTenantIsolation, e.Path, e.ObjectKey, expectedNS.KeyPrefix())
		}
		objSize, ok := objSizes[e.ObjectKey]
		if !ok {
			return nil, fmt.Errorf("%w: entry %q references undeclared object %q",
				ErrCorruptManifest, e.Path, e.ObjectKey)
		}
		if e.Offset < 0 || e.Length < 0 || e.Size != e.Length || e.Offset+e.Length > objSize {
			return nil, fmt.Errorf("%w: entry %q slice [%d, %d) out of bounds for %q (size %d)",
				ErrCorruptManifest, e.Path, e.Offset, e.Offset+e.Length, e.ObjectKey, objSize)
		}
	}
	return &m, nil
}
