// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

// Package aiturbo implements a clean-room grouped checkpoint IO planner
// inspired by AITURBO (Hao et al., USENIX FAST '26) for bulk AI-model and
// agent-workspace checkpoint uploads and restores over S3-compatible object
// stores.
//
// The planner preserves canonical vfs -> hanzoai/s3 -> visor boundaries and
// strict Head conditional PUT consistency: immutable grouped super-objects and
// their content-addressed manifest are uploaded and verified before a single
// atomic conditional Head PUT (If-None-Match / If-Match) publishes the new
// checkpoint generation.
package aiturbo

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"path"
	"sort"
	"strings"
	"time"
)

var (
	// ErrHeadConflict is returned when an atomic conditional Head PUT fails
	// with HTTP 412 Precondition Failed because another writer won the race.
	ErrHeadConflict = errors.New("aiturbo: head conditional PUT conflict (HTTP 412)")

	// ErrNotFound is returned when a requested object, manifest, or head is absent.
	ErrNotFound = errors.New("aiturbo: object not found")

	// ErrTenantViolation is returned when an operation attempts to cross tenant
	// namespace boundaries or supplies an invalid tenant identifier.
	ErrTenantViolation = errors.New("aiturbo: tenant isolation violation")

	// ErrIntegrityMismatch is returned when restored bytes fail SHA-256 verification.
	ErrIntegrityMismatch = errors.New("aiturbo: SHA-256 integrity mismatch")

	// ErrInvalidConfig is returned when planner or store configuration is invalid.
	ErrInvalidConfig = errors.New("aiturbo: invalid configuration")
)

// Strategy identifies one of the five matched checkpoint IO execution strategies.
type Strategy string

const (
	// StrategyPerObjectSequential uploads and downloads one object per file sequentially.
	StrategyPerObjectSequential Strategy = "per_object_sequential_baseline"

	// StrategyPerObjectParallel uploads and downloads one object per file concurrently.
	StrategyPerObjectParallel Strategy = "per_object_parallel_baseline"

	// StrategyNaiveTarball packs the entire checkpoint into one single monolithic object.
	StrategyNaiveTarball Strategy = "naive_tarball_monolith"

	// StrategyAITurboGrouped uses size-aware bounded super-objects, parallel grouped
	// upload/download pipelines, coalesced HTTP Range GETs, and atomic conditional Head PUTs.
	StrategyAITurboGrouped Strategy = "aiturbo_grouped_io_planner"

	// StrategyOverCoalesced is a documented negative control using an oversized 256 MiB
	// group target with full-group fetch to demonstrate parallelism loss and read amplification.
	StrategyOverCoalesced Strategy = "over_coalesced_giant_group_negative"
)

// AllStrategies lists the five matched strategies in benchmark comparison order.
var AllStrategies = []Strategy{
	StrategyPerObjectSequential,
	StrategyPerObjectParallel,
	StrategyNaiveTarball,
	StrategyAITurboGrouped,
	StrategyOverCoalesced,
}

// CheckpointFile is a single file or tensor shard belonging to a checkpoint.
type CheckpointFile struct {
	Path     string
	TenantID string
	Data     []byte
	SHA256   [32]byte
}

// Checkpoint is an immutable snapshot of files/tensors to publish or restore.
type Checkpoint struct {
	ID         string
	TenantID   string
	Files      []CheckpointFile
	TotalBytes int64
}

// GroupManifestEntry maps one logical checkpoint file to its byte slice inside
// an immutable content-addressed group super-object.
type GroupManifestEntry struct {
	Path      string `json:"path"`
	GroupKey  string `json:"group_key"`
	Offset    int64  `json:"offset"`
	Length    int64  `json:"length"`
	SHA256Hex string `json:"sha256"`
	GroupSize int64  `json:"group_size"`
}

// GroupObjectInfo describes one immutable super-object uploaded to the store.
type GroupObjectInfo struct {
	Key        string `json:"key"`
	Size       int64  `json:"size"`
	SHA256Hex  string `json:"sha256"`
	EntryCount int    `json:"entry_count"`
}

// CheckpointManifest is the immutable, content-addressed index of a published
// checkpoint generation. It is uploaded after all group objects succeed and
// before the atomic conditional Head PUT.
type CheckpointManifest struct {
	Version        int                  `json:"version"`
	CheckpointID   string               `json:"checkpoint_id"`
	TenantID       string               `json:"tenant_id"`
	Strategy       Strategy             `json:"strategy"`
	Generation     uint64               `json:"generation"`
	CreatedAt      time.Time            `json:"created_at"`
	TotalBytes     int64                `json:"total_bytes"`
	FileCount      int                  `json:"file_count"`
	Groups         []GroupObjectInfo    `json:"groups"`
	Entries        []GroupManifestEntry `json:"entries"`
	ManifestSHA256 string               `json:"manifest_sha256"`
}

// HeadRecord is the single mutable pointer per tenant volume, updated solely
// via atomic conditional PUT (If-None-Match: * or If-Match: <prevETag>).
type HeadRecord struct {
	Version        int       `json:"version"`
	TenantID       string    `json:"tenant_id"`
	Generation     uint64    `json:"generation"`
	CheckpointID   string    `json:"checkpoint_id"`
	Strategy       Strategy  `json:"strategy"`
	ManifestKey    string    `json:"manifest_key"`
	ManifestSHA256 string    `json:"manifest_sha256"`
	TotalBytes     int64     `json:"total_bytes"`
	FileCount      int       `json:"file_count"`
	CommittedAt    time.Time `json:"committed_at"`
}

// CommitResult summarizes a committed checkpoint publication.
type CommitResult struct {
	Head         HeadRecord
	HeadETag     string
	ManifestKey  string
	GroupCount   int
	TotalBytes   int64
	Duration     time.Duration
	StoreMetrics StoreMetrics
	PackingBytes int64
}

// RestoreResult summarizes a full or selective checkpoint restore.
type RestoreResult struct {
	Head                HeadRecord
	Files               map[string][]byte
	RequestedBytes      int64
	DataBytesDownloaded int64
	DownloadedBytes     int64
	DataReadAmp         float64
	ReadAmp             float64
	Duration            time.Duration
	StoreMetrics        StoreMetrics
}

// ValidateTenantID validates that tenantID is a safe lowercase namespace label
// (`a-z`, `0-9`, `-`, 1..64 bytes) matching vfs key-segment rules.
func ValidateTenantID(tenantID string) error {
	if tenantID == "" || len(tenantID) > 64 {
		return fmt.Errorf("%w: tenant id %q must be 1..64 chars", ErrTenantViolation, tenantID)
	}
	for i := 0; i < len(tenantID); i++ {
		c := tenantID[i]
		ok := (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-'
		if !ok {
			return fmt.Errorf("%w: tenant id %q contains invalid char %q", ErrTenantViolation, tenantID, c)
		}
	}
	return nil
}

// ValidateRelativePath verifies that a checkpoint file path is clean, relative,
// and cannot escape its tenant namespace.
func ValidateRelativePath(p string) error {
	if p == "" || strings.HasPrefix(p, "/") || strings.Contains(p, "\\") {
		return fmt.Errorf("%w: invalid checkpoint file path %q", ErrTenantViolation, p)
	}
	cleaned := path.Clean(p)
	if cleaned == "." || cleaned == ".." || strings.HasPrefix(cleaned, "../") || cleaned != p {
		return fmt.Errorf("%w: non-canonical checkpoint file path %q", ErrTenantViolation, p)
	}
	return nil
}

// TenantHeadKey returns the canonical conditional-PUT head key for a tenant.
func TenantHeadKey(tenantID string) (string, error) {
	if err := ValidateTenantID(tenantID); err != nil {
		return "", err
	}
	return "tenants/" + tenantID + "/head", nil
}

// TenantGroupKey returns a content-addressed immutable group object key scoped
// strictly under the tenant's prefix.
func TenantGroupKey(tenantID, sha256Hex string) (string, error) {
	if err := ValidateTenantID(tenantID); err != nil {
		return "", err
	}
	if len(sha256Hex) < 4 {
		return "", fmt.Errorf("%w: invalid group digest %q", ErrInvalidConfig, sha256Hex)
	}
	return "tenants/" + tenantID + "/groups/" + sha256Hex[:2] + "/" + sha256Hex, nil
}

// TenantManifestKey returns a content-addressed immutable manifest key scoped
// strictly under the tenant's prefix.
func TenantManifestKey(tenantID, sha256Hex string) (string, error) {
	if err := ValidateTenantID(tenantID); err != nil {
		return "", err
	}
	if len(sha256Hex) < 4 {
		return "", fmt.Errorf("%w: invalid manifest digest %q", ErrInvalidConfig, sha256Hex)
	}
	return "tenants/" + tenantID + "/manifests/" + sha256Hex[:2] + "/" + sha256Hex, nil
}

// KeyBelongsToTenant returns true iff key is strictly inside tenantID's prefix.
func KeyBelongsToTenant(tenantID, key string) bool {
	if ValidateTenantID(tenantID) != nil {
		return false
	}
	prefix := "tenants/" + tenantID + "/"
	return strings.HasPrefix(key, prefix) && !strings.Contains(key, "..")
}

// ComputeManifestDigest deterministically hashes the manifest's entries and
// tenant identity so corruption or cross-tenant substitution is always caught.
func ComputeManifestDigest(m *CheckpointManifest) string {
	entries := make([]GroupManifestEntry, len(m.Entries))
	copy(entries, m.Entries)
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Path < entries[j].Path
	})
	h := sha256.New()
	_, _ = fmt.Fprintf(h, "v=%d|tenant=%s|ckpt=%s|gen=%d|strat=%s\n",
		m.Version, m.TenantID, m.CheckpointID, m.Generation, m.Strategy)
	for _, e := range entries {
		_, _ = fmt.Fprintf(h, "%s|%s|%d|%d|%s\n",
			e.Path, e.GroupKey, e.Offset, e.Length, e.SHA256Hex)
	}
	return hex.EncodeToString(h.Sum(nil))
}
