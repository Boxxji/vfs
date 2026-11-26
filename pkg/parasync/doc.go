// Copyright 2026 Hanzo AI Inc.
// SPDX-License-Identifier: Apache-2.0

// Package parasync implements a clean-room parallel delta planner and restore
// pipeline for compatible volume checkpoints (HIP-1345, inspired by ParaSync,
// USENIX FAST 2026).
//
// Canonical architecture:
//
//	vfs -> Hanzo S3 (pkg/backend.Backend) -> Visor
//
// Overview:
//
// When restoring or switching a Visor sandbox volume from an already-local
// SourceCheckpoint to a compatible remote TargetCheckpoint stored in Hanzo S3,
// a naive full restore re-downloads 100% of the target checkpoint. Package
// parasync instead provides:
//
//  1. Dual-mode chunking (chunker.go): FastCDC (gear-hash rolling fingerprint
//     with normalized chunking, default 4 KiB / 16 KiB / 64 KiB) alongside
//     fixed-size chunking (default 16 KiB) so callers and benchmarks can
//     directly evaluate shift-resilience on insertion/deletion edits versus
//     in-place overwrites.
//  2. Multi-level parallel delta planner (planner.go): partitions the target
//     file/extent tree into byte-balanced diff shards across P workers,
//     identifies local reusable chunks by exact BLAKE3-256 digest within the
//     same (TenantID, VolumeID) namespace, deduplicates identical remote chunks
//     single-flight, and coalesces nearby remote pack ranges into gap-bounded
//     GetRange batches.
//  3. Pipelined 3-stage concurrent restore (restore.go):
//     Diff/Plan -> Parallel S3 Range Fetch + BLAKE3 Verify -> Parallel
//     Offset-Partitioned Pwrite/Assembly into a private staging directory,
//     followed by full-file BLAKE3 verification and atomic rename commit.
//
// Security & Isolation Guarantees:
//
//   - Strict multi-tenant isolation: cross-tenant and cross-volume delta
//     attempts fail closed with ErrTenantIsolation. Local chunk indexes and
//     remote single-flight deduplication never cross (TenantID, VolumeID).
//   - Compatibility guard: mismatched CipherSuiteID or Chunking parameters
//     fail closed with ErrIncompatibleCheckpoint.
//   - End-to-end cryptographic verification: every reused local chunk, every
//     fetched remote chunk, every assembled file, and the checkpoint root
//     digest are verified with BLAKE3-256 before atomic commit.
//   - Atomic restore semantics: mid-restore cancellation or corruption cleans
//     up the staging directory and leaves existing destination files untouched.
package parasync
