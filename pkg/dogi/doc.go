// Copyright 2026 Hanzo AI Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Package dogi implements clean-room lifetime-aware object packing inspired by
// DOGI ("Data Placement with Oracle-Guided Insights for Log-Structured Systems",
// Kim et al., USENIX FAST '26) for the Hanzo VFS -> Hanzo S3 -> Visor stack.
//
// In hanzoai/vfs, sealed chunks are appended into immutable S3 packs
// (pack/<2hex>/<id>). A single arrival-order stream mixes short-lived build
// artifacts (target/, node_modules/.cache/, .next/, tmp/, *.o, *.tmp, *.pyc)
// with long-lived workspace chunks (.git/objects/, vendor/, go.sum, models/,
// stable source files). At collection time, packs that become mostly dead still
// trap surviving long-lived chunks, forcing ranged S3 GETs, re-encryption, and
// pack rewrites (live-data copying / write amplification).
//
// Package dogi focuses strictly on packing decisions — which chunks are
// co-packed into immutable S3 packs — and measures their direct impact on
// live-data copying, zero-copy dead-pack reclamation, storage amplification,
// and S3 PUT overhead across four comparable policies:
//
//   - PolicyFIFOMixed: single-stream arrival-order baseline (current VFS packing).
//   - PolicyPathGrouped: static path/extension prior grouping.
//   - PolicyDOGILifetime: multi-signal online predictor combining path priors,
//     per-directory EWMA death-rate and lifetime tracking, burst/size correlation,
//     and DOGI-style GC-relocation re-prediction.
//   - PolicyOracleUpperBound: hindsight NoDaP-style upper bound using exact
//     remaining lifetime (benchmark reference only; never used by online policies).
//
// All packs are strictly isolated per (TenantID, VolumeID) namespace, verified
// with BLAKE3-256, and optionally sealed with any per-volume vfs.Cipher (such as
// post-quantum vfs.Hybrid X25519 + ML-KEM-768).
package dogi
