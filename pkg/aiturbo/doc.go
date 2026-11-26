// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

// Package aiturbo implements optional grouped IO planning for bulk checkpoint
// upload and download between hanzoai/vfs and S3-compatible object storage
// (hanzoai/s3), inspired by AITURBO (Hao et al., FAST 2026,
// https://www.usenix.org/conference/fast26/presentation/hao).
//
// Bulk AI training and agent checkpoints typically contain hundreds of small
// metadata, tokenizer, optimizer, and LoRA files (1 KiB–64 KiB), medium tensor
// layers (128 KiB–2 MiB), and large weight shards (4 MiB–64 MiB). Transferring
// each file as an individual S3 object incurs severe per-request RTT overhead
// and high requests/GiB API load; conversely, packing an entire checkpoint into
// a single monolithic archive eliminates upload/download parallelism and forces
// full-blob fetches for selective restores.
//
// Package aiturbo plans bounded, locality-preserving grouped transfers over
// hanzoai/vfs's canonical pkg/backend.Backend contract while preserving five
// strict invariants:
//
//  1. Unchanged S3 consistency semantics: every grouped pack and direct object
//     is immutable and content-addressed by its 256-bit BLAKE3 digest. The
//     checkpoint manifest ("head") is published atomically (via PutIfAbsent /
//     PutIfVersion when conditional head publication is enabled) ONLY after
//     100% of data object PUTs succeed. Any partial failure aborts fail-closed
//     without publishing the manifest, guaranteeing zero torn checkpoints and
//     exact read-after-write visibility.
//  2. End-to-end content verification: both the checkpoint manifest and every
//     restored entry slice are verified against their 256-bit BLAKE3 digest.
//  3. Multi-tenant isolation: upload groups NEVER mix entries belonging to
//     different (TenantID, VolumeID) namespaces. Every backend key is scoped
//     under a collision-free BLAKE3-derived namespace prefix satisfying
//     backend.ValidKey, and manifest loading rejects cross-namespace object
//     references.
//  4. Zero-copy large-object bypass: entries at or above LargeObjectThreshold
//     bypass group packing and are transferred directly in parallel, avoiding
//     redundant memory copies on already-large shards.
//  5. Gap-bounded Range-GET coalescing: downloads sort requested slices by
//     offset within each group object and merge adjacent byte ranges separated
//     by at most MaxGapBytes (bounded by MaxCoalescedRangeBytes) into a single
//     backend.Backend.GetRange call, reducing GETs/GiB while keeping read
//     amplification explicitly bounded and measurable.
package aiturbo
