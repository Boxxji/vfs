# HIP-1344 Research-to-PR Dossier: Grouped Checkpoint IO (`vfs` / `hanzoai/s3`, AITURBO)

- **Tracking Issue**: `hanzoai/vfs#60` — `[Bounty #3] Grouped checkpoint IO · vfs / s3 (AITURBO)`
- **Umbrella HIP**: [HIP-1344: Agent-Optimized Storage and Recovery](https://hips.hanzo.ai/docs/hip-1344-agent-optimized-storage-and-recovery/)
- **Primary Reference**: AITURBO — Hao et al., USENIX FAST '26 ([https://www.usenix.org/conference/fast26/presentation/hao](https://www.usenix.org/conference/fast26/presentation/hao))
- **Deliverable Packages**:
  - Clean-room Go stdlib planner & loopback S3 harness: `internal/checkpoint/aiturbo/`
  - Reproducible benchmark CLI: `tools/aiturbo-bench/main.go`
  - Measured outputs: [`score_table.md`](./score_table.md), [`raw_results.json`](./raw_results.json)

---

## 1. Paper & License Audit

### 1.1 Design Summary (AITURBO, USENIX FAST '26)
Modern AI training and agent-workspace checkpoints exhibit a strongly bimodal object-size distribution: hundreds to thousands of small optimizer, tokenizer, metadata, and agent-state files (`4 KiB`–`64 KiB`) alongside medium layer/adapter shards (`256 KiB`–`2 MiB`) and large weight tensors (`4 MiB`–`64 MiB+`). Writing and restoring these files one object per S3 request incurs high HTTP round-trip latency and high per-request S3 API costs (`PUTs/GiB` and `GETs/GiB`), whereas packing an entire checkpoint into one monolithic archive destroys multi-stream parallelism and forces catastrophic read amplification on selective/partial restores.

AITURBO addresses this trade-off through **grouped checkpoint IO planning**:
- Coalescing small and medium files/tensors into bounded-size super-objects indexed by a compact manifest (`(path -> group_key, offset, length, digest)`).
- Bypassing packing buffers for tensors that are already large enough to saturate per-stream object-store throughput.
- Scheduling super-object transfers in parallel across bounded worker pools (largest-first / LPT to minimize straggler tail latency) and merging adjacent/nearby byte ranges into batched HTTP `Range` GETs for selective restores.

### 1.2 License & Patent Compatibility Audit
- **Implementation provenance**: 100% **clean-room Go implementation** written from first principles using only the Go standard library (`go.mod` / `go.sum` untouched, zero external dependencies added).
- **License**: Apache-2.0 (`Copyright 2026 Hanzo AI Inc.`), matching `hanzoai/vfs`. No upstream paper artifact code was copied or linked.
- **Patent/IP note**: The techniques implemented (size-threshold object coalescing, Merkle/SHA-256 content-addressed immutable packs, bounded worker pools, Longest-Processing-Time-First dispatch, HTTP RFC 9110 `Range` interval merging, and S3 conditional `If-None-Match` / `If-Match` Head CAS) are standard systems/object-storage primitives already aligned with `hanzoai/vfs`'s existing immutable `pack/` + conditional `head` design (`pack.go`, `head.go`, `pkg/fence`).
- **Benchmark attribution discipline**: Figures published in the FAST '26 paper apply solely to the original authors' hardware and storage environment and are not claimed as Hanzo expectations; all numbers in this dossier are freshly measured in this repository via `tools/aiturbo-bench`.

---

## 2. Matched Baselines & Canonical Stack Boundaries

### 2.1 Canonical Stack Boundaries Preserved
This implementation strictly preserves `vfs -> hanzoai/s3 -> visor` boundaries:
- **`vfs`** owns checkpoint grouping, content-addressed immutable group objects (`tenants/<tid>/groups/<2hex>/<digest>`), immutable manifests (`tenants/<tid>/manifests/<2hex>/<digest>`), and the single mutable Head pointer (`tenants/<tid>/head`).
- **`hanzoai/s3`** (exercised here via a real loopback `net/http/httptest` path-style S3 server enforcing atomic `If-None-Match: *` / `If-Match: <etag>` -> `HTTP 412 Precondition Failed` and `Range: bytes=start-end` -> `HTTP 206 Partial Content`) owns durable object storage and server-side conditional-write atomicity.
- **Strict Head Conditional PUT Consistency**: `CommitCheckpoint` enforces a three-phase barrier:
  1. Upload all immutable group super-objects (bounded parallel workers, LPT order). Any failure aborts immediately without touching the manifest or Head (matching `vfs` `LLM.md` Rule #3: orphaned immutable objects are GC garbage, never corruption).
  2. Upload the immutable content-addressed `CheckpointManifest`.
  3. Issue exactly one atomic conditional Head PUT (`If-None-Match: *` for initial creation, `If-Match: <prevETag>` for subsequent generations). Concurrent losers receive `ErrHeadConflict` (HTTP 412) with zero silent overwrite and zero torn state.

### 2.2 Relation to Existing `vfs` Internals
`vfs` already packs FastCDC chunks into `16 MiB` immutable packs (`pack.go:20`, `packTarget = 16 << 20`) and fetches individual chunks via `v.be.GetRange` (`pack.go:159`), fenced by `pkg/fence` (`head.go:186`). What `vfs` currently lacks for bulk AI/agent checkpoints is:
1. Optional **size-aware bulk grouped IO planning** (with large-tensor passthrough + LPT parallel dispatch), and
2. **Coalesced HTTP `Range` GET merging** (`MergeRanges`) so selective/bulk reads of nearby files or chunks inside a pack do not issue one `GetRange` per small item.

### 2.3 Reproducible Five-Strategy Matched Harness
All five strategies run against the identical deterministic mixed checkpoint (`272` files: `240` small `4 KiB`–`64 KiB` files including a fixed `16 KiB` sentinel `config/checkpoint_index.json`, `28` medium `256 KiB`–`1.5 MiB` layer shards, `4` large `4 MiB`–`8 MiB` weight tensors; total `58.87 MiB`, seed `20260401`) over the same loopback HTTP S3 server (`RTT=2 ms`, per-stream cap `150 MiB/s`, `W=8`, `5` iterations/strategy):

1. `per_object_sequential_baseline`: `W=1`, 1 HTTP PUT/GET per file sequentially.
2. `per_object_parallel_baseline`: `W=8`, 1 HTTP PUT/GET per file concurrently.
3. `naive_tarball_monolith`: `W=1`, single monolithic object (lowest PUT count, zero parallelism, full download on selective restore).
4. `aiturbo_grouped_io_planner`: `W=8`, `4 MiB` bounded super-objects, large-tensor passthrough (`>= 4 MiB`), parallel super-object assembly with Merkle content-addressing, LPT dispatch, coalesced HTTP `Range` GETs (`MaxRangeGapBytes=64 KiB`, `MaxMergedRangeBytes=4 MiB`), strict atomic conditional Head PUT barrier.
5. `over_coalesced_giant_group_negative`: documented negative control (`W=8`, `256 MiB` giant group target, full-group fetch without HTTP Range).

Reproduce end-to-end:
```bash
go test -race -v -count=1 ./internal/checkpoint/aiturbo/...
go run ./tools/aiturbo-bench \
  --out-dir docs/research/hip-1344-aiturbo \
  --iterations 5 --rtt 2ms --stream-bw-mib 150 --workers 8 --group-mib 4 --cas-racers 16
```

---

## 3. Falsifiable Hypotheses & Expected Failure Modes

| Hypothesis | Falsification Criterion | Result |
|---|---|---|
| **H1 (Request & Cost Reduction)**: `aiturbo_grouped_io_planner` reduces S3 `PUTs/GiB` and `GETs/GiB` by `>= 10x` vs `per_object_parallel_baseline` on mixed ML/agent checkpoints. | Fails if request reduction `< 10x`. | **Confirmed**: **17.12x** reduction (`4766.4` -> `278.3` PUTs/GiB and GETs/GiB; **94.2%** S3 API cost savings per 100 GiB cycle). |
| **H2 (Throughput & Tail Latency)**: Bounded `4 MiB` super-objects + parallel assembly + LPT outperform both `per_object_parallel_baseline` and `naive_tarball_monolith` on upload/restore throughput and commit p50/p99 latency. | Fails if AITURBO upload or restore throughput trails either baseline. | **Confirmed**: Upload `262.23 MiB/s` (**1.24x** vs parallel per-object, **3.71x** vs monolith, **7.10x** vs sequential); Full Restore `276.36 MiB/s` (**1.35x** vs parallel per-object, **3.35x** vs monolith); lowest commit p50/p99 (`224.48 / 259.26 ms`). |
| **H3 (Selective Partial Restore Locality)**: Coalesced HTTP `Range` GET keeps selective `16 KiB` single-file data read amplification at `1.00x`, avoiding the monolith/over-coalesced penalty. | Fails if `DataReadAmp > 1.05x`. | **Confirmed**: `1.00x` data read-amp (`13.44 ms`) vs `3767.39x` (`474.40 ms` / `492.08 ms`) on monolith & over-coalesced control. |
| **H4 (Strict Head CAS Consistency)**: Under 16-way concurrent Head publication races, exactly 1 writer wins per generation (`15` receive `ErrHeadConflict`), with `0` silent overwrites and `0` torn checkpoints. | Fails on any silent overwrite, torn read, or race detector warning. | **Confirmed**: `1/15` on `If-None-Match: *`, `1/15` on `If-Match`, `0` silent overwrites, `0` torn reads, clean rebase/retry to gen 3 under `-race`. |

### Documented Honest Negative & Boundary Regimes
1. **`selective_single_file_restore_read_amp`**: Without HTTP `Range` support (full-group fetch), restoring a `16 KiB` file from a `3.01 MiB` super-object incurs **192.59x** payload read amplification (`3,155,365` data bytes fetched, `33.55 ms`). Enabling HTTP `Range` GET recovers exact **1.00x** (`16,384` bytes, `10.29 ms`) with the same GET count (`3` GETs including `head` + `manifest`). On the over-coalesced `256 MiB` negative control, read amplification explodes to **3767.39x** (`492.08 ms`).
2. **`already_large_tensor_only_checkpoint`**: On a checkpoint containing only `4 x 16 MiB` tensors (`64 MiB`), grouping yields **0.0%** S3 PUT reduction (`96.0` PUTs/GiB for both `per_object_parallel_baseline` and `aiturbo_grouped_io_planner`). Size-aware passthrough (`>= 4 MiB`) prevents allocating `64.00 MiB` of redundant copy buffers and maintains `223.96 MiB/s` upload throughput (vs `57.58 MiB/s` when blindly forced to pack into one giant group).
3. **`concurrent_head_cas_conflict_retry`**: Simultaneous writers racing on the same volume Head do incur wasted phase-1 upload bandwidth for losing writers before HTTP 412 rejection at phase 3; this is the intentional cost of preserving zero-lock, zero-torn-state optimistic Head CAS (identical to `vfs`'s round-fenced `Head` commit contract). Losing writers cleanly rebase onto the latest Head ETag and retry.

---

## 4. Deliverable & Implementation Map

- [`internal/checkpoint/aiturbo/types.go`](../../../internal/checkpoint/aiturbo/types.go): Data types, error sentinels (`ErrHeadConflict`, `ErrTenantViolation`, `ErrIntegrityMismatch`), strict tenant/path validation, and deterministic `ComputeManifestDigest`.
- [`internal/checkpoint/aiturbo/store.go`](../../../internal/checkpoint/aiturbo/store.go): `ObjectStore` interface, real `net/http/httptest` `LoopbackS3Server` (atomic `If-None-Match`/`If-Match` -> HTTP 412, `Range` -> HTTP 206, optional RTT/per-stream bandwidth/concurrency controls, exact atomic telemetry), and pooled `HTTPObjectStore`.
- [`internal/checkpoint/aiturbo/planner.go`](../../../internal/checkpoint/aiturbo/planner.go): `Planner`, `PlanGroups` (parallel assembly, size-aware passthrough, Merkle content-addressed group keys), `CommitCheckpoint` (3-phase atomic Head barrier + LPT upload scheduling), `MergeRanges`, `RestoreCheckpoint`, and `RestoreSubset`.
- [`internal/checkpoint/aiturbo/workload.go`](../../../internal/checkpoint/aiturbo/workload.go): Deterministic mixed & large-tensor checkpoint generators, `RunHeadCASRace`, AWS S3 Standard request cost model (aligned with `pkg/cost`), and `RunBenchmarkSuite`.
- [`internal/checkpoint/aiturbo/aiturbo_test.go`](../../../internal/checkpoint/aiturbo/aiturbo_test.go): Full `-race` test suite.
- [`tools/aiturbo-bench/main.go`](../../../tools/aiturbo-bench/main.go): Reproducible CLI writing `score_table.md` and `raw_results.json`.

---

## 5. Evidence & Safety Verification

All safety properties are enforced in code and verified under `go test -race -v -count=1 ./internal/checkpoint/aiturbo/...`:
- **Bit-exact integrity**: Every full and selective restore verifies manifest digest and per-file SHA-256 (`TestRoundTripAllStrategiesBitExact`, `TestTamperedObjectDetectedBySHA256`).
- **Concurrency & Head CAS atomicity**: 16 goroutines racing on `If-None-Match: *` and `If-Match: <etag>` yield strictly 1 winner and 15 `ErrHeadConflict` rejections per generation, 0 torn checkpoints, 0 data races (`TestStrictHeadConditionalPUTConcurrency`).
- **Crash/error barrier**: Injected phase-1 data upload failures leave the previous Head generation completely untouched (`TestDataUploadFailureLeavesHeadUntouched`).
- **Multi-tenant isolation**: Concurrent multi-tenant commits/restores isolate every group, manifest, and head object strictly under `tenants/<tenant-id>/...`; cross-tenant file mixing and `../` path traversal fail closed with `ErrTenantViolation` (`TestMultiTenantIsolationConcurrent`).
- **Zero dependency & hygiene footprint**: `go.mod` and `go.sum` are untouched (100% Go stdlib).

---

## 6. Measurements & HIP-1344 Status Update

Full raw measurements and formatted score tables are published in:
- [`score_table.md`](./score_table.md)
- [`raw_results.json`](./raw_results.json)

### Headline Summary (`58.87 MiB` mixed checkpoint, `272` files, `RTT=2 ms`, `150 MiB/s` per-stream cap, `W=8`)
- **`aiturbo_grouped_io_planner`**: `262.23 MiB/s` upload, `276.36 MiB/s` full restore, `278.3` PUTs/GiB, `278.3` GETs/GiB (`$0.1503` API cost per 100 GiB W+R cycle), `224.48 / 259.26 ms` commit p50/p99, `1.00x` selective `16 KiB` data read-amp (`13.44 ms`), `PASS` SHA-256 bit-exact, `PASS` Head CAS.
- **HIP-1344 Status**: **Research + Clean-Room Prototype Complete (Ready for Optional `vfs` Bulk-Checkpoint Wiring)**. Recommended next integration step is wiring `MergeRanges` into `vfs` bulk read-ahead (`pack.go` `fetchChunk` batching) and exposing an opt-in `Volume` checkpoint import/export helper layered directly over `pkg/backend.Backend` and `pkg/fence.Store` without altering the on-disk `head`/`meta`/`pack` wire format.
