# HIP-1344: Parallel Delta Restore for Encrypted VFS Checkpoints (ParaSync)

Refs `hanzoai/vfs#64`. Tracked under **HIP-1344**. Library: [`internal/restore/parasync`](../../../internal/restore/parasync). Benchmark CLI: [`tools/parasync-bench`](../../../tools/parasync-bench). Raw results: [`raw_results.json`](./raw_results.json). Detailed score table: [`score_table.md`](./score_table.md).

## 1. Primary Reference & Clean-Room License Audit

- **Primary Paper:** Zhihao Zhang et al., *ParaSync*, 24th USENIX Conference on File and Storage Technologies (FAST '26), https://www.usenix.org/conference/fast26/presentation/zhang-zhihao-parasync.
- **Supporting Chunking Reference:** Wen Xia et al., *FastCDC: A Fast and Efficient Content-Defined Chunking Approach for Data Deduplication*, USENIX ATC '16 (matching the per-volume keyed Gear rolling-hash model in `hanzoai/vfs/chunk.go`).
- **Clean-Room & Dependency Audit:** All code under `internal/restore/parasync` and `tools/parasync-bench` is an independent clean-room Go implementation written from first principles using only the Go standard library and `hanzoai/vfs/internal/blake3`. Zero new external dependencies were added (`go.mod` and `go.sum` are completely untouched).

## 2. Problem Statement & `vfs -> hanzoai/s3 -> visor` Boundary

When `hanzoai/visor` restores or rolls forward an encrypted sandbox checkpoint backed by `hanzoai/vfs` over `hanzoai/s3`, naive full-checkpoint restore re-downloads 100% of ciphertext chunks even when only 2%–15% of workspace bytes changed. Conversely, fixed-64 KiB sequential delta sync loses chunk alignment on mid-file byte insertions (re-transferring nearly entire shifted files) and stalls on serialized per-object S3 RTTs.

Preserving `HIP-0302` zero-knowledge storage invariants requires:
1. **Ciphertext-Only `hanzoai/s3` Layout:** Object storage sees only opaque AES-256-GCM blobs addressed by ciphertext BLAKE3 digest (`tenants/<nsHash>/chunks/<prefix>/<cipherDigest>`) and AEAD-sealed checkpoint manifests. Plaintext file contents, paths, tenant names, and plaintext hashes never leave the trusted `vfs` client.
2. **Strict Multi-Tenant Isolation:** Encryption keys, keyed BLAKE3 `ChunkID`s, FastCDC Gear tables, and S3 namespace hashes are cryptographically bound to `(TenantID, KeyEpoch, MasterSecret)`.
3. **Fail-Closed Key Rotation:** Rotating a tenant's key epoch cleanly diverges ciphertext digests 100% and triggers deterministic full-parallel fallback without silent corruption.

## 3. Architecture (`internal/restore/parasync`)

1. **Stage 1 — Parallel Sharded Chunk-Index Diff (`planner.go`):** Validates `(TenantID, KeyEpoch, ChunkingMode)` compatibility, partitions `BaseCheckpoint` across `S` hash shards in parallel, and diffs `TargetCheckpoint` file ranges concurrently to classify every chunk into `localCopyTask` (zero network transfer) vs deduplicated `remoteFetchTask`.
2. **Stage 2 — Work-Stealing Parallel Ciphertext Transfer & AEAD Verify (`planner.go`, `cipher.go`):** Assigns unique changed ciphertext chunks across `W` per-worker work-stealing deques. Idle workers steal from skewed hot-file deques, fetch ciphertext over HTTP `GET`, verify `BLAKE3(ciphertext) == CipherDigest`, authenticate AES-256-GCM with AAD `(TenantID, KeyEpoch, ChunkID)`, and confirm decrypted `ChunkID`.
3. **Stage 3 — Overlapped Parallel Materialize & Local Bitrot Repair:** Local chunk-reuse workers run concurrently with remote transfer workers, verifying local `ReadAt` chunks against their tenant-keyed `ChunkID` (auto-repairing from S3 on local corruption) and writing disjoint byte ranges lock-free via `WriteAt` on pre-sized target files.
4. **Per-Tenant Keyed FastCDC (`cdc.go`):** Normalized Gear rolling-hash chunking (`8 KiB` min / `32 KiB` avg / `128 KiB` max) with secret per-tenant, per-epoch Gear tables re-synchronizes after mid-file insertions without exposing cross-tenant chunk boundaries.

## 4. Summary of Measured Results

Full matrix and negative-regime breakdowns are in [`score_table.md`](./score_table.md) and [`raw_results.json`](./raw_results.json). Key measured highlights on `16 MiB` volumes (`W=8`, `2.00 ms` RTT, `100.0 MiB/s` S3 link):

| Workload | `full_parallel` (MiB / ms) | `fixed_64k_seq_delta` (MiB / ms) | **`parasync_parallel_delta` (MiB / ms)** | BW Saved vs Full Par | Speedup vs Full Par |
|---|---:|---:|---:|---:|---:|
| `small_edit_2pct_churn` | 16.01 MiB / 241.27 ms | 0.50 MiB / 51.48 ms | **0.65 MiB / 27.54 ms** | **+95.91%** | **8.76x** |
| `medium_build_15pct_churn` | 16.01 MiB / 272.16 ms | 2.75 MiB / 213.71 ms | **2.89 MiB / 70.13 ms** | **+81.96%** | **3.88x** |
| `byte_shift_insertion_5pct` | 16.81 MiB / 244.93 ms | 12.81 MiB / 818.67 ms | **1.83 MiB / 44.57 ms** | **+89.10%** | **5.50x** |
| `full_rewrite_95pct_churn` | 16.01 MiB / 234.40 ms | 15.44 MiB / 896.15 ms | **15.46 MiB / 237.17 ms** | **+3.48%** | **0.99x** |

## 5. Honest Negative & Boundary Regimes

1. **`full_rewrite_95pct_churn_overhead`:** At `>=95%` churn, delta diffing adds CPU/planning overhead while saving `<5%` network bytes vs `full_checkpoint_parallel`. When a checkpoint is known to be a full rewrite or fresh empty base, skipping diffing directly to parallel full fetch is optimal.
2. **`incompatible_key_or_cipher_epoch_fallback`:** Rotating tenant keys (`Epoch 1 -> Epoch 2`) causes `100.00%` ciphertext digest divergence. The planner deterministically detects `incompatible_key_epoch`, reuses `0` local chunks, falls back to parallel full fetch (`243.84 ms`), and preserves bit-exact SHA-256 fidelity.
3. **`cross_tenant_dedup_forbidden`:** Uploading identical `16.00 MiB` plaintext under Tenant A and Tenant B yields **`0` shared ciphertext digests** (`0.00%` cross-tenant dedup) and **`100.00%` distinct FastCDC boundary layouts** — deliberately trading cross-tenant dedup for `HIP-0302` zero-knowledge confidentiality.

## 6. Reproducing

```sh
go vet ./internal/restore/parasync/... ./tools/parasync-bench/...
go test -race -v -count=1 ./internal/restore/parasync/...
go run ./tools/parasync-bench -size-mb=16 -workers=8 -diff-shards=4 -rtt=2ms -bw-mbps=100 -runs=3 -out=docs/research/hip-1344-parasync
```
