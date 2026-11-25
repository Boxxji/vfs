# HIP-1344 Empirical Score Table — ParaSync Parallel Delta Restore

- **Generated (UTC):** `2026-11-25T14:03:00Z`
- **Platform:** `linux/amd64`, `2` vCPU, `go1.27.1`
- **Volume Size:** `16 MiB` (`16` files), **Workers (`W`):** `8`, **Diff Shards:** `4`, **Runs:** `3` (median)
- **Loopback S3 Shaping:** `2.00 ms` per-GET RTT, `100.0 MiB/s` aggregate GET bandwidth cap
- **Ciphertext-Only S3 Audit:** `5805` objects scanned (`247.03 MiB` ciphertext), **`0` plaintext leaks**

## 1. Matched Restore Strategy Comparison (4 Workloads x 4 Strategies)

| Workload | Strategy | Transferred (MiB) | BW Saved vs Full Par | HTTP GETs | Reused Chunks | Wall Time (ms) | Eff. Throughput (MiB/s) | Speedup vs Full Par | Speedup vs Full Seq | CPU Time (ms) | CPU Delta vs Full Par | Bit-Exact |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|:---:|
| `small_edit_2pct_churn` | `full_checkpoint_sequential` | 16.01 | +0.00% | 452 | 0 | 1493.84 | 10.71 | 0.16x | 1.00x | 366.29 | +110.13% | PASS |
| `small_edit_2pct_churn` | `full_checkpoint_parallel` | 16.01 | +0.00% | 452 | 0 | 241.27 | 66.32 | 1.00x | 6.19x | 174.31 | +0.00% | PASS |
| `small_edit_2pct_churn` | `fixed_chunk_sequential_delta` | 0.50 | +96.88% | 8 | 248 | 51.48 | 310.83 | 4.69x | 29.02x | 35.36 | -79.71% | PASS |
| `small_edit_2pct_churn` | `parasync_parallel_delta` | 0.65 | +95.91% | 18 | 434 | 27.54 | 581.07 | 8.76x | 54.24x | 43.37 | -75.12% | PASS |
| `medium_build_15pct_churn` | `full_checkpoint_sequential` | 16.01 | +0.00% | 452 | 0 | 1913.55 | 8.36 | 0.14x | 1.00x | 357.41 | +106.09% | PASS |
| `medium_build_15pct_churn` | `full_checkpoint_parallel` | 16.01 | +0.00% | 452 | 0 | 272.16 | 58.79 | 1.00x | 7.03x | 173.43 | +0.00% | PASS |
| `medium_build_15pct_churn` | `fixed_chunk_sequential_delta` | 2.75 | +82.82% | 44 | 212 | 213.71 | 74.87 | 1.27x | 8.95x | 59.17 | -65.88% | PASS |
| `medium_build_15pct_churn` | `parasync_parallel_delta` | 2.89 | +81.96% | 82 | 370 | 70.13 | 228.14 | 3.88x | 27.29x | 54.91 | -68.34% | PASS |
| `byte_shift_insertion_5pct` | `full_checkpoint_sequential` | 16.81 | +0.00% | 475 | 0 | 1777.11 | 9.45 | 0.14x | 1.00x | 437.94 | +108.04% | PASS |
| `byte_shift_insertion_5pct` | `full_checkpoint_parallel` | 16.81 | +0.00% | 475 | 0 | 244.93 | 68.59 | 1.00x | 7.26x | 210.51 | +0.00% | PASS |
| `byte_shift_insertion_5pct` | `fixed_chunk_sequential_delta` | 12.81 | +23.83% | 216 | 64 | 818.67 | 20.52 | 0.30x | 2.17x | 233.35 | +10.85% | PASS |
| `byte_shift_insertion_5pct` | `parasync_parallel_delta` | 1.83 | +89.10% | 50 | 425 | 44.57 | 376.97 | 5.50x | 39.87x | 56.24 | -73.28% | PASS |
| `full_rewrite_95pct_churn` | `full_checkpoint_sequential` | 16.01 | +0.00% | 460 | 0 | 1742.09 | 9.18 | 0.13x | 1.00x | 535.80 | +173.90% | PASS |
| `full_rewrite_95pct_churn` | `full_checkpoint_parallel` | 16.01 | +0.00% | 460 | 0 | 234.40 | 68.26 | 1.00x | 7.43x | 195.62 | +0.00% | PASS |
| `full_rewrite_95pct_churn` | `fixed_chunk_sequential_delta` | 15.44 | +3.55% | 247 | 9 | 896.15 | 17.85 | 0.26x | 1.94x | 256.55 | +31.15% | PASS |
| `full_rewrite_95pct_churn` | `parasync_parallel_delta` | 15.46 | +3.48% | 445 | 15 | 237.17 | 67.46 | 0.99x | 7.35x | 200.06 | +2.27% | PASS |

## 2. Documented Honest Negative & Boundary Regimes

### A. `full_rewrite_95pct_churn_overhead` (High-Churn Negative Regime)

When `>=95%` of checkpoint volume data is rewritten (`full_rewrite_95pct_churn`), `parasync_parallel_delta` transfers `15.46 MiB` vs `16.01 MiB` for `full_checkpoint_parallel` (`+3.48%` bytes saved), taking `237.17 ms` wall time (`0.99x`) and `200.06 ms` CPU (`+2.27%` CPU delta vs `full_checkpoint_parallel`, including `0.362 ms` manifest diffing). Delta diffing provides negligible transfer benefit at `>=95%` churn.

### B. `incompatible_key_or_cipher_epoch_fallback` (Rotated Key Epoch Boundary)

| Metric | Measured Value |
|---|---:|
| Key Rotation | Epoch `1` -> Epoch `2` |
| Ciphertext Digest Divergence | `100.00%` |
| Deterministic Fallback Applied | `true` (`incompatible_key_epoch`) |
| Local Reused Chunks | `0` |
| Transferred Ciphertext | `16.01 MiB` (`465` HTTP GETs) |
| Fallback Wall Time / Eff. Throughput | `243.84 ms` / `65.62 MiB/s` |
| Bit-Exact SHA-256 Fidelity | `true` |

### C. `cross_tenant_dedup_forbidden` (Zero-Knowledge Isolation Boundary)

| Metric | Measured Value |
|---|---:|
| Identical Plaintext Volume Size | `16.00 MiB` |
| Tenant A / Tenant B Chunks | `470` / `454` |
| Shared Ciphertext Digests / Shared ChunkIDs | `0` / `0` (`0.00%` cross-tenant dedup) |
| Files with Distinct FastCDC Cut Boundaries | `100.00%` |
| Cross-Tenant Restore Fallback | `incompatible_tenant` (bit-exact: `true`) |
