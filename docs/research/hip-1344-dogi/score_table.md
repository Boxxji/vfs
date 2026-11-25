# HIP-1344 / DOGI Real-Disk Benchmark Score Table

> **Measurement Scope**: All figures below were generated on the authoring host using real on-disk `.pack` files (`os.MkdirTemp`, `WriteAt`/`ReadAt`, Linux `/proc/self/io`, and SHA-256 bit-exact verification). They are reproducible measurements of this clean-room Go implementation, **not** Hanzo production SLA guarantees and **not** copied from the USENIX FAST '26 paper.

- **Generated (UTC)**: `2026-11-25T13:12:22Z`
- **Toolchain / Host**: `go1.27.1` (`linux/amd64`, `2` vCPUs)
- **Geometry**: `SlabTargetBytes = 256 KiB`, `MaxOpenSlabsPerTenant = 4`, `Seed = 42`

## 1. Primary Workload: `ai_agent_build_churn` (5 Matched Initial-Packing Policies)

| Policy | Live Copied (MiB) | Live-Copy Delta vs Baseline | WAF (Phys/Log) | Final Space Amp | Zero-Copy Clean Reclaim | Slabs Sealed (PUTs) | Avg Slab Fill | Peak Open-Slab RAM (MiB) | `/proc/self/io` `wchar` (MiB) | `syscw` | SHA-256 Verified |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `mixed_fifo_packing_baseline` | 22.04 | baseline | 1.331x | 1.212x | 0.4% (1/224) | 345 | 97.1% | 0.50 | 89.20 | 345 | 860 (0 err) |
| `size_bucket_packing` | 20.50 | -7.0% | 1.308x | 1.232x | 4.1% (9/219) | 355 | 92.7% | 1.00 | 87.66 | 355 | 860 (0 err) |
| `static_path_heuristic_packing` | 12.22 | -44.6% | 1.185x | 1.245x | 39.9% (79/198) | 331 | 90.3% | 1.50 | 79.37 | 331 | 860 (0 err) |
| `dogi_adaptive_lifetime_packing` | 4.26 | -80.6% | 1.066x | 1.110x | 72.7% (128/176) | 287 | 93.7% | 1.50 | 71.39 | 287 | 860 (0 err) |
| `over_fragmented_class_negative` | 1.36 | -93.8% | 1.022x | 1.015x | 96.1% (1325/1379) | 2213 | 12.4% | 2.00 | 68.48 | 2213 | 860 (0 err) |

## 2. Honest Negative Regime 1: `uniform_random_delete_no_lifetime_skew`

Every object follows an identical memoryless geometric lifetime regardless of path or size. Lifetime segregation provides near-zero reduction in live-data copying while holding additional open slab buffers.

| Policy | Live Copied (MiB) | WAF (Phys/Log) | Final Space Amp | Zero-Copy Clean Reclaim | Peak Open Slabs | Peak Open-Slab RAM (MiB) | SHA-256 Verified |
|---|---:|---:|---:|---:|---:|---:|---:|
| `mixed_fifo_packing_baseline` | 10.56 | 1.445x | 1.138x | 1.8% (2/109) | 1 | 0.25 | 199 (0 err) |
| `static_path_heuristic_packing` | 10.24 | 1.432x | 1.185x | 5.3% (6/114) | 3 | 0.75 | 199 (0 err) |
| `dogi_adaptive_lifetime_packing` | 9.76 | 1.412x | 1.146x | 0.9% (1/109) | 3 | 0.75 | 199 (0 err) |

## 3. Honest Negative Regime 2: `over_fragmented_64_bucket_underfill`

Moderate-rate workload with periodic generation flushes. Splitting into 64 ultra-fine buckets causes severe partial-slab tail flushing, low slab fill ratio, high slab count, and either open-slab eviction thrashing (when RAM-capped) or open-slab RAM explosion (when uncapped).

| Policy (MaxOpenCap) | Slabs Sealed | Avg Slab Fill | Open-Slab Evictions | Peak Open Slabs | Peak Open-Slab RAM (MiB) | Final Space Amp | Live Copied (MiB) |
|---|---:|---:|---:|---:|---:|---:|---:|
| `mixed_fifo_packing_baseline` (`cap=4`) | 51 | 87.0% | 0 | 1 | 0.25 | 1.202x | 2.89 |
| `dogi_adaptive_lifetime_packing` (`cap=4`) | 41 | 83.2% | 0 | 2 | 0.50 | 1.003x | 0.18 |
| `over_fragmented_class_negative` (`cap=4`) | 400 | 8.7% | 362 | 4 | 1.00 | 1.006x | 0.04 |
| `over_fragmented_class_negative` (`cap=64`) | 337 | 11.0% | 0 | 38 | 9.50 | 1.095x | 0.61 |

## 4. Honest Boundary Regime 3: `adversarial_renamed_ephemeral_cold_start`

Ephemeral build objects written under deceptive `.bin` paths alongside long-lived `.parquet` files. Measures exact causal cold-start misclassification window before online EWMA survival feedback converges.

| Policy | Cold-Start Misclassified | Convergence Delete Count | Convergence Epoch | Post-Convergence Accuracy | Live Copied (MiB) | WAF (Phys/Log) | Zero-Copy Clean Reclaim |
|---|---:|---:|---:|---:|---:|---:|---:|
| `mixed_fifo_packing_baseline` | n/a (no classifier) | n/a | n/a | n/a | 1.65 | 1.175x | 20.0% (5/25) |
| `static_path_heuristic_packing` | 240 / 240 | never | never | 0.0% | 1.65 | 1.175x | 20.0% (5/25) |
| `dogi_adaptive_lifetime_packing` | 12 / 240 | 12 | 2 | 100.0% | 0.35 | 1.039x | 81.5% (22/27) |

