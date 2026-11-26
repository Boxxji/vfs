# HIP-1344 RASK Compact Sparse-Range Index — Measured Score Table

Environment: `linux/amd64`, `go1.27.1`, `2` CPUs (`2026-11-26T07:53:37Z`).

## 1. Engine Element-Size Alignment (100,000 Append-Grown Extents)

| Engine / Baseline Element | Source | Bytes/Slot | Sorted Slice B/Ext | Matched RASK Mode | RASK B/Ext | Ratio (RASK / Slice) |
|---|---|---:|---:|---|---:|---:|
| `run{a, b uint64} (payload-free)` | `file.go:590 (layer.void []run)` | 16 | 18.84 | `modeConstVal32 (8 B leaf payload)` | **8.53** | **0.45x** |
| `Extent{Start, End, Val uint64} (value-carrying)` | `internal/index/rask/index.go:39` | 24 | 29.49 | `modeCompact32 (12 B leaf payload)` | **12.53** | **0.42x** |
| `extent{off uint64, n uint32, id [32]byte} (settled record)` | `record.go:235 + chunk.go:87 (fileState.ext)` | 48 | 53.66 | `modeCompact32 (12 B index handle)` | **12.53** | **0.23x** |

## 2. Payload-Free Run Workload: 100,000 `run{a, b}` Intervals (`file.go:590`, 16 B vs `modeConstVal32`)

| Baseline | Struct Bytes | B/Extent | HeapAlloc Delta | Heap B/Ext | GC Pointers | Hit p50 / p99 (ns) | Hole p50 / p99 (ns) | RangeScan (M ext/s) | Random Insert (K ops/s) | PunchHole (K ops/s) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `sorted_run_slice_16b` | 1884184 | **18.84** | 1884296 | 18.84 | 1 | 166.1 / 576.1 | 162.0 / 455.2 | 192.26 | 42.8 | 30.8 |
| `rask_payload_free_constval32` | 853056 | **8.53** | 859712 | 8.60 | 832 | 148.8 / 405.2 | 148.6 / 347.3 | 250.94 | 1012.4 | 1288.2 |

## 3. Value-Carrying Workload: 100,000 Sparse-Run Extents (4 KiB Data / 4 KiB Hole, Compact 32-Bit Values)

| Baseline | Struct Bytes | B/Extent | HeapAlloc Delta | Heap B/Ext | GC Pointers | GC Pause (us) | Hit p50 / p95 / p99 (ns) | Hole p50 / p99 (ns) | RangeScan (M ext/s) | Random Insert (K ops/s) | PunchHole (K ops/s) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `sorted_slice_baseline` | 2949144 | **29.49** | 2949144 | 29.49 | 1 | 64.5 | 161.5 / 198.2 / 476.8 | 161.3 / 445.5 | 296.57 | 4.1 | 12.8 |
| `pointer_btree_baseline` | 5238280 | **52.38** | 5838192 | 58.38 | 26240 | 64.2 | 223.5 / 401.3 / 1595.8 | 237.2 / 1510.7 | 197.38 | 3648.8 | 2210.5 |
| `rask_compact_range_index` | 1253056 | **12.53** | 1259712 | 12.60 | 832 | 66.4 | 154.7 / 254.5 / 1752.9 | 152.6 / 612.9 | 224.65 | 930.0 | 984.2 |
| `flat_page_bitmap_negative` | 1625000 | 16.25 | n/a | n/a | n/a | n/a | n/a | n/a | n/a | n/a | OOM_CAPPED (requires 1.55 MiB dense table for 781 MiB logical span) |

## 4. Arbitrary 64-Bit Random Values Workload (100,000 Extents, `modeCompact64`)

| Baseline | Struct Bytes | B/Extent | Heap B/Ext | GC Pointers | Hit p50 / p99 (ns) | Random Insert (K ops/s) | PunchHole (K ops/s) |
|---|---:|---:|---:|---:|---:|---:|---:|
| `sorted_slice_baseline` | 2949144 | **29.49** | 29.49 | 1 | 160.3 / 401.4 | 4.0 | 15.0 |
| `pointer_btree_baseline` | 5238280 | **52.38** | 58.38 | 26240 | 221.7 / 1250.1 | 2450.6 | 2352.8 |
| `rask_compact_range_index` | 1653056 | **16.53** | 16.60 | 832 | 170.8 / 1354.3 | 832.1 | 849.0 |

## 5. 1 PiB (`1 << 50` B) Sparse-File Stress Workload (101 Extents Across `0`, `1 TiB`, `256 TiB`, `512 TiB`, `1 PiB - 4 KiB`)

| Baseline | Extents | Struct Memory | GC Pointers | Hit p50 / p99 (ns) | 1 PiB Hole p50 / p99 (ns) | Status |
|---|---:|---:|---:|---:|---:|---|
| `sorted_slice_baseline` | 101 | **3.02 KiB** (3096 B) | 1 | 47.2 / 70.1 | **14.2 / 16.8** | ok |
| `pointer_btree_baseline` | 101 | **5.29 KiB** (5416 B) | 26 | 46.1 / 69.9 | **15.3 / 46.6** | ok |
| `rask_compact_range_index` | 101 | **2.75 KiB** (2816 B) | 9 | 51.9 / 83.4 | **13.6 / 22.2** | ok |
| `flat_page_bitmap_negative` | 101 | **2.03 TiB** | n/a | n/a | n/a | OOM_IMPOSSIBLE (2.03 TiB RAM required for 1 PiB @ 4 KiB) |

## 6. Documented Honest Negative & Boundary Regimes

### A. `dense_tiny_random_100pct_fragmented_writes` (25,000 Random 4 KiB Writes, Uncoalescable 64-bit Values)

| Baseline | Extents | Insert Throughput (K ops/s) | B/Extent | GC Pointers | Hit p99 (ns) |
|---|---:|---:|---:|---:|---:|
| `sorted_slice_baseline` | 17462 | 517.6 | 26.74 | 1 | 509.9 |
| `pointer_btree_baseline` | 17462 | 4815.0 | 36.28 | 3170 | 975.3 |
| `rask_compact_range_index` | 17462 | 1092.0 | 18.78 | 201 | 287.1 |

### B. `tiny_file_under_16_extents` (8 Extents)

| Baseline | Extents | Struct Bytes | Hit p50 (ns) | Hit p99 (ns) |
|---|---:|---:|---:|---:|
| `sorted_slice_baseline` | 8 | 216 | 8.5 | 90.0 |
| `pointer_btree_baseline` | 8 | 840 | 10.8 | 18.3 |
| `rask_compact_range_index` | 8 | 544 | 19.6 | 21.6 |

### C. `flat_page_table_1pib_explosion` (Dense 4 KiB Page Table vs RASK on 101 Sparse Extents)

| Logical File Size | Dense 4 KiB Page Table RAM | RASK (101 Sparse Extents) | Memory Amplification |
|---|---:|---:|---:|
| 64 MiB | **130.00 KiB** (133120 B) | 2816 B | 47.3x |
| 1 GiB | **2.03 MiB** (2129920 B) | 2816 B | 756.4x |
| 1 TiB | **2.03 GiB** (2181038080 B) | 2816 B | 774516.4x |
| 1 PiB (1<<50 B) | **2.03 TiB** (2233382993920 B) | 2816 B | 793104756.4x |
