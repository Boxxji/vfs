# HIP-1344 RASK Compact Sparse-Range Index — Measured Score Table

Environment: `linux/amd64`, `go1.27.1`, `2` CPUs (`2026-11-25T13:28:42Z`).

## 1. Primary Workload: 100,000 Sparse-Run Extents (4 KiB Data / 4 KiB Hole, Compact Values)

| Baseline | Struct Bytes | B/Extent | HeapAlloc Delta | Heap B/Ext | GC Pointers | GC Pause (us) | Hit p50 / p95 / p99 (ns) | Hole p50 / p99 (ns) | RangeScan (M ext/s) | Random Insert (K ops/s) | PunchHole (K ops/s) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `sorted_slice_baseline` | 2949144 | **29.49** | 2949256 | 29.49 | 1 | 80.5 | 161.8 / 189.3 / 1634.3 | 159.4 / 393.0 | 292.84 | 4.0 | 13.9 |
| `pointer_btree_baseline` | 5238280 | **52.38** | 5838192 | 58.38 | 26240 | 56.2 | 212.2 / 260.2 / 792.2 | 212.7 / 803.3 | 211.38 | 2715.9 | 2541.0 |
| `rask_compact_range_index` | 1253056 | **12.53** | 1259712 | 12.60 | 832 | 53.8 | 154.8 / 188.2 / 390.0 | 151.6 / 373.5 | 221.99 | 859.4 | 1135.9 |
| `flat_page_bitmap_negative` | 1625000 | 16.25 | n/a | n/a | n/a | n/a | n/a | n/a | n/a | n/a | OOM_CAPPED (requires 1.55 MiB dense table for 781 MiB logical span) |

## 2. Arbitrary 64-Bit Random Values Workload (100,000 Extents, `modeCompact64`)

| Baseline | Struct Bytes | B/Extent | Heap B/Ext | GC Pointers | Hit p50 / p99 (ns) | Random Insert (K ops/s) | PunchHole (K ops/s) |
|---|---:|---:|---:|---:|---:|---:|---:|
| `sorted_slice_baseline` | 2949144 | **29.49** | 29.49 | 1 | 160.6 / 911.2 | 3.8 | 13.6 |
| `pointer_btree_baseline` | 5238280 | **52.38** | 58.38 | 26240 | 223.4 / 1231.4 | 2511.7 | 2528.6 |
| `rask_compact_range_index` | 1653056 | **16.53** | 16.60 | 832 | 168.6 / 413.9 | 642.3 | 728.4 |

## 3. 1 PiB (`1 << 50` B) Sparse-File Stress Workload (101 Extents Across `0`, `1 TiB`, `256 TiB`, `512 TiB`, `1 PiB - 4 KiB`)

| Baseline | Extents | Struct Memory | GC Pointers | Hit p50 / p99 (ns) | 1 PiB Hole p50 / p99 (ns) | Status |
|---|---:|---:|---:|---:|---:|---|
| `sorted_slice_baseline` | 101 | **3.02 KiB** (3096 B) | 1 | 47.7 / 61.3 | **14.6 / 18.4** | ok |
| `pointer_btree_baseline` | 101 | **5.29 KiB** (5416 B) | 26 | 46.3 / 63.0 | **15.3 / 21.2** | ok |
| `rask_compact_range_index` | 101 | **2.75 KiB** (2816 B) | 9 | 52.7 / 77.7 | **13.6 / 18.7** | ok |
| `flat_page_bitmap_negative` | 101 | **2.03 TiB** | n/a | n/a | n/a | OOM_IMPOSSIBLE (2.03 TiB RAM required for 1 PiB @ 4 KiB) |

## 4. Documented Honest Negative & Boundary Regimes

### A. `dense_tiny_random_100pct_fragmented_writes` (25,000 Random 4 KiB Writes, Uncoalescable 64-bit Values)

| Baseline | Extents | Insert Throughput (K ops/s) | B/Extent | GC Pointers | Hit p99 (ns) |
|---|---:|---:|---:|---:|---:|
| `sorted_slice_baseline` | 17462 | 514.5 | 26.74 | 1 | 315.9 |
| `pointer_btree_baseline` | 17462 | 2831.2 | 36.28 | 3170 | 312.4 |
| `rask_compact_range_index` | 17462 | 1191.5 | 18.78 | 201 | 273.2 |

### B. `tiny_file_under_16_extents` (8 Extents)

| Baseline | Extents | Struct Bytes | Hit p50 (ns) | Hit p99 (ns) |
|---|---:|---:|---:|---:|
| `sorted_slice_baseline` | 8 | 216 | 8.5 | 18.9 |
| `pointer_btree_baseline` | 8 | 840 | 10.8 | 11.2 |
| `rask_compact_range_index` | 8 | 544 | 18.3 | 31.5 |

### C. `flat_page_table_1pib_explosion` (Dense 4 KiB Page Table vs RASK on 101 Sparse Extents)

| Logical File Size | Dense 4 KiB Page Table RAM | RASK (101 Sparse Extents) | Memory Amplification |
|---|---:|---:|---:|
| 64 MiB | **130.00 KiB** (133120 B) | 2816 B | 47.3x |
| 1 GiB | **2.03 MiB** (2129920 B) | 2816 B | 756.4x |
| 1 TiB | **2.03 GiB** (2181038080 B) | 2816 B | 774516.4x |
| 1 PiB (1<<50 B) | **2.03 TiB** (2233382993920 B) | 2816 B | 793104756.4x |
