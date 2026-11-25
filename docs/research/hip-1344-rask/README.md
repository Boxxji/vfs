# Compact Sparse-Range Indexing for `hanzoai/vfs` (RASK · HIP-1344)

Refs `hanzoai/vfs#59`. Tracked by HIP-1344.

- **Implementation:** `internal/index/rask/` (`index.go`, `rask.go`, `baselines.go`, `concurrent.go`, `rask_test.go`)
- **Benchmark Runner:** `tools/rask-bench/main.go`
- **Measured Artifacts:** `score_table.md`, `raw_results.json`

---

## 1. Primary Reference, Scope, and Clean-Room License Audit

- **Primary reference:** RASK (Zhao et al., *24th USENIX Conference on File and Storage Technologies (FAST '26)*, <https://www.usenix.org/conference/fast26/presentation/zhao>).
- **Scope:** In-memory compact sparse-range indexing for open-file unsettled write spans, punched-hole void lists, and settled extent maps across logical file sizes up to **1 PiB (`1 << 50` bytes)** and `vfs.MaxSize` (`1 << 62` bytes).
- **Format-v2 immutability guarantee:** The persistent prolly/Merkle tree layout (`tree.go`, `record.go`, `shard.go`, `spec_test.go`, `pin_test.go`) and on-disk object formats (`head`, `meta/`, `pack/`) are **completely untouched**.
- **Clean-room & dependency audit:** All code in `internal/index/rask/` and `tools/rask-bench/` is an independent Apache-2.0 Go implementation written from first principles using **100% Go standard library only** (`go.mod` and `go.sum` are unmodified).

---

## 2. Motivation in `hanzoai/vfs` Sparse-File and Journal-Span Workloads

In `hanzoai/vfs` (`file.go`), each open regular file maintains three logical content layers:

1. Settled chunk extents (`fileState.ext []extent`, sorted by `uint64` offset).
2. Frozen generation writes (`fileState.fz *layer`), immutable during background publication.
3. Live unsettled writes (`fileState.lv layer`), holding `dirty []span` and `void []run` as flat sorted slices.

While `hanzoai/vfs` already handles sparse files up to `1 PiB` (`TestSparsePetabyte` in `sparse_test.go`) without materializing holes in persistent shards, in-memory extent/span tracking exhibits three fundamental trade-offs at scale:

- **Flat sorted slices (`layer.dirty`, `layer.void`):** Minimal header overhead for tiny files (`<= 16` extents), but random writes, overwrites, and `File.Punch` (`file.go:474-552`) incur `O(N)` slice shifts/rebuilds—collapsing to `~4.0 K ops/s` at `100,000` extents.
- **Idiomatic pointer B-trees:** Restore `O(log N)` mutation throughput, but uncompressed 24-byte `Extent` entries plus node headers, child-pointer arrays, and ~69% average leaf fill consume **`52.38 B/extent`** structural memory (**`58.38 B/extent`** real Go `HeapAlloc`) and retain **`26,240` Go heap pointers** per 100,000 extents (amplifying L1/L2 cache misses and Go GC mark-phase scanning).
- **Dense fixed-page tables (negative control):** Fail catastrophically on sparse files—a 4 KiB dense page table for a `1 PiB` sparse file requires **`2.03 TiB`** of RAM (`2,233,382,993,920` bytes) regardless of how few extents were written.

`TestSmallWritesAmplifyMemory` (`sparse_test.go:54`) explicitly documents why per-span memory overhead (`spanCost = 64` bytes/span) becomes the dominant memory pressure for fragmented unsettled writes.

---

## 3. Clean-Room Architecture (`internal/index/rask/`)

`RASKIndex` implements the shared `RangeIndex` interface (`Insert`, `DeleteRange`, `Lookup`, `RangeScan`, `Len`, `MemoryBytes`, `PointerCount`) with exact canonical interval semantics (half-open `[Start, End)`, strictly sorted, pairwise disjoint, globally coalesced across adjacent identical-`Val` runs—even across leaf and group boundaries):

1. **Zero-Pointer Packed-Delta Leaf Blocks (`raskLeaf`, max 128 extents):**
   - Each leaf stores all extent payloads in a single pointer-free `[]uint32` slice (allocated in Go's `noscan` spans—zero GC traversal inside leaf data).
   - Adaptive per-leaf encoding automatically selects the tightest lossless mode:
     - `modeCompact32` (**12 B/extent**): `[relStart, length, relVal]` relative to `(baseOff, baseVal)` when `maxEnd - baseOff <= math.MaxUint32` and `maxVal - minVal <= math.MaxUint32`.
     - `modeCompact64` (**16 B/extent**): `[relStart, length, valLo, valHi]` when offsets fit in a 4 GiB window with arbitrary 64-bit random values.
     - `modeFull64` (**24 B/extent**): full 64-bit `[start, end, val]` fallback for individual extents exceeding 4 GiB.
   - Single-leaf overwrites and hole punches repack **in place** (`packLeafInto`) using stack-allocated scratch buffers (zero heap allocation); splits partition **50/50 evenly** within 4 GiB windows and split cleanly across `>4 GiB` sparse gaps so petabyte clusters stay in `modeCompact32`.
2. **Two-Level Cache-Aligned Directory (`raskGroup`, max 64 leaves/group):**
   - Leaf headers (`raskLeaf`) are stored **by value** inside contiguous group slices (no per-leaf heap pointer chains), paired with a dense `leafMaxEnd []uint64` fence slice (8 bytes/leaf) for L1-resident binary search.
3. **Open-Addressed 4 GiB Segment Radix Filter (`radix []radixSlot`):**
   - Maps `seg = uint32(offset >> 32)` (covering up to 16 EiB) to `[firstGroup, lastGroup]` in a compact pointer-free open-addressed table (256 B minimum).
   - Provides **`O(1)` sparse-hole rejection (`13.6 ns` p50)** across 1 PiB empty gaps without touching any leaf block, while remaining strictly read-only during `Lookup` and `RangeScan` for race-free concurrent `RLock` reads.
4. **Concurrency & Multi-Tenant Isolation (`concurrent.go`):**
   - `ConcurrentIndex` (`sync.RWMutex`) and `TenantRegistry` (per-tenant/per-inode isolated `RASKIndex` map) provide concurrent multi-tenant read/write isolation verified under `go test -race`.

---

## 4. Matched 4-Baseline Experimental Results

All four baselines implement the exact `RangeIndex` contract and are verified bit-identical by randomized differential testing. Full tables and raw JSON live in [`score_table.md`](score_table.md) and [`raw_results.json`](raw_results.json).

Measured on `linux/amd64`, `go1.27.1`, `2` CPUs:

### A. 100,000 Sparse-Run Extents (4 KiB Data / 4 KiB Hole, Compact Values)

| Baseline | Struct B/Ext | Real Heap B/Ext | GC Pointers | Hit p50 / p99 (ns) | Hole p50 / p99 (ns) | RangeScan (M ext/s) | Random Insert (K ops/s) | PunchHole (K ops/s) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `sorted_slice_baseline` | 29.49 | 29.49 | 1 | 161.8 / 1634.3 | 159.4 / 393.0 | 292.84 | 4.0 | 13.9 |
| `pointer_btree_baseline` | 52.38 | 58.38 | 26,240 | 212.2 / 792.2 | 212.7 / 803.3 | 211.38 | 2,715.9 | 2,541.0 |
| **`rask_compact_range_index`** | **12.53** | **12.60** | **832** | **154.8 / 390.0** | **151.6 / 373.5** | **221.99** | **859.4** | **1,135.9** |
| `flat_page_bitmap_negative` | 16.25 (capped) | n/a | n/a | n/a | n/a | n/a | OOM_CAPPED (1.55 MiB @ 781 MiB) | n/a |

**Key measured wins (`rask_compact_range_index` vs `pointer_btree_baseline`):**
- **-76.1% structural memory** (`12.53` vs `52.38 B/extent`; **4.18x smaller**).
- **-78.4% real Go `HeapAlloc`** (`1.20 MiB` vs `5.57 MiB`; **4.63x smaller**).
- **-96.8% GC heap pointers** (`832` vs `26,240` pointers; **31.5x fewer**).
- **-27.0% p50 / -50.8% p99 hit lookup latency** (`154.8 / 390.0 ns` vs `212.2 / 792.2 ns`) and **-53.5% p99 hole lookup latency** (`373.5 ns` vs `803.3 ns`).
- **+5.0% faster range-scan throughput** (`221.99` vs `211.38 M ext/s`) and **215x faster random inserts** than `sorted_slice_baseline` (`859.4` vs `4.0 K ops/s`).

Even with **arbitrary 64-bit uncompressible random values** (`modeCompact64`, 100,000 extents), RASK uses **`16.53 B/extent`** (`16.60 B/extent` real heap)—**68.4% smaller than `pointer_btree_baseline`** and **43.9% smaller than `sorted_slice_baseline`**—while cutting hit p99 latency by **-66.4%** (`413.9 ns` vs `1,231.4 ns`).

### B. 1 PiB (`1 << 50` B) Sparse-File Stress Workload (101 Extents Across `0`, `1 TiB`, `256 TiB`, `512 TiB`, `1 PiB - 4 KiB`)

| Baseline | Struct Memory | GC Pointers | Hit p50 / p99 (ns) | 1 PiB Hole p50 / p99 (ns) | Status |
|---|---:|---:|---:|---:|---|
| `sorted_slice_baseline` | 3.02 KiB (3,096 B) | 1 | 47.7 / 61.3 | 14.6 / 18.4 | ok |
| `pointer_btree_baseline` | 5.29 KiB (5,416 B) | 26 | 46.3 / 63.0 | 15.3 / 21.2 | ok |
| **`rask_compact_range_index`** | **2.75 KiB (2,816 B)** | **9** | **52.7 / 77.7** | **13.6 / 18.7** | **ok** |
| `flat_page_bitmap_negative` | **2.03 TiB** | n/a | n/a | n/a | `OOM_IMPOSSIBLE` |

---

## 5. Documented Honest Negative and Boundary Regimes

1. **`dense_tiny_random_100pct_fragmented_writes` (25,000 random 4 KiB writes with distinct uncoalescable 64-bit values):**
   - Because every write forces leaf repacking/splitting across 128-entry packed-delta blocks rather than mutating 32-entry uncompressed arrays, `rask_compact_range_index` achieves **`1,191.5 K ops/s`** vs `pointer_btree_baseline`'s **`2,831.2 K ops/s`** (2.38x lower peak random-split write rate, though still 2.3x faster than `sorted_slice_baseline` at `514.5 K ops/s`).
   - Even in this write-hostile regime, RASK retains **-48.2% memory/extent** (`18.78` vs `36.28 B/ext`), **-93.7% GC pointers** (`201` vs `3,170`), and **-12.5% lookup p99** (`273.2` vs `312.4 ns`).
2. **`tiny_file_under_16_extents` (8 extents):**
   - A flat `sorted_slice_baseline` uses **`216 B`** and **`8.5 ns` p50** vs RASK's **`544 B`** (group + radix header overhead) and **`18.3 ns` p50**.
   - **Production recommendation for `hanzoai/vfs`:** Keep tiny open files (`<= 16` spans/extents) in a flat `[]span` / `[]extent` slice and promote dynamically to `RASKIndex` once extent count exceeds 16.
3. **`flat_page_table_1pib_explosion`:**
   - Dense 4 KiB page tables scale linearly with logical file size: `64 MiB -> 130.00 KiB` (`47.3x` RASK), `1 GiB -> 2.03 MiB` (`756.4x`), `1 TiB -> 2.03 GiB` (`774,516x`), and `1 PiB -> 2.03 TiB` (`793,104,756x` RASK's `2,816 B`).

---

## 6. Verification and Reproducibility

```sh
# 1. Run full unit, 12,000-op differential fuzz, 1 PiB boundary, memory-per-extent,
#    and multi-tenant concurrent race tests:
go test -race -v -count=1 ./internal/index/rask/...

# 2. Verify zero regression on root hanzoai/vfs sparse-file tests (format-v2 untouched):
go test -short -run 'TestSparse' .

# 3. Static analysis & build check:
go vet ./internal/index/rask/... ./tools/rask-bench/...
go build -o /dev/null ./tools/rask-bench

# 4. Reproduce the benchmark score table and JSON artifacts:
go run ./tools/rask-bench -write-dossier docs/research/hip-1344-rask
go run ./tools/rask-bench -json
```
