# HIP-1344 Research Dossier: Selective Hot-Data Mirroring for Immutable Shared Toolchains (`MOST`, USENIX FAST '26)

- **Tracking Issue:** `hanzoai/vfs#62` — *Selective hot-data mirroring · vfs (MOST)*
- **Umbrella HIP:** [HIP-1344: Agent-Optimized Storage and Recovery](https://hips.hanzo.ai/docs/hip-1344-agent-optimized-storage-and-recovery/)
- **Implementation:** `internal/cache/most/` (`keys.go`, `nvme.go`, `sketch.go`, `engine.go`, `bench.go`, `most_test.go`)
- **Benchmark Runner:** `tools/most-bench/main.go`
- **Artifacts:** [`score_table.md`](./score_table.md), [`raw_results.json`](./raw_results.json)

---

## 1. Paper & License Audit

- **Primary Paper:** Kaiwei Tu (University of Wisconsin–Madison), Kan Wu (Google), Andrea C. Arpaci-Dusseau (University of Wisconsin–Madison), and Remzi H. Arpaci-Dusseau (University of Wisconsin–Madison), *"Getting the MOST out of your Storage Hierarchy with Mirror-Optimized Storage Tiering"*, 24th USENIX Conference on File and Storage Technologies (FAST '26), arXiv:2512.03279, [https://www.usenix.org/conference/fast26/presentation/tu](https://www.usenix.org/conference/fast26/presentation/tu).
- **Design Summary:** Traditional two-tier storage hierarchies force a tradeoff between **exclusive tiering** (space-efficient, but promoting/evicting data between RAM and NVMe incurs continuous migration writeback traffic and tail-latency spikes under skewed bursts) and **full mirroring** (good read load balancing, but wastes fast-tier capacity on cold or one-hit items). **MOST** selectively mirrors only a small hot subset in the fast tier (RAM) while leaving the authoritative copy resident on the capacity tier (NVMe). Because the authoritative copy remains on NVMe, evicting a mirrored block from RAM is a zero-writeback `O(1)` drop with instant NVMe fallback, while a frequency+recency admission filter prevents cold and sequential-scan bursts from polluting RAM.
- **License & Clean-Room Audit:**
  - The authors' academic prototype (*Cerberus*) builds on Meta's C++ CacheLib (Apache-2.0).
  - Everything in `internal/cache/most/`, `tools/most-bench/`, and `docs/research/hip-1344-most/` is an **independent clean-room Apache-2.0 Go implementation** written from first principles using only the Go standard library and existing `hanzoai/vfs` internal packages (`internal/blake3` and `internal/bufpool`).
  - `go.mod` and `go.sum` are completely untouched (zero new external dependencies).
  - No patent restrictions are known to affect clean-room content-addressed selective mirroring with Count-Min Sketch frequency admission over immutable base toolchains.
- **Canonical Stack Boundary Check:** All Hanzo stack boundaries remain strictly intact (`vfs -> hanzoai/s3 -> visor`, Cloud owning lifecycle & billing, Enso routing models, Zen generating outputs, Kai returning structured non-generative decisions). Reported figures in the FAST '26 paper belong strictly to the original authors' hardware and workloads and are **not** treated as Hanzo expectations; all numbers in this dossier are freshly measured in this repository.

---

## 2. Matched Baselines & Architecture

To evaluate selective RAM/NVMe mirroring cleanly within `hanzoai/vfs`'s cache architecture (`lru.go`, `internal/bufpool`, `internal/blake3`), `internal/cache/most/` provides a unified two-tier engine backed by:
- **Real RAM tier (`TierRAM`)**: Byte-bounded slice pools drawn from `internal/bufpool.Budget` (`sharedPool = budget.Pool(false)` with `*bufpool.Shared` zero-copy `Pin`/`Unpin`/`Release` sharing for immutable toolchains; `privatePool = budget.Pool(true)` with mandatory memory zeroing on release for tenant-private workspace blocks).
- **Real NVMe backing tier (`TierNVMe`)**: Dedicated on-disk `os.File` backing files (`shared_toolchains.nvme` and `private_workspaces.nvme`) accessed via concurrent positional `ReadAt` (`pread64`) and `WriteAt` (`pwrite64`) with explicit slot allocation/reclamation and eviction writeback accounting.
- **Cryptographic Verification (`internal/blake3` / `crypto/sha256`)**: Every immutable shared toolchain block is attested in a `Manifest` by `(ToolchainID, ContentHash)` and verified in constant time on NVMe reads before serving or mirroring.

All four policies execute under the **exact same `HostRAMBudget`**, same 8-shard concurrency layout, same `runtime.GC()` pre-run barrier, and identical pre-generated deterministic per-agent traces:

1. **`private_per_agent_lru_baseline`**: Each of the `N = 32` agents gets an isolated per-agent LRU capped at `HostRAMBudget / N`. Hot immutable toolchain blocks are duplicated up to `N` times across agent caches.
2. **`ordinary_tier_migration_exclusive`**: Classic exclusive RAM <-> NVMe tiering. Each block resides in either RAM or NVMe, never both. Promoting from NVMe frees its NVMe slot; evicting a victim from RAM demotes it back to NVMe via real `os.File.WriteAt` (`WriteEviction`), generating NVMe writeback churn and tail-latency contention.
3. **`most_selective_hot_mirroring`**: MOST-inspired selective mirroring. Attested immutable toolchain blocks stay permanently resident on `shared_toolchains.nvme`; only hot blocks passing a 4-bit Count-Min Sketch frequency+recency gate (`freq >= MinMirrorHits` and `freq > victimFreq`) are mirrored in the host-shared RAM pool (`MirrorFraction` of `HostRAMBudget`). RAM eviction is a zero-writeback `O(1)` drop (`0.00 MiB` NVMe eviction writes).
4. **`mirror_all_unselective_negative`**: Documented negative control that mirrors every read (including cold, one-hit, and scan-burst blocks) blindly into RAM without frequency admission filtering.

### Hard Tenant Isolation Gate

Multi-tenant safety is enforced structurally and cryptographically across **all** policies:
- **Disjoint key types & backing files**: Shared immutable blocks use `SharedToolchainKey{ToolchainID, ContentHash}` and `shared_toolchains.nvme`; private tenant blocks use `PrivateBlockKey{TenantID, VolumeID, BlockID}` and `private_workspaces.nvme`. Private blocks are structurally unrepresentable in the host-shared mirror pool.
- **Fail-closed pre-lookup authorization**: `ReadPrivate` and `RegisterPrivateBlock` validate `(ctx.TenantID, ctx.VolumeID)` against `(key.TenantID, key.VolumeID)` in constant time **before** consulting any cache map or NVMe slot table, returning `ErrTenantAccessDenied` identically for existing and non-existing cross-tenant keys (zero existence or timing oracle).
- **Attestation enforcement**: `ReadShared` rejects any key not present in the immutable `Manifest` (`ErrUnattestedBlock`) and rejects corrupted backing bytes (`ErrDigestMismatch`).

---

## 3. Falsifiable Hypothesis & Expected Failure Modes

### Primary Hypothesis
Under a `32`-agent concurrent burst reading Zipfian-skewed immutable shared toolchains (`70%`), cold toolchain scan bursts (`15%`), and hot per-tenant private workspace blocks (`15%`) under a fixed `16 MiB` host RAM budget (`64 MiB` on-disk NVMe corpus):
- `most_selective_hot_mirroring` will achieve **lower `p99` read latency**, **lower resident RAM per agent**, **higher RAM hit rate** than `private_per_agent_lru_baseline`, and **strictly `0.00 MiB` NVMe eviction writeback traffic** (vs `> 0 MiB` for `ordinary_tier_migration_exclusive`), with **`0` cross-tenant leaks**.

### Documented Expected Failure Modes & Negative Regimes
1. **`uniform_flat_access_no_hot_skew`**: When toolchain reads are uniformly random (`s = 0`, zero hot-set skew) over a working set `4x` larger than host RAM, selective mirroring **cannot** achieve high RAM hit rate or escape NVMe read latency — it honestly falls back to NVMe read latency while avoiding useless RAM bloat and NVMe writeback churn.
2. **`mutable_private_workspace_heavy_workload`**: When `90%` of reads target per-tenant mutable private workspace blocks (strictly barred from host-shared mirroring for tenant isolation), MOST's resident RAM per agent **must** rise toward the `private_per_agent_lru_baseline` ceiling — proving security isolation bounds host memory deduplication.
3. **`mirror_all_ram_thrash_collapse`**: Under alternating hot Zipfian toolchain reads (`55%`) and contiguous `48`-block cold scan bursts (`40%`), unselective mirroring (`mirror_all_unselective_negative`) saturates host RAM, evicts hot toolchain blocks, and suffers severe `p99` latency collapse, whereas MOST's frequency admission gate rejects one-hit scan blocks.

---

## 4. Empirical Measurements (`tools/most-bench`)

Measured on `linux/amd64`, Go `1.27.1`, `2` CPU, `32` concurrent agents, `32 KiB` blocks, real `internal/bufpool` RAM slices + real `os.File` `ReadAt`/`WriteAt` NVMe files + `internal/blake3` digest verification. Full machine-readable output is in [`raw_results.json`](./raw_results.json) and full tables in [`score_table.md`](./score_table.md).

### A. Primary Scenario: `skewed_multi_agent_toolchain_burst` (`64,000` concurrent ops, `16.0 MiB` RAM cap)

| Policy | p50 (µs) | p95 (µs) | p99 (µs) | Resident RAM/Agent (MiB) | Total Host RAM (MiB) | RAM Hit Rate (%) | NVMe Eviction Writes (MiB) | Throughput (ops/s) | Cross-Tenant Leaks |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `private_per_agent_lru_baseline` | 17.37 | 46.34 | 4216.89 | 0.500 | 16.00 | 42.40% | 0.00 | 109,799 | 0 |
| `ordinary_tier_migration_exclusive` | 2.14 | 44.32 | 3774.00 | 0.500 | 16.00 | 74.53% | 493.44 | 171,300 | 0 |
| **`most_selective_hot_mirroring`** | **1.88** | **21.32** | **65.57** | **0.375** | **12.00** | **73.52%** | **0.00** | **261,448** | **0** |
| `mirror_all_unselective_negative` | 2.17 | 39.67 | 5041.20 | 0.500 | 16.00 | 74.64% | 0.00 | 213,809 | 0 |

**Key Primary Findings:**
- **p99 read latency:** `65.57 µs` vs `4216.89 µs` (`private_per_agent_lru_baseline`, **-98.4%**) and `3774.00 µs` (`ordinary_tier_migration_exclusive`, **-98.3%**).
- **Resident memory per agent:** `0.375 MiB/agent` (`12.00 MiB` host total) vs `0.500 MiB/agent` (`16.00 MiB` host total) across all three other policies (**-25.0% resident RAM/agent**) while matching exclusive migration's RAM hit rate (`73.52%` vs `74.53%`) in half the shared RAM (`4 MiB` vs `8 MiB`).
- **NVMe eviction writeback elimination:** `0.00 MiB` for MOST vs **`493.44 MiB`** of demotion writeback traffic for `ordinary_tier_migration_exclusive` (**-100%** write amplification on eviction).
- **Throughput:** `261,448 ops/s` (**2.38x** private LRU, **1.53x** exclusive migration, **1.22x** mirror-all).

### B. Negative / Boundary Control Summary

| Negative / Boundary Scenario | Policy | p50 (µs) | p99 (µs) | Resident RAM/Agent (MiB) | RAM Hit Rate (%) | NVMe Eviction Writes (MiB) | Observation |
|---|---|---:|---:|---:|---:|---:|---|
| `uniform_flat_access_no_hot_skew` | `most_selective_hot_mirroring` | 19.77 | 163.65 | 0.125 | **7.26%** | **0.00** | Honest negative: low hit rate at NVMe p50 latency when skew=0, but 0 eviction writes vs `958.16 MiB` for exclusive migration (`p99 = 15628.95 µs`). |
| `mutable_private_workspace_heavy_workload` | `most_selective_hot_mirroring` | 1.23 | 34.12 | **0.438** | 81.56% | 0.00 | Honest boundary: 90% private reads bar shared dedup, raising RAM/agent to `87.5%` of `private_per_agent_lru_baseline` (`0.500 MiB/agent`) with `0` leaks. |
| `mirror_all_ram_thrash_collapse` | `mirror_all_unselective_negative` | 2.36 | **6307.34** | 0.250 | 60.12% | 0.00 | Negative control collapse: 48-block cold scans thrash RAM (`p99 = 6307.34 µs`), while MOST holds `p99 = 67.25 µs` at `0.188 MiB/agent` and `60.81%` hit rate. |

---

## 5. Evidence, Safety & Reproduction

### Verification & Race-Free Concurrency Tests
`internal/cache/most/most_test.go` verifies:
- Bit-exact integrity and cryptographic digest verification (`BLAKE3` and `SHA-256`), including failing closed with `ErrDigestMismatch` on on-disk NVMe tampering.
- Hard tenant isolation across all 4 policies (`ErrTenantAccessDenied` on cross-tenant existing/missing reads, empty tenant context, mismatched volume; `ErrUnattestedBlock` on forged private digests; `AuditSharedTierIntegrity` verifying zero private entries in shared RAM).
- Zero-writeback eviction invariant (`NVMeEvictionWriteOps(MOST) == 0` vs `> 0` for `ordinary_tier_migration_exclusive`).
- Primary comparative assertions and all three honest negative/boundary regimes.
- 32-agent concurrent burst stress with interleaved cross-tenant adversarial probes under `go test -race`.

### Reproduction Commands
```sh
go test -race -v -count=1 ./internal/cache/most/...
go vet ./internal/cache/most/... ./tools/most-bench/...
go run ./tools/most-bench
go run ./tools/most-bench -out-dir docs/research/hip-1344-most
```

---

## 6. HIP-1344 Status Update & Integration Recommendation

- **Status:** **Validated (Positive under skewed shared toolchains; bounded honestly by uniform-access and private-heavy regimes).**
- **Recommendation for `hanzoai/vfs`:** Adopt MOST-style selective hot mirroring strictly for **immutable, content-addressed base-image / toolchain chunks** (attested by `BLAKE3` content digest from read-only base snapshots, complementing `pkg/cofs` read-only metadata indexing under HIP-1342), backed by host NVMe cache files and `internal/bufpool.Shared` slices. Keep mutable per-sandbox `Volume` chunks and shards strictly inside isolated per-tenant `bufpool.Pool(true)` (zeroing) caches so tenant isolation invariants remain unconditional.
