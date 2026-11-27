# HIP-1345 Research Dossier: Selective Hot-Data Mirroring for Shared Toolchains (MOST)

- **Issue:** `hanzoai/vfs#50` (`[p2, perf, research, bounty, hip-1345]`)
- **Primary Paper:** Kaiwei Tu, Kan Wu, Andrea C. Arpaci-Dusseau, and Remzi H. Arpaci-Dusseau, *"Getting the MOST out of your Storage Hierarchy with Mirror-Optimized Storage Tiering"*, 24th USENIX Conference on File and Storage Technologies (FAST '26), Santa Clara, CA, February 24–26, 2026, pp. 561–578, ISBN 978-1-939133-53-3. URL: https://www.usenix.org/conference/fast26/presentation/tu
- **Implementation:** Clean-room Apache-2.0 Go package [`pkg/most`](../../../pkg/most) and reproducible benchmark CLI [`cmd/most-bench`](../../../cmd/most-bench)
- **Raw Data:** [`results.json`](./results.json) (deterministic `--seed 1345`)
- **Canonical Architecture:** `vfs -> Hanzo S3 -> Visor` (unchanged; zero new services, zero wire/on-disk format changes to `head`, `meta/`, or `pack/`)

---

## 1. Primary Reference, Summary, and License Audit

### 1.1 Paper Summary (MOST, FAST '26)
Modern multi-tier storage hierarchies (DRAM/RAM `L1` + Local NVMe `L2` + Remote Object Storage `L3`) face a classic structural tension:
- **Exclusive cache-tier migration** avoids storing duplicate copies across tiers (maximizing raw capacity), but whenever the active working set exceeds `L1`, every `L1` miss triggers both a promotion read from `L2` and a **demotion writeback** of the evicted victim back to `L2`. Under concurrent multi-agent bursts, demotion writes saturate NVMe write queues, amplify flash wear, and inject severe **p99 read tail-latency spikes**.
- **Full mirroring / naive RAM pinning** eliminates demotions and minimizes read latency, but destroys memory efficiency (`O(working_set)` RAM or `O(M)` RAM if unshared per agent), causing OOM pressure on dense multi-agent Visor hosts.

**Mirror-Optimized Storage Tiering (MOST)** resolves this tension by combining tiering's space efficiency with mirroring's load-balancing and zero-demotion properties: it retains the canonical copy of data on the lower local tier (`L2` NVMe) and **selectively mirrors only a small hot subset** into the upper tier (`L1` RAM). In `hanzoai/vfs` running under `hanzoai/visor`, this insight maps directly onto **shared immutable developer toolchains** (Go stdlib, Rust core/sysroot, Node runtime, Python site-packages):
1. All `M` concurrent agents on a node read the same Zipf-skewed hot subset of immutable toolchain blocks.
2. Keeping the canonical toolchain blocks on node-local NVMe (`L2`, populated on cold miss from Hanzo S3 `L3`) and selectively mirroring only top-hotness, content-addressed immutable blocks into **one deduplicated read-only RAM mirror** eliminates **100% of NVMe demotion writeback IOPS** (`O(1)` instant drop from RAM on eviction) while amortizing RAM per agent as **`O(hot_set / M)`**.

### 1.2 License & Clean-Room Audit
- **Paper & Upstream Artifact Status:** The USENIX FAST '26 paper is Open Access; the authors' prototype (*Cerberus*) is a C++ CacheLib-based user-level research layer. No upstream C++ code or third-party CacheLib source was copied or translated.
- **Clean-Room Implementation:** Every file in `pkg/most/` and `cmd/most-bench/` is an independent, clean-room **Apache-2.0** Go implementation authored from first principles.
- **Zero New Dependencies:** `go.mod` and `go.sum` are **completely untouched**. The implementation uses only the Go standard library plus packages already present in `go.mod`: `github.com/zeebo/blake3`, `github.com/luxfi/age`, `github.com/hanzoai/vfs`, `github.com/hanzoai/vfs/pkg/backend`, `github.com/hanzoai/vfs/pkg/backend/file`, and `github.com/hanzoai/vfs/pkg/backend/s3`.
- **Cross-Platform Pure-Go Build:** Compiles cleanly with `CGO_ENABLED=0` for both `linux/amd64` and `darwin/arm64`.

---

## 2. Reproducible Baseline & Experimental Methodology

### 2.1 Hardware & Runtime Environment
All experiments were executed on a single reproducible host configuration (recorded in `results.json`):
- **OS / Arch / CPUs:** `linux/amd64`, `2` vCPU (`Intel Xeon @ 2.30 GHz`), `7.8 GiB` RAM
- **Go Toolchain:** `go1.27.1`
- **RNG Seed:** `1345` (deterministic `math/rand/v2` PCG streams per agent)
- **Storage Stack (Canonical `vfs -> Hanzo S3 -> Visor`):**
  - **L1 (RAM Tier):** In-process CLOCK-LRU / selective mirror (`pkg/most/ram.go`) with parallel `sync.RWMutex` `RLock` reads and `HotnessTracker` (`32.125 KiB` exact Count-Min Sketch metadata).
  - **L2 (Local NVMe Tier):** Live `pkg/backend/file` on local disk wrapped by `NVMeTier` (`4` bounded I/O channels modeling NVMe queue contention, calibrated `80 µs` read service floor and `320 µs` write/program service floor).
  - **L3 (Remote Hanzo S3 Tier):** Live `pkg/backend/s3` HTTP client talking to a local path-style S3 HTTP server (`If-None-Match` / `If-Match` / `GET` / `PUT` / `DELETE`).

### 2.2 Raw Storage Baseline on Identical Hardware
Before running multi-agent tiering sweeps, `cmd/most-bench` measures direct `16 KiB` block operations against the exact `pkg/backend/file` (NVMe `L2`) and live HTTP `pkg/backend/s3` (`L3`) backends on this host:

| Backend Operation (`16 KiB` block, `n = 64`) | p50 (`µs`) | p99 (`µs`) | Notes |
|---|---:|---:|---|
| Local NVMe (`pkg/backend/file`) direct `Get` + BLAKE3 verify | 53.2 | 152.4 | OS page-cache / filesystem read + BLAKE3-256 |
| Local NVMe (`pkg/backend/file`) direct `Put` (atomic temp+rename+flock) | 5,221.3 | 11,245.1 | Real durable-style atomic rename write cost |
| Live HTTP Hanzo S3 (`pkg/backend/s3`) `Get` + BLAKE3 verify | 216.6 | 4,563.5 | Localhost HTTP client round-trip (0 ms WAN RTT) |

> **Baseline Comparison to FAST '26 MOST:** Tu et al. evaluate Cerberus on multi-tier NVMe/SSD/DRAM stacks and report that selective mirroring avoids migration write amplification and raises throughput/tail-latency stability under skewed I/O-intensive workloads. Our Hanzo hardware baseline reproduces the same underlying asymmetry: local atomic NVMe writes (`5.22 ms` p50, `11.25 ms` p99 on `pkg/backend/file` + `320 µs` channel service time) are **~74–98× slower** than local reads (`53.2 µs` p50), so any tiering regime that triggers demotion writebacks on RAM eviction severely degrades concurrent read p99 latency.

### 2.3 Deterministic Shared Toolchain Corpus
Grounded in real repository files plus deterministic synthetic compiler/runtime ELF blocks (`--seed 1345`):
- **Real Repository Files Sampled:** `302` files (`*.go`, `*.md`, `*.mod`, `*.sum`, `*.s`, `*.json`, `*.yml`, `*.sh`) -> `358` unique `16 KiB` blocks
- **Synthetic Compiler/Runtime ELF & Header Blocks:** `154` unique `16 KiB` blocks
- **Total Corpus:** `512` blocks × `16 KiB` = **`8.00 MiB`** across 3 allowlisted namespaces (`go-1-27-stdlib`, `rust-1-85-core`, `node-22-runtime`)
- **Corpus BLAKE3 Root Digest:** `818cd5c0a630bf8276670d13ca43fa2c8869abc6db4f9e8eee727300d3ac8ce2`
- **Shared RAM Budget (`B`):** `96` blocks = **`1.50 MiB`** (`18.75%` of working set)
- **Workload:** `2,400` total read operations per run across `M ∈ {1, 4, 16, 32}` concurrent agents following a Zipf distribution (`s = 1.18`, `v = 1.0`), with **100% BLAKE3-256 verification on every read**.

---

## 3. Falsifiable Hypothesis, Metrics, and Expected Failure Modes

### 3.1 Hypothesis
> **H1 (MOST Selective Mirroring for Shared Toolchains):** On a multi-agent Visor node (`M ≥ 4` concurrent agents) sharing an immutable Zipf-skewed toolchain whose working set exceeds node RAM budget (`W > B`), selectively mirroring only top-hotness read-dominant immutable blocks into a single deduplicated RAM mirror (`most_selective_mirror`) while keeping canonical blocks on NVMe will:
> 1. Reduce **p99 read latency by ≥ 50%** vs ordinary exclusive cache-tier migration (`exclusive_tier_migration`) by eliminating **100% of NVMe demotion writeback IOPS** (`NVMeDemotionWrites == 0`), and
> 2. Amortize **RAM per agent as `O(B / M)`** — achieving **≥ 4× lower RAM/agent** at `M ≥ 16` than unshared per-agent caches (`per_agent_private_cache`) and **≥ 2× lower RAM/agent** than `naive_full_ram_pin` while staying strictly within RAM budget `B`.

### 3.2 Falsification Criteria & Expected Failure Modes
The hypothesis is **falsified or bounded** under three explicit regimes (all evaluated in Section 6):
1. **Failure Mode 1 — Uniform / Flat-Hotness Scan (`neg_uniform_no_skew_scan`, Zipf `α = 0`):** Without access skew, no hot subset exists; mirror hit rate collapses to `≤ B/W` (~17–19%), admission filtering rejects most accesses, and `HotnessTracker` adds `+32.125 KiB` metadata overhead without raising RAM hit rate.
2. **Failure Mode 2 — Write-Heavy Mutable Workloads (`neg_write_heavy_invalidation_churn`):** Mirroring mutable blocks across RAM + NVMe requires synchronous NVMe write-through and RAM invalidation on every write, producing **worse p99 latency and higher NVMe writeback IOPS** than write-back cache-tier migration.
3. **Failure Mode 3 — Single-Agent (`M = 1`) / Ample-RAM (`B ≥ W`) Nodes (`neg_single_agent_ample_ram`):** When a single agent's entire working set already fits in RAM, ordinary caching incurs `0` demotions, so selective mirroring saves `0` RAM/agent while adding tracker metadata overhead and `MinHits` warm-up misses.

---

## 4. Clean-Room Implementation (`pkg/most/`)

The bounded library `pkg/most/` implements all four matched tiering regimes behind a single `Engine` abstraction over `pkg/backend.Backend` and `vfs.Cipher`:

1. **`types.go` — Addressing & Strict Security Schema:**
   - Separates `ClassSharedToolchain` (content-addressed BLAKE3-256 key `tc/<ns>/<2hex>/<hex>`, empty `Scope`, allowlisted `ToolchainNamespace`) from `ClassPrivateWorkspace` (tenant-scoped key `priv/<tenant>/<vol>/<key>`, non-empty `Scope{TenantID, VolumeID}`).
   - Enforces `backend.ValidKey` grammar and strict `[a-z0-9-]` segment validation (zero path traversal or namespace escape).
2. **`tracker.go` — Bounded `HotnessTracker`:**
   - Fixed `4 × 2048` 16-bit Count-Min Sketch for reads + separate write sketch (`32,896 bytes` = `32.125 KiB`, explicitly charged to MOST's memory footprint).
   - Periodic counter halving (`DecayInterval = 8192`), read-dominance filter (`MinReadDominance = 0.85`), and EWMA victim-hotness dynamic admission threshold to resist one-hit scan pollution.
3. **`ram.go` — Parallel-Read CLOCK-LRU RAM Tier:**
   - Hot RAM hits acquire only `sync.RWMutex.RLock` and mark an atomic reference bit, enabling parallel multi-agent reads without write-lock contention.
   - `TryAdmitMirror` performs **`O(1)` instant-drop eviction** (`0` NVMe writeback IOPS) because NVMe already holds the clean canonical block.
   - `PutExclusive` tracks in-flight demoting victims in a visibility map so concurrent reads during demotion never observe lost blocks.
   - All reads and writes perform defensive byte copies so caller buffer mutations cannot corrupt shared RAM.
4. **`singleflight.go` — Detached Single-Flight Coalescing:**
   - Coalesces concurrent L2/L3 misses for the same block onto a single detached fetch context (`context.WithTimeout(context.Background(), ...)`).
   - Canceling the initiating caller immediately returns `context.Canceled` to that caller **without aborting live concurrent waiters**; only when active waiter count reaches `0` is the detached flight canceled.
5. **`nvme.go` & `engine.go` — 3-Tier Engine & Per-Volume PQ Crypto:**
   - Local NVMe `L2` wraps real `pkg/backend/file` with a bounded channel semaphore (`Channels = 4`) so demotion writes and concurrent reads contend realistically.
   - Private workspace blocks (`PutPrivate` / `ReadPrivate`) are sealed per-volume with `vfs.Hybrid` (`X25519 + ML-KEM-768` via `github.com/luxfi/age`) before hitting NVMe or S3 and are **strictly forbidden** from entering the shared RAM mirror (`ErrTenantIsolation`).

---

## 5. Correctness, Concurrency, Security, and Regression Evidence

All tests execute cleanly with the Go race detector (`go test -v -race -count=1 ./pkg/most/... ./cmd/most-bench/...`) and cross-compile with `GOOS=darwin GOARCH=arm64 CGO_ENABLED=0`:

| Test Suite / Check | Verification Scope | Result |
|---|---|---|
| `TestAllFourRegimesLiveFileAndS3` (4 subtests) | All 4 regimes over live `pkg/backend/file` + live HTTP `storetest.NewS3` (`pkg/backend/s3`), warm-NVMe + cold-S3 paths, 100% BLAKE3 bit-exactness | **PASS** |
| `TestMOSTZeroDemotionWritebackInvariant` | Proves MOST incurs `0` NVMe demotion writes & `MirrorInstantDrops > 0` under hot-set shift, vs `NVMeDemotionWrites > 0` for migration | **PASS** |
| `TestMOSTOutperformsMigrationTailLatencyUnderMultiAgentBurst` | `M = 16` concurrent Zipf burst: proves MOST p99 (`12.24 ms`) < migration p99 (`143.77 ms`) with `0` demotions | **PASS** |
| `TestPerAgentMemoryScalingAmortization` | `M = 16` agents: proves MOST amortizes RAM/agent (`O(hot_set/M)`) vs `O(M)` linear scaling in `per_agent_private_cache` | **PASS** |
| `TestNaiveFullPinStrictCapOOM` | Proves `naive_full_ram_pin` returns `ErrRAMExceeded` under strict cap and flags `OverBudget == true` when uncapped | **PASS** |
| `TestCrossTenantIsolationAndSharedMirrorBoundary` | Proves private blocks fail `PutShared`/`ReadShared`/`AttemptMirrorPrivate` and cross-tenant `ReadPrivate`/`PutPrivate` with `ErrTenantIsolation` | **PASS** |
| `TestNamespaceAllowlistEnforcement` | Rejects non-allowlisted toolchain namespaces with `ErrNamespaceDenied` | **PASS** |
| `TestPrivateWorkspaceHybridPQEncryptionLiveS3` | Verifies `age-encryption.org/v1` + `-> mlkem768x25519` stanzas on both NVMe and live S3, `0` plaintext bytes on backends, and wrong-tenant PQ decryption failure | **PASS** |
| `TestCorruptionRejectionNVMeAndS3` | 1-bit flip on NVMe or S3 returns `ErrCorruptBlock`, increments `CorruptionRejects`, and never caches corrupt bytes | **PASS** |
| `TestSingleFlightCancelIsolation` (2 subtests) | Initiator cancel does NOT abort live waiter (`SingleFlightCoalesced >= 1`); all-waiters cancel cleanly terminates detached flight | **PASS** |
| `TestDefensiveCopyImmutability` | In-place mutation of caller read buffer does not corrupt shared RAM mirror | **PASS** |
| `TestHotnessTrackerAgingAndScanResistance` | Verifies CMS decay, write-heavy rejection (`readRatio < 0.85`), and scan resistance against one-hit eviction | **PASS** |
| `TestMultiAgentConcurrentRaceStress` (2 subtests) | `M = 32` concurrent agents interleaving shared reads + per-tenant PQ private reads/writes under `-race` | **PASS (0 races)** |
| `TestNegativeRegime1UniformNoSkewScan` | Confirms low hit rate (`<= 35%`) & non-zero tracker overhead under flat uniform scan | **PASS** |
| `TestNegativeRegime2WriteHeavyInvalidationChurn` | Confirms mutable mirroring causes invalidations & higher writeback IOPS than write-back migration, and production MOST blocks mutable mirroring | **PASS** |
| `TestNegativeRegime3SingleAgentAmpleRAM` | Confirms `M = 1` ample-RAM saves `0` RAM/agent and adds tracker metadata overhead | **PASS** |
| `TestBenchPipelineEndToEndAndNoPathLeaks` | End-to-end `cmd/most-bench` JSON schema, all 3 negative regimes, PQ check, and zero local path leaks | **PASS** |

---

## 6. Before/After Empirical Results (`results.json`, `--seed 1345`)

### 6.1 Primary Scaling Sweep: Shared Zipf Toolchain (`8.00 MiB` corpus, `1.50 MiB` RAM budget, `2,400` reads/run)

| Agents (`M`) | Regime | p50 (`µs`) | p95 (`ms`) | **p99 (`ms`)** | Total RAM (`MiB`) | **RAM / Agent (`MiB`)** | NVMe Demotions | NVMe WB (`MiB`) | RAM Hit Rate | Throughput (`MiB/s`) | Over Budget |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|:---:|
| 1 | `exclusive_tier_migration` | 6.8 | 11.08 | 14.78 | 1.5000 | 1.5000 | 452 | 7.06 | 77.17% | 8.17 | No |
| 1 | `naive_full_ram_pin` | 6.6 | 1.79 | 4.44 | 4.9531 | 4.9531 | 0 | 0.00 | 86.79% | 51.51 | **Yes (3.30×)** |
| 1 | `per_agent_private_cache` | 7.1 | 2.90 | 4.63 | 1.5000 | 1.5000 | 0 | 0.00 | 77.17% | 31.30 | No |
| 1 | **`most_selective_mirror`** | **6.8** | **2.78** | **4.36** | **1.5314** | **1.5314** | **0** | **0.00** | **77.96%** | **34.05** | **No** |
| 4 | `exclusive_tier_migration` | 8.7 | 24.91 | 34.39 | 1.5000 | 0.3750 | 495 | 7.73 | 75.04% | 13.18 | No |
| 4 | `naive_full_ram_pin` | 5.7 | 0.89 | 4.55 | 5.0156 | 1.2539 | 0 | 0.00 | 86.33% | 376.29 | **Yes (3.34×)** |
| 4 | `per_agent_private_cache` | 6.5 | 4.00 | 4.62 | 6.0000 | 1.5000 | 0 | 0.00 | 72.58% | 149.72 | **Yes (4.00×)** |
| 4 | **`most_selective_mirror`** | **6.2** | **4.00** | **4.71** | **1.5314** | **0.3828** | **0** | **0.00** | **76.92%** | **203.96** | **No** |
| 16 | `exclusive_tier_migration` | 7.8 | 82.22 | 95.60 | 1.5000 | 0.0938 | 455 | 7.11 | 74.88% | 15.85 | No |
| 16 | `naive_full_ram_pin` | 5.4 | 4.00 | 7.25 | 4.9063 | 0.3066 | 0 | 0.00 | 85.29% | 506.02 | **Yes (3.27×)** |
| 16 | `per_agent_private_cache` | 11.9 | 9.79 | 12.78 | 15.0469 | 0.9404 | 0 | 0.00 | 59.88% | 110.35 | **Yes (10.03×)** |
| 16 | **`most_selective_mirror`** | **6.5** | **4.83** | **6.40** | **1.5314** | **0.0957** | **0** | **0.00** | **76.83%** | **398.40** | **No** |
| 32 | `exclusive_tier_migration` | 8.5 | 149.89 | 166.92 | 1.5000 | 0.0469 | 437 | 6.83 | 72.79% | 16.14 | No |
| 32 | `naive_full_ram_pin` | 5.9 | 6.50 | 11.89 | 5.0156 | 0.1567 | 0 | 0.00 | 83.71% | 597.94 | **Yes (3.34×)** |
| 32 | `per_agent_private_cache` | 16.6 | 10.81 | 14.54 | 18.0000 | 0.5625 | 0 | 0.00 | 52.00% | 157.44 | **Yes (12.00×)** |
| 32 | **`most_selective_mirror`** | **6.1** | **8.40** | **12.82** | **1.5314** | **0.0479** | **0** | **0.00** | **75.83%** | **278.36** | **No** |

*Every single read across all 16 runs (`38,400 / 38,400` reads) passed bit-exact BLAKE3-256 verification (`0` corruption rejects, `0` isolation rejects).*

#### Key Quantitative Takeaways (Hypothesis H1 Validated)
- **p99 Read Latency vs Exclusive Cache-Tier Migration:**
  - At `M = 4`: **`-86.3%`** (`34.39 ms` -> `4.71 ms`, **`7.30×` faster**).
  - At `M = 16`: **`-93.3%`** (`95.60 ms` -> `6.40 ms`, **`14.93×` faster**), with **`25.1×`** higher aggregate throughput (`398.40` vs `15.85 MiB/s`).
  - At `M = 32`: **`-92.3%`** (`166.92 ms` -> `12.82 ms`, **`13.02×` faster**), with **`17.2×`** higher aggregate throughput (`278.36` vs `16.14 MiB/s`).
- **NVMe Write Amplification Eliminated:** `exclusive_tier_migration` wrote `437–495` demotion blocks (`6.83–7.73 MiB`) back to NVMe per run; `most_selective_mirror` incurred **`0` NVMe demotion writes (`0` writeback bytes)** across all agent counts via `O(1)` instant drops (`3–18` drops/run) while keeping `HotnessTracker` admission selectivity high (`416–481` cold rejections/run).
- **Memory per Agent Amortization:** `most_selective_mirror` stays within the `1.50 MiB` RAM cap (+`32.125 KiB` tracker metadata = `1.5314 MiB` total), amortizing cleanly from **`1.5314 MiB/agent`** (`M = 1`) down to **`0.0479 MiB/agent`** (`M = 32`) — **`11.74×` less RAM/agent** than `per_agent_private_cache` (`0.5625 MiB/agent`, `18.00 MiB` total) and **`3.27×` less RAM/agent** than `naive_full_ram_pin` (`0.1567 MiB/agent`, `5.02 MiB` total).

### 6.2 Cold-Start Multi-Agent Burst (`M = 16`, Cold NVMe -> Live HTTP Hanzo S3, `2.0 ms` RTT)
When `16` agents start simultaneously on an empty local NVMe tier and read the same `24` hot toolchain blocks (`384` total reads):
- **Live HTTP S3 Requests:** **`24`** (exact 1:1 with unique hot blocks; `360` reads coalesced by detached single-flight = **`93.75%` S3 GET reduction**)
- **Latency:** p50 `9.34 ms`, p99 `21.51 ms`
- **Verification:** `384 / 384` BLAKE3-256 verified

### 6.3 Three Explicit Honest Negative Regimes

| Negative Regime | Configuration | Regime | p50 (`µs`) | p99 (`ms`) | RAM / Agent (`MiB`) | NVMe WB IOPS | Mirror Inval. | RAM Hit Rate | Throughput (`MiB/s`) |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|
| **1. Uniform No-Skew Scan** (`neg_uniform_no_skew_scan`) | `M=16`, Zipf `α=0`, `8 MiB` WS, `1.5 MiB` RAM | `exclusive_tier_migration` | 76,775.0 | 117.25 | 0.0938 | 1,797 | 0 | 19.00% | 3.91 |
| **1. Uniform No-Skew Scan** (`neg_uniform_no_skew_scan`) | `M=16`, Zipf `α=0`, `8 MiB` WS, `1.5 MiB` RAM | `most_selective_mirror` | 1,914.8 | 13.40 | 0.0957 | 0 | 0 | **16.96%** | 86.26 |
| **2. Write-Heavy Mutable Churn** (`neg_write_heavy_invalidation_churn`) | `M=8`, `50%` W / `50%` R, `1 MiB` WS, `1 MiB` RAM | `exclusive_tier_migration` (write-back) | **5.2** | **4.29** | **0.1250** | **0** | **0** | **91.21%** | **1,352.83** |
| **2. Write-Heavy Mutable Churn** (`neg_write_heavy_invalidation_churn`) | `M=8`, `50%` W / `50%` R, `1 MiB` WS, `1 MiB` RAM | `most_selective_mirror` (mutable ablation) | **18,690.3** | **52.45** | **0.1289** | **600** | **306** | **52.20%** | **4.18** |
| **3. Single-Agent Ample RAM** (`neg_single_agent_ample_ram`) | `M=1`, `1 MiB` WS `≤ 2 MiB` RAM | `naive_full_ram_pin` | 5.5 | 4.31 | 1.0000 | 0 | 0 | 94.67% | 113.02 |
| **3. Single-Agent Ample RAM** (`neg_single_agent_ample_ram`) | `M=1`, `1 MiB` WS `≤ 2 MiB` RAM | `exclusive_tier_migration` | 5.6 | 4.39 | 1.0000 | 0 | 0 | 94.67% | 97.91 |
| **3. Single-Agent Ample RAM** (`neg_single_agent_ample_ram`) | `M=1`, `1 MiB` WS `≤ 2 MiB` RAM | `per_agent_private_cache` | 5.2 | 4.34 | 1.0000 | 0 | 0 | 94.67% | 106.87 |
| **3. Single-Agent Ample RAM** (`neg_single_agent_ample_ram`) | `M=1`, `1 MiB` WS `≤ 2 MiB` RAM | `most_selective_mirror` | 5.8 | 4.31 | **1.0001** | 0 | 0 | **89.50%** | **64.94** |

#### Honest Analysis of Negative Regimes
1. **`neg_uniform_no_skew_scan`:** Under flat uniform scans (`α = 0`), MOST's RAM hit rate collapses to **`16.96%`** (2.04 percentage points *lower* than `exclusive_tier_migration`'s `19.00%`) because there is no hot subset to discover: `HotnessTracker` rejects `1,819` cold candidates while consuming `+32.125 KiB` metadata RAM, and `1,916 / 2,400` reads (`79.8%`) still hit NVMe (`mean = 2.83 ms`, `5.3×` slower than under Zipf skew). While instant-drop eviction still avoids `1,797` demotion writebacks (`28.08 MiB`), selective mirroring **cannot rescue hit rate on uniform full-corpus scans**.
2. **`neg_write_heavy_invalidation_churn`:** This is a **strictly negative result** for mutable mirroring. When `50%` of operations overwrite blocks, maintaining a dual RAM+NVMe mirror causes `306` mirror invalidations and `600` synchronous NVMe writebacks (`9.38 MiB`), degrading **p99 latency by `12.24×`** (`52.45 ms` vs `4.29 ms`), **p50 by `3,600×`** (`18.69 ms` vs `5.19 µs`), and throughput by **`324×`** (`4.18` vs `1,352.83 MiB/s`) compared to ordinary write-back migration. This directly validates our hard security & policy rule: **MOST MUST NEVER mirror mutable blocks** (enforced by `ErrTenantIsolation` / `ErrImmutableMutation` in production mode).
3. **`neg_single_agent_ample_ram`:** For `M = 1` with `RAM >= WorkingSet`, `exclusive_tier_migration` and `naive_full_ram_pin` already fit 100% in RAM with `0` demotions and `94.67%` hit rate (`64` compulsory misses). `most_selective_mirror` saves **`0` RAM/agent** (actually using `+32.125 KiB` more RAM for the sketch), suffers **`126` NVMe reads vs `64`** due to `MinHits = 2` warm-up delay (`89.50%` hit rate), and runs **`1.74×` slower** (`64.94` vs `113.02 MiB/s`).

### 6.4 Post-Quantum Private Workspace Overhead (`vfs.Hybrid`)
Over `20` live `16 KiB` private workspace chunks sealed with `vfs.Hybrid` (`X25519 + ML-KEM-768`) onto live `pkg/backend/file` + `pkg/backend/s3`:
- **Seal + persist p50:** `5.72 ms` (dominated by atomic NVMe + S3 `Put`)
- **Open (decrypt) p50:** `379.7 µs`
- **Plaintext leaks on NVMe or S3:** `0`
- **Cross-tenant access:** `denied` (`ErrTenantIsolation` + cryptographic key isolation)

---

## 7. Limitations & HIP-1345 Integration Recommendation

### 7.1 Limitations
- **Calibrated NVMe Channel Model over Local Disk:** Because the test host is a 2-vCPU virtualized Linux devbox, `NVMeTier` layers a deterministic 4-channel queue semaphore and calibrated `80 µs / 320 µs` NVMe service floor over real `pkg/backend/file` filesystem I/O. Production bare-metal NVMe PCIe Gen4/Gen5 drives with `io_uring` will exhibit lower absolute write latencies (`~200–500 µs` vs `5.2 ms` `flock`+rename), though the **relative elimination of demotion write amplification** and **`O(hot_set / M)` RAM amortization** hold across all flash devices.
- **Fixed `16 KiB` Block Granularity:** Real `vfs` packs hold variable-size chunks (`pack.go`, up to `256 KiB`–`4 MiB`). Integrating MOST at the `vfs` chunk/extent cache layer should operate on content-addressed chunk IDs (BLAKE3 digests already present in `vfs` metadata) with byte-accurate RAM accounting.
- **Immutable Content Prerequisite:** Shared RAM mirroring depends on immutable, content-addressed base images / toolchains. Any mutable overlay layer must bypass MOST completely and remain inside each sandbox's encrypted `vfs.Volume`.

### 7.2 HIP-1345 Integration Recommendation
**Recommendation: CONDITIONAL ADOPT (Bounded to Allowlisted Immutable Shared Toolchains on Multi-Agent Visor Nodes).**

1. **Where to Integrate (Canonical `vfs -> Hanzo S3 -> Visor` Unchanged):**
   - Wire `pkg/most.Engine` as an opt-in node-local read-only shared cache tier inside the Visor `vfs` mount host (`pkg/mount`) beneath per-sandbox read-only base mounts (alongside `pkg/cofs` metadata indexing from HIP-1342).
   - Keep `head`, `meta/`, and `pack/` S3 formats completely unchanged.
2. **Mandatory Admission Guardrails:**
   - Enable MOST **only** when `AgentCount >= 4` AND the mounted toolchain is explicitly marked immutable + allowlisted (`AllowedNamespaces`) + verified by BLAKE3-256.
   - **Never** admit `ClassPrivateWorkspace` or mutable volume chunks into the shared mirror (`ErrTenantIsolation`), preserving per-volume `vfs.Hybrid` post-quantum encryption.
   - Automatically bypass MOST (`MinHits` gate + tracker disabled) on single-agent (`M == 1`) sandboxes or nodes where `ram_budget >= working_set`, avoiding the `neg_single_agent_ample_ram` overhead.

---

## 8. Reproducing All Results

```bash
# 1. Verify formatting, vet, and cross-compilation
gofmt -s -w pkg/most cmd/most-bench
go vet ./...
GOOS=darwin GOARCH=arm64 CGO_ENABLED=0 go build ./pkg/most/... ./cmd/most-bench/...

# 2. Run all 17 unit, concurrency (-race), live file+S3, PQ crypto, and negative-regime tests
go test -v -race -count=1 ./pkg/most/... ./cmd/most-bench/...

# 3. Regenerate raw benchmark measurements (results.json)
go run ./cmd/most-bench --seed 1345 --repo-root . --out-dir docs/research/hip-1345-most-selective-mirroring
```
