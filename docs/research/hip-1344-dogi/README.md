# HIP-1344 / Bounty #6 (`hanzoai/vfs#63`): Lifetime-Aware Initial Object Packing (DOGI)

- **Issue**: [`hanzoai/vfs#63`](https://github.com/hanzoai/vfs/issues/63) — *[Bounty #6] Lifetime-aware object packing · vfs (DOGI)*
- **Umbrella HIP**: HIP-1344 — Agent-Optimized Storage and Recovery (`hanzoai/hips`)
- **Primary Reference**: DOGI (Kim et al., USENIX FAST '26 — <https://www.usenix.org/conference/fast26/presentation/kim-jeeyun>)
- **Scope Boundary**: Governs **initial object co-packing placement by predicted lifetime** before pack slabs are sealed — explicitly distinct from `hanzoai/vfs#24` (asynchronous garbage collection scheduling/execution).
- **Canonical Stack Boundaries**: Unchanged (`vfs -> hanzoai/s3 -> visor`, Cloud owning lifecycle & billing, Enso routing models, Zen generating outputs, Kai returning structured non-generative decisions).

> **Important Attribution & Measurement Boundary**: Performance figures reported in the USENIX FAST '26 DOGI paper belong strictly to the original authors' hardware and workloads and are **not** Hanzo expectations. All measurements in this dossier (`score_table.md`, `raw_results.json`) were produced on the authoring Linux host by executing our clean-room Go implementation (`internal/packing/dogi` + `tools/dogi-bench`) against real on-disk `.pack` files (`os.MkdirTemp`, `WriteAt`/`ReadAt`, Linux `/proc/self/io`, and SHA-256 bit-exact object verification).

---

## 1. Paper & License Audit

### Design Summary (DOGI, USENIX FAST '26)
Log-structured and immutable-pack object stores (including `hanzoai/vfs`, which appends FastCDC chunks into immutable 16 MiB `pack/<2 hex>/<id>` objects) suffer severe live-data copying and write amplification when short-lived build artifacts (`target/`, `node_modules/.cache/`, `*.o`, `*.tmp`, `dist/`, `*.pyc`) are co-packed into the same immutable slabs as long-lived workspace chunks (pinned repositories, immutable datasets, model weights, committed snapshots). When ephemeral chunks expire, the surrounding slabs become partially dead and cannot be freed without reading, copying, and rewriting every surviving long-lived chunk.

DOGI's central insight is that **initial co-packing placement** — grouping objects with similar expected lifetimes into separate open slabs *before* sealing — causes short-lived slabs to expire nearly in unison, enabling **clean-slab zero-copy reclamation** while leaving long-lived slabs dense and undisturbed.

### License & Clean-Room Audit
- **Implementation**: 100% clean-room Go standard library (`internal/packing/dogi/` and `tools/dogi-bench/`).
- **External Dependencies**: **Zero** new Go dependencies (`go.mod` and `go.sum` are completely untouched).
- **License Compatibility**: Clean-room implementation compatible with `hanzoai/vfs`'s repository license (Apache-2.0 / MIT compatible); no third-party or paper-artifact source code was copied or vendored.
- **Patent / IP Posture**: Implements general-purpose storage-systems principles (deterministic path/extension priors, strictly causal per-directory EWMA short-horizon death-rate tracking, coarse 3-tier slab routing with bounded LRU open-slab eviction) without adopting proprietary encodings, hardware-specific NVMe ZNS/FDP extensions, or external code.

---

## 2. Matched Baselines & Reproduction

All 5 policies run through the **exact same** on-disk `SlabStore` (`internal/packing/dogi/slabstore.go`), identical slab geometry (`SlabTargetBytes = 256 KiB`, `MaxOpenSlabsPerTenant = 4`), identical deterministic multi-tenant workload traces (`internal/packing/dogi/workload.go`), identical fixed downstream reclaim consumer (`ReclaimDeadSlabs(0.45)`), and full end-of-run bit-exact SHA-256 verification via `ReadAt`:

1. **`mixed_fifo_packing_baseline`**: Packs all writes for a tenant into a single open slab regardless of lifetime (mirrors `hanzoai/vfs/pack.go`'s current single `packer.cur` open pack).
2. **`size_bucket_packing`**: Partitions objects strictly by payload size (`< 8 KiB`, `8–32 KiB`, `>= 32 KiB`) without lifetime awareness.
3. **`static_path_heuristic_packing`**: Routes objects into 3 lifetime classes (`ClassEphemeral`, `ClassMedium`, `ClassLongLived`) using deterministic path-prefix and file-extension rules alone.
4. **`dogi_adaptive_lifetime_packing`**: Clean-room DOGI-inspired hybrid combining deterministic path priors with strictly causal online EWMA per-`(tenant, dirPrefix)` short-horizon survival feedback, routing into 3 segregated lifetime slabs per tenant under a bounded open-slab RAM cap.
5. **`over_fragmented_class_negative`**: Documented negative control spreading writes across 64 ultra-fine buckets to expose slab underfill, pack-PUT/syscall explosion, and open-slab memory pressure.

### Reproducing All Measurements and Tests
```bash
# Run full correctness, concurrency (-race), causality, multi-tenant isolation,
# and negative-regime test suite:
go test -race -v ./internal/packing/dogi/...

# Regenerate real-disk benchmark score_table.md and raw_results.json:
go run ./tools/dogi-bench -out docs/research/hip-1344-dogi
```

---

## 3. Falsifiable Hypothesis & Expected Failure Modes

### Primary Hypothesis
Under realistic AI-agent / software-build workspace churn (`ai_agent_build_churn`, mixing persistent repo assets/weights, medium-lived source edits, known-path build artifacts, and opaque/misnamed scratch outputs), segregating initial object placement into **3 coarse lifetime tiers** using a strictly causal hybrid classifier (`dogi_adaptive_lifetime_packing`) will:
1. Reduce live-data bytes copied during reclamation by **>= 50%** relative to `mixed_fifo_packing_baseline`.
2. Beat both `size_bucket_packing` and `static_path_heuristic_packing` on live-data copying and WAF.
3. Substantially increase the **zero-copy clean-slab reclaim rate** (reclaimed slabs requiring 0 live-byte copies) while maintaining **>= 85% average slab fill** and strictly bounded open-slab memory.

### Expected Failure & Boundary Regimes (Explicitly Tested)
1. **`uniform_random_delete_no_lifetime_skew` (Negative)**: When every object follows an identical memoryless deletion distribution regardless of path or size, lifetime segregation cannot separate death times and should yield near-zero reduction in live-data copying while keeping extra open slab buffers (`3x` open slabs vs single-slab FIFO).
2. **`over_fragmented_64_bucket_underfill` (Negative)**: Splitting into 64 fine-grained buckets under moderate write rates and periodic generation flushes collapses average slab fill ratio (`~8–12%`), explodes total sealed pack PUTs (`7–8x`), and either thrashes open-slab evictions (when RAM-capped) or explodes open-slab RAM (`19x` when uncapped).
3. **`adversarial_renamed_ephemeral_cold_start` (Boundary)**: When ephemeral intermediates are written into newly coined deceptive paths (e.g., `opaque_drop/ephemeral_stage/*.bin`), a strictly causal classifier with zero look-ahead *must* misclassify initial writes until `N >= MinObservations` causal deletes accumulate, after which online EWMA feedback flips the directory to `ClassEphemeral`.

---

## 4. Deliverable & Canonical `hanzoai/vfs` Integration Path

### Package Structure
- [`internal/packing/dogi/classifier.go`](../../../internal/packing/dogi/classifier.go): `LifetimeClass`, `StaticPathClassifier`, and strictly causal `OnlineSurvivalClassifier` (per-tenant isolated EWMA survival feedback + monotonic epoch guard).
- [`internal/packing/dogi/slabstore.go`](../../../internal/packing/dogi/slabstore.go): Real on-disk `.pack` slab store (`WriteAt`/`ReadAt`), SHA-256 frame integrity, strict per-tenant slab isolation (`VerifyTenantIsolation`), bounded LRU open-slab RAM cap, reopen persistence, and shared `ReclaimDeadSlabs` evaluator.
- [`internal/packing/dogi/policy.go`](../../../internal/packing/dogi/policy.go): The 5 matched initial-packing policies.
- [`internal/packing/dogi/workload.go`](../../../internal/packing/dogi/workload.go): Deterministic multi-tenant workload and negative/boundary regime traces + end-to-end verifier.
- [`internal/packing/dogi/procio.go`](../../../internal/packing/dogi/procio.go): Real Linux `/proc/self/io` counter reader.
- [`internal/packing/dogi/dogi_test.go`](../../../internal/packing/dogi/dogi_test.go): Full `-race` unit, concurrency, causality, integrity, and negative-regime test suite.
- [`tools/dogi-bench/main.go`](../../../tools/dogi-bench/main.go): Reproducible real-disk benchmark CLI emitting `score_table.md` and `raw_results.json`.

### Zero-Wire-Format-Change Integration Seam into `hanzoai/vfs`
In `hanzoai/vfs`:
- `pack.go` currently maintains a single open `packer.cur pack` per generation, uploaded at `packTarget = 16 << 20` (16 MiB) or on `packer.flush()`.
- Every chunk's physical location is stored in the prolly-tree chunk table `[4][chunk id] -> (packID, offset, length)` (`pack.go:155-159`, `generation.go:441-446`), and packs have **no pack header** and **no layout version change** (`LLM.md`).
- Therefore, adopting lifetime-aware initial packing in `pack.go` requires **zero on-disk format changes** and zero changes to `spec_test.go` / `pin_test.go`:
  1. Replace `packer.cur pack` with `packer.cur [dogi.NumLifetimeClasses]pack` (3 bounded slots, allocated lazily from `packSlabs` and governed by the existing `v.mem` budget in `packer.send`).
  2. Pass the file's normalized path hint from `generation.settleFile` into `generation.putChunk` -> `packer.addForClass(class, frame)` using `dogi.OnlineSurvivalClassifier` (scoped per `Volume`, ensuring tenants/volumes never share packs or classifier state).
  3. Feed causal `Volume.Remove` / unlink events into `ObserveDelete` so opaque build directories adapt automatically across generations.

---

## 5. Evidence & Safety Verification

All tests pass cleanly under Go's race detector (`go test -race -v ./internal/packing/dogi/...`):

| Safety / Correctness Property | Verification Test | Result |
|---|---|---|
| Bit-exact SHA-256 read-back across seal, reclaim, and disk reopen + bit-flip corruption rejection | `TestBitExactSHA256IntegrityAcrossReclaimAndReopen` | PASS (0 corruption misses) |
| Live-copy & WAF reduction vs matched baselines under bounded open-slab cap | `TestDOGIReducesLiveCopyAndWAFOverBaseline` | PASS (-80.6% live copy) |
| Strict temporal causality / zero look-ahead + non-monotonic epoch rejection | `TestOnlineClassifierTemporalCausality` | PASS |
| Concurrent multi-tenant slab & classifier isolation (4 goroutines, cross-tenant `ErrTenantMismatch`) | `TestMultiTenantSlabIsolationConcurrent` | PASS (`-race` clean) |
| Honest negative boundary: zero lifetime skew | `TestNegativeRegimeUniformRandomDelete` | PASS |
| Honest negative boundary: 64-bucket over-fragmentation underfill & RAM overhead | `TestNegativeRegimeOverFragmented64Buckets` | PASS |
| Honest boundary: adversarial cold-start misclassification window & convergence | `TestBoundaryRegimeAdversarialColdStartConvergence` | PASS (12-delete window, 100% post-conv) |

---

## 6. Measurements & HIP-1344 Status Update

Full machine-generated tables and raw JSON telemetry are committed alongside this report in:
- [`score_table.md`](./score_table.md)
- [`raw_results.json`](./raw_results.json)

### Headline Real-Disk Results (`ai_agent_build_churn`, `SlabTargetBytes = 256 KiB`, `MaxOpenSlabsPerTenant = 4`, `Seed = 42`)

| Policy | Live Copied (MiB) | Delta vs Baseline | WAF (Phys/Log) | Final Space Amp | Zero-Copy Reclaim Rate | Slabs Sealed (`syscw`) | Avg Slab Fill | Peak Open-Slab RAM | `/proc/self/io` `wchar` |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `mixed_fifo_packing_baseline` | 22.04 | baseline | 1.331x | 1.212x | 0.4% (1/224) | 345 | 97.1% | 0.50 MiB | 89.20 MiB |
| `size_bucket_packing` | 20.50 | -7.0% | 1.308x | 1.232x | 4.1% (9/219) | 355 | 92.7% | 1.00 MiB | 87.66 MiB |
| `static_path_heuristic_packing` | 12.22 | -44.6% | 1.185x | 1.245x | 39.9% (79/198) | 331 | 90.3% | 1.50 MiB | 79.37 MiB |
| `dogi_adaptive_lifetime_packing` | **4.26** | **-80.6%** | **1.066x** | **1.110x** | **72.7% (128/176)** | **287** | **93.7%** | 1.50 MiB | **71.39 MiB** |
| `over_fragmented_class_negative` | 1.36 | -93.8% | 1.022x | 1.015x | 96.1% (1325/1379) | **2213** | **12.4%** | 2.00 MiB | 68.48 MiB |

### Key Findings & HIP-1344 Recommendation
1. **Strong positive effect under realistic agent/build churn**: `dogi_adaptive_lifetime_packing` cut live-data copying by **-80.6%** (`22.04 MiB -> 4.26 MiB`), reduced total physical write volume (`/proc/self/io` `wchar`) by **-20.0%** (`89.20 MiB -> 71.39 MiB`, WAF `1.331x -> 1.066x`), reduced sealed pack PUTs (`syscw`) by **-16.8%** (`345 -> 287`), and boosted clean zero-copy slab reclamation from **0.4% to 72.7%**, while preserving **93.7%** slab fill and **0** SHA-256 integrity errors across 860 verified live objects.
2. **Size bucketing is insufficient (-7.0%)** because build artifacts and long-lived chunks overlap heavily in chunk size; **static path heuristics alone capture only half the gain (-44.6%)** because opaque/renamed scratch paths misclassify permanently.
3. **Coarse 3-class tiering is essential**: 64-bucket over-fragmentation destroys object-store packing efficiency (`12.4%` avg slab fill and **7.7x** sealed pack PUTs: `2,213` vs `287`, and up to **19x** open-slab RAM when uncapped on moderate workloads).
4. **Honest boundaries confirmed**: Zero-skew memoryless workloads gain almost nothing (`10.56 MiB -> 9.76 MiB`, `3x` open-slab buffers), and deceptive opaque paths incur a measurable cold-start window (`12` misclassified writes / `12` deletes across 1 epoch before reaching `100.0%` post-convergence accuracy).
5. **HIP-1344 Status**: Ready for optional opt-in integration into `hanzoai/vfs/pack.go` behind a bounded 3-class `packer` toggle once paired with `hanzoai/vfs#24` asynchronous pack reclamation, with zero on-disk format changes required.
