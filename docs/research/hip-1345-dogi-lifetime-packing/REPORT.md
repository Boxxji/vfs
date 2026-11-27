# HIP-1345 Research Dossier: Lifetime-Aware Object Packing (DOGI)

Refs `hanzoai/vfs#51`. Tracked by **HIP-1345** (`[p2, perf, research, bounty, hip-1345]`).
Library: [`pkg/dogi`](../../../pkg/dogi/). Reproducible benchmark runner: [`cmd/dogi-bench`](../../../cmd/dogi-bench/). Raw machine-readable output: [`results.json`](./results.json).

## 1. Primary Reference, Summary, and License Audit

* **Primary paper:** Jeeyun Kim, Seonggyun Oh, Jungwoo Kim, Jisung Park, Jaeho Kim, Sungjin Lee, and Sam H. Noh, *"DOGI: Data Placement with Oracle-Guided Insights for Log-Structured Systems"*, Proceedings of the 24th USENIX Conference on File and Storage Technologies (**FAST '26**), February 2026.
  * Presentation & Open Access PDF: <https://www.usenix.org/conference/fast26/presentation/kim-jeeyun>
* **Paper summary:** Log-structured storage systems suffer write amplification (WAF) during garbage collection when short-lived and long-lived blocks are co-located in the same immutable segments. The paper introduces **NoDaP**, a near-optimal oracle baseline that partitions segments into groups by Block Invalidation-time Range (BIR), and derives three oracle-guided insights:
  1. *User-written placement accuracy* strongly drives WAF (existing SOTA predictors achieve 76–87% vs. NoDaP 100%, leaving substantial live-copy overhead).
  2. *GC-relocated live blocks* must also be re-predicted by remaining lifetime rather than dumped into one cold GC bucket or blindly cascaded by age (adopting NoDaP's GC relocation cuts WAF by 46% on average in the paper's ablation).
  3. *Group granularity must match prediction accuracy* — naively adopting NoDaP's fine-grained group table under an imperfect predictor (`ML-DT+NGrp`) *increased* WAF by 52% due to misprediction and fragmentation penalties.
  * *Published paper numbers (authors' own evaluation on zoned devices/LSS simulations; NOT Hanzo claims):* DOGI reports up to 23.2% WAF reduction and up to 13.3% write throughput gain over the best prior baseline.
* **Mapping to `hanzoai/vfs -> Hanzo S3 -> Visor`:** In `hanzoai/vfs/pack.go`, every generation currently appends sealed chunks into a single open `pack` (`pack/<2hex>/<id>`, up to `16 MiB` or flushed at generation publication). When an AI coding agent runs `cargo check`, `go test`, or `npm build` inside a Visor sandbox, short-lived build artifacts (`target/`, `node_modules/.cache/`, `.next/`, `tmp/`, `*.o`, `*.tmp`, `*.pyc`) arrive interleaved with long-lived workspace chunks (`.git/objects/`, `vendor/`, `go.sum`, `models/*.safetensors`, source code). Separating streams by predicted lifetime allows ephemeral packs to reach 100% dead bytes and be reclaimed via **zero-copy S3 DELETE** (`0 B` copied), while leaving long-lived workspace packs untouched by compaction.
* **License & clean-room audit:**
  * Zero upstream code, traces, datasets, or model weights were copied or imported.
  * Everything in `pkg/dogi/`, `cmd/dogi-bench/`, and this dossier is an original, clean-room **Apache-2.0** Go implementation written from first principles.
  * **Zero new dependencies:** `go.mod` and `go.sum` are completely untouched (uses only Go stdlib, `github.com/zeebo/blake3`, `github.com/hanzoai/vfs/pkg/backend`, `pkg/backend/file`, `pkg/backend/s3`, and `vfs.Hybrid` / `github.com/luxfi/age` in tests).
  * Cross-compiles cleanly with `CGO_ENABLED=0` for `linux/amd64` and `darwin/arm64`.

## 2. Reproducible Baseline & Measurement Environment

| Parameter | Value |
|---|---|
| Go Toolchain | `go1.27.1` (`linux/amd64`, `2` vCPUs) |
| Deterministic Seed | `--seed 1345` |
| Simulation Horizon | `20` epochs (flush + GC sweep per epoch) |
| Default Pack Target | `1.00 MiB` (`1048576` bytes, immutable packs via `pkg/backend/file`) |
| GC Compaction Threshold | `dead_ratio >= 0.50` (100%-dead packs always zero-copy dropped) |
| Grounded Real Repo Corpus | `319` real repository files (`2.99 MiB` total) |

Four packing policies execute the exact same deterministic write/delete schedule, pack format, BLAKE3 verification, and collection-time evaluator:

1. **`fifo_mixed_baseline`** — Upstream baseline mirroring `hanzoai/vfs/pack.go`: single arrival-order pack stream.
2. **`path_grouped_baseline`** — Static path/directory/extension priors only (`target/`, `tmp/`, `*.o`, `vendor/`, `.git/objects/`, etc.).
3. **`dogi_lifetime_packed`** — Clean-room DOGI multi-signal non-oracle predictor: static priors + `OnlineDirLifetimeTracker` (per-directory EWMA death-rate, observed lifetime, overwrite frequency, and multi-epoch survival tracking) + size/burst correlation + DOGI GC-relocation re-prediction (`ClassifyRelocation`). Strictly verified never to inspect `OracleDeathEpoch`.
4. **`oracle_lifetime_upper_bound`** — Hindsight NoDaP-style upper bound using exact remaining lifetime to establish the theoretical ceiling.

## 3. Falsifiable Hypothesis, Metrics, and Expected Failure Modes

**Hypothesis (H1):** On multi-epoch developer/agent workloads where ephemeral build artifacts are interleaved with long-lived workspace chunks, partitioning immutable pack streams by a non-oracle DOGI lifetime predictor (`dogi_lifetime_packed`) reduces collection-time live-data copying (`LiveCopyBytes`) by **>= 70%** and reduces peak storage amplification (`PeakStorageAmp`) relative to `fifo_mixed_baseline`, while outperforming `path_grouped_baseline` by **>= 50%** on opaque/renamed directory trees.

**Primary metrics:**
* `LiveCopyBytes` & `WriteAmplification = (TotalIngestBytes + LiveCopyBytes) / TotalIngestBytes`
* `ZeroCopyReclaimedPacks` (100%-dead packs deleted directly via S3 `DELETE` with `0 B` copied)
* `PeakStorageAmp = max_t(StoredBytes(t) / LiveBytes(t))` and `MeanStorageAmp`
* `PacksUploaded` (S3 `PUT` count — transparently captures multi-stream open-pack fragmentation)
* `GCGetRangeOps` (ranged S3 `GET` requests needed to rescue surviving live chunks during compaction)

**Expected failure modes (tested explicitly as honest negative controls):**
1. *Memoryless random lifetimes (`neg_uniform_random_lifetime`):* When invalidation probability is i.i.d. across all directories and sizes, no non-oracle predictor can separate death times; DOGI should show negligible GC-copy gain while paying extra S3 `PUT`s.
2. *Append-only / zero-delete archives (`neg_append_only_no_deletes`):* When GC never runs, multi-stream packing yields `0 B` savings and strictly increases `PacksUploaded` and partially-filled tail pack overhead.
3. *Adversarial inverted path priors (`neg_adversarial_inverted_priors`):* Storing long-lived golden artifacts under `tmp/golden/` and churning `src/ephemeral/` permanently breaks static path heuristics and imposes a measurable warm-up adaptation lag on DOGI until online EWMA evidence overrides the static prior.

## 4. Before / After Empirical Results (Raw Measurements)

### ci_build_plus_workspace (`Positive Workload`)

Real repository workspace files interleaved with 20 epochs of short-lived CI/agent build churn (target/, node_modules/.cache/, .next/, tmp/, *.o, *.tmp, *.pyc), medium-lived active source edits, and pinned immutable assets (.git/objects/, models/*.safetensors).

| Policy | Ingest | Live Copied (GC) | LiveCopy vs FIFO | WAF | Peak Storage Amp | Mean Storage Amp | S3 PUTs (Packs) | PUTs vs FIFO | Zero-Copy Drops | Compacted Packs | GC GetRange Ops | Bit-Exact |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|:---:|
| `fifo_mixed_baseline` | 43.43 MiB | 7.85 MiB | 0.0% | 1.181x | 1.279x | 1.143x | 79 | 0.0% | 8 | 49 | 380 | PASS |
| `path_grouped_baseline` | 43.43 MiB | 0.70 MiB | -91.0% | 1.016x | 1.010x | 1.002x | 113 | +43.0% | 53 | 13 | 30 | PASS |
| `dogi_lifetime_packed` | 43.43 MiB | 0.78 MiB | -90.0% | 1.018x | 1.037x | 1.017x | 111 | +40.5% | 51 | 13 | 40 | PASS |
| `oracle_lifetime_upper_bound` | 43.43 MiB | 0.70 MiB | -91.0% | 1.016x | 1.010x | 1.002x | 93 | +17.7% | 53 | 13 | 30 | PASS |

### opaque_renamed_paths_online_learning (`Positive Workload`)

Identical multi-tier lifetime dynamics under opaque, non-standard directory names (ws/slot-01..04/*.dat) with zero recognizable path or extension priors; isolates DOGI's online per-directory EWMA learner vs static path heuristics.

| Policy | Ingest | Live Copied (GC) | LiveCopy vs FIFO | WAF | Peak Storage Amp | Mean Storage Amp | S3 PUTs (Packs) | PUTs vs FIFO | Zero-Copy Drops | Compacted Packs | GC GetRange Ops | Bit-Exact |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|:---:|
| `fifo_mixed_baseline` | 43.48 MiB | 7.85 MiB | 0.0% | 1.181x | 1.267x | 1.130x | 79 | 0.0% | 8 | 49 | 380 | PASS |
| `path_grouped_baseline` | 43.48 MiB | 7.85 MiB | 0.0% | 1.181x | 1.267x | 1.130x | 79 | 0.0% | 8 | 49 | 380 | PASS |
| `dogi_lifetime_packed` | 43.48 MiB | 0.47 MiB | -94.0% | 1.011x | 1.133x | 1.075x | 77 | -2.5% | 46 | 3 | 20 | PASS |
| `oracle_lifetime_upper_bound` | 43.48 MiB | 0.00 MiB | -100.0% | 1.000x | 1.002x | 1.002x | 80 | +1.3% | 53 | 0 | 0 | PASS |

### neg_uniform_random_lifetime (`Honest Negative Control`)

Honest Negative Control 1: i.i.d. memoryless geometric deletion probability (p=0.35/epoch) uncorrelated with path, directory, size, or burst. No non-oracle predictor can separate death times.

| Policy | Ingest | Live Copied (GC) | LiveCopy vs FIFO | WAF | Peak Storage Amp | Mean Storage Amp | S3 PUTs (Packs) | PUTs vs FIFO | Zero-Copy Drops | Compacted Packs | GC GetRange Ops | Bit-Exact |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|:---:|
| `fifo_mixed_baseline` | 29.44 MiB | 15.92 MiB | 0.0% | 1.541x | 1.315x | 1.214x | 65 | 0.0% | 1 | 56 | 524 | PASS |
| `path_grouped_baseline` | 29.44 MiB | 14.68 MiB | -7.8% | 1.499x | 1.341x | 1.209x | 105 | +61.5% | 4 | 87 | 486 | PASS |
| `dogi_lifetime_packed` | 29.44 MiB | 15.17 MiB | -4.7% | 1.515x | 1.364x | 1.218x | 70 | +7.7% | 3 | 59 | 502 | PASS |
| `oracle_lifetime_upper_bound` | 29.44 MiB | 9.66 MiB | -39.3% | 1.328x | 1.161x | 1.061x | 92 | +41.5% | 26 | 47 | 320 | PASS |

### neg_append_only_no_deletes (`Honest Negative Control`)

Honest Negative Control 2: append-only immutable archive with zero deletes and zero overwrites across all 20 epochs. GC never triggers; multi-stream packing only adds tail-pack PUT overhead.

| Policy | Ingest | Live Copied (GC) | LiveCopy vs FIFO | WAF | Peak Storage Amp | Mean Storage Amp | S3 PUTs (Packs) | PUTs vs FIFO | Zero-Copy Drops | Compacted Packs | GC GetRange Ops | Bit-Exact |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|:---:|
| `fifo_mixed_baseline` | 9.26 MiB | 0.00 MiB | 0.0% | 1.000x | 1.001x | 1.001x | 20 | 0.0% | 0 | 0 | 0 | PASS |
| `path_grouped_baseline` | 9.26 MiB | 0.00 MiB | 0.0% | 1.000x | 1.002x | 1.001x | 80 | +300.0% | 0 | 0 | 0 | PASS |
| `dogi_lifetime_packed` | 9.26 MiB | 0.00 MiB | 0.0% | 1.000x | 1.002x | 1.001x | 37 | +85.0% | 0 | 0 | 0 | PASS |
| `oracle_lifetime_upper_bound` | 9.26 MiB | 0.00 MiB | 0.0% | 1.000x | 1.001x | 1.001x | 20 | 0.0% | 0 | 0 | 0 | PASS |

### neg_adversarial_inverted_priors (`Honest Negative Control`)

Honest Negative Control 3: long-lived golden artifacts stored under tmp/golden/*.tmp while 1-epoch churn lives under src/ephemeral/*.go, interleaved with normal tmp/scratch and src/core files. Quantifies static-prior failure and DOGI's early-epoch adaptation lag.

| Policy | Ingest | Live Copied (GC) | LiveCopy vs FIFO | WAF | Peak Storage Amp | Mean Storage Amp | S3 PUTs (Packs) | PUTs vs FIFO | Zero-Copy Drops | Compacted Packs | GC GetRange Ops | Bit-Exact |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|:---:|
| `fifo_mixed_baseline` | 34.84 MiB | 4.65 MiB | 0.0% | 1.133x | 1.002x | 1.002x | 60 | 0.0% | 0 | 39 | 228 | PASS |
| `path_grouped_baseline` | 34.84 MiB | 4.65 MiB | 0.0% | 1.133x | 1.002x | 1.002x | 80 | +33.3% | 0 | 40 | 228 | PASS |
| `dogi_lifetime_packed` | 34.84 MiB | 0.57 MiB | -87.7% | 1.016x | 1.002x | 1.002x | 67 | +11.7% | 31 | 7 | 30 | PASS |
| `oracle_lifetime_upper_bound` | 34.84 MiB | 0.00 MiB | -100.0% | 1.000x | 1.002x | 1.002x | 60 | 0.0% | 38 | 0 | 0 | PASS |

**Adversarial Inverted-Prior Adaptation Lag Breakdown:**

* **Warm-up phase (Epochs `0..3`):** DOGI copies `0.48 MiB` vs FIFO `0.75 MiB` while strong inverted static priors (`tmp/golden` -> ShortLived, `src/ephemeral` -> LongLived) initially mispack opposite lifetimes.
* **Adaptation crossover:** DOGI first beats FIFO on per-epoch live-data copying at **Epoch `2`**.
* **Post-adaptation phase (Epochs `4..19`):** DOGI copies `0.09 MiB` vs FIFO `3.90 MiB` (**97.6% reduction**) and vs static `path_grouped_baseline` `3.90 MiB` (which never recovers).

### Pack-Size Sensitivity Sweep (`ci_build_plus_workspace`)

| Pack Target | FIFO Live Copy | DOGI Live Copy | Live-Copy Reduction | FIFO WAF | DOGI WAF | FIFO Peak Amp | DOGI Peak Amp | FIFO PUTs | DOGI PUTs | DOGI Zero-Copy Drops |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `0.25 MiB` | 7.54 MiB | 0.79 MiB | **89.5%** | 1.174x | 1.018x | 1.306x | 1.032x | 212 | 215 | 146 |
| `1.00 MiB` | 7.85 MiB | 0.78 MiB | **90.0%** | 1.181x | 1.018x | 1.279x | 1.037x | 79 | 111 | 51 |
| `4.00 MiB` | 7.85 MiB | 0.78 MiB | **90.0%** | 1.181x | 1.018x | 1.279x | 1.037x | 39 | 91 | 32 |

## 5. Correctness, Concurrency, Security, and Regression Evidence

* **Bit-exact post-GC data integrity:** Every workload × policy run above passed `VerifyAllLive` (ranged backend read + BLAKE3-256 verification of 100% of surviving files) and `CheckAccountingInvariant` (`sum(pack.LiveBytes) == global LiveBytes`).
* **Crash-safe GC ordering:** `Packer.Collect` reads surviving live chunks via `backend.GetRange`, writes and durably flushes all relocated destination packs (`PutIfAbsent`) **before** issuing `backend.Delete` on compacted victim packs.
* **Strict multi-tenant & per-volume isolation:** `Packer` binds immutably to a single `Namespace{TenantID, VolumeID}` with collision-resistant `backend.ValidKey`-compliant prefixes (`dogi/<tenant-hash>/<volume-hash>/pack/<2hex>/<id>`). Cross-tenant or cross-volume writes fail closed with `ErrTenantIsolation` before touching any buffer.
* **Post-quantum per-volume encryption:** `TestHybridPostQuantumEncryptionPerVolume` exercises real `vfs.Hybrid` (`X25519 + ML-KEM-768`) sealing with domain-separated `(TenantID, VolumeID, ChunkID)` AAD binding, verifying zero plaintext leakage in backend packs, presence of `-> mlkem768x25519` age stanzas, fail-closed tamper rejection, wrong-volume key rejection, and bit-exact post-GC decryption.
* **Live backend integration:** Verified against both real on-disk `pkg/backend/file` and real HTTP `pkg/backend/s3` (`internal/storetest.NewS3`).
* **Concurrency:** Verified with `go test -race -count=1 ./pkg/dogi/... ./cmd/dogi-bench/...` including concurrent writers, readers, deleters, flushers, and GC sweepers (`0` data races).
* **Non-oracle guarantee:** `TestNonOraclePoliciesNeverPeekOracleDeathEpoch` proves bit-identical predictions for `PolicyFIFOMixed`, `PolicyPathGrouped`, and `PolicyDOGILifetime` regardless of `OracleDeathEpoch`.

## 6. Limitations, Integration Recommendation, and HIP-1345 Update

### Limitations
1. **Open-pack PUT overhead on low-churn / append-only volumes:** Maintaining `K = 4` open lifetime streams per generation flush increases S3 `PUT` counts when volume writes are small or append-only (quantified in `neg_append_only_no_deletes`).
2. **Memoryless churn ceiling:** When lifetimes have zero correlation with directory, path, size, or burst (`neg_uniform_random_lifetime`), DOGI cannot reduce live-data copying and adds modest stream-splitting PUT overhead.
3. **Cold-start adaptation lag:** Under adversarial inverted path semantics (`neg_adversarial_inverted_priors`), strong static priors take ~2–3 epochs of counter-evidence before `OnlineDirLifetimeTracker` flips the directory class.
4. **Scope boundary:** Per `hanzoai/vfs#51`, this study evaluates *packing decisions* (co-location of chunks into immutable packs) and their collection-time impact — not asynchronous GC scheduling or S3 lifecycle policy orchestration.

### Integration Recommendation for `hanzoai/vfs -> Hanzo S3 -> Visor`
**Verdict: Recommended for opt-in / adaptive integration into `vfs/pack.go` for mutable agent/developer workspaces; keep single-stream FIFO for append-only archives and immutable base images.**

1. **Phase 1 (Zero wire-format breakage):** Keep the canonical `pack/<2hex>/<id>` S3 layout and `vfs.Hybrid` per-volume encryption completely unchanged. Replace `packer.cur pack` in `pack.go` with `streams [4]pack` keyed by `dogi.Classifier` when a volume is mounted in mutable workspace mode (e.g. Visor coding sandboxes). Because every chunk's exact `(packID, off, n)` already lives in the prolly chunk table, **readers require zero changes**.
2. **Phase 2 (Online EWMA persistence):** Persist compact `OnlineDirLifetimeTracker` summaries in an encrypted volume extension (`Volume.PutExt(ctx, "dogi", ...)` per `SPECIFICATION 3.1`) so directory lifetime knowledge survives sandbox stop/resume cycles without warm-up lag.
3. **Phase 3 (Automatic fallback guard for negative regimes):** Keep `K = 1` (single-stream FIFO) until a volume observes non-zero deletion churn (`deletes > 0`) and cross-directory death-rate variance above a threshold; this eliminates the append-only and memoryless PUT overhead completely while preserving the full DOGI win on CI/build workspaces.

### Reproducing

```sh
go test -v -race -count=1 ./pkg/dogi/... ./cmd/dogi-bench/...
GOOS=darwin GOARCH=arm64 CGO_ENABLED=0 go build ./pkg/dogi/... ./cmd/dogi-bench/...
go run ./cmd/dogi-bench --seed 1345 --repo-root . --out-dir docs/research/hip-1345-dogi-lifetime-packing
```
