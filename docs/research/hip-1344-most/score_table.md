# HIP-1344 Score Table — Selective Hot-Data Mirroring (MOST, USENIX FAST '26)

- **Generated (UTC):** `2026-11-25T14:16:34Z`
- **Runtime:** `go1.27.1` on `linux/amd64` (`2` CPU)
- **Backing Tiers:** Real pooled RAM slices (`internal/bufpool`) + real on-disk `os.File` `ReadAt`/`WriteAt` NVMe backing + `internal/blake3` cryptographic verification.

## Scenario: `skewed_multi_agent_toolchain_burst` (primary)

32 concurrent agents reading Zipfian-skewed immutable shared toolchains (70%), cold toolchain scan bursts (15%), and hot per-tenant private workspace blocks (15%) under a fixed 16 MiB host RAM budget (64 MiB NVMe corpus).

- Agents: `32`, BlockSize: `32 KiB`, Host RAM Ceiling: `16.0 MiB`, Shared Corpus: `1024` blocks (`32.0 MiB`), Private Corpus: `32` blocks/agent, Ops/Agent: `2000` (`64000` total ops)

| Policy | p50 (µs) | p95 (µs) | p99 (µs) | Resident RAM/Agent (MiB) | Total Host RAM (MiB) | RAM Hit Rate (%) | NVMe Eviction Writes (MiB) | Throughput (ops/s) | Cross-Tenant Leaks |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `private_per_agent_lru_baseline` | 17.37 | 46.34 | 4216.89 | 0.500 | 16.00 | 42.40% | 0.00 | 109799 | 0 |
| `ordinary_tier_migration_exclusive` | 2.14 | 44.32 | 3774.00 | 0.500 | 16.00 | 74.53% | 493.44 | 171300 | 0 |
| `most_selective_hot_mirroring` | 1.88 | 21.32 | 65.57 | 0.375 | 12.00 | 73.52% | 0.00 | 261448 | 0 |
| `mirror_all_unselective_negative` | 2.17 | 39.67 | 5041.20 | 0.500 | 16.00 | 74.64% | 0.00 | 213809 | 0 |

## Scenario: `uniform_flat_access_no_hot_skew` (negative_boundary)

Negative/boundary regime: 32 agents issuing uniformly random shared toolchain reads (zero skew) across a 32 MiB corpus under an 8 MiB host RAM budget (4x working-set overflow). Selective mirroring cannot beat NVMe p99 latency, while keeping NVMe eviction writes at 0.00 MiB.

- Agents: `32`, BlockSize: `32 KiB`, Host RAM Ceiling: `8.0 MiB`, Shared Corpus: `1024` blocks (`32.0 MiB`), Private Corpus: `8` blocks/agent, Ops/Agent: `1200` (`38400` total ops)

| Policy | p50 (µs) | p95 (µs) | p99 (µs) | Resident RAM/Agent (MiB) | Total Host RAM (MiB) | RAM Hit Rate (%) | NVMe Eviction Writes (MiB) | Throughput (ops/s) | Cross-Tenant Leaks |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `private_per_agent_lru_baseline` | 25.31 | 63.73 | 9201.00 | 0.250 | 8.00 | 0.96% | 0.00 | 68502 | 0 |
| `ordinary_tier_migration_exclusive` | 32.09 | 96.18 | 15628.95 | 0.250 | 8.00 | 19.49% | 958.16 | 63013 | 0 |
| `most_selective_hot_mirroring` | 19.77 | 54.33 | 163.65 | 0.125 | 4.00 | 7.26% | 0.00 | 89870 | 0 |
| `mirror_all_unselective_negative` | 25.47 | 1613.64 | 9948.76 | 0.250 | 8.00 | 18.99% | 0.00 | 78791 | 0 |

## Scenario: `mutable_private_workspace_heavy_workload` (negative_boundary)

Negative/boundary regime: 90% of reads hit per-tenant private mutable workspace blocks (strictly barred from host-shared mirroring by tenant isolation) and 10% hit shared toolchains. Resident RAM/agent approaches private-LRU baseline, proving isolation bounds deduplication.

- Agents: `32`, BlockSize: `32 KiB`, Host RAM Ceiling: `16.0 MiB`, Shared Corpus: `512` blocks (`16.0 MiB`), Private Corpus: `32` blocks/agent, Ops/Agent: `1500` (`48000` total ops)

| Policy | p50 (µs) | p95 (µs) | p99 (µs) | Resident RAM/Agent (MiB) | Total Host RAM (MiB) | RAM Hit Rate (%) | NVMe Eviction Writes (MiB) | Throughput (ops/s) | Cross-Tenant Leaks |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `private_per_agent_lru_baseline` | 1.35 | 26.00 | 77.92 | 0.500 | 16.00 | 72.82% | 0.00 | 296966 | 0 |
| `ordinary_tier_migration_exclusive` | 1.31 | 25.56 | 87.73 | 0.500 | 16.00 | 77.45% | 322.28 | 287399 | 0 |
| `most_selective_hot_mirroring` | 1.23 | 6.59 | 34.12 | 0.438 | 14.00 | 81.56% | 0.00 | 615466 | 0 |
| `mirror_all_unselective_negative` | 1.33 | 17.18 | 88.76 | 0.500 | 16.00 | 77.47% | 0.00 | 390474 | 0 |

## Scenario: `mirror_all_ram_thrash_collapse` (negative_boundary)

Negative control regime: 32 agents alternating hot Zipfian toolchain reads (55%) with contiguous 48-block cold toolchain scan bursts (40%) and private reads (5%) under an 8 MiB host RAM cap. Unselective mirroring thrashes RAM and degrades p99 latency; MOST filters one-hit scans.

- Agents: `32`, BlockSize: `32 KiB`, Host RAM Ceiling: `8.0 MiB`, Shared Corpus: `1024` blocks (`32.0 MiB`), Private Corpus: `8` blocks/agent, Ops/Agent: `1500` (`48000` total ops)

| Policy | p50 (µs) | p95 (µs) | p99 (µs) | Resident RAM/Agent (MiB) | Total Host RAM (MiB) | RAM Hit Rate (%) | NVMe Eviction Writes (MiB) | Throughput (ops/s) | Cross-Tenant Leaks |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `private_per_agent_lru_baseline` | 23.34 | 45.02 | 4526.84 | 0.250 | 8.00 | 30.28% | 0.00 | 99851 | 0 |
| `ordinary_tier_migration_exclusive` | 2.68 | 61.55 | 7761.80 | 0.250 | 8.00 | 61.24% | 573.38 | 126081 | 0 |
| `most_selective_hot_mirroring` | 2.33 | 22.49 | 67.25 | 0.188 | 6.00 | 60.81% | 0.00 | 199306 | 0 |
| `mirror_all_unselective_negative` | 2.36 | 65.95 | 6307.34 | 0.250 | 8.00 | 60.12% | 0.00 | 158280 | 0 |

