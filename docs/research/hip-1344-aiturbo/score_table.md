# HIP-1344 AITURBO Grouped Checkpoint IO — Score Table

- **Generated (UTC)**: `2026-11-25T13:44:04Z`
- **Workload**: Mixed AI-model / agent-workspace checkpoint (`272` files, `58.87 MiB`, seed `20260401`)
- **Loopback S3 Server**: real `net/http/httptest` path-style S3 with atomic `If-None-Match`/`If-Match` (HTTP 412) and `Range` (HTTP 206), `RTT=2.00 ms`, per-stream cap `150 MiB/s`, `W=8`, `AITURBO group=4 MiB`, `5` measured iterations/strategy
- **S3 API Pricing Basis**: AWS S3 Standard list prices from `pkg/cost` (`$0.0050 / 1k PUTs`, `$0.0004 / 1k GETs`)

## 1. Matched 5-Strategy Checkpoint IO Score Table

| Strategy | Workers | Groups | Upload (MiB/s) | Full Restore (MiB/s) | PUTs / GiB | GETs / GiB | Est. API Cost / 100 GiB (W+R) | Commit p50 / p99 (ms) | Selective 16 KiB Data Read-Amp | Selective Latency (ms) | SHA-256 Bit-Exact | Head CAS |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|:---:|:---:|
| `per_object_sequential_baseline` | 1 | 272 | 36.91 | 37.03 | 4766.4 | 4766.4 | $2.5738 | 1594.92 / 1644.86 | 1.00x | 11.23 | PASS | PASS |
| `per_object_parallel_baseline` | 8 | 272 | 212.27 | 204.01 | 4766.4 | 4766.4 | $2.5738 | 277.32 / 320.84 | 1.00x | 11.51 | PASS | PASS |
| `naive_tarball_monolith` | 1 | 1 | 70.62 | 82.61 | 52.2 | 52.2 | $0.0282 | 833.50 / 1005.24 | 3767.39x | 474.40 | PASS | PASS |
| `aiturbo_grouped_io_planner` | 8 | 14 | 262.23 | 276.36 | 278.3 | 278.3 | $0.1503 | 224.48 / 259.26 | 1.00x | 13.44 | PASS | PASS |
| `over_coalesced_giant_group_negative` | 8 | 1 | 70.22 | 81.72 | 52.2 | 52.2 | $0.0282 | 838.31 / 892.87 | 3767.39x | 492.08 | PASS | PASS |

### Relative Delta Summary (`aiturbo_grouped_io_planner`)

- **S3 Request Reduction vs `per_object_parallel_baseline`**: **17.12x fewer PUTs/GiB** (`4766.4` -> `278.3`) and **17.12x fewer GETs/GiB** (`4766.4` -> `278.3`), cutting S3 API request cost per 100 GiB cycle by **94.2%** (`$2.5738` -> `$0.1503`).
- **Upload Throughput**: **7.10x faster** than `per_object_sequential_baseline` (`262.23` vs `36.91 MiB/s`), **1.24x** vs `per_object_parallel_baseline` (`212.27 MiB/s`), and **3.71x faster** than `naive_tarball_monolith` (`70.62 MiB/s`).
- **Selective 16 KiB Restore Read Amplification**: `1.00x` data read-amp (`13.44 ms`) vs `3767.39x` (`474.40 ms`) on `naive_tarball_monolith` and `3767.39x` (`492.08 ms`) on `over_coalesced_giant_group_negative`.

## 2. Documented Honest Negative & Boundary Regimes

### Regime 1: `selective_single_file_restore_read_amp`

Restoring `config/checkpoint_index.json` (`16384` bytes) from a `3.01 MiB` AITURBO super-object:

| Fetch Mode | Data Bytes Fetched | Data Read-Amp | Total GETs | Latency (ms) | SHA-256 Exact |
|---|---:|---:|---:|---:|:---:|
| Full-Group Fetch (no HTTP Range) | 3155365 | 192.59x | 3 | 33.55 | true |
| Coalesced HTTP `Range` GET | 16384 | 1.00x | 3 | 10.29 | true |

Without HTTP `Range` support, grouped super-objects suffer **192.59x** payload read amplification; HTTP `Range` GET recovers exact **1.00x** data read amplification (`192.59x` fewer payload bytes) with the same GET count.

### Regime 2: `already_large_tensor_only_checkpoint`

Checkpoint consisting solely of `4` x `16 MiB` weight tensors (`64.00 MiB` total):

| Strategy | PUTs / GiB | Packing Copy Buffer (MiB) | Upload Throughput (MiB/s) |
|---|---:|---:|---:|
| `per_object_parallel_baseline` | 96.0 | 0.00 | 197.32 |
| `aiturbo_grouped_io_planner` (passthrough) | 96.0 | 0.00 | 223.96 |
| `over_coalesced_giant_group_negative` (forced pack) | 48.0 | 64.00 | 57.58 |

Request reduction for already-large tensors is **0.0%** (zero gain). Size-aware passthrough avoids allocating `64.00 MiB` of redundant coalescing copy buffers and preserves full parallel stream throughput.

### Regime 3: `concurrent_head_cas_conflict_retry`

| Metric | Measured Value |
|---|---:|
| Concurrent Racing Writers | 16 |
| Initial `If-None-Match: *` Winners / Conflicts | 1 / 15 |
| Update `If-Match: <etag>` Winners / Conflicts | 1 / 15 |
| Silent Overwrites | 0 |
| Torn Checkpoints Observed | 0 |
| Rebase & Retry to Next Generation | true (final gen `3`) |
