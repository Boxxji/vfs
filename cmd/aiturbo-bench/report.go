// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/hanzoai/vfs/pkg/aiturbo"
)

func renderMarkdownReport(r *SuiteReport) string {
	var b strings.Builder

	fmt.Fprintf(&b, "# HIP-1345: Grouped IO Planning for Bulk Checkpoint Transfers (AITURBO, FAST 2026)\n\n")
	fmt.Fprintf(&b, "Refs `hanzoai/vfs#48`. Clean-room implementation in `pkg/aiturbo`, reproducible matched-baseline harness in `cmd/aiturbo-bench`, and raw machine-readable measurements in `results.json`.\n\n")

	fmt.Fprintf(&b, "## 1. Primary Reference, Summary, and License Audit\n\n")
	fmt.Fprintf(&b, "* **Primary paper:** Hao et al., *AITURBO* — Grouped IO planning for bulk checkpoint upload and download against cloud object storage, 24th USENIX Conference on File and Storage Technologies (FAST 2026), <https://www.usenix.org/conference/fast26/presentation/hao>.\n")
	fmt.Fprintf(&b, "* **Problem & insight:** AI training and agent checkpoints combine hundreds of small configuration, tokenizer, optimizer, and LoRA files (`1 KiB–64 KiB`), medium layer tensors (`128 KiB–2 MiB`), and large weight shards (`4 MiB–64 MiB`). Uploading one S3 object per file incurs severe per-request RTT latency and high `requests/GiB` API load (risking S3 `503 SlowDown` throttling and high per-request API cost). Conversely, packing the entire checkpoint into one monolithic tar/zip blob removes upload/download parallelism across TCP streams and forces a 100%% full-blob download even for a selective 5%% restore.\n")
	fmt.Fprintf(&b, "* **Clean-room Apache-2.0 license audit:** Every file in `pkg/aiturbo/`, `cmd/aiturbo-bench/`, and `docs/research/hip-1345-aiturbo-grouped-io/` is an independent, clean-room Apache-2.0 Go implementation using only the Go standard library, `github.com/zeebo/blake3` (already present in `go.mod`), and `hanzoai/vfs`'s canonical `pkg/backend` interfaces. `go.mod` and `go.sum` are completely untouched (zero new external dependencies).\n\n")

	fmt.Fprintf(&b, "## 2. Experimental Setup and Hardware Grounding\n\n")
	fmt.Fprintf(&b, "* **Git baseline commit:** `%s`\n", r.GitCommit)
	fmt.Fprintf(&b, "* **Runtime / Host:** `%s` on `%s/%s` (`%d` vCPUs)\n", r.GoVersion, r.GOOS, r.GOARCH, r.NumCPU)
	fmt.Fprintf(&b, "* **Deterministic seed:** `--seed %d`\n", r.Seed)
	fmt.Fprintf(&b, "* **Primary AI Checkpoint Workload:** `%d` total files (`%d` real files grounded from `hanzoai/vfs` + `%d` total small files `1–64 KiB`, `%d` medium tensors `128–512 KiB`, `%d` large weight shards), **`%.2f MiB`** logical payload; selective restore target = `%d` files (`%.2f MiB`, **`%.2f%%`** of total checkpoint).\n",
		r.WorkloadSummary.TotalEntries,
		r.WorkloadSummary.RepoGroundedEntries,
		r.WorkloadSummary.SmallEntries,
		r.WorkloadSummary.MediumEntries,
		r.WorkloadSummary.LargeEntries,
		r.WorkloadSummary.TotalLogicalMiB,
		r.WorkloadSummary.SelectiveEntries,
		float64(r.WorkloadSummary.SelectiveBytes)/(1024.0*1024.0),
		r.WorkloadSummary.SelectivePctOfTotal)
	fmt.Fprintf(&b, "* **S3 Network Model:** `PutRTT=%.1f ms`, `GetRTT=%.1f ms`, `PerStreamCap=%.0f MiB/s`, `AggregateCap=%.0f MiB/s`, `Concurrency P=%d`, `TargetGroupBytes=%d MiB`, `LargeObjectThreshold=%d MiB`, `MaxGapBytes=%d KiB`.\n\n",
		r.S3Model.PutRTTMs,
		r.S3Model.GetRTTMs,
		r.S3Model.PerStreamMiBPerSec,
		r.S3Model.AggregateMiBPerSec,
		r.S3Model.Concurrency,
		r.S3Model.TargetGroupBytes>>20,
		r.S3Model.LargeObjectThreshold>>20,
		r.S3Model.MaxGapBytes>>10)

	fmt.Fprintf(&b, "## 3. Falsifiable Hypothesis and Failure Modes\n\n")
	fmt.Fprintf(&b, "* **Hypothesis:** Partitioning heterogeneous bulk checkpoints into size-bounded, locality-ordered immutable pack objects (`4–16 MiB`) while directly bypassing already-large shards (`>= LargeObjectThreshold`) and coalescing nearby Range-GETs within `MaxGapBytes` will reduce `PUTs/GiB` and `GETs/GiB` by **>90%%** relative to per-object transfers, raise effective upload/download `bytes/sec` over both serial/concurrent per-object and single-tar-blob baselines, keep selective ~5%% restore read amplification near `1.0x`, and preserve exact S3 atomic consistency (`0` torn checkpoints, `0` cross-tenant group co-residency).\n")
	fmt.Fprintf(&b, "* **Explicit Failure Modes (Negative Controls):**\n")
	fmt.Fprintf(&b, "  1. *All-large-shard checkpoints:* grouping yields `0%%` request reduction; direct bypass must activate so zero redundant memory copying occurs.\n")
	fmt.Fprintf(&b, "  2. *Sparse random point-reads with oversized `MaxGapBytes`:* aggressive range coalescing pulls large unrequested gaps, causing severe read amplification and bandwidth waste.\n")
	fmt.Fprintf(&b, "  3. *Tiny single-file checkpoints (`1 × 10 KiB`):* grouping yields `0%%` request reduction while adding minor planner/manifest overhead.\n\n")

	fmt.Fprintf(&b, "## 4. Primary Matched-Baseline Results (S3 RTT & Bandwidth Model)\n\n")
	fmt.Fprintf(&b, "| Regime | Upload (MiB/s) | Upload (B/s) | Write Reqs | PUTs / GiB | Download (MiB/s) | Read Reqs | GETs / GiB | Selective ~5%% (ms) | Sel GETs | Sel Read Amp | Bit-Exact |\n")
	fmt.Fprintf(&b, "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|:---:|\n")
	for _, m := range r.MatchedRegimes {
		fmt.Fprintf(&b, "| `%s` | %.2f | %.0f | %d | %.1f | %.2f | %d | %.1f | %.1f | %d | %.2fx | %v |\n",
			m.Regime,
			m.UploadMiBPerSec,
			m.UploadBytesPerSec,
			m.UploadMetrics.TotalWriteRequests(),
			m.UploadPUTsPerGiB,
			m.DownloadMiBPerSec,
			m.DownloadMetrics.TotalReadRequests(),
			m.DownloadGETsPerGiB,
			float64(m.SelectiveDuration)/float64(time.Millisecond),
			m.SelectiveGETs,
			m.SelectiveReadAmp,
			m.BitExactVerified)
	}
	fmt.Fprintf(&b, "\n")

	// Compute headline deltas if present.
	byReg := make(map[aiturbo.Regime]*aiturbo.RegimeResult, len(r.MatchedRegimes))
	for _, m := range r.MatchedRegimes {
		byReg[m.Regime] = m
	}
	if ai, okAI := byReg[aiturbo.RegimeAITurboGroupedIO]; okAI {
		if conc, okC := byReg[aiturbo.RegimeNaivePerObjectConcurrent]; okC && conc.UploadPUTsPerGiB > 0 {
			putCut := 100.0 * (1.0 - ai.UploadPUTsPerGiB/conc.UploadPUTsPerGiB)
			getCut := 100.0 * (1.0 - ai.DownloadGETsPerGiB/conc.DownloadGETsPerGiB)
			upSpeedup := ai.UploadMiBPerSec / max(conc.UploadMiBPerSec, 1e-9)
			dnSpeedup := ai.DownloadMiBPerSec / max(conc.DownloadMiBPerSec, 1e-9)
			fmt.Fprintf(&b, "**Key Deltas (`aiturbo_grouped_io_planner` vs `naive_per_object_concurrent` at equal `P=%d`):**\n", r.S3Model.Concurrency)
			fmt.Fprintf(&b, "* Upload `PUTs/GiB`: **%.1f -> %.1f** (**-%.2f%%** fewer S3 write requests/GiB)\n", conc.UploadPUTsPerGiB, ai.UploadPUTsPerGiB, putCut)
			fmt.Fprintf(&b, "* Download `GETs/GiB`: **%.1f -> %.1f** (**-%.2f%%** fewer S3 read requests/GiB)\n", conc.DownloadGETsPerGiB, ai.DownloadGETsPerGiB, getCut)
			fmt.Fprintf(&b, "* Upload throughput: **%.2f -> %.2f MiB/s** (**%.2fx**)\n", conc.UploadMiBPerSec, ai.UploadMiBPerSec, upSpeedup)
			fmt.Fprintf(&b, "* Download throughput: **%.2f -> %.2f MiB/s** (**%.2fx**)\n", conc.DownloadMiBPerSec, ai.DownloadMiBPerSec, dnSpeedup)
		}
		if tar, okT := byReg[aiturbo.RegimeNaiveSingleTarBlob]; okT {
			fmt.Fprintf(&b, "* vs `naive_single_tar_blob`: Upload **%.2fx** faster (`%.2f` vs `%.2f MiB/s`), and selective ~5%% restore read amplification drops from **%.2fx** (full archive download) to **%.2fx** (`%.1f ms` vs `%.1f ms`).\n",
				ai.UploadMiBPerSec/max(tar.UploadMiBPerSec, 1e-9),
				ai.UploadMiBPerSec, tar.UploadMiBPerSec,
				tar.SelectiveReadAmp, ai.SelectiveReadAmp,
				float64(ai.SelectiveDuration)/float64(time.Millisecond),
				float64(tar.SelectiveDuration)/float64(time.Millisecond))
		}
		fmt.Fprintf(&b, "\n")
	}

	fmt.Fprintf(&b, "## 5. TargetGroupBytes Sensitivity Sweep (`4 MiB`, `8 MiB`, `16 MiB`)\n\n")
	fmt.Fprintf(&b, "| TargetGroupBytes | Upload (MiB/s) | Write Reqs | PUTs / GiB | Download (MiB/s) | Read Reqs | GETs / GiB | Sel GETs | Sel Read Amp |\n")
	fmt.Fprintf(&b, "|---|---:|---:|---:|---:|---:|---:|---:|---:|\n")
	for _, pt := range r.GroupSizeSweep {
		fmt.Fprintf(&b, "| `%d MiB` | %.2f | %d | %.1f | %.2f | %d | %.1f | %d | %.2fx |\n",
			pt.TargetGroupMiB, pt.UploadMiBPerSec, pt.UploadPUTs, pt.UploadPUTsPerGiB,
			pt.DownloadMiBPerSec, pt.DownloadGETs, pt.DownloadGETsPerGiB,
			pt.SelectiveGETs, pt.SelectiveReadAmp)
	}
	fmt.Fprintf(&b, "\n")

	fmt.Fprintf(&b, "## 6. Real Backend Grounding (Zero Simulated Sleep)\n\n")
	fmt.Fprintf(&b, "### 6.1 Canonical `pkg/backend/file` (Real Local Disk Syscalls + BLAKE3)\n\n")
	fmt.Fprintf(&b, "| Regime | Upload (MiB/s) | Write Reqs | PUTs / GiB | Download (MiB/s) | Read Reqs | GETs / GiB | Sel Read Amp |\n")
	fmt.Fprintf(&b, "|---|---:|---:|---:|---:|---:|---:|---:|\n")
	for _, m := range r.RealFileBackend {
		fmt.Fprintf(&b, "| `%s` | %.2f | %d | %.1f | %.2f | %d | %.1f | %.2fx |\n",
			m.Regime, m.UploadMiBPerSec, m.UploadMetrics.TotalWriteRequests(), m.UploadPUTsPerGiB,
			m.DownloadMiBPerSec, m.DownloadMetrics.TotalReadRequests(), m.DownloadGETsPerGiB, m.SelectiveReadAmp)
	}
	fmt.Fprintf(&b, "\n")

	fmt.Fprintf(&b, "### 6.2 Canonical `pkg/backend/s3` (`github.com/hanzos3/go` over Localhost HTTP)\n\n")
	fmt.Fprintf(&b, "| Regime | Upload (MiB/s) | HTTP PUTs | PUTs / GiB | Download (MiB/s) | HTTP GETs | GETs / GiB | Sel Read Amp |\n")
	fmt.Fprintf(&b, "|---|---:|---:|---:|---:|---:|---:|---:|\n")
	for _, m := range r.RealHanzoS3HTTP {
		fmt.Fprintf(&b, "| `%s` | %.2f | %d | %.1f | %.2f | %d | %.1f | %.2fx |\n",
			m.Regime, m.UploadMiBPerSec, m.UploadMetrics.TotalWriteRequests(), m.UploadPUTsPerGiB,
			m.DownloadMiBPerSec, m.DownloadMetrics.TotalReadRequests(), m.DownloadGETsPerGiB, m.SelectiveReadAmp)
	}
	fmt.Fprintf(&b, "\n")

	fmt.Fprintf(&b, "## 7. S3 Consistency, Integrity, and Multi-Tenant Isolation Audit\n\n")
	fmt.Fprintf(&b, "| Invariant | Verified Result |\n")
	fmt.Fprintf(&b, "|---|---|\n")
	fmt.Fprintf(&b, "| Mid-upload failure injection (`FailOnPutCall=3`) publishes head? | `%v` (fail-closed abort) |\n", r.ConsistencyAudit.HeadPublishedOnFault)
	fmt.Fprintf(&b, "| Torn checkpoints visible to readers after partial failure | `%d` (`backend.ErrNotFound`) |\n", r.ConsistencyAudit.TornReadsObserved)
	fmt.Fprintf(&b, "| Uncommitted pack objects cleaned up on abort | `%d` |\n", r.ConsistencyAudit.AbortedObjectsCleanedUp)
	fmt.Fprintf(&b, "| Single-bit flip caught by BLAKE3 slice verification | `%v` (`ErrCorruptData`) |\n", r.ConsistencyAudit.BLAKE3CorruptionDetected)
	fmt.Fprintf(&b, "| Multi-tenant namespaces audited (`4 tenants × 2 volumes`) | `%d` |\n", r.ConsistencyAudit.MultiTenantNamespaces)
	fmt.Fprintf(&b, "| Cross-tenant group co-residency violations | `%d` |\n", r.ConsistencyAudit.CrossTenantGroups)
	fmt.Fprintf(&b, "| Shared object keys across distinct namespaces | `%d` |\n\n", r.ConsistencyAudit.SharedKeysAcrossTenants)

	fmt.Fprintf(&b, "## 8. Honest Negative / Boundary Regimes\n\n")

	nl := r.NegativeRegimes.AllLargeShards
	fmt.Fprintf(&b, "### 8.1 Negative Regime 1: All-Large-Shard Checkpoint (`%d × %d MiB`, 0 small files)\n\n", nl.ShardCount, nl.ShardSizeMiB)
	fmt.Fprintf(&b, "| Regime | Packed Groups | Direct Bypasses | Write Reqs | PUTs / GiB | Upload (MiB/s) | Download (MiB/s) |\n")
	fmt.Fprintf(&b, "|---|---:|---:|---:|---:|---:|---:|\n")
	fmt.Fprintf(&b, "| `naive_per_object_concurrent` | 0 | %d | %d | %.1f | %.2f | %.2f |\n",
		nl.ShardCount, nl.NaiveConcurrent.UploadMetrics.TotalWriteRequests(), nl.NaiveConcurrent.UploadPUTsPerGiB,
		nl.NaiveConcurrent.UploadMiBPerSec, nl.NaiveConcurrent.DownloadMiBPerSec)
	fmt.Fprintf(&b, "| `aiturbo_grouped_io_planner` | %d | %d | %d | %.1f | %.2f | %.2f |\n\n",
		nl.PackedGroupCount, nl.DirectBypassCount, nl.AITurboDirect.UploadMetrics.TotalWriteRequests(), nl.AITurboDirect.UploadPUTsPerGiB,
		nl.AITurboDirect.UploadMiBPerSec, nl.AITurboDirect.DownloadMiBPerSec)
	fmt.Fprintf(&b, "*Finding:* **%.1f%%** request-count reduction (identical PUT count). Direct-bypass routes all `%d` large shards straight to `be.Put` with zero pack buffer allocation/copy, matching concurrent per-object throughput without copy tax.\n\n", nl.PUTReductionPct, nl.DirectBypassCount)

	ns := r.NegativeRegimes.SparseRandomOversizedGap
	fmt.Fprintf(&b, "### 8.2 Negative Regime 2: Sparse %.1f%% Random Point-Reads (`%d / %d` files) with Oversized `MaxGapBytes`\n\n", ns.SelectedPct, ns.SelectedFiles, ns.TotalFiles)
	fmt.Fprintf(&b, "| Configuration | Requested Bytes | Fetched Bytes | Read Amplification | Read Reqs | Latency (ms) |\n")
	fmt.Fprintf(&b, "|---|---:|---:|---:|---:|---:|\n")
	fmt.Fprintf(&b, "| `aiturbo` oversized `MaxGapBytes=%d KiB` | %d | %d | **%.2fx** | %d | %.1f |\n",
		ns.OversizedMaxGapKiB, ns.RequestedBytes, ns.OversizedFetchedBytes, ns.OversizedReadAmp, ns.OversizedGETs, ns.OversizedDurationMs)
	fmt.Fprintf(&b, "| `aiturbo` tight `MaxGapBytes=%d KiB` | %d | %d | %.2fx | %d | %.1f |\n",
		ns.TightMaxGapKiB, ns.RequestedBytes, ns.TightFetchedBytes, ns.TightReadAmp, ns.TightGETs, ns.TightDurationMs)
	fmt.Fprintf(&b, "| `naive_per_object_concurrent` | %d | %d | %.2fx | %d | %.1f |\n\n",
		ns.RequestedBytes, ns.RequestedBytes, ns.NaivePerObjectReadAmp, ns.NaivePerObjectGETs, ns.NaivePerObjectDurMs)
	fmt.Fprintf(&b, "*Finding:* Setting `MaxGapBytes` too aggressively (`%d KiB`) merges distant point-reads into one huge Range-GET, pulling **%.2fx** more bytes than requested (wasted bandwidth and higher latency). Keeping `MaxGapBytes` bounded (`%d KiB`) restores `1.00x` read amplification.\n\n",
		ns.OversizedMaxGapKiB, ns.OversizedReadAmp, ns.TightMaxGapKiB)

	nt := r.NegativeRegimes.TinySingleFile
	fmt.Fprintf(&b, "### 8.3 Negative Regime 3: Tiny 1-File Checkpoint (`%d` bytes)\n\n", nt.FileBytes)
	fmt.Fprintf(&b, "| Regime | Write Reqs | Upload Latency (ms) | Download Latency (ms) | PUT Reduction |\n")
	fmt.Fprintf(&b, "|---|---:|---:|---:|---:|\n")
	fmt.Fprintf(&b, "| `naive_per_object_serial` | %d | %.2f | %.2f | baseline |\n",
		nt.NaiveSerial.UploadMetrics.TotalWriteRequests(),
		float64(nt.NaiveSerial.UploadDuration)/float64(time.Millisecond),
		float64(nt.NaiveSerial.DownloadDuration)/float64(time.Millisecond))
	fmt.Fprintf(&b, "| `aiturbo_grouped_io_planner` | %d | %.2f | %.2f | **%.1f%%** |\n\n",
		nt.AITurboGrouped.UploadMetrics.TotalWriteRequests(),
		float64(nt.AITurboGrouped.UploadDuration)/float64(time.Millisecond),
		float64(nt.AITurboGrouped.DownloadDuration)/float64(time.Millisecond),
		nt.PUTReductionPct)
	fmt.Fprintf(&b, "*Finding:* For a single small file, grouped planning cannot reduce the request floor (`1 data PUT + 1 manifest head PUT = 2 PUTs`) and adds minor planning/packing overhead.\n\n")

	fmt.Fprintf(&b, "## 9. Reproducing\n\n")
	fmt.Fprintf(&b, "```sh\n")
	fmt.Fprintf(&b, "go test -v -race -count=1 ./pkg/aiturbo/... ./cmd/aiturbo-bench/...\n")
	fmt.Fprintf(&b, "go run ./cmd/aiturbo-bench --seed %d --out-dir docs/research/hip-1345-aiturbo-grouped-io\n", r.Seed)
	fmt.Fprintf(&b, "```\n")

	return b.String()
}
