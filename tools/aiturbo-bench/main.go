// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

// Command aiturbo-bench runs reproducible loopback HTTP S3-compatible
// benchmarks comparing five matched checkpoint IO strategies and three
// documented honest negative/boundary regimes for HIP-1344 (hanzoai/vfs#60).
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/hanzoai/vfs/internal/checkpoint/aiturbo"
)

func main() {
	var (
		outDir      = flag.String("out-dir", "docs/research/hip-1344-aiturbo", "directory to write raw_results.json and score_table.md")
		iterations  = flag.Int("iterations", 5, "measured iterations per strategy")
		rtt         = flag.Duration("rtt", 2*time.Millisecond, "simulated per-HTTP-request S3 RTT (0 for native loopback)")
		streamBWMiB = flag.Int64("stream-bw-mib", 150, "per-HTTP-stream bandwidth cap in MiB/s (0 for unthrottled)")
		workers     = flag.Int("workers", aiturbo.DefaultWorkers, "bounded concurrent worker streams")
		groupMiB    = flag.Int64("group-mib", 4, "AITURBO super-object target size in MiB")
		casRacers   = flag.Int("cas-racers", 16, "concurrent goroutines for Head CAS conflict race test")
		seed        = flag.Uint64("seed", 20260401, "deterministic PRNG seed")
		writeFiles  = flag.Bool("write-dossier", true, "write raw_results.json and score_table.md to out-dir")
	)
	flag.Parse()

	cfg := aiturbo.BenchmarkConfig{
		Seed:                          *seed,
		Iterations:                    *iterations,
		RequestRTT:                    *rtt,
		PerStreamBandwidthBytesPerSec: (*streamBWMiB) << 20,
		MaxConcurrentRequests:         64,
		Workers:                       *workers,
		AITurboGroupTargetBytes:       (*groupMiB) << 20,
		CASRacers:                     *casRacers,
	}

	ctx := context.Background()
	report, err := aiturbo.RunBenchmarkSuite(ctx, cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "aiturbo-bench: %v\n", err)
		os.Exit(1)
	}

	md := renderScoreTableMarkdown(report)
	fmt.Print(md)

	if *writeFiles {
		if err := os.MkdirAll(*outDir, 0o755); err != nil {
			fmt.Fprintf(os.Stderr, "aiturbo-bench: mkdir %s: %v\n", *outDir, err)
			os.Exit(1)
		}
		rawJSON, err := json.MarshalIndent(report, "", "  ")
		if err != nil {
			fmt.Fprintf(os.Stderr, "aiturbo-bench: marshal json: %v\n", err)
			os.Exit(1)
		}
		rawJSON = append(rawJSON, '\n')
		rawPath := filepath.Join(*outDir, "raw_results.json")
		if err := os.WriteFile(rawPath, rawJSON, 0o644); err != nil {
			fmt.Fprintf(os.Stderr, "aiturbo-bench: write %s: %v\n", rawPath, err)
			os.Exit(1)
		}
		tablePath := filepath.Join(*outDir, "score_table.md")
		if err := os.WriteFile(tablePath, []byte(md), 0o644); err != nil {
			fmt.Fprintf(os.Stderr, "aiturbo-bench: write %s: %v\n", tablePath, err)
			os.Exit(1)
		}
		fmt.Fprintf(os.Stderr, "\nWrote %s and %s\n", rawPath, tablePath)
	}
}

func renderScoreTableMarkdown(r *aiturbo.SuiteReport) string {
	var b strings.Builder
	ckptMiB := float64(r.CheckpointBytes) / (1024.0 * 1024.0)
	streamCapMiB := float64(r.Config.PerStreamBandwidthBytesPerSec) / (1024.0 * 1024.0)

	fmt.Fprintf(&b, "# HIP-1344 AITURBO Grouped Checkpoint IO — Score Table\n\n")
	fmt.Fprintf(&b, "- **Generated (UTC)**: `%s`\n", r.TimestampUTC)
	fmt.Fprintf(&b, "- **Workload**: Mixed AI-model / agent-workspace checkpoint (`%d` files, `%.2f MiB`, seed `%d`)\n",
		r.CheckpointFiles, ckptMiB, r.Config.Seed)
	fmt.Fprintf(&b, "- **Loopback S3 Server**: real `net/http/httptest` path-style S3 with atomic `If-None-Match`/`If-Match` (HTTP 412) and `Range` (HTTP 206), `RTT=%.2f ms`, per-stream cap `%.0f MiB/s`, `W=%d`, `AITURBO group=%d MiB`, `%d` measured iterations/strategy\n",
		r.Config.RequestRTTMs, streamCapMiB, r.Config.Workers, r.Config.AITurboGroupTargetBytes>>20, r.Config.Iterations)
	fmt.Fprintf(&b, "- **S3 API Pricing Basis**: AWS S3 Standard list prices from `pkg/cost` (`$%.4f / 1k PUTs`, `$%.4f / 1k GETs`)\n\n",
		aiturbo.AWSS3StandardPutPer1k, aiturbo.AWSS3StandardGetPer1k)

	fmt.Fprintf(&b, "## 1. Matched 5-Strategy Checkpoint IO Score Table\n\n")
	fmt.Fprintf(&b, "| Strategy | Workers | Groups | Upload (MiB/s) | Full Restore (MiB/s) | PUTs / GiB | GETs / GiB | Est. API Cost / 100 GiB (W+R) | Commit p50 / p99 (ms) | Selective 16 KiB Data Read-Amp | Selective Latency (ms) | SHA-256 Bit-Exact | Head CAS |\n")
	fmt.Fprintf(&b, "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|:---:|:---:|\n")
	for _, s := range r.Scores {
		exact := "PASS"
		if !s.SHA256BitExact {
			exact = "FAIL"
		}
		cas := "PASS"
		if !s.HeadCASVerified {
			cas = "FAIL"
		}
		fmt.Fprintf(&b, "| `%s` | %d | %d | %.2f | %.2f | %.1f | %.1f | $%.4f | %.2f / %.2f | %.2fx | %.2f | %s | %s |\n",
			s.Strategy,
			s.Workers,
			s.GroupCount,
			s.UploadMBps,
			s.DownloadMBps,
			s.PutsPerGiB,
			s.GetsPerGiB,
			s.EstApiCostPer100GiBUSD,
			s.CommitP50Ms,
			s.CommitP99Ms,
			s.SelectiveDataReadAmp,
			s.SelectiveLatencyMs,
			exact,
			cas,
		)
	}

	// Highlight key relative improvements cleanly.
	var perObjSeq, perObjPar, monolith, aiStrat, overStrat *aiturbo.StrategyScore
	for i := range r.Scores {
		sc := &r.Scores[i]
		switch sc.Strategy {
		case aiturbo.StrategyPerObjectSequential:
			perObjSeq = sc
		case aiturbo.StrategyPerObjectParallel:
			perObjPar = sc
		case aiturbo.StrategyNaiveTarball:
			monolith = sc
		case aiturbo.StrategyAITurboGrouped:
			aiStrat = sc
		case aiturbo.StrategyOverCoalesced:
			overStrat = sc
		}
	}
	if aiStrat != nil && perObjPar != nil && perObjSeq != nil && monolith != nil && overStrat != nil {
		fmt.Fprintf(&b, "\n### Relative Delta Summary (`aiturbo_grouped_io_planner`)\n\n")
		fmt.Fprintf(&b, "- **S3 Request Reduction vs `per_object_parallel_baseline`**: **%.2fx fewer PUTs/GiB** (`%.1f` -> `%.1f`) and **%.2fx fewer GETs/GiB** (`%.1f` -> `%.1f`), cutting S3 API request cost per 100 GiB cycle by **%.1f%%** (`$%.4f` -> `$%.4f`).\n",
			perObjPar.PutsPerGiB/aiStrat.PutsPerGiB, perObjPar.PutsPerGiB, aiStrat.PutsPerGiB,
			perObjPar.GetsPerGiB/aiStrat.GetsPerGiB, perObjPar.GetsPerGiB, aiStrat.GetsPerGiB,
			(1.0-aiStrat.EstApiCostPer100GiBUSD/perObjPar.EstApiCostPer100GiBUSD)*100.0,
			perObjPar.EstApiCostPer100GiBUSD, aiStrat.EstApiCostPer100GiBUSD)
		fmt.Fprintf(&b, "- **Upload Throughput**: **%.2fx faster** than `per_object_sequential_baseline` (`%.2f` vs `%.2f MiB/s`), **%.2fx** vs `per_object_parallel_baseline` (`%.2f MiB/s`), and **%.2fx faster** than `naive_tarball_monolith` (`%.2f MiB/s`).\n",
			aiStrat.UploadMBps/perObjSeq.UploadMBps, aiStrat.UploadMBps, perObjSeq.UploadMBps,
			aiStrat.UploadMBps/perObjPar.UploadMBps, perObjPar.UploadMBps,
			aiStrat.UploadMBps/monolith.UploadMBps, monolith.UploadMBps)
		fmt.Fprintf(&b, "- **Selective 16 KiB Restore Read Amplification**: `%.2fx` data read-amp (`%.2f ms`) vs `%.2fx` (`%.2f ms`) on `naive_tarball_monolith` and `%.2fx` (`%.2f ms`) on `over_coalesced_giant_group_negative`.\n",
			aiStrat.SelectiveDataReadAmp, aiStrat.SelectiveLatencyMs,
			monolith.SelectiveDataReadAmp, monolith.SelectiveLatencyMs,
			overStrat.SelectiveDataReadAmp, overStrat.SelectiveLatencyMs)
	}

	// Negative & boundary regimes.
	n1 := r.NegativeRegimes.SelectiveSingleFileReadAmp
	n2 := r.NegativeRegimes.AlreadyLargeTensorOnly
	n3 := r.NegativeRegimes.ConcurrentHeadCASConflict

	fmt.Fprintf(&b, "\n## 2. Documented Honest Negative & Boundary Regimes\n\n")
	fmt.Fprintf(&b, "### Regime 1: `selective_single_file_restore_read_amp`\n\n")
	fmt.Fprintf(&b, "Restoring `%s` (`%d` bytes) from a `%.2f MiB` AITURBO super-object:\n\n",
		n1.TargetPath, n1.TargetBytes, float64(n1.GroupSizeBytes)/(1024.0*1024.0))
	fmt.Fprintf(&b, "| Fetch Mode | Data Bytes Fetched | Data Read-Amp | Total GETs | Latency (ms) | SHA-256 Exact |\n")
	fmt.Fprintf(&b, "|---|---:|---:|---:|---:|:---:|\n")
	fmt.Fprintf(&b, "| Full-Group Fetch (no HTTP Range) | %d | %.2fx | %d | %.2f | %v |\n",
		n1.FullGroupDataBytes, n1.FullGroupDataReadAmp, n1.FullGroupGetRequests, n1.FullGroupLatencyMs, n1.SHA256BitExactBoth)
	fmt.Fprintf(&b, "| Coalesced HTTP `Range` GET | %d | %.2fx | %d | %.2f | %v |\n\n",
		n1.RangeGetDataBytes, n1.RangeGetDataReadAmp, n1.RangeGetRequests, n1.RangeGetLatencyMs, n1.SHA256BitExactBoth)
	fmt.Fprintf(&b, "Without HTTP `Range` support, grouped super-objects suffer **%.2fx** payload read amplification; HTTP `Range` GET recovers exact **%.2fx** data read amplification (`%.2fx` fewer payload bytes) with the same GET count.\n\n",
		n1.FullGroupDataReadAmp, n1.RangeGetDataReadAmp, n1.BytesSavedRatio)

	fmt.Fprintf(&b, "### Regime 2: `already_large_tensor_only_checkpoint`\n\n")
	fmt.Fprintf(&b, "Checkpoint consisting solely of `%d` x `%d MiB` weight tensors (`%.2f MiB` total):\n\n",
		n2.FileCount, n2.TensorSizeBytes>>20, float64(n2.TotalBytes)/(1024.0*1024.0))
	fmt.Fprintf(&b, "| Strategy | PUTs / GiB | Packing Copy Buffer (MiB) | Upload Throughput (MiB/s) |\n")
	fmt.Fprintf(&b, "|---|---:|---:|---:|\n")
	fmt.Fprintf(&b, "| `per_object_parallel_baseline` | %.1f | 0.00 | %.2f |\n", n2.PerObjectParallelPutsPerGiB, n2.PerObjectUploadMBps)
	fmt.Fprintf(&b, "| `aiturbo_grouped_io_planner` (passthrough) | %.1f | %.2f | %.2f |\n", n2.AITurboPutsPerGiB, float64(n2.AITurboPassthroughCopyBytes)/(1024.0*1024.0), n2.AITurboUploadMBps)
	fmt.Fprintf(&b, "| `over_coalesced_giant_group_negative` (forced pack) | %.1f | %.2f | %.2f |\n\n", n2.ForcedPackPutsPerGiB, float64(n2.ForcedPackCopyBytes)/(1024.0*1024.0), n2.ForcedPackUploadMBps)
	fmt.Fprintf(&b, "Request reduction for already-large tensors is **%.1f%%** (zero gain). Size-aware passthrough avoids allocating `%.2f MiB` of redundant coalescing copy buffers and preserves full parallel stream throughput.\n\n",
		n2.RequestReductionPct, float64(n2.ForcedPackCopyBytes)/(1024.0*1024.0))

	fmt.Fprintf(&b, "### Regime 3: `concurrent_head_cas_conflict_retry`\n\n")
	fmt.Fprintf(&b, "| Metric | Measured Value |\n")
	fmt.Fprintf(&b, "|---|---:|\n")
	fmt.Fprintf(&b, "| Concurrent Racing Writers | %d |\n", n3.ConcurrentWriters)
	fmt.Fprintf(&b, "| Initial `If-None-Match: *` Winners / Conflicts | %d / %d |\n", n3.InitialCreateWinners, n3.InitialCreateConflicts)
	fmt.Fprintf(&b, "| Update `If-Match: <etag>` Winners / Conflicts | %d / %d |\n", n3.UpdateRaceWinners, n3.UpdateRaceConflicts)
	fmt.Fprintf(&b, "| Silent Overwrites | %d |\n", n3.SilentOverwrites)
	fmt.Fprintf(&b, "| Torn Checkpoints Observed | %d |\n", n3.TornReadsObserved)
	fmt.Fprintf(&b, "| Rebase & Retry to Next Generation | %v (final gen `%d`) |\n", n3.RetrySucceeded, n3.FinalGeneration)

	return b.String()
}
