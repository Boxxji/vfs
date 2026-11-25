// Command dogi-bench executes reproducible real-disk benchmarks comparing 5
// matched initial object-packing policies and 3 documented negative/boundary
// regimes for hanzoai/vfs#63 (HIP-1344, DOGI, USENIX FAST '26).
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/hanzoai/vfs/internal/packing/dogi"
)

type BenchReport struct {
	SchemaVersion     string           `json:"schema_version"`
	GeneratedAtUTC    string           `json:"generated_at_utc"`
	Environment       BenchEnv         `json:"environment"`
	PrimaryWorkload   []dogi.RunResult `json:"primary_workload"`
	NegativeUniform   []dogi.RunResult `json:"negative_uniform_random_delete"`
	NegativeUnderfill []dogi.RunResult `json:"negative_over_fragmented_64_bucket_underfill"`
	BoundaryColdStart []dogi.RunResult `json:"boundary_adversarial_renamed_ephemeral_cold_start"`
}

type BenchEnv struct {
	GoVersion             string `json:"go_version"`
	GOOS                  string `json:"goos"`
	GOARCH                string `json:"goarch"`
	NumCPU                int    `json:"num_cpu"`
	SlabTargetBytes       int    `json:"slab_target_bytes"`
	MaxOpenSlabsPerTenant int    `json:"max_open_slabs_per_tenant"`
	Seed                  int64  `json:"seed"`
}

func main() {
	var (
		outDir     = flag.String("out", "", "optional directory to write raw_results.json and score_table.md")
		slabTarget = flag.Int("slab-bytes", dogi.DefaultSlabTargetBytes, "packed slab target size in bytes")
		maxOpen    = flag.Int("max-open-slabs", dogi.DefaultMaxOpenSlabsPerTenant, "bounded open-slab cap per tenant")
		seed       = flag.Int64("seed", 42, "deterministic PRNG seed")
	)
	flag.Parse()

	report := BenchReport{
		SchemaVersion:  "hip-1344-dogi-v1",
		GeneratedAtUTC: time.Now().UTC().Format(time.RFC3339),
		Environment: BenchEnv{
			GoVersion:             runtime.Version(),
			GOOS:                  runtime.GOOS,
			GOARCH:                runtime.GOARCH,
			NumCPU:                runtime.NumCPU(),
			SlabTargetBytes:       *slabTarget,
			MaxOpenSlabsPerTenant: *maxOpen,
			Seed:                  *seed,
		},
	}

	// 1. Primary workload: 5 matched policies on ai_agent_build_churn.
	primaryOps := dogi.GenerateWorkload(dogi.WorkloadAIAgentBuildChurn, *seed)
	for _, pol := range dogi.AllPolicies {
		res, err := dogi.RunWorkload(dogi.WorkloadAIAgentBuildChurn, primaryOps, pol, *slabTarget, *maxOpen)
		if err != nil {
			fatal("primary %s: %v", pol, err)
		}
		report.PrimaryWorkload = append(report.PrimaryWorkload, res)
	}

	// 2. Negative Regime 1: uniform_random_delete_no_lifetime_skew.
	uniformOps := dogi.GenerateWorkload(dogi.WorkloadUniformRandomDelete, *seed+57)
	for _, pol := range []dogi.PolicyKind{dogi.PolicyMixedFIFO, dogi.PolicyStaticPath, dogi.PolicyDOGIAdaptive} {
		res, err := dogi.RunWorkload(dogi.WorkloadUniformRandomDelete, uniformOps, pol, *slabTarget, *maxOpen)
		if err != nil {
			fatal("uniform %s: %v", pol, err)
		}
		report.NegativeUniform = append(report.NegativeUniform, res)
	}

	// 3. Negative Regime 2: over_fragmented_64_bucket_underfill.
	// Compare 3-class DOGI (bounded 4 open slabs) vs 64-bucket over-fragmented
	// under both bounded (eviction thrash) and uncapped (64 open slabs RAM) settings.
	underfillOps := dogi.GenerateWorkload(dogi.WorkloadOverFragmentedUnderfill, *seed+35)
	for _, tc := range []struct {
		pol     dogi.PolicyKind
		capOpen int
	}{
		{dogi.PolicyMixedFIFO, *maxOpen},
		{dogi.PolicyDOGIAdaptive, *maxOpen},
		{dogi.PolicyOverFragmented, *maxOpen},
		{dogi.PolicyOverFragmented, 64},
	} {
		res, err := dogi.RunWorkload(dogi.WorkloadOverFragmentedUnderfill, underfillOps, tc.pol, *slabTarget, tc.capOpen)
		if err != nil {
			fatal("underfill %s (cap=%d): %v", tc.pol, tc.capOpen, err)
		}
		report.NegativeUnderfill = append(report.NegativeUnderfill, res)
	}

	// 4. Boundary Regime 3: adversarial_renamed_ephemeral_cold_start.
	coldOps := dogi.GenerateWorkload(dogi.WorkloadAdversarialColdStart, *seed+81)
	for _, pol := range []dogi.PolicyKind{dogi.PolicyMixedFIFO, dogi.PolicyStaticPath, dogi.PolicyDOGIAdaptive} {
		res, err := dogi.RunWorkload(dogi.WorkloadAdversarialColdStart, coldOps, pol, *slabTarget, *maxOpen)
		if err != nil {
			fatal("cold-start %s: %v", pol, err)
		}
		report.BoundaryColdStart = append(report.BoundaryColdStart, res)
	}

	md := renderScoreTable(report)
	fmt.Print(md)

	if *outDir != "" {
		if err := os.MkdirAll(*outDir, 0o755); err != nil {
			fatal("mkdir %s: %v", *outDir, err)
		}
		rawBytes, err := json.MarshalIndent(report, "", "  ")
		if err != nil {
			fatal("marshal json: %v", err)
		}
		rawBytes = append(rawBytes, '\n')
		if err := os.WriteFile(filepath.Join(*outDir, "raw_results.json"), rawBytes, 0o644); err != nil {
			fatal("write raw_results.json: %v", err)
		}
		if err := os.WriteFile(filepath.Join(*outDir, "score_table.md"), []byte(md), 0o644); err != nil {
			fatal("write score_table.md: %v", err)
		}
	}
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "dogi-bench: "+format+"\n", args...)
	os.Exit(1)
}

func toMiB(b int64) float64 {
	return float64(b) / (1024.0 * 1024.0)
}

func toMiBU(b uint64) float64 {
	return float64(b) / (1024.0 * 1024.0)
}

func renderScoreTable(r BenchReport) string {
	var sb strings.Builder
	sb.WriteString("# HIP-1344 / DOGI Real-Disk Benchmark Score Table\n\n")
	sb.WriteString("> **Measurement Scope**: All figures below were generated on the authoring host using real on-disk `.pack` files (`os.MkdirTemp`, `WriteAt`/`ReadAt`, Linux `/proc/self/io`, and SHA-256 bit-exact verification). They are reproducible measurements of this clean-room Go implementation, **not** Hanzo production SLA guarantees and **not** copied from the USENIX FAST '26 paper.\n\n")
	fmt.Fprintf(&sb, "- **Generated (UTC)**: `%s`\n", r.GeneratedAtUTC)
	fmt.Fprintf(&sb, "- **Toolchain / Host**: `%s` (`%s/%s`, `%d` vCPUs)\n",
		r.Environment.GoVersion, r.Environment.GOOS, r.Environment.GOARCH, r.Environment.NumCPU)
	fmt.Fprintf(&sb, "- **Geometry**: `SlabTargetBytes = %d KiB`, `MaxOpenSlabsPerTenant = %d`, `Seed = %d`\n\n",
		r.Environment.SlabTargetBytes>>10, r.Environment.MaxOpenSlabsPerTenant, r.Environment.Seed)

	// Section 1: Primary Matched 5-Policy Table
	sb.WriteString("## 1. Primary Workload: `ai_agent_build_churn` (5 Matched Initial-Packing Policies)\n\n")
	sb.WriteString("| Policy | Live Copied (MiB) | Live-Copy Delta vs Baseline | WAF (Phys/Log) | Final Space Amp | Zero-Copy Clean Reclaim | Slabs Sealed (PUTs) | Avg Slab Fill | Peak Open-Slab RAM (MiB) | `/proc/self/io` `wchar` (MiB) | `syscw` | SHA-256 Verified |\n")
	sb.WriteString("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n")

	var baseCopied int64
	for _, res := range r.PrimaryWorkload {
		if res.Policy == dogi.PolicyMixedFIFO {
			baseCopied = res.Metrics.LiveBytesCopied
			break
		}
	}
	for _, res := range r.PrimaryWorkload {
		m := res.Metrics
		deltaStr := "baseline"
		if res.Policy != dogi.PolicyMixedFIFO && baseCopied > 0 {
			pct := (float64(m.LiveBytesCopied-baseCopied) / float64(baseCopied)) * 100.0
			deltaStr = fmt.Sprintf("%+.1f%%", pct)
		}
		wcharStr := "n/a"
		syscwStr := "n/a"
		if res.ProcIO.Available {
			wcharStr = fmt.Sprintf("%.2f", toMiBU(res.ProcIO.WChar))
			syscwStr = fmt.Sprintf("%d", res.ProcIO.SysCW)
		}
		fmt.Fprintf(&sb, "| `%s` | %.2f | %s | %.3fx | %.3fx | %.1f%% (%d/%d) | %d | %.1f%% | %.2f | %s | %s | %d (0 err) |\n",
			res.Policy,
			toMiB(m.LiveBytesCopied),
			deltaStr,
			m.WAF,
			m.FinalSpaceAmp,
			m.ZeroCopyReclaimRate*100.0, m.CleanSlabsZeroCopy, m.SlabsReclaimedTotal,
			m.SlabsSealedTotal,
			m.AvgSlabFillRatio*100.0,
			toMiB(m.PeakOpenSlabBytes),
			wcharStr,
			syscwStr,
			res.LiveObjectsVerified,
		)
	}
	sb.WriteString("\n")

	// Section 2: Negative Regime 1
	sb.WriteString("## 2. Honest Negative Regime 1: `uniform_random_delete_no_lifetime_skew`\n\n")
	sb.WriteString("Every object follows an identical memoryless geometric lifetime regardless of path or size. Lifetime segregation provides near-zero reduction in live-data copying while holding additional open slab buffers.\n\n")
	sb.WriteString("| Policy | Live Copied (MiB) | WAF (Phys/Log) | Final Space Amp | Zero-Copy Clean Reclaim | Peak Open Slabs | Peak Open-Slab RAM (MiB) | SHA-256 Verified |\n")
	sb.WriteString("|---|---:|---:|---:|---:|---:|---:|---:|\n")
	for _, res := range r.NegativeUniform {
		m := res.Metrics
		fmt.Fprintf(&sb, "| `%s` | %.2f | %.3fx | %.3fx | %.1f%% (%d/%d) | %d | %.2f | %d (0 err) |\n",
			res.Policy,
			toMiB(m.LiveBytesCopied),
			m.WAF,
			m.FinalSpaceAmp,
			m.ZeroCopyReclaimRate*100.0, m.CleanSlabsZeroCopy, m.SlabsReclaimedTotal,
			m.PeakOpenSlabs,
			toMiB(m.PeakOpenSlabBytes),
			res.LiveObjectsVerified,
		)
	}
	sb.WriteString("\n")

	// Section 3: Negative Regime 2
	sb.WriteString("## 3. Honest Negative Regime 2: `over_fragmented_64_bucket_underfill`\n\n")
	sb.WriteString("Moderate-rate workload with periodic generation flushes. Splitting into 64 ultra-fine buckets causes severe partial-slab tail flushing, low slab fill ratio, high slab count, and either open-slab eviction thrashing (when RAM-capped) or open-slab RAM explosion (when uncapped).\n\n")
	sb.WriteString("| Policy (MaxOpenCap) | Slabs Sealed | Avg Slab Fill | Open-Slab Evictions | Peak Open Slabs | Peak Open-Slab RAM (MiB) | Final Space Amp | Live Copied (MiB) |\n")
	sb.WriteString("|---|---:|---:|---:|---:|---:|---:|---:|\n")
	for _, res := range r.NegativeUnderfill {
		m := res.Metrics
		fmt.Fprintf(&sb, "| `%s` (`cap=%d`) | %d | %.1f%% | %d | %d | %.2f | %.3fx | %.2f |\n",
			res.Policy, m.MaxOpenSlabsPerTenant,
			m.SlabsSealedTotal,
			m.AvgSlabFillRatio*100.0,
			m.OpenSlabEvictions,
			m.PeakOpenSlabs,
			toMiB(m.PeakOpenSlabBytes),
			m.FinalSpaceAmp,
			toMiB(m.LiveBytesCopied),
		)
	}
	sb.WriteString("\n")

	// Section 4: Boundary Regime 3
	sb.WriteString("## 4. Honest Boundary Regime 3: `adversarial_renamed_ephemeral_cold_start`\n\n")
	sb.WriteString("Ephemeral build objects written under deceptive `.bin` paths alongside long-lived `.parquet` files. Measures exact causal cold-start misclassification window before online EWMA survival feedback converges.\n\n")
	sb.WriteString("| Policy | Cold-Start Misclassified | Convergence Delete Count | Convergence Epoch | Post-Convergence Accuracy | Live Copied (MiB) | WAF (Phys/Log) | Zero-Copy Clean Reclaim |\n")
	sb.WriteString("|---|---:|---:|---:|---:|---:|---:|---:|\n")
	for _, res := range r.BoundaryColdStart {
		m := res.Metrics
		misclassStr := "n/a (no classifier)"
		convDel := "n/a"
		convEp := "n/a"
		postAcc := "n/a"
		if res.DeceptiveObjectsTotal > 0 {
			misclassStr = fmt.Sprintf("%d / %d", res.ColdStartMisclassified, res.DeceptiveObjectsTotal)
			if res.ConvergenceEpoch > 0 {
				convDel = fmt.Sprintf("%d", res.ConvergenceDeleteCount)
				convEp = fmt.Sprintf("%d", res.ConvergenceEpoch)
				postAcc = fmt.Sprintf("%.1f%%", res.PostConvergenceAccuracy*100.0)
			} else {
				convDel = "never"
				convEp = "never"
				postAcc = "0.0%"
			}
		}
		fmt.Fprintf(&sb, "| `%s` | %s | %s | %s | %s | %.2f | %.3fx | %.1f%% (%d/%d) |\n",
			res.Policy,
			misclassStr,
			convDel,
			convEp,
			postAcc,
			toMiB(m.LiveBytesCopied),
			m.WAF,
			m.ZeroCopyReclaimRate*100.0, m.CleanSlabsZeroCopy, m.SlabsReclaimedTotal,
		)
	}
	sb.WriteString("\n")
	return sb.String()
}
