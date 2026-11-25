// Command most-bench runs the reproducible HIP-1344 evaluation comparing
// Mirror-Optimized Storage Tiering (MOST) selective hot-data mirroring against
// private per-agent LRU, ordinary exclusive RAM/NVMe tier migration, and
// unselective mirror-all caching across 32 concurrent agents.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/hanzoai/vfs/internal/cache/most"
)

func main() {
	outDir := flag.String("out-dir", "", "optional directory to write raw_results.json and score_table.md")
	jsonOnly := flag.Bool("json", false, "print raw JSON report to stdout instead of Markdown table")
	scenarioFilter := flag.String("scenario", "", "run only a single named scenario")
	opsScale := flag.Float64("ops-scale", 1.0, "scale factor for OpsPerAgent (e.g., 0.25 for quick smoke runs)")
	flag.Parse()

	scenarios := most.DefaultScenarios()
	if *scenarioFilter != "" {
		var filtered []most.ScenarioSpec
		for _, s := range scenarios {
			if s.Name == *scenarioFilter {
				filtered = append(filtered, s)
			}
		}
		if len(filtered) == 0 {
			fmt.Fprintf(os.Stderr, "unknown scenario %q\n", *scenarioFilter)
			os.Exit(2)
		}
		scenarios = filtered
	}
	if *opsScale > 0 && *opsScale != 1.0 {
		for i := range scenarios {
			scaled := int(float64(scenarios[i].OpsPerAgent) * (*opsScale))
			if scaled < 100 {
				scaled = 100
			}
			scenarios[i].OpsPerAgent = scaled
		}
	}

	report, err := most.RunSuite(scenarios)
	if err != nil {
		fmt.Fprintf(os.Stderr, "most-bench failed: %v\n", err)
		os.Exit(1)
	}

	rawJSON, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "marshal report: %v\n", err)
		os.Exit(1)
	}

	md := formatScoreTableMarkdown(report)

	if *outDir != "" {
		if err := os.MkdirAll(*outDir, 0o755); err != nil {
			fmt.Fprintf(os.Stderr, "mkdir %s: %v\n", *outDir, err)
			os.Exit(1)
		}
		if err := os.WriteFile(filepath.Join(*outDir, "raw_results.json"), append(rawJSON, '\n'), 0o644); err != nil {
			fmt.Fprintf(os.Stderr, "write raw_results.json: %v\n", err)
			os.Exit(1)
		}
		if err := os.WriteFile(filepath.Join(*outDir, "score_table.md"), []byte(md), 0o644); err != nil {
			fmt.Fprintf(os.Stderr, "write score_table.md: %v\n", err)
			os.Exit(1)
		}
	}

	if *jsonOnly {
		fmt.Println(string(rawJSON))
	} else {
		fmt.Print(md)
	}
}

func formatScoreTableMarkdown(rep most.SuiteReport) string {
	var b strings.Builder
	b.WriteString("# HIP-1344 Score Table — Selective Hot-Data Mirroring (MOST, USENIX FAST '26)\n\n")
	fmt.Fprintf(&b, "- **Generated (UTC):** `%s`\n", rep.GeneratedAt)
	fmt.Fprintf(&b, "- **Runtime:** `%s` on `%s/%s` (`%d` CPU)\n", rep.GoVersion, rep.GOOS, rep.GOARCH, rep.NumCPU)
	b.WriteString("- **Backing Tiers:** Real pooled RAM slices (`internal/bufpool`) + real on-disk `os.File` `ReadAt`/`WriteAt` NVMe backing + `internal/blake3` cryptographic verification.\n\n")

	for _, sc := range rep.Scenarios {
		fmt.Fprintf(&b, "## Scenario: `%s` (%s)\n\n", sc.Spec.Name, sc.Spec.Kind)
		fmt.Fprintf(&b, "%s\n\n", sc.Spec.Description)
		fmt.Fprintf(&b, "- Agents: `%d`, BlockSize: `%d KiB`, Host RAM Ceiling: `%.1f MiB`, Shared Corpus: `%d` blocks (`%.1f MiB`), Private Corpus: `%d` blocks/agent, Ops/Agent: `%d` (`%d` total ops)\n\n",
			sc.Spec.NumAgents,
			sc.Spec.BlockSize>>10,
			float64(sc.Spec.HostRAMBudget)/(1024*1024),
			sc.Spec.SharedBlocks,
			float64(sc.Spec.SharedBlocks*sc.Spec.BlockSize)/(1024*1024),
			sc.Spec.PrivateBlocksPerAg,
			sc.Spec.OpsPerAgent,
			sc.Spec.NumAgents*sc.Spec.OpsPerAgent,
		)

		b.WriteString("| Policy | p50 (µs) | p95 (µs) | p99 (µs) | Resident RAM/Agent (MiB) | Total Host RAM (MiB) | RAM Hit Rate (%) | NVMe Eviction Writes (MiB) | Throughput (ops/s) | Cross-Tenant Leaks |\n")
		b.WriteString("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n")
		for _, r := range sc.Results {
			fmt.Fprintf(&b, "| `%s` | %.2f | %.2f | %.2f | %.3f | %.2f | %.2f%% | %.2f | %.0f | %d |\n",
				r.Policy,
				r.P50LatencyUs,
				r.P95LatencyUs,
				r.P99LatencyUs,
				r.Stats.ResidentRAMPerAgentMiB,
				r.Stats.TotalHostRAMMiB,
				r.Stats.RAMHitRatePct,
				r.Stats.NVMeEvictionWriteMiB,
				r.ThroughputOpsSec,
				r.Isolation.CrossTenantLeaks,
			)
		}
		b.WriteString("\n")
	}
	return b.String()
}
