// Copyright 2026 Hanzo AI Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hanzoai/vfs/pkg/dogi"
)

func TestRunBenchmarkDeterministicAndAllWorkloads(t *testing.T) {
	// Point repoRoot to repository root (two levels up from cmd/dogi-bench).
	repoRoot := filepath.Join("..", "..")
	rep, err := runBenchmark(1345, repoRoot, 8, 256<<10, 0.50)
	if err != nil {
		t.Fatalf("runBenchmark failed: %v", err)
	}
	if len(rep.Workloads) != 5 {
		t.Fatalf("expected 5 workloads, got %d", len(rep.Workloads))
	}
	if rep.Environment.RepoFilesLoaded == 0 {
		t.Fatalf("expected real repo files loaded > 0")
	}

	byName := make(map[string]workloadResult)
	for _, w := range rep.Workloads {
		byName[w.Name] = w
		for _, pol := range allPolicies {
			pr := w.Policies[string(pol)]
			if pr == nil {
				t.Fatalf("workload %s missing policy %s", w.Name, pol)
			}
			if !pr.BitExactVerified || !pr.AccountingInvariantOK {
				t.Fatalf("workload %s policy %s failed verification", w.Name, pol)
			}
		}
	}

	// Verify positive workload 1: DOGI beats FIFO substantially on LiveCopyBytes.
	ci := byName["ci_build_plus_workspace"]
	if ci.Policies[string(dogi.PolicyDOGILifetime)].Metrics.LiveCopyBytes >= ci.Policies[string(dogi.PolicyFIFOMixed)].Metrics.LiveCopyBytes/2 {
		t.Fatalf("expected DOGI to cut ci_build_plus_workspace LiveCopyBytes by >50%%")
	}

	// Verify positive workload 2: DOGI beats static path grouping on opaque paths.
	op := byName["opaque_renamed_paths_online_learning"]
	if op.Policies[string(dogi.PolicyDOGILifetime)].Metrics.LiveCopyBytes >= op.Policies[string(dogi.PolicyPathGrouped)].Metrics.LiveCopyBytes {
		t.Fatalf("expected DOGI to beat static path grouping on opaque paths")
	}

	// Verify negative control 2: append-only has 0 GC copy for all policies and DOGI PUTs > FIFO PUTs.
	ao := byName["neg_append_only_no_deletes"]
	if ao.Policies[string(dogi.PolicyDOGILifetime)].Metrics.LiveCopyBytes != 0 {
		t.Fatalf("expected 0 LiveCopyBytes on append-only archive")
	}
	if ao.Policies[string(dogi.PolicyDOGILifetime)].Metrics.PacksUploaded <= ao.Policies[string(dogi.PolicyFIFOMixed)].Metrics.PacksUploaded {
		t.Fatalf("expected multi-stream DOGI PUTs > FIFO PUTs on append-only archive")
	}

	// Verify rendered markdown report contains all required sections and no local path leaks.
	md := renderMarkdownReport(rep)
	for _, needle := range []string{
		"HIP-1345",
		"ci_build_plus_workspace",
		"opaque_renamed_paths_online_learning",
		"neg_uniform_random_lifetime",
		"neg_append_only_no_deletes",
		"neg_adversarial_inverted_priors",
		"Pack-Size Sensitivity",
		"Integration Recommendation",
	} {
		if !strings.Contains(md, needle) {
			t.Fatalf("rendered markdown missing %q", needle)
		}
	}
	if strings.Contains(md, string(os.PathSeparator)+"home"+string(os.PathSeparator)) {
		t.Fatalf("rendered markdown leaked local home path")
	}
}
