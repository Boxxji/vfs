// Copyright 2026 Hanzo AI, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hanzoai/vfs/pkg/most"
)

func TestBenchPipelineEndToEndAndNoPathLeaks(t *testing.T) {
	outDir := t.TempDir()
	cfg := benchConfig{
		Seed:           1345,
		RepoRoot:       "../..",
		OutDir:         outDir,
		BlockSize:      4096,
		CorpusBlocks:   64,
		RAMBlocks:      16,
		OpsPerRun:      160,
		AgentCounts:    []int{1, 4},
		NVMeChannels:   4,
		NVMeReadLatUS:  40,
		NVMeWriteLatUS: 160,
	}

	rep, err := runBench(context.Background(), cfg)
	if err != nil {
		t.Fatalf("runBench: %v", err)
	}
	if err := writeResults(outDir, rep); err != nil {
		t.Fatalf("writeResults: %v", err)
	}

	raw, err := os.ReadFile(filepath.Join(outDir, "results.json"))
	if err != nil {
		t.Fatalf("read results.json: %v", err)
	}
	if strings.Contains(string(raw), "/home/") {
		t.Fatalf("results.json leaked local host path")
	}

	var decoded benchReport
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("unmarshal results.json: %v", err)
	}
	if decoded.Corpus.TotalBlocks != 64 || decoded.Corpus.RepoBlocksCount == 0 {
		t.Fatalf("unexpected corpus stats: %+v", decoded.Corpus)
	}
	if len(decoded.ScalingSweep) != 2*4 {
		t.Fatalf("ScalingSweep len = %d, want 8", len(decoded.ScalingSweep))
	}
	for _, negKey := range []string{
		"neg_uniform_no_skew_scan",
		"neg_write_heavy_invalidation_churn",
		"neg_single_agent_ample_ram",
	} {
		if len(decoded.NegativeRegimes[negKey]) == 0 {
			t.Fatalf("missing negative regime %q", negKey)
		}
	}
	if decoded.PQSecurityCheck.PlaintextLeaks != 0 || !decoded.PQSecurityCheck.CrossTenantDenied {
		t.Fatalf("unexpected PQ security check: %+v", decoded.PQSecurityCheck)
	}

	// Verify MOST incurred 0 NVMe demotion writes in scaling sweep.
	for _, r := range decoded.ScalingSweep {
		if r.Regime == most.RegimeMOSTSelectiveMirror && r.NVMeDemotionWrites != 0 {
			t.Fatalf("MOST had non-zero demotions: %+v", r)
		}
	}
}
