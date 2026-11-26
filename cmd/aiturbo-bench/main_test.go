// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package main

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/hanzoai/vfs/pkg/aiturbo"
)

func TestBenchSuiteEndToEndAndReportGeneration(t *testing.T) {
	ctx := context.Background()
	cfg := SuiteConfig{
		Seed:           1345,
		RepoRoot:       "../..",
		PutRTT:         1 * time.Millisecond,
		GetRTT:         1 * time.Millisecond,
		PerStreamMiBS:  100.0,
		AggregateMiBS:  500.0,
		Concurrency:    4,
		TargetGroupMiB: 1,
		MaxGapKiB:      64,
		ScaleDownForCI: true,
	}

	report, err := runSuite(ctx, cfg)
	if err != nil {
		t.Fatalf("runSuite failed: %v", err)
	}

	if report.WorkloadSummary.RepoGroundedEntries == 0 {
		t.Fatalf("expected repo-grounded real files from hanzoai/vfs, got 0")
	}
	if len(report.MatchedRegimes) != 4 {
		t.Fatalf("expected 4 matched regimes, got %d", len(report.MatchedRegimes))
	}
	if len(report.RealFileBackend) != 3 {
		t.Fatalf("expected 3 real file backend regimes, got %d", len(report.RealFileBackend))
	}
	if len(report.RealHanzoS3HTTP) != 2 {
		t.Fatalf("expected 2 real Hanzo S3 HTTP regimes, got %d", len(report.RealHanzoS3HTTP))
	}

	byReg := make(map[aiturbo.Regime]*aiturbo.RegimeResult, len(report.MatchedRegimes))
	for _, m := range report.MatchedRegimes {
		if !m.BitExactVerified {
			t.Fatalf("regime %s was not bit-exact verified", m.Regime)
		}
		byReg[m.Regime] = m
	}

	// Verify AITURBO sharply reduces PUTs/GiB and GETs/GiB vs concurrent per-object baseline.
	ai := byReg[aiturbo.RegimeAITurboGroupedIO]
	conc := byReg[aiturbo.RegimeNaivePerObjectConcurrent]
	tar := byReg[aiturbo.RegimeNaiveSingleTarBlob]
	if ai.UploadPUTsPerGiB >= conc.UploadPUTsPerGiB*0.3 {
		t.Fatalf("expected AITURBO PUTs/GiB (%.1f) << concurrent naive (%.1f)", ai.UploadPUTsPerGiB, conc.UploadPUTsPerGiB)
	}
	if ai.DownloadGETsPerGiB >= conc.DownloadGETsPerGiB*0.3 {
		t.Fatalf("expected AITURBO GETs/GiB (%.1f) << concurrent naive (%.1f)", ai.DownloadGETsPerGiB, conc.DownloadGETsPerGiB)
	}
	if ai.SelectiveReadAmp >= tar.SelectiveReadAmp*0.5 {
		t.Fatalf("expected AITURBO selective read amp (%.2fx) << single tar blob (%.2fx)", ai.SelectiveReadAmp, tar.SelectiveReadAmp)
	}

	// Verify Consistency & Isolation Audit invariants.
	audit := report.ConsistencyAudit
	if !audit.MidUploadFaultInjected || audit.HeadPublishedOnFault || audit.TornReadsObserved != 0 {
		t.Fatalf("atomic S3 consistency audit failed: %+v", audit)
	}
	if !audit.BLAKE3CorruptionDetected {
		t.Fatalf("BLAKE3 corruption audit failed: %+v", audit)
	}
	if audit.CrossTenantGroups != 0 || audit.SharedKeysAcrossTenants != 0 {
		t.Fatalf("multi-tenant isolation audit failed: %+v", audit)
	}

	// Verify Negative Regimes report honest boundary behavior.
	neg := report.NegativeRegimes
	if neg.AllLargeShards.PackedGroupCount != 0 || neg.AllLargeShards.PUTReductionPct != 0 {
		t.Fatalf("expected 0 packed groups and 0%% PUT reduction on all-large shards: %+v", neg.AllLargeShards)
	}
	if neg.SparseRandomOversizedGap.OversizedReadAmp <= neg.SparseRandomOversizedGap.TightReadAmp*5.0 {
		t.Fatalf("expected oversized MaxGapBytes read amp (%.2fx) >> tight (%.2fx)",
			neg.SparseRandomOversizedGap.OversizedReadAmp, neg.SparseRandomOversizedGap.TightReadAmp)
	}
	if neg.TinySingleFile.PUTReductionPct != 0 {
		t.Fatalf("expected 0%% PUT reduction on tiny 1-file checkpoint, got %.2f%%", neg.TinySingleFile.PUTReductionPct)
	}

	// Verify JSON serialization and Markdown report rendering.
	raw, err := json.Marshal(report)
	if err != nil || len(raw) == 0 {
		t.Fatalf("json marshal failed: %v", err)
	}
	md := renderMarkdownReport(report)
	for _, section := range []string{
		"HIP-1345",
		"Primary Reference, Summary, and License Audit",
		"Primary Matched-Baseline Results",
		"TargetGroupBytes Sensitivity Sweep",
		"Real Backend Grounding",
		"S3 Consistency, Integrity, and Multi-Tenant Isolation Audit",
		"Honest Negative / Boundary Regimes",
	} {
		if !strings.Contains(md, section) {
			t.Fatalf("rendered markdown missing section %q", section)
		}
	}
}
