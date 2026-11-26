// Copyright 2026 Hanzo AI Inc.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCalibratedBackendRTTAndBandwidth(t *testing.T) {
	srv, err := startMiniS3("pacer-test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = srv.Close() }()

	rawBE, err := newLiveHTTPBackend(srv, "pacer")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rawBE.Close() }()

	ctx := context.Background()
	payload := make([]byte, 256<<10)
	if err := rawBE.Put(ctx, "obj-a", payload); err != nil {
		t.Fatal(err)
	}

	cal := newCalibratedBackend(rawBE, 5*time.Millisecond, 100.0)
	start := time.Now()
	got, err := cal.GetRange(ctx, "obj-a", 0, int64(len(payload)))
	elapsed := time.Since(start)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(payload) {
		t.Fatalf("len=%d want=%d", len(got), len(payload))
	}
	if elapsed < 4*time.Millisecond {
		t.Fatalf("expected calibrated RTT >= 5ms, got %v", elapsed)
	}
	if cal.getOps.Load() != 1 || cal.bytes.Load() != int64(len(payload)) {
		t.Fatalf("counters mismatch: ops=%d bytes=%d", cal.getOps.Load(), cal.bytes.Load())
	}
}

func TestRunBenchmarkQuickSmoke(t *testing.T) {
	outDir := t.TempDir()
	repoRoot := filepath.Join("..", "..")
	if err := runBenchmark(context.Background(), 1345, repoRoot, outDir, 1, true); err != nil {
		t.Fatalf("runBenchmark quick: %v", err)
	}

	rawJSON, err := os.ReadFile(filepath.Join(outDir, "results.json"))
	if err != nil {
		t.Fatalf("read results.json: %v", err)
	}
	var res benchResults
	if err := json.Unmarshal(rawJSON, &res); err != nil {
		t.Fatalf("unmarshal results.json: %v", err)
	}
	if len(res.PositiveResults) != 3*3*4 {
		t.Fatalf("expected 36 positive cells (3 scenarios x 3 profiles x 4 regimes), got %d", len(res.PositiveResults))
	}
	if len(res.NegativeResults) < 9 {
		t.Fatalf("expected >= 9 negative regime cells, got %d", len(res.NegativeResults))
	}
	for _, cr := range res.CompatRejections {
		if !cr.Passed || cr.WireBytes != 0 {
			t.Fatalf("compat rejection failed: %+v", cr)
		}
	}

	mdBytes, err := os.ReadFile(filepath.Join(outDir, "REPORT.md"))
	if err != nil {
		t.Fatalf("read REPORT.md: %v", err)
	}
	md := string(mdBytes)
	for _, needle := range []string{
		"HIP-1345",
		"code_edit_5pct_shift",
		"inplace_update_10pct",
		"incremental_branch_switch_20pct",
		"neg_high_churn_reencrypted_100pct",
		"neg_tiny_checkpoint_cold_meta",
		"neg_unrelated_base_zero_overlap",
		"ErrTenantIsolation",
	} {
		if !strings.Contains(md, needle) {
			t.Fatalf("REPORT.md missing expected section %q", needle)
		}
	}
}
