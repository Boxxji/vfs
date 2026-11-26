// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package aiturbo

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"testing"

	"github.com/zeebo/blake3"

	"github.com/hanzoai/vfs/pkg/backend"
)

func makeDeterministicBytes(seed int64, n int) []byte {
	rng := rand.New(rand.NewSource(seed))
	b := make([]byte, n)
	_, _ = rng.Read(b)
	return b
}

func TestNamespaceValidationAndKeyGrammar(t *testing.T) {
	valid := []Namespace{
		{TenantID: "acme", VolumeID: "vol-1"},
		{TenantID: "org/sub", VolumeID: "ckpt_prod.2026"},
		{TenantID: "tenant:123", VolumeID: "v:456"},
	}
	for _, ns := range valid {
		if err := ns.Validate(); err != nil {
			t.Fatalf("expected valid namespace %+v, got %v", ns, err)
		}
		sum := blake3.Sum256([]byte("sample"))
		for _, k := range []string{ns.PackKey(sum), ns.DirectKey(sum), ns.HeadKey("step-1000/v1")} {
			if err := backend.ValidKey(k); err != nil {
				t.Fatalf("generated key %q failed backend.ValidKey: %v", k, err)
			}
			if !ns.OwnsKey(k) {
				t.Fatalf("ns %+v does not own its own key %q", ns, k)
			}
		}
	}

	// Length-prefixed BLAKE3 namespace encoding must never collide on delimiter shifts.
	nsA := Namespace{TenantID: "a/b", VolumeID: "c"}
	nsB := Namespace{TenantID: "a", VolumeID: "b/c"}
	if nsA.KeyPrefix() == nsB.KeyPrefix() {
		t.Fatalf("namespace prefix collision between %+v and %+v", nsA, nsB)
	}

	invalid := []Namespace{
		{TenantID: "", VolumeID: "vol"},
		{TenantID: "acme", VolumeID: ""},
		{TenantID: " acme", VolumeID: "vol"},
		{TenantID: "acme\x00", VolumeID: "vol"},
		{TenantID: "acme", VolumeID: "vol\n"},
	}
	for _, ns := range invalid {
		if err := ns.Validate(); !errors.Is(err, ErrInvalidNamespace) {
			t.Fatalf("expected ErrInvalidNamespace for %+v, got %v", ns, err)
		}
	}
}

func TestConfigValidation(t *testing.T) {
	cfg, err := (Config{}).Normalized()
	if err != nil {
		t.Fatalf("zero Config should normalize with defaults: %v", err)
	}
	if cfg.TargetGroupBytes != DefaultTargetGroupBytes || cfg.Concurrency != DefaultConcurrency {
		t.Fatalf("unexpected normalized defaults: %+v", cfg)
	}

	bad := []Config{
		{TargetGroupBytes: 1024},                    // below MinTargetGroupBytes
		{TargetGroupBytes: MaxTargetGroupBytes + 1}, // above MaxTargetGroupBytes
		{MaxGapBytes: -1},
		{Concurrency: -2},
		{Concurrency: 512},
		{TargetGroupBytes: 8 << 20, MaxCoalescedRangeBytes: 4 << 20}, // smaller than TargetGroupBytes
	}
	for i, c := range bad {
		if _, err := c.Normalized(); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("bad[%d]=%+v: expected ErrInvalidConfig, got %v", i, c, err)
		}
	}
}

func TestPlanUploadGroupingDirectBypassAndDeterminism(t *testing.T) {
	ns := Namespace{TenantID: "tenant-alpha", VolumeID: "vol-ckpt-01"}
	entries := make([]Entry, 0, 70)

	// 60 small files (32 KiB each = 1.875 MiB) across 3 layers.
	for i := 0; i < 60; i++ {
		entries = append(entries, Entry{
			TenantID: ns.TenantID,
			VolumeID: ns.VolumeID,
			Path:     fmt.Sprintf("layers/layer-%02d/param-%03d.bin", i/20, i),
			Data:     makeDeterministicBytes(int64(100+i), 32<<10),
		})
	}
	// 4 medium files (512 KiB each = 2 MiB).
	for i := 0; i < 4; i++ {
		entries = append(entries, Entry{
			TenantID: ns.TenantID,
			VolumeID: ns.VolumeID,
			Path:     fmt.Sprintf("opt/state-%02d.bin", i),
			Data:     makeDeterministicBytes(int64(500+i), 512<<10),
		})
	}
	// 2 distinct large shards (2 MiB each) + 1 duplicate large shard.
	big0 := makeDeterministicBytes(900, 2<<20)
	big1 := makeDeterministicBytes(901, 2<<20)
	entries = append(entries,
		Entry{TenantID: ns.TenantID, VolumeID: ns.VolumeID, Path: "weights/shard-00.safetensors", Data: big0},
		Entry{TenantID: ns.TenantID, VolumeID: ns.VolumeID, Path: "weights/shard-01.safetensors", Data: big1},
		Entry{TenantID: ns.TenantID, VolumeID: ns.VolumeID, Path: "weights/shard-00-dup.safetensors", Data: big0},
		Entry{TenantID: ns.TenantID, VolumeID: ns.VolumeID, Path: "meta/empty.marker", Data: []byte{}},
	)

	cfg := DefaultConfig()
	cfg.TargetGroupBytes = 2 << 20
	cfg.LargeObjectThreshold = 2 << 20

	planA, err := PlanUpload(entries, cfg)
	if err != nil {
		t.Fatalf("PlanUpload failed: %v", err)
	}
	if planA.DirectObjectCount != 2 {
		t.Fatalf("expected 2 deduplicated direct objects, got %d", planA.DirectObjectCount)
	}
	if planA.PackedGroupCount != 2 {
		t.Fatalf("expected 2 packed groups for ~3.875 MiB at 2 MiB target, got %d", planA.PackedGroupCount)
	}
	if len(planA.EmptyEntries) != 1 {
		t.Fatalf("expected 1 empty entry, got %d", len(planA.EmptyEntries))
	}
	for _, g := range planA.Groups {
		if !g.DirectBypass && g.TotalBytes > cfg.TargetGroupBytes {
			t.Fatalf("packed group %s exceeded TargetGroupBytes: %d > %d", g.ObjectKey, g.TotalBytes, cfg.TargetGroupBytes)
		}
	}

	// Reversed input order must yield an identical deterministic plan.
	rev := make([]Entry, len(entries))
	for i := range entries {
		rev[len(entries)-1-i] = entries[i]
	}
	planB, err := PlanUpload(rev, cfg)
	if err != nil {
		t.Fatalf("PlanUpload reversed failed: %v", err)
	}
	if len(planA.Groups) != len(planB.Groups) {
		t.Fatalf("non-deterministic group count: %d vs %d", len(planA.Groups), len(planB.Groups))
	}
	for i := range planA.Groups {
		if planA.Groups[i].ObjectKey != planB.Groups[i].ObjectKey || planA.Groups[i].TotalBytes != planB.Groups[i].TotalBytes {
			t.Fatalf("non-deterministic group[%d]: %+v vs %+v", i, planA.Groups[i].ObjectKey, planB.Groups[i].ObjectKey)
		}
	}
}

func TestMultiTenantIsolationZeroCoresidency(t *testing.T) {
	sharedPayload := makeDeterministicBytes(42, 64<<10)
	entries := make([]Entry, 0)
	for tenant := 0; tenant < 6; tenant++ {
		for vol := 0; vol < 3; vol++ {
			for f := 0; f < 5; f++ {
				entries = append(entries, Entry{
					TenantID: fmt.Sprintf("tenant-%02d", tenant),
					VolumeID: fmt.Sprintf("vol-%02d", vol),
					Path:     fmt.Sprintf("ckpt/file-%02d.bin", f),
					Data:     sharedPayload, // identical payload across tenants!
				})
			}
		}
	}

	cfg := DefaultConfig()
	cfg.TargetGroupBytes = 8 << 20
	plan, err := PlanUpload(entries, cfg)
	if err != nil {
		t.Fatalf("PlanUpload multi-tenant failed: %v", err)
	}
	if plan.TenantNamespaceCount != 18 {
		t.Fatalf("expected 18 namespaces, got %d", plan.TenantNamespaceCount)
	}
	// Each namespace must have at least 1 separate group (never merged even though
	// total data across all 18 namespaces is only ~5.6 MiB < 8 MiB TargetGroupBytes).
	if len(plan.Groups) != 18 {
		t.Fatalf("expected 18 isolated groups (0 cross-tenant co-residency), got %d", len(plan.Groups))
	}
	if err := VerifyTenantIsolation(plan); err != nil {
		t.Fatalf("VerifyTenantIsolation failed: %v", err)
	}

	// Verify no ObjectKey is shared across namespaces even when content bytes are identical.
	seenKeys := make(map[string]Namespace, len(plan.Groups))
	for _, g := range plan.Groups {
		if prev, dup := seenKeys[g.ObjectKey]; dup {
			t.Fatalf("cross-tenant key collision: %s shared by %+v and %+v", g.ObjectKey, prev, g.Namespace)
		}
		seenKeys[g.ObjectKey] = g.Namespace
	}
}

func TestPlanDownloadGapCoalescingAndReadAmplification(t *testing.T) {
	ns := Namespace{TenantID: "t1", VolumeID: "v1"}
	entries := make([]Entry, 10)
	for i := 0; i < 10; i++ {
		entries[i] = Entry{
			TenantID: ns.TenantID,
			VolumeID: ns.VolumeID,
			Path:     fmt.Sprintf("layer/chunk-%02d.bin", i),
			Data:     makeDeterministicBytes(int64(2000+i), 32<<10), // 32 KiB each
		}
	}

	cfg := DefaultConfig()
	cfg.TargetGroupBytes = 4 << 20
	upPlan, err := PlanUpload(entries, cfg)
	if err != nil {
		t.Fatal(err)
	}
	manifest := buildManifest(ns, "ckpt-gap", StrategyAITurboGrouped, upPlan)

	// Select chunks 0, 2, 4 (separated by 32 KiB gaps each).
	sel := []string{"layer/chunk-00.bin", "layer/chunk-02.bin", "layer/chunk-04.bin"}

	// With MaxGapBytes = 0 -> 3 separate Range-GETs, 1.0x read amplification.
	cfgStrict := cfg
	cfgStrict.MaxGapBytes = 0
	dpStrict, err := PlanDownload(manifest, sel, cfgStrict)
	if err != nil {
		t.Fatal(err)
	}
	if len(dpStrict.Ranges) != 3 || dpStrict.TotalGapBytes != 0 || dpStrict.ReadAmplification != 1.0 {
		t.Fatalf("strict gap=0 plan unexpected: ranges=%d gaps=%d amp=%.3f",
			len(dpStrict.Ranges), dpStrict.TotalGapBytes, dpStrict.ReadAmplification)
	}

	// With MaxGapBytes = 64 KiB -> coalesced into 1 Range-GET spanning chunks 0..4 (64 KiB gap).
	cfgCoalesce := cfg
	cfgCoalesce.MaxGapBytes = 64 << 10
	dpCoalesce, err := PlanDownload(manifest, sel, cfgCoalesce)
	if err != nil {
		t.Fatal(err)
	}
	if len(dpCoalesce.Ranges) != 1 {
		t.Fatalf("expected 1 coalesced Range-GET, got %d", len(dpCoalesce.Ranges))
	}
	wantGap := int64(64 << 10) // chunks 1 and 3 skipped
	if dpCoalesce.TotalGapBytes != wantGap {
		t.Fatalf("expected %d gap bytes, got %d", wantGap, dpCoalesce.TotalGapBytes)
	}
	if dpCoalesce.ReadAmplification <= 1.0 {
		t.Fatalf("expected ReadAmplification > 1.0 with skipped gaps, got %.3f", dpCoalesce.ReadAmplification)
	}

	// Missing entry must return ErrEntryNotFound.
	if _, err := PlanDownload(manifest, []string{"layer/does-not-exist.bin"}, cfg); !errors.Is(err, ErrEntryNotFound) {
		t.Fatalf("expected ErrEntryNotFound, got %v", err)
	}
}

func TestManifestTamperAndCrossNamespaceRejection(t *testing.T) {
	nsA := Namespace{TenantID: "tenant-a", VolumeID: "vol-1"}
	nsB := Namespace{TenantID: "tenant-b", VolumeID: "vol-1"}

	entries := []Entry{{TenantID: nsA.TenantID, VolumeID: nsA.VolumeID, Path: "cfg.json", Data: []byte(`{"lr":1e-4}`)}}
	plan, err := PlanUpload(entries, DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	m := buildManifest(nsA, "ckpt-1", StrategyAITurboGrouped, plan)
	raw, err := m.MarshalSigned()
	if err != nil {
		t.Fatal(err)
	}

	// Valid unmarshal under nsA succeeds.
	if _, err := UnmarshalManifest(raw, nsA); err != nil {
		t.Fatalf("valid manifest failed: %v", err)
	}
	// Reading nsA's manifest as nsB fails with ErrTenantIsolation.
	if _, err := UnmarshalManifest(raw, nsB); !errors.Is(err, ErrTenantIsolation) {
		t.Fatalf("expected ErrTenantIsolation, got %v", err)
	}
	// Tampering a single byte fails with ErrCorruptManifest.
	tampered := bytes.Replace(raw, []byte("ckpt-1"), []byte("ckpt-2"), 1)
	if _, err := UnmarshalManifest(tampered, nsA); !errors.Is(err, ErrCorruptManifest) {
		t.Fatalf("expected ErrCorruptManifest on tampered payload, got %v", err)
	}

	// Forging a signed manifest inside nsA that references an ObjectKey from nsB must fail.
	forgedKey := nsB.PackKey(blake3.Sum256([]byte("stolen")))
	m.GroupObjects[0].ObjectKey = forgedKey
	m.Entries[0].ObjectKey = forgedKey
	forgedRaw, err := m.MarshalSigned()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := UnmarshalManifest(forgedRaw, nsA); !errors.Is(err, ErrTenantIsolation) {
		t.Fatalf("expected ErrTenantIsolation on cross-namespace object reference, got %v", err)
	}
}

func TestNegativeRegimesPlannerBehavior(t *testing.T) {
	ns := Namespace{TenantID: "neg-tenant", VolumeID: "neg-vol"}

	t.Run("all_large_shards_bypass_zero_reduction", func(t *testing.T) {
		entries := make([]Entry, 6)
		for i := range entries {
			entries[i] = Entry{
				TenantID: ns.TenantID,
				VolumeID: ns.VolumeID,
				Path:     fmt.Sprintf("weights/shard-%02d.bin", i),
				Data:     makeDeterministicBytes(int64(700+i), 4<<20),
			}
		}
		cfg := DefaultConfig()
		cfg.TargetGroupBytes = 4 << 20
		cfg.LargeObjectThreshold = 4 << 20
		plan, err := PlanUpload(entries, cfg)
		if err != nil {
			t.Fatal(err)
		}
		if plan.PackedGroupCount != 0 || plan.DirectObjectCount != 6 {
			t.Fatalf("expected 0 packed groups and 6 direct bypasses, got packed=%d direct=%d",
				plan.PackedGroupCount, plan.DirectObjectCount)
		}
		for _, g := range plan.Groups {
			if !g.DirectBypass {
				t.Fatalf("expected DirectBypass=true for all large shards")
			}
		}
	})

	t.Run("sparse_random_oversized_gap_read_amplification", func(t *testing.T) {
		// 100 chunks x 32 KiB = 3.2 MiB packed into one 4 MiB group.
		entries := make([]Entry, 100)
		for i := range entries {
			entries[i] = Entry{
				TenantID: ns.TenantID,
				VolumeID: ns.VolumeID,
				Path:     fmt.Sprintf("dense/chunk-%03d.bin", i),
				Data:     makeDeterministicBytes(int64(3000+i), 32<<10),
			}
		}
		cfg := DefaultConfig()
		cfg.TargetGroupBytes = 4 << 20
		upPlan, err := PlanUpload(entries, cfg)
		if err != nil {
			t.Fatal(err)
		}
		manifest := buildManifest(ns, "ckpt-sparse", StrategyAITurboGrouped, upPlan)
		// Pick 2 distant chunks: #0 and #99 (2% of checkpoint, separated by ~3.1 MiB gap).
		sparseSel := []string{"dense/chunk-000.bin", "dense/chunk-099.bin"}

		wideCfg := cfg
		wideCfg.MaxGapBytes = 4 << 20 // oversized gap threshold
		dpWide, err := PlanDownload(manifest, sparseSel, wideCfg)
		if err != nil {
			t.Fatal(err)
		}
		if dpWide.ReadAmplification < 20.0 {
			t.Fatalf("expected high read amplification (>20x) under oversized MaxGapBytes, got %.2fx", dpWide.ReadAmplification)
		}

		tightCfg := cfg
		tightCfg.MaxGapBytes = 64 << 10
		dpTight, err := PlanDownload(manifest, sparseSel, tightCfg)
		if err != nil {
			t.Fatal(err)
		}
		if dpTight.ReadAmplification != 1.0 || len(dpTight.Ranges) != 2 {
			t.Fatalf("expected 1.0x read amp and 2 tight ranges, got amp=%.2fx ranges=%d",
				dpTight.ReadAmplification, len(dpTight.Ranges))
		}
	})

	t.Run("tiny_single_file_zero_reduction", func(t *testing.T) {
		entries := []Entry{{
			TenantID: ns.TenantID,
			VolumeID: ns.VolumeID,
			Path:     "tiny/config.json",
			Data:     makeDeterministicBytes(99, 10<<10),
		}}
		plan, err := PlanUpload(entries, DefaultConfig())
		if err != nil {
			t.Fatal(err)
		}
		if len(plan.Groups) != 1 || plan.PackedGroupCount != 1 {
			t.Fatalf("expected 1 packed group for tiny single file, got %d", len(plan.Groups))
		}
	})
}
