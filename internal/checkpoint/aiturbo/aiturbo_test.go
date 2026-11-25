// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package aiturbo

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

func TestRoundTripAllStrategiesBitExact(t *testing.T) {
	ctx := context.Background()
	spec := MixedWorkloadSpec{
		Seed:            42,
		TenantID:        "tenant-rt",
		CheckpointID:    "ckpt-rt",
		SmallFileCount:  64,
		MediumFileCount: 8,
		LargeFileCount:  2,
		SmallMinBytes:   4 << 10,
		SmallMaxBytes:   32 << 10,
		MediumMinBytes:  256 << 10,
		MediumMaxBytes:  512 << 10,
		LargeMinBytes:   4 << 20,
		LargeMaxBytes:   4 << 20,
	}
	baseCkpt := GenerateMixedCheckpoint(spec)
	subsetPaths := []string{
		"config/checkpoint_index.json",
		"agent/state/step-0004.json",
		"layers/block-00/adapter-weights.safetensors",
	}

	for i, strat := range AllStrategies {
		t.Run(string(strat), func(t *testing.T) {
			srv := NewLoopbackS3Server(LoopbackS3Config{Bucket: "test-bucket"})
			defer srv.Close()

			tenantID := fmt.Sprintf("tenant-strat-%d", i)
			ckpt := cloneCheckpointForTenant(baseCkpt, tenantID, fmt.Sprintf("ckpt-%d", i))

			cfg := DefaultConfigForStrategy(strat)
			planner, err := NewPlanner(srv.Client(), cfg)
			if err != nil {
				t.Fatalf("NewPlanner(%s): %v", strat, err)
			}

			cRes, err := planner.CommitCheckpoint(ctx, ckpt, "", 1)
			if err != nil {
				t.Fatalf("CommitCheckpoint(%s): %v", strat, err)
			}
			if cRes.Head.Generation != 1 || cRes.HeadETag == "" {
				t.Fatalf("unexpected head result: %+v", cRes.Head)
			}

			// Full bit-exact SHA-256 restore.
			fRes, err := planner.RestoreCheckpoint(ctx, tenantID)
			if err != nil {
				t.Fatalf("RestoreCheckpoint(%s): %v", strat, err)
			}
			if !VerifyCheckpointExact(ckpt, fRes.Files) {
				t.Fatalf("full restore failed SHA-256 bit-exact check for %s", strat)
			}

			// Selective subset restore.
			sRes, err := planner.RestoreSubset(ctx, tenantID, subsetPaths)
			if err != nil {
				t.Fatalf("RestoreSubset(%s): %v", strat, err)
			}
			if len(sRes.Files) != len(subsetPaths) {
				t.Fatalf("got %d subset files, want %d", len(sRes.Files), len(subsetPaths))
			}
			expectedByPath := make(map[string][]byte, len(ckpt.Files))
			for _, f := range ckpt.Files {
				expectedByPath[f.Path] = f.Data
			}
			for _, pth := range subsetPaths {
				got, ok := sRes.Files[pth]
				if !ok || !bytes.Equal(got, expectedByPath[pth]) {
					t.Fatalf("selective restore mismatch on %q for %s", pth, strat)
				}
			}
		})
	}
}

func TestS3RequestsPerGiBReduction(t *testing.T) {
	ctx := context.Background()
	spec := DefaultMixedWorkloadSpec("tenant-req", "ckpt-req", 777)
	baseCkpt := GenerateMixedCheckpoint(spec)
	gib := float64(baseCkpt.TotalBytes) / bytesPerGiB

	srv := NewLoopbackS3Server(LoopbackS3Config{Bucket: "req-bench"})
	defer srv.Close()

	// Parallel per-object baseline.
	perObjPlanner, err := NewPlanner(srv.Client(), DefaultConfigForStrategy(StrategyPerObjectParallel))
	if err != nil {
		t.Fatal(err)
	}
	perObjCkpt := cloneCheckpointForTenant(baseCkpt, "tenant-perobj", "ckpt-perobj")
	perObjCommit, err := perObjPlanner.CommitCheckpoint(ctx, perObjCkpt, "", 1)
	if err != nil {
		t.Fatal(err)
	}
	perObjRestore, err := perObjPlanner.RestoreCheckpoint(ctx, "tenant-perobj")
	if err != nil {
		t.Fatal(err)
	}

	// AITURBO grouped IO planner.
	aiPlanner, err := NewPlanner(srv.Client(), DefaultConfigForStrategy(StrategyAITurboGrouped))
	if err != nil {
		t.Fatal(err)
	}
	aiCkpt := cloneCheckpointForTenant(baseCkpt, "tenant-aiturbo", "ckpt-aiturbo")
	aiCommit, err := aiPlanner.CommitCheckpoint(ctx, aiCkpt, "", 1)
	if err != nil {
		t.Fatal(err)
	}
	aiRestore, err := aiPlanner.RestoreCheckpoint(ctx, "tenant-aiturbo")
	if err != nil {
		t.Fatal(err)
	}
	if !VerifyCheckpointExact(aiCkpt, aiRestore.Files) {
		t.Fatal("aiturbo restore not bit-exact")
	}

	perObjReqsPerGiB := float64(perObjCommit.StoreMetrics.TotalPutRequests()+perObjRestore.StoreMetrics.TotalGetRequests()) / gib
	aiReqsPerGiB := float64(aiCommit.StoreMetrics.TotalPutRequests()+aiRestore.StoreMetrics.TotalGetRequests()) / gib

	reductionFactor := perObjReqsPerGiB / aiReqsPerGiB
	t.Logf("Per-object parallel: %.1f reqs/GiB | AITURBO grouped: %.1f reqs/GiB (%.2fx reduction)",
		perObjReqsPerGiB, aiReqsPerGiB, reductionFactor)

	if reductionFactor < 10.0 {
		t.Fatalf("expected >= 10x S3 requests/GiB reduction, got %.2fx (perObj=%.1f, aiturbo=%.1f)",
			reductionFactor, perObjReqsPerGiB, aiReqsPerGiB)
	}
}

func TestStrictHeadConditionalPUTConcurrency(t *testing.T) {
	ctx := context.Background()
	srv := NewLoopbackS3Server(LoopbackS3Config{Bucket: "cas-bucket"})
	defer srv.Close()

	const racers = 16
	res, err := RunHeadCASRace(ctx, srv, "tenant-cas", racers)
	if err != nil {
		t.Fatalf("RunHeadCASRace: %v", err)
	}

	if res.InitialCreateWinners != 1 || res.InitialCreateConflicts != racers-1 {
		t.Fatalf("initial create race: got %d winners, %d conflicts; want 1 winner, %d conflicts",
			res.InitialCreateWinners, res.InitialCreateConflicts, racers-1)
	}
	if res.UpdateRaceWinners != 1 || res.UpdateRaceConflicts != racers-1 {
		t.Fatalf("update race: got %d winners, %d conflicts; want 1 winner, %d conflicts",
			res.UpdateRaceWinners, res.UpdateRaceConflicts, racers-1)
	}
	if res.TornReadsObserved != 0 {
		t.Fatalf("observed %d torn reads during concurrent Head CAS", res.TornReadsObserved)
	}
	if res.SilentOverwrites != 0 {
		t.Fatalf("observed %d silent overwrites during concurrent Head CAS", res.SilentOverwrites)
	}
	if !res.RetrySucceeded || res.FinalGeneration != 3 {
		t.Fatalf("expected clean retry to generation 3, got retry=%v gen=%d", res.RetrySucceeded, res.FinalGeneration)
	}
}

type failingPutStore struct {
	ObjectStore
	failAfter atomic.Int64
	puts      atomic.Int64
}

func (f *failingPutStore) Put(ctx context.Context, key string, data []byte) (string, error) {
	n := f.puts.Add(1)
	if n > f.failAfter.Load() {
		return "", errors.New("injected group upload failure")
	}
	return f.ObjectStore.Put(ctx, key, data)
}

func TestDataUploadFailureLeavesHeadUntouched(t *testing.T) {
	ctx := context.Background()
	srv := NewLoopbackS3Server(LoopbackS3Config{Bucket: "barrier-bucket"})
	defer srv.Close()

	// Commit generation 1 cleanly first.
	planner1, err := NewPlanner(srv.Client(), DefaultConfigForStrategy(StrategyAITurboGrouped))
	if err != nil {
		t.Fatal(err)
	}
	ckpt1 := GenerateMixedCheckpoint(MixedWorkloadSpec{
		Seed: 1, TenantID: "tenant-barrier", CheckpointID: "ckpt-gen1",
		SmallFileCount: 16, MediumFileCount: 2, LargeFileCount: 0,
		SmallMinBytes: 4 << 10, SmallMaxBytes: 8 << 10,
		MediumMinBytes: 256 << 10, MediumMaxBytes: 256 << 10,
	})
	res1, err := planner1.CommitCheckpoint(ctx, ckpt1, "", 1)
	if err != nil {
		t.Fatalf("gen1 commit failed: %v", err)
	}

	// Attempt generation 2 through a store that fails mid-way through phase-1 group uploads.
	failStore := &failingPutStore{ObjectStore: srv.Client()}
	failStore.failAfter.Store(1)
	planner2, err := NewPlanner(failStore, DefaultConfigForStrategy(StrategyAITurboGrouped))
	if err != nil {
		t.Fatal(err)
	}
	ckpt2 := GenerateMixedCheckpoint(MixedWorkloadSpec{
		Seed: 2, TenantID: "tenant-barrier", CheckpointID: "ckpt-gen2-aborted",
		SmallFileCount: 16, MediumFileCount: 4, LargeFileCount: 2,
		SmallMinBytes: 4 << 10, SmallMaxBytes: 8 << 10,
		MediumMinBytes: 256 << 10, MediumMaxBytes: 512 << 10,
		LargeMinBytes: 4 << 20, LargeMaxBytes: 4 << 20,
	})
	if _, err := planner2.CommitCheckpoint(ctx, ckpt2, res1.HeadETag, 2); err == nil {
		t.Fatal("expected phase-1 group upload failure to abort commit")
	}

	// Verify Head is still intact at generation 1 with bit-exact contents.
	head, etag, err := planner1.ReadHead(ctx, "tenant-barrier")
	if err != nil {
		t.Fatalf("ReadHead after aborted gen2: %v", err)
	}
	if head.Generation != 1 || head.CheckpointID != "ckpt-gen1" || etag != res1.HeadETag {
		t.Fatalf("Head mutated despite phase-1 failure: head=%+v etag=%s", head, etag)
	}
	restored, err := planner1.RestoreCheckpoint(ctx, "tenant-barrier")
	if err != nil || !VerifyCheckpointExact(ckpt1, restored.Files) {
		t.Fatalf("gen1 checkpoint corrupted after aborted gen2: err=%v", err)
	}
}

func TestMultiTenantIsolationConcurrent(t *testing.T) {
	ctx := context.Background()
	srv := NewLoopbackS3Server(LoopbackS3Config{Bucket: "mt-bucket"})
	defer srv.Close()

	tenants := []string{"tenant-alpha", "tenant-beta", "tenant-gamma", "tenant-delta"}
	ckpts := make(map[string]*Checkpoint, len(tenants))
	for i, tid := range tenants {
		ckpts[tid] = GenerateMixedCheckpoint(MixedWorkloadSpec{
			Seed: uint64(500 + i), TenantID: tid, CheckpointID: "ckpt-" + tid,
			SmallFileCount: 32, MediumFileCount: 4, LargeFileCount: 1,
			SmallMinBytes: 4 << 10, SmallMaxBytes: 16 << 10,
			MediumMinBytes: 128 << 10, MediumMaxBytes: 256 << 10,
			LargeMinBytes: 4 << 20, LargeMaxBytes: 4 << 20,
		})
	}

	// Commit and restore all tenants concurrently under -race.
	var (
		wg    sync.WaitGroup
		errCh = make(chan error, len(tenants))
	)
	for _, tid := range tenants {
		wg.Add(1)
		go func(tenantID string) {
			defer wg.Done()
			p, err := NewPlanner(srv.Client(), DefaultConfigForStrategy(StrategyAITurboGrouped))
			if err != nil {
				errCh <- err
				return
			}
			cRes, err := p.CommitCheckpoint(ctx, ckpts[tenantID], "", 1)
			if err != nil {
				errCh <- fmt.Errorf("%s commit: %w", tenantID, err)
				return
			}
			if !KeyBelongsToTenant(tenantID, cRes.ManifestKey) {
				errCh <- fmt.Errorf("%s manifest key %q leaked namespace", tenantID, cRes.ManifestKey)
				return
			}
			rRes, err := p.RestoreCheckpoint(ctx, tenantID)
			if err != nil {
				errCh <- fmt.Errorf("%s restore: %w", tenantID, err)
				return
			}
			if !VerifyCheckpointExact(ckpts[tenantID], rRes.Files) {
				errCh <- fmt.Errorf("%s restored bytes mismatch", tenantID)
				return
			}
		}(tid)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}

	// Verify every object key in the server belongs strictly to one tenant namespace
	// and tenants never share super-objects.
	keys := srv.ObjectKeys()
	if len(keys) == 0 {
		t.Fatal("expected stored objects")
	}
	for _, k := range keys {
		matchedTenants := 0
		for _, tid := range tenants {
			if KeyBelongsToTenant(tid, k) {
				matchedTenants++
			}
		}
		if matchedTenants != 1 {
			t.Fatalf("key %q matched %d tenants, want exactly 1", k, matchedTenants)
		}
	}

	// Verify cross-tenant file injection is rejected before any upload occurs.
	p, err := NewPlanner(srv.Client(), DefaultConfigForStrategy(StrategyAITurboGrouped))
	if err != nil {
		t.Fatal(err)
	}
	badData := []byte("cross-tenant-payload")
	mixedTenantCkpt := &Checkpoint{
		ID:       "ckpt-bad-tenant",
		TenantID: "tenant-alpha",
		Files: []CheckpointFile{
			{Path: "ok.bin", TenantID: "tenant-alpha", Data: badData, SHA256: sha256.Sum256(badData)},
			{Path: "leak.bin", TenantID: "tenant-beta", Data: badData, SHA256: sha256.Sum256(badData)},
		},
	}
	if _, err := p.CommitCheckpoint(ctx, mixedTenantCkpt, "", 2); !errors.Is(err, ErrTenantViolation) {
		t.Fatalf("expected ErrTenantViolation on mixed tenant checkpoint, got %v", err)
	}

	// Verify path traversal is rejected.
	traversalCkpt := &Checkpoint{
		ID:       "ckpt-traversal",
		TenantID: "tenant-alpha",
		Files: []CheckpointFile{
			{Path: "../tenant-beta/secret.bin", TenantID: "tenant-alpha", Data: badData, SHA256: sha256.Sum256(badData)},
		},
	}
	if _, err := p.CommitCheckpoint(ctx, traversalCkpt, "", 2); !errors.Is(err, ErrTenantViolation) {
		t.Fatalf("expected ErrTenantViolation on path traversal, got %v", err)
	}
}

func TestRangeCoalescingAndSelectiveNegativeRegime(t *testing.T) {
	ctx := context.Background()
	srv := NewLoopbackS3Server(LoopbackS3Config{Bucket: "range-bucket"})
	defer srv.Close()

	// Unit test MergeRanges directly: merge adjacent/small gaps, split large gaps.
	entries := []GroupManifestEntry{
		{Path: "a", GroupKey: "g1", Offset: 0, Length: 4096, GroupSize: 4 << 20},
		{Path: "b", GroupKey: "g1", Offset: 8192, Length: 4096, GroupSize: 4 << 20},       // 4 KiB gap -> merge
		{Path: "c", GroupKey: "g1", Offset: 512 << 10, Length: 4096, GroupSize: 4 << 20},  // >64 KiB gap -> split
		{Path: "d", GroupKey: "g1", Offset: 520 << 10, Length: 16384, GroupSize: 4 << 20}, // 4 KiB gap -> merge
	}
	spans := MergeRanges(entries, 64<<10, 4<<20)
	if len(spans) != 2 {
		t.Fatalf("MergeRanges produced %d spans, want 2: %+v", len(spans), spans)
	}
	if spans[0].Start != 0 || spans[0].End != 12288 || len(spans[0].Entries) != 2 {
		t.Fatalf("unexpected span[0]: %+v", spans[0])
	}
	if spans[1].Start != 512<<10 || spans[1].End != (520<<10)+16384 || len(spans[1].Entries) != 2 {
		t.Fatalf("unexpected span[1]: %+v", spans[1])
	}

	// Negative regime #1 + over-coalesced control:
	// Compare 16 KiB single-file restore:
	// (a) AITURBO grouped with HTTP Range GET (~1.00x data read amp)
	// (b) AITURBO grouped WITHOUT HTTP Range GET (full-group fetch -> high read amp)
	// (c) over_coalesced_giant_group_negative (256 MiB target, full-group fetch -> massive read amp)
	ckpt := GenerateMixedCheckpoint(DefaultMixedWorkloadSpec("tenant-sel", "ckpt-sel", 99))
	target := "config/checkpoint_index.json"

	aiPlanner, err := NewPlanner(srv.Client(), DefaultConfigForStrategy(StrategyAITurboGrouped))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := aiPlanner.CommitCheckpoint(ctx, ckpt, "", 1); err != nil {
		t.Fatal(err)
	}
	rangeRes, err := aiPlanner.RestoreSubsetWithRangeMode(ctx, "tenant-sel", []string{target}, true)
	if err != nil {
		t.Fatal(err)
	}
	fullGrpRes, err := aiPlanner.RestoreSubsetWithRangeMode(ctx, "tenant-sel", []string{target}, false)
	if err != nil {
		t.Fatal(err)
	}

	overPlanner, err := NewPlanner(srv.Client(), DefaultConfigForStrategy(StrategyOverCoalesced))
	if err != nil {
		t.Fatal(err)
	}
	overCkpt := cloneCheckpointForTenant(ckpt, "tenant-over", "ckpt-over")
	if _, err := overPlanner.CommitCheckpoint(ctx, overCkpt, "", 1); err != nil {
		t.Fatal(err)
	}
	overRes, err := overPlanner.RestoreSubset(ctx, "tenant-over", []string{target})
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("16 KiB selective restore data read-amp: AITURBO Range=%.2fx | AITURBO FullGroup=%.2fx | OverCoalesced(256MiB)=%.2fx",
		rangeRes.DataReadAmp, fullGrpRes.DataReadAmp, overRes.DataReadAmp)

	if rangeRes.DataReadAmp != 1.0 {
		t.Fatalf("expected exact 1.00x data read amp with HTTP Range GET, got %.2fx", rangeRes.DataReadAmp)
	}
	if fullGrpRes.DataReadAmp < 100.0 {
		t.Fatalf("expected high read amp (>=100x) when fetching full 4 MiB group for 16 KiB file, got %.2fx", fullGrpRes.DataReadAmp)
	}
	if overRes.DataReadAmp <= fullGrpRes.DataReadAmp*5 {
		t.Fatalf("expected over_coalesced_giant_group_negative read amp (%.2fx) >> 4 MiB full-group (%.2fx)",
			overRes.DataReadAmp, fullGrpRes.DataReadAmp)
	}
}

func TestLargeTensorOnlyZeroGainNegativeRegime(t *testing.T) {
	ctx := context.Background()
	srv := NewLoopbackS3Server(LoopbackS3Config{Bucket: "large-only"})
	defer srv.Close()

	largeCkpt := GenerateLargeTensorOnlyCheckpoint("tenant-lg-per", "ckpt-lg", 4, 16<<20, 31415)

	perObj, err := NewPlanner(srv.Client(), DefaultConfigForStrategy(StrategyPerObjectParallel))
	if err != nil {
		t.Fatal(err)
	}
	perRes, err := perObj.CommitCheckpoint(ctx, largeCkpt, "", 1)
	if err != nil {
		t.Fatal(err)
	}

	ai, err := NewPlanner(srv.Client(), DefaultConfigForStrategy(StrategyAITurboGrouped))
	if err != nil {
		t.Fatal(err)
	}
	aiCkpt := cloneCheckpointForTenant(largeCkpt, "tenant-lg-ai", "ckpt-lg")
	aiRes, err := ai.CommitCheckpoint(ctx, aiCkpt, "", 1)
	if err != nil {
		t.Fatal(err)
	}

	// Zero S3 request reduction because every tensor already exceeds the 4 MiB passthrough threshold.
	if perRes.StoreMetrics.TotalPutRequests() != aiRes.StoreMetrics.TotalPutRequests() {
		t.Fatalf("expected identical PUT count for 16 MiB tensors: perObj=%d aiturbo=%d",
			perRes.StoreMetrics.TotalPutRequests(), aiRes.StoreMetrics.TotalPutRequests())
	}
	// Passthrough avoids allocating 64 MiB of redundant coalescing copy buffers.
	if aiRes.PackingBytes != 0 {
		t.Fatalf("expected 0 packing copy bytes via passthrough, got %d", aiRes.PackingBytes)
	}

	// Verify forced packing without passthrough DOES incur 64 MiB packing buffer overhead.
	forced, err := NewPlanner(srv.Client(), DefaultConfigForStrategy(StrategyOverCoalesced))
	if err != nil {
		t.Fatal(err)
	}
	forcedCkpt := cloneCheckpointForTenant(largeCkpt, "tenant-lg-forced", "ckpt-lg")
	forcedRes, err := forced.CommitCheckpoint(ctx, forcedCkpt, "", 1)
	if err != nil {
		t.Fatal(err)
	}
	if forcedRes.PackingBytes != largeCkpt.TotalBytes {
		t.Fatalf("expected forced packing buffer overhead == %d bytes, got %d",
			largeCkpt.TotalBytes, forcedRes.PackingBytes)
	}
}

func TestTamperedObjectDetectedBySHA256(t *testing.T) {
	ctx := context.Background()
	srv := NewLoopbackS3Server(LoopbackS3Config{Bucket: "tamper-bucket"})
	defer srv.Close()

	ckpt := GenerateMixedCheckpoint(MixedWorkloadSpec{
		Seed: 123, TenantID: "tenant-tamper", CheckpointID: "ckpt-tamper",
		SmallFileCount: 8, MediumFileCount: 0, LargeFileCount: 0,
		SmallMinBytes: 4 << 10, SmallMaxBytes: 8 << 10,
	})
	planner, err := NewPlanner(srv.Client(), DefaultConfigForStrategy(StrategyAITurboGrouped))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := planner.CommitCheckpoint(ctx, ckpt, "", 1); err != nil {
		t.Fatal(err)
	}

	// Flip 1 byte in a stored group object directly on the server.
	srv.mu.Lock()
	for k, obj := range srv.objs {
		if strings.Contains(k, "/groups/") && len(obj.data) > 0 {
			obj.data[0] ^= 0xff
			srv.objs[k] = obj
			break
		}
	}
	srv.mu.Unlock()

	if _, err := planner.RestoreCheckpoint(ctx, "tenant-tamper"); !errors.Is(err, ErrIntegrityMismatch) {
		t.Fatalf("expected ErrIntegrityMismatch on tampered group bytes, got %v", err)
	}
}
