package parasync

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/hanzoai/vfs/internal/blake3"
)

func TestAllStrategiesAndWorkloadsBitExact(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()

	// Use realistic loopback RTT & bandwidth shaping while keeping test runtime fast.
	store := NewHTTPObjectStore(HTTPStoreConfig{
		BaseRTT:              1500 * time.Microsecond,
		BandwidthBytesPerSec: 128 << 20, // 128 MiB/s
	})
	defer store.Close()

	key, err := DeriveTenantKey("tenant-alpha", 1, "test-seed-alpha")
	if err != nil {
		t.Fatalf("derive key: %v", err)
	}
	cfg := DefaultCDCConfig()

	for _, wk := range AllWorkloads() {
		wk := wk
		t.Run(string(wk), func(t *testing.T) {
			fix, err := GenerateWorkload(tmp, wk, 4) // 4 MiB (16 x 256 KiB) for fast -race execution
			if err != nil {
				t.Fatalf("generate workload: %v", err)
			}

			// Build both FastCDC and Fixed-64K checkpoint pairs for matched strategy comparison.
			baseCDC, err := BuildAndUploadCheckpoint(ctx, store, key, ChunkModeFastCDC, cfg, string(wk)+"-cdc-base", fix.BaseDir, nil)
			if err != nil {
				t.Fatalf("build base cdc: %v", err)
			}
			targetCDC, err := BuildAndUploadCheckpoint(ctx, store, key, ChunkModeFastCDC, cfg, string(wk)+"-cdc-target", fix.TargetDir, baseCDC)
			if err != nil {
				t.Fatalf("build target cdc: %v", err)
			}
			baseFixed, err := BuildAndUploadCheckpoint(ctx, store, key, ChunkModeFixed64K, cfg, string(wk)+"-fix-base", fix.BaseDir, nil)
			if err != nil {
				t.Fatalf("build base fixed: %v", err)
			}
			targetFixed, err := BuildAndUploadCheckpoint(ctx, store, key, ChunkModeFixed64K, cfg, string(wk)+"-fix-target", fix.TargetDir, baseFixed)
			if err != nil {
				t.Fatalf("build target fixed: %v", err)
			}

			if targetCDC.VolumeSHA256 != targetFixed.VolumeSHA256 {
				t.Fatalf("volume sha256 mismatch between cdc (%s) and fixed (%s)", targetCDC.VolumeSHA256, targetFixed.VolumeSHA256)
			}

			results := make(map[Strategy]*RestoreResult, 4)
			for _, strat := range AllStrategies() {
				outDir := filepath.Join(tmp, string(wk)+"_out_"+string(strat))
				store.ResetStats()

				baseCP, targetCP := baseCDC, targetCDC
				if strat == StrategyFixedChunkSequentialDelta {
					baseCP, targetCP = baseFixed, targetFixed
				}
				res, err := ExecuteRestore(ctx, store, key, baseCP, targetCP, fix.BaseDir, outDir, RestoreOptions{
					Strategy:           strat,
					Workers:            8,
					DiffShards:         4,
					VerifyVolumeSHA256: true,
				})
				if err != nil {
					t.Fatalf("strategy %s failed: %v", strat, err)
				}
				if !res.BitExact {
					t.Fatalf("strategy %s not bit-exact: got %s want %s", strat, res.RestoredVolumeSHA256, res.ExpectedVolumeSHA256)
				}
				results[strat] = res
				t.Logf("%-30s transferred=%8d B  gets=%3d  reused=%3d  wall=%8.2f ms  steals=%d",
					strat, res.TransferredBytes, res.HTTPGetCount, res.Plan.ReusedLocalChunks,
					float64(res.WallTime)/float64(time.Millisecond), res.StealCount)
			}

			fullSeq := results[StrategyFullSequential]
			fullPar := results[StrategyFullParallel]
			fixedDelta := results[StrategyFixedChunkSequentialDelta]
			paraSync := results[StrategyParaSyncParallelDelta]

			switch wk {
			case WorkloadSmallEdit2Pct:
				if float64(paraSync.TransferredBytes) >= 0.20*float64(fullPar.TransferredBytes) {
					t.Fatalf("expected parasync transfer << full_parallel on 2%% edit: parasync=%d full=%d",
						paraSync.TransferredBytes, fullPar.TransferredBytes)
				}
				if paraSync.WallTime >= fullPar.WallTime {
					t.Fatalf("expected parasync faster than full_parallel on 2%% edit: parasync=%v full=%v",
						paraSync.WallTime, fullPar.WallTime)
				}
				if paraSync.WallTime >= fullSeq.WallTime {
					t.Fatalf("expected parasync faster than full_sequential on 2%% edit: parasync=%v seq=%v",
						paraSync.WallTime, fullSeq.WallTime)
				}

			case WorkloadMediumBuild15Pct:
				if float64(paraSync.TransferredBytes) >= 0.40*float64(fullPar.TransferredBytes) {
					t.Fatalf("expected parasync transfer << full_parallel on 15%% build: parasync=%d full=%d",
						paraSync.TransferredBytes, fullPar.TransferredBytes)
				}
				if paraSync.WallTime >= fullPar.WallTime {
					t.Fatalf("expected parasync faster than full_parallel on 15%% build: parasync=%v full=%v",
						paraSync.WallTime, fullPar.WallTime)
				}

			case WorkloadByteShift5Pct:
				// FastCDC must dramatically beat BOTH full_parallel AND fixed_64k delta on byte-shift insertions.
				if float64(paraSync.TransferredBytes) >= 0.35*float64(fullPar.TransferredBytes) {
					t.Fatalf("expected parasync transfer << full_parallel on byte-shift: parasync=%d full=%d",
						paraSync.TransferredBytes, fullPar.TransferredBytes)
				}
				if float64(paraSync.TransferredBytes) >= 0.50*float64(fixedDelta.TransferredBytes) {
					t.Fatalf("expected parasync FastCDC transfer << fixed_64k delta on byte-shift: parasync=%d fixed=%d",
						paraSync.TransferredBytes, fixedDelta.TransferredBytes)
				}
				if paraSync.WallTime >= fixedDelta.WallTime {
					t.Fatalf("expected parasync faster than fixed_64k sequential delta on byte-shift: parasync=%v fixed=%v",
						paraSync.WallTime, fixedDelta.WallTime)
				}
				if paraSync.WallTime >= fullPar.WallTime {
					t.Fatalf("expected parasync faster than full_parallel on byte-shift: parasync=%v full=%v",
						paraSync.WallTime, fullPar.WallTime)
				}

			case WorkloadFullRewrite95Pct:
				// Honest negative regime: >=95% rewrite leaves almost nothing to reuse.
				savedRatio := 1.0 - float64(paraSync.TransferredBytes)/float64(fullPar.TransferredBytes)
				if savedRatio > 0.15 {
					t.Fatalf("expected minimal transfer savings on 95%% rewrite, got %.1f%%", savedRatio*100)
				}
			}
		})
	}
}

func TestCiphertextOnlyS3AuditAndTamperRejection(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()

	store := NewHTTPObjectStore(HTTPStoreConfig{})
	defer store.Close()

	key, err := DeriveTenantKey("tenant-zk-audit", 1, "zk-seed-001")
	if err != nil {
		t.Fatalf("derive key: %v", err)
	}
	fix, err := GenerateWorkload(tmp, WorkloadSmallEdit2Pct, 2)
	if err != nil {
		t.Fatalf("generate workload: %v", err)
	}

	cp, err := BuildAndUploadCheckpoint(ctx, store, key, ChunkModeFastCDC, DefaultCDCConfig(), "zk-cp-1", fix.TargetDir, nil)
	if err != nil {
		t.Fatalf("build checkpoint: %v", err)
	}

	// 1. Audit every single object in S3: zero plaintext canary markers, file paths, or tenant IDs.
	markers := append([][]byte(nil), fix.CanaryMarkers...)
	markers = append(markers, []byte("tenant-zk-audit"), []byte("module_00.o"))
	leaks, scanned, totalBytes := store.AuditNoPlaintext(markers)
	if len(leaks) > 0 {
		t.Fatalf("ciphertext-only S3 violation: %v", leaks)
	}
	if scanned < cp.UniqueChunks+1 || totalBytes < cp.TotalPlainBytes {
		t.Fatalf("unexpected store audit counts: scanned=%d totalBytes=%d", scanned, totalBytes)
	}

	// 2. Encrypted manifest round-trip verification.
	loaded, err := LoadCheckpointManifest(ctx, store, key, "zk-cp-1")
	if err != nil {
		t.Fatalf("load manifest: %v", err)
	}
	if loaded.VolumeSHA256 != cp.VolumeSHA256 || loaded.TotalChunks != cp.TotalChunks {
		t.Fatalf("loaded manifest mismatch")
	}

	// 3. Direct AEAD tamper rejection even if ciphertext digest were recomputed.
	plain := []byte("sensitive-checkpoint-chunk-payload-for-aead-verification-test")
	cid, _, ct, err := key.SealChunk(plain)
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	tampered := append([]byte(nil), ct...)
	tampered[len(tampered)-1] ^= 0x01
	// Pass a forged ciphertext digest matching the tampered blob to prove AES-GCM tag still fails closed.
	_, err = key.OpenChunk(cid, Digest(blake3.Sum(tampered)), tampered)
	if !errors.Is(err, ErrAuthFailed) {
		t.Fatalf("expected ErrAuthFailed on forged-digest tampered ciphertext, got %v", err)
	}

	// 4. End-to-end S3 object tampering fails closed during ExecuteRestore.
	victimRef := cp.Files[0].Chunks[0]
	if err := store.TamperObject(victimRef.ObjectKey, len(victimRef.ObjectKey)%16+12, 0xFF); err != nil {
		t.Fatalf("tamper object: %v", err)
	}
	outDir := filepath.Join(tmp, "tampered_out")
	_, err = ExecuteRestore(ctx, store, key, nil, cp, "", outDir, RestoreOptions{
		Strategy: StrategyParaSyncParallelDelta,
		Workers:  4,
	})
	if !errors.Is(err, ErrCiphertextCorrupt) {
		t.Fatalf("expected ErrCiphertextCorrupt on tampered S3 chunk, got %v", err)
	}
}

func TestCrossTenantIsolationAndZeroDedup(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()

	store := NewHTTPObjectStore(HTTPStoreConfig{})
	defer store.Close()

	fix, err := GenerateWorkload(tmp, WorkloadSmallEdit2Pct, 2)
	if err != nil {
		t.Fatalf("generate workload: %v", err)
	}

	keyA, err := DeriveTenantKey("tenant-A", 1, "secret-tenant-A")
	if err != nil {
		t.Fatalf("derive key A: %v", err)
	}
	keyB, err := DeriveTenantKey("tenant-B", 1, "secret-tenant-B")
	if err != nil {
		t.Fatalf("derive key B: %v", err)
	}

	// Store IDENTICAL plaintext directory for both Tenant A and Tenant B.
	cpA, err := BuildAndUploadCheckpoint(ctx, store, keyA, ChunkModeFastCDC, DefaultCDCConfig(), "cp-identical", fix.BaseDir, nil)
	if err != nil {
		t.Fatalf("build cpA: %v", err)
	}
	cpB, err := BuildAndUploadCheckpoint(ctx, store, keyB, ChunkModeFastCDC, DefaultCDCConfig(), "cp-identical", fix.BaseDir, nil)
	if err != nil {
		t.Fatalf("build cpB: %v", err)
	}

	if cpA.VolumeSHA256 != cpB.VolumeSHA256 {
		t.Fatalf("expected identical plaintext volume SHA256")
	}

	// Verify 0% overlap in CipherDigest, ChunkID, and S3 object keys.
	cipherA := make(map[Digest]struct{})
	chunkIDA := make(map[Digest]struct{})
	keysA := make(map[string]struct{})
	for _, f := range cpA.Files {
		for _, c := range f.Chunks {
			cipherA[c.CipherDigest] = struct{}{}
			chunkIDA[c.ChunkID] = struct{}{}
			keysA[c.ObjectKey] = struct{}{}
		}
	}

	for _, f := range cpB.Files {
		for _, c := range f.Chunks {
			if _, dup := cipherA[c.CipherDigest]; dup {
				t.Fatalf("cross-tenant CipherDigest collision detected: %s", c.CipherDigest)
			}
			if _, dup := chunkIDA[c.ChunkID]; dup {
				t.Fatalf("cross-tenant ChunkID collision detected: %s", c.ChunkID)
			}
			if _, dup := keysA[c.ObjectKey]; dup {
				t.Fatalf("cross-tenant S3 object key collision detected: %s", c.ObjectKey)
			}
		}
	}

	// Verify per-tenant keyed FastCDC produces distinct chunk boundaries on identical files.
	sameBoundaryFiles := 0
	for i := range cpA.Files {
		if len(cpA.Files[i].Chunks) == len(cpB.Files[i].Chunks) {
			allEq := true
			for j := range cpA.Files[i].Chunks {
				if cpA.Files[i].Chunks[j].PlainSize != cpB.Files[i].Chunks[j].PlainSize {
					allEq = false
					break
				}
			}
			if allEq && len(cpA.Files[i].Chunks) > 1 {
				sameBoundaryFiles++
			}
		}
	}
	if sameBoundaryFiles == len(cpA.Files) {
		t.Fatalf("expected distinct per-tenant FastCDC boundaries across tenants")
	}

	// Verify Tenant A cannot decrypt Tenant B's ciphertext chunk.
	refB := cpB.Files[0].Chunks[0]
	rawB, err := store.Get(ctx, refB.ObjectKey)
	if err != nil {
		t.Fatalf("get refB: %v", err)
	}
	if _, err := keyA.OpenChunk(refB.ChunkID, refB.CipherDigest, rawB); !errors.Is(err, ErrAuthFailed) {
		t.Fatalf("expected ErrAuthFailed when Tenant A opens Tenant B chunk, got %v", err)
	}

	// Verify planner forbids cross-tenant delta reuse and falls back deterministically.
	outDir := filepath.Join(tmp, "tenantB_from_tenantA_base")
	res, err := ExecuteRestore(ctx, store, keyB, cpA, cpB, fix.BaseDir, outDir, RestoreOptions{
		Strategy:           StrategyParaSyncParallelDelta,
		Workers:            4,
		VerifyVolumeSHA256: true,
	})
	if err != nil {
		t.Fatalf("cross-tenant fallback restore failed: %v", err)
	}
	if !res.Plan.FallbackApplied || res.Plan.FallbackReason != FallbackIncompatibleTenant {
		t.Fatalf("expected incompatible_tenant fallback, got applied=%v reason=%q", res.Plan.FallbackApplied, res.Plan.FallbackReason)
	}
	if res.Plan.ReusedLocalChunks != 0 {
		t.Fatalf("expected 0 reused chunks across tenants, got %d", res.Plan.ReusedLocalChunks)
	}
	if !res.BitExact {
		t.Fatalf("expected bit-exact fallback restore for Tenant B")
	}
}

func TestNegativeRegimeRotatedKeyEpochFallback(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()

	store := NewHTTPObjectStore(HTTPStoreConfig{})
	defer store.Close()

	fix, err := GenerateWorkload(tmp, WorkloadSmallEdit2Pct, 2)
	if err != nil {
		t.Fatalf("generate workload: %v", err)
	}

	// Same tenant, rotated key epoch: Epoch 1 -> Epoch 2.
	keyEpoch1, err := DeriveTenantKey("tenant-rotate", 1, "master-rotate-seed")
	if err != nil {
		t.Fatalf("derive epoch 1: %v", err)
	}
	keyEpoch2, err := DeriveTenantKey("tenant-rotate", 2, "master-rotate-seed")
	if err != nil {
		t.Fatalf("derive epoch 2: %v", err)
	}

	cpEpoch1, err := BuildAndUploadCheckpoint(ctx, store, keyEpoch1, ChunkModeFastCDC, DefaultCDCConfig(), "ep1", fix.BaseDir, nil)
	if err != nil {
		t.Fatalf("build epoch 1: %v", err)
	}
	cpEpoch2, err := BuildAndUploadCheckpoint(ctx, store, keyEpoch2, ChunkModeFastCDC, DefaultCDCConfig(), "ep2", fix.TargetDir, cpEpoch1)
	if err != nil {
		t.Fatalf("build epoch 2: %v", err)
	}

	// Verify 100% ciphertext divergence across epochs.
	ep1Digests := make(map[Digest]struct{})
	for _, f := range cpEpoch1.Files {
		for _, c := range f.Chunks {
			ep1Digests[c.CipherDigest] = struct{}{}
		}
	}
	for _, f := range cpEpoch2.Files {
		for _, c := range f.Chunks {
			if _, dup := ep1Digests[c.CipherDigest]; dup {
				t.Fatalf("expected 100%% ciphertext divergence across key epochs")
			}
		}
	}

	store.ResetStats()
	outDir := filepath.Join(tmp, "rotated_epoch_out")
	res, err := ExecuteRestore(ctx, store, keyEpoch2, cpEpoch1, cpEpoch2, fix.BaseDir, outDir, RestoreOptions{
		Strategy:           StrategyParaSyncParallelDelta,
		Workers:            8,
		VerifyVolumeSHA256: true,
	})
	if err != nil {
		t.Fatalf("rotated epoch fallback failed: %v", err)
	}
	if !res.Plan.FallbackApplied || res.Plan.FallbackReason != FallbackIncompatibleKeyEpoch {
		t.Fatalf("expected incompatible_key_epoch fallback, got applied=%v reason=%q", res.Plan.FallbackApplied, res.Plan.FallbackReason)
	}
	if res.Plan.ReusedLocalChunks != 0 {
		t.Fatalf("expected 0 local reused chunks on rotated epoch, got %d", res.Plan.ReusedLocalChunks)
	}
	if res.TransferredBytes < cpEpoch2.TotalPlainBytes {
		t.Fatalf("expected full target ciphertext download on rotated epoch: got %d B", res.TransferredBytes)
	}
	if !res.BitExact {
		t.Fatalf("expected bit-exact restore on rotated key epoch fallback")
	}
}

func TestLocalBitrotAutomaticRepair(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()

	store := NewHTTPObjectStore(HTTPStoreConfig{})
	defer store.Close()

	key, err := DeriveTenantKey("tenant-bitrot", 1, "bitrot-seed")
	if err != nil {
		t.Fatalf("derive key: %v", err)
	}
	fix, err := GenerateWorkload(tmp, WorkloadSmallEdit2Pct, 2)
	if err != nil {
		t.Fatalf("generate workload: %v", err)
	}

	baseCP, err := BuildAndUploadCheckpoint(ctx, store, key, ChunkModeFastCDC, DefaultCDCConfig(), "br-base", fix.BaseDir, nil)
	if err != nil {
		t.Fatalf("build base: %v", err)
	}
	targetCP, err := BuildAndUploadCheckpoint(ctx, store, key, ChunkModeFastCDC, DefaultCDCConfig(), "br-target", fix.TargetDir, baseCP)
	if err != nil {
		t.Fatalf("build target: %v", err)
	}

	// Corrupt one unchanged local base file on disk (file index 15 is untouched in 2% edit).
	unchangedRel := baseCP.Files[len(baseCP.Files)-1].Path
	unchangedFull := filepath.Join(fix.BaseDir, filepath.FromSlash(unchangedRel))
	raw, err := os.ReadFile(unchangedFull)
	if err != nil {
		t.Fatalf("read base file: %v", err)
	}
	raw[100] ^= 0xAA
	if err := os.WriteFile(unchangedFull, raw, 0o644); err != nil {
		t.Fatalf("corrupt base file: %v", err)
	}

	outDir := filepath.Join(tmp, "repaired_out")
	res, err := ExecuteRestore(ctx, store, key, baseCP, targetCP, fix.BaseDir, outDir, RestoreOptions{
		Strategy:           StrategyParaSyncParallelDelta,
		Workers:            8,
		VerifyVolumeSHA256: true,
	})
	if err != nil {
		t.Fatalf("restore with local bitrot failed: %v", err)
	}
	if res.LocalBitrotRepairs < 1 {
		t.Fatalf("expected >= 1 local bitrot repair, got %d", res.LocalBitrotRepairs)
	}
	if !res.BitExact {
		t.Fatalf("expected bit-exact restore after automatic local bitrot repair")
	}
}

func TestConcurrentRestoresUnderRace(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()

	store := NewHTTPObjectStore(HTTPStoreConfig{})
	defer store.Close()

	key, err := DeriveTenantKey("tenant-race", 1, "race-seed")
	if err != nil {
		t.Fatalf("derive key: %v", err)
	}
	fix, err := GenerateWorkload(tmp, WorkloadByteShift5Pct, 2)
	if err != nil {
		t.Fatalf("generate workload: %v", err)
	}
	baseCP, err := BuildAndUploadCheckpoint(ctx, store, key, ChunkModeFastCDC, DefaultCDCConfig(), "race-base", fix.BaseDir, nil)
	if err != nil {
		t.Fatalf("build base: %v", err)
	}
	targetCP, err := BuildAndUploadCheckpoint(ctx, store, key, ChunkModeFastCDC, DefaultCDCConfig(), "race-target", fix.TargetDir, baseCP)
	if err != nil {
		t.Fatalf("build target: %v", err)
	}

	const concurrency = 4
	var wg sync.WaitGroup
	errs := make(chan error, concurrency)
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			outDir := filepath.Join(tmp, "race_out_"+string(rune('a'+id)))
			res, err := ExecuteRestore(ctx, store, key, baseCP, targetCP, fix.BaseDir, outDir, RestoreOptions{
				Strategy:           StrategyParaSyncParallelDelta,
				Workers:            8,
				DiffShards:         4,
				VerifyVolumeSHA256: true,
			})
			if err != nil {
				errs <- err
				return
			}
			if !res.BitExact {
				errs <- errors.New("concurrent restore was not bit-exact")
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent restore error: %v", err)
	}
}

func TestNegativeRegime95PctChurnOverhead(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()

	store := NewHTTPObjectStore(HTTPStoreConfig{
		BaseRTT:              500 * time.Microsecond,
		BandwidthBytesPerSec: 200 << 20,
	})
	defer store.Close()

	key, err := DeriveTenantKey("tenant-neg95", 1, "neg95-seed")
	if err != nil {
		t.Fatalf("derive key: %v", err)
	}
	fix, err := GenerateWorkload(tmp, WorkloadFullRewrite95Pct, 2)
	if err != nil {
		t.Fatalf("generate workload: %v", err)
	}

	baseCP, err := BuildAndUploadCheckpoint(ctx, store, key, ChunkModeFastCDC, DefaultCDCConfig(), "neg95-base", fix.BaseDir, nil)
	if err != nil {
		t.Fatalf("build base: %v", err)
	}
	targetCP, err := BuildAndUploadCheckpoint(ctx, store, key, ChunkModeFastCDC, DefaultCDCConfig(), "neg95-target", fix.TargetDir, baseCP)
	if err != nil {
		t.Fatalf("build target: %v", err)
	}

	store.ResetStats()
	fullRes, err := ExecuteRestore(ctx, store, key, baseCP, targetCP, fix.BaseDir, filepath.Join(tmp, "out_full"), RestoreOptions{
		Strategy:           StrategyFullParallel,
		Workers:            8,
		VerifyVolumeSHA256: true,
	})
	if err != nil {
		t.Fatalf("full parallel restore failed: %v", err)
	}

	store.ResetStats()
	paraRes, err := ExecuteRestore(ctx, store, key, baseCP, targetCP, fix.BaseDir, filepath.Join(tmp, "out_para"), RestoreOptions{
		Strategy:           StrategyParaSyncParallelDelta,
		Workers:            8,
		DiffShards:         4,
		VerifyVolumeSHA256: true,
	})
	if err != nil {
		t.Fatalf("parasync restore failed: %v", err)
	}
	if !fullRes.BitExact || !paraRes.BitExact {
		t.Fatalf("expected bit-exact restores on 95%% rewrite regime")
	}

	savedPct := (1.0 - float64(paraRes.TransferredBytes)/float64(fullRes.TransferredBytes)) * 100.0
	if savedPct > 10.0 {
		t.Fatalf("expected <= 10%% transfer savings on >=95%% churn, got %.2f%%", savedPct)
	}
	if paraRes.Plan.DiffShardsUsed < 2 || paraRes.Plan.PlanDuration < 0 {
		t.Fatalf("unexpected diff plan telemetry: shards=%d dur=%v", paraRes.Plan.DiffShardsUsed, paraRes.Plan.PlanDuration)
	}
	t.Logf("95%% churn honest negative regime: full_par=%d B (%.2f ms) vs parasync=%d B (%.2f ms, diff=%.3f ms, saved=%.2f%%)",
		fullRes.TransferredBytes, float64(fullRes.WallTime)/float64(time.Millisecond),
		paraRes.TransferredBytes, float64(paraRes.WallTime)/float64(time.Millisecond),
		float64(paraRes.DiffTime)/float64(time.Millisecond), savedPct)
}
