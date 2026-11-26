// Copyright 2026 Hanzo AI Inc.
// SPDX-License-Identifier: Apache-2.0

package parasync_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/internal/storetest"
	"github.com/hanzoai/vfs/pkg/backend"
	_ "github.com/hanzoai/vfs/pkg/backend/file"
	s3backend "github.com/hanzoai/vfs/pkg/backend/s3"
	"github.com/hanzoai/vfs/pkg/parasync"
	"github.com/luxfi/age"
)

func testSeed(v byte) [32]byte {
	var s [32]byte
	for i := range s {
		s[i] = v ^ byte(i*17+3)
	}
	return s
}

func makeDeterministicBytes(seed int64, size int) []byte {
	rng := rand.New(rand.NewSource(seed))
	out := make([]byte, size)
	rng.Read(out)
	return out
}

func writeTree(t *testing.T, root string, files map[string][]byte) {
	t.Helper()
	for rel, data := range files {
		full := filepath.Join(root, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", full, err)
		}
		if err := os.WriteFile(full, data, 0o644); err != nil {
			t.Fatalf("write %s: %v", full, err)
		}
	}
}

func assertTreeEquals(t *testing.T, dir string, expected map[string][]byte) {
	t.Helper()
	for rel, want := range expected {
		full := filepath.Join(dir, filepath.FromSlash(rel))
		got, err := os.ReadFile(full)
		if err != nil {
			t.Fatalf("read restored %s: %v", rel, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("restored %s mismatch: len=%d want=%d", rel, len(got), len(want))
		}
	}
}

func openFileStore(t *testing.T) backend.Backend {
	t.Helper()
	dir := t.TempDir()
	be, err := backend.Open(context.Background(), "file://"+dir)
	if err != nil {
		t.Fatalf("open file backend: %v", err)
	}
	t.Cleanup(func() { _ = be.Close() })
	return be
}

func openLiveS3Store(t *testing.T) (backend.Backend, *storetest.S3) {
	t.Helper()
	srv := storetest.NewS3(t, "parasync-test")
	be, err := s3backend.New(s3backend.Config{
		Endpoint:  srv.Endpoint(),
		Insecure:  true,
		Region:    "us-east-1",
		Bucket:    "parasync-test",
		Prefix:    "hanzo/vfs",
		AccessKey: "k",
		SecretKey: "s",
	})
	if err != nil {
		t.Fatalf("open s3 backend: %v", err)
	}
	t.Cleanup(func() { _ = be.Close() })
	return be, srv
}

func TestFastCDCShiftResilienceVsFixed(t *testing.T) {
	base := makeDeterministicBytes(1345, 512<<10) // 512 KiB
	// Insert 137 bytes near the beginning (offset 4096) to trigger shift misalignment.
	insert := makeDeterministicBytes(9999, 137)
	shifted := make([]byte, 0, len(base)+len(insert))
	shifted = append(shifted, base[:4096]...)
	shifted = append(shifted, insert...)
	shifted = append(shifted, base[4096:]...)

	cdcChunker, err := parasync.NewChunker(parasync.DefaultFastCDCConfig(testSeed(1)))
	if err != nil {
		t.Fatal(err)
	}
	fixedChunker, err := parasync.NewChunker(parasync.DefaultFixedConfig())
	if err != nil {
		t.Fatal(err)
	}

	reuseRatio := func(ch *parasync.Chunker) float64 {
		srcSpans := ch.ChunkBytes(base)
		dstSpans := ch.ChunkBytes(shifted)
		have := make(map[parasync.Digest]struct{}, len(srcSpans))
		for _, sp := range srcSpans {
			have[sp.Digest] = struct{}{}
		}
		var reused int64
		for _, sp := range dstSpans {
			if _, ok := have[sp.Digest]; ok {
				reused += sp.Length
			}
		}
		return float64(reused) / float64(len(shifted))
	}

	cdcReuse := reuseRatio(cdcChunker)
	fixedReuse := reuseRatio(fixedChunker)
	t.Logf("shift edit reuse: FastCDC=%.2f%% Fixed=%.2f%%", cdcReuse*100, fixedReuse*100)

	if cdcReuse < 0.85 {
		t.Fatalf("FastCDC shift reuse too low: %.2f%% (want >= 85%%)", cdcReuse*100)
	}
	if fixedReuse > 0.05 {
		t.Fatalf("Fixed-block unexpectedly survived byte shift: %.2f%%", fixedReuse*100)
	}
}

func TestFastCDCChunkSizeDistribution(t *testing.T) {
	cfg := parasync.DefaultFastCDCConfig(testSeed(7))
	ch, err := parasync.NewChunker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	data := makeDeterministicBytes(42, 4<<20) // 4 MiB
	spans := ch.ChunkBytes(data)
	if len(spans) == 0 {
		t.Fatal("expected chunks")
	}
	var total int64
	for i, sp := range spans {
		if sp.Offset != total {
			t.Fatalf("chunk %d offset %d != %d", i, sp.Offset, total)
		}
		isLast := i == len(spans)-1
		if !isLast && (sp.Length < int64(cfg.MinSize) || sp.Length > int64(cfg.MaxSize)) {
			t.Fatalf("chunk %d length %d outside [%d, %d]", i, sp.Length, cfg.MinSize, cfg.MaxSize)
		}
		if isLast && sp.Length > int64(cfg.MaxSize) {
			t.Fatalf("last chunk length %d > max %d", sp.Length, cfg.MaxSize)
		}
		total += sp.Length
	}
	if total != int64(len(data)) {
		t.Fatalf("total coverage %d != %d", total, len(data))
	}
	avg := float64(total) / float64(len(spans))
	// Normalized FastCDC with AvgSize=16 KiB should average well within [10 KiB, 26 KiB].
	if avg < 10*1024 || avg > 26*1024 {
		t.Fatalf("unexpected average chunk size %.1f bytes for AvgSize=%d", avg, cfg.AvgSize)
	}
}

func TestWorkBalancedDiffShards(t *testing.T) {
	ctx := context.Background()
	store := openFileStore(t)
	srcDir := t.TempDir()
	dstDir := t.TempDir()

	files := map[string][]byte{
		"large/model-weights.bin": makeDeterministicBytes(100, 2<<20),
	}
	for i := 0; i < 40; i++ {
		files[fmt.Sprintf("small/file-%02d.go", i)] = makeDeterministicBytes(int64(200+i), 8<<10)
	}
	writeTree(t, srcDir, files)

	// Mutate 10% of the large file and 2 small files.
	dstFiles := make(map[string][]byte, len(files))
	for k, v := range files {
		cp := append([]byte(nil), v...)
		dstFiles[k] = cp
	}
	copy(dstFiles["large/model-weights.bin"][128<<10:], makeDeterministicBytes(777, 128<<10))
	dstFiles["small/file-03.go"] = makeDeterministicBytes(888, 8<<10)
	writeTree(t, dstDir, dstFiles)

	meta := parasync.CheckpointMeta{
		ID:            "ckpt-src",
		TenantID:      "tenant-alpha",
		VolumeID:      "vol-01",
		CipherSuiteID: "plain-test-v1",
	}
	cfg := parasync.DefaultFastCDCConfig(testSeed(2))
	srcCkpt, err := parasync.PublishCheckpoint(ctx, srcDir, store, meta, cfg, parasync.PublishOptions{Workers: 4})
	if err != nil {
		t.Fatal(err)
	}
	meta.ID = "ckpt-dst"
	dstCkpt, err := parasync.PublishCheckpoint(ctx, dstDir, store, meta, cfg, parasync.PublishOptions{Workers: 4})
	if err != nil {
		t.Fatal(err)
	}

	plan, err := parasync.PlanDelta(ctx, srcCkpt, dstCkpt, parasync.PlanOptions{Workers: 8})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.ShardLoads) < 2 {
		t.Fatalf("expected multi-shard partitioning, got %d shards", len(plan.ShardLoads))
	}
	var minLoad, maxLoad int64 = plan.ShardLoads[0], plan.ShardLoads[0]
	for _, l := range plan.ShardLoads {
		if l < minLoad {
			minLoad = l
		}
		if l > maxLoad {
			maxLoad = l
		}
	}
	if minLoad == 0 {
		t.Fatalf("empty shard detected in LPT balance: %v", plan.ShardLoads)
	}
	// With sub-file splitting, max/min load ratio should be well balanced (< 2.5x).
	if float64(maxLoad)/float64(minLoad) > 2.5 {
		t.Fatalf("poor shard balance: loads=%v", plan.ShardLoads)
	}
}

func TestSingleFlightRemoteDedupAndCoalescing(t *testing.T) {
	ctx := context.Background()
	store := openFileStore(t)
	srcDir := t.TempDir()
	dstDir := t.TempDir()

	sharedBlob := makeDeterministicBytes(505, 64<<10)
	writeTree(t, srcDir, map[string][]byte{
		"keep.bin": makeDeterministicBytes(101, 64<<10),
	})
	writeTree(t, dstDir, map[string][]byte{
		"keep.bin":   makeDeterministicBytes(101, 64<<10),
		"dup-a.bin":  sharedBlob,
		"dup-b.bin":  sharedBlob,
		"dup-c.bin":  sharedBlob,
		"extra1.bin": makeDeterministicBytes(606, 32<<10),
		"extra2.bin": makeDeterministicBytes(707, 32<<10),
	})

	meta := parasync.CheckpointMeta{
		ID:            "sf-src",
		TenantID:      "tenant-alpha",
		VolumeID:      "vol-sf",
		CipherSuiteID: "plain-test-v1",
	}
	cfg := parasync.DefaultFastCDCConfig(testSeed(3))
	srcCkpt, err := parasync.PublishCheckpoint(ctx, srcDir, store, meta, cfg, parasync.PublishOptions{Workers: 4})
	if err != nil {
		t.Fatal(err)
	}
	meta.ID = "sf-dst"
	dstCkpt, err := parasync.PublishCheckpoint(ctx, dstDir, store, meta, cfg, parasync.PublishOptions{Workers: 4})
	if err != nil {
		t.Fatal(err)
	}

	plan, err := parasync.PlanDelta(ctx, srcCkpt, dstCkpt, parasync.PlanOptions{Workers: 4})
	if err != nil {
		t.Fatal(err)
	}
	if plan.SingleFlightDedupChunks == 0 || plan.SingleFlightDedupBytes < 120<<10 {
		t.Fatalf("expected single-flight dedup across dup-a/b/c, got chunks=%d bytes=%d",
			plan.SingleFlightDedupChunks, plan.SingleFlightDedupBytes)
	}
	if len(plan.FetchBatches) >= plan.UniqueRemoteChunkCount {
		t.Fatalf("expected gap/contiguous coalescing to reduce GetRange batches (%d) below unique chunks (%d)",
			len(plan.FetchBatches), plan.UniqueRemoteChunkCount)
	}

	restoreDir := t.TempDir()
	rep, err := parasync.Restore(ctx, store, srcCkpt, srcDir, dstCkpt, restoreDir, parasync.RestoreOptions{Workers: 4})
	if err != nil {
		t.Fatal(err)
	}
	if rep.WireBytesTransferred >= plan.RemotePayloadBytes {
		t.Fatalf("expected wire bytes (%d) < undeduped remote payload (%d)", rep.WireBytesTransferred, plan.RemotePayloadBytes)
	}
	assertTreeEquals(t, restoreDir, map[string][]byte{
		"keep.bin":   makeDeterministicBytes(101, 64<<10),
		"dup-a.bin":  sharedBlob,
		"dup-b.bin":  sharedBlob,
		"dup-c.bin":  sharedBlob,
		"extra1.bin": makeDeterministicBytes(606, 32<<10),
		"extra2.bin": makeDeterministicBytes(707, 32<<10),
	})
}

func TestDeltaRestoreLiveFileBackend(t *testing.T) {
	ctx := context.Background()
	store := openFileStore(t)
	srcDir := t.TempDir()
	dstDir := t.TempDir()

	srcFiles := map[string][]byte{
		"src/a.go":   makeDeterministicBytes(1, 128<<10),
		"src/b.go":   makeDeterministicBytes(2, 128<<10),
		"data/c.bin": makeDeterministicBytes(3, 256<<10),
		"empty.txt":  {},
	}
	dstFiles := map[string][]byte{
		"src/a.go":   srcFiles["src/a.go"],
		"src/b.go":   append(append([]byte("inserted-header-line\n"), srcFiles["src/b.go"][:8192]...), srcFiles["src/b.go"][8192:]...),
		"data/c.bin": srcFiles["data/c.bin"],
		"empty.txt":  {},
	}
	writeTree(t, srcDir, srcFiles)
	writeTree(t, dstDir, dstFiles)

	meta := parasync.CheckpointMeta{
		ID:            "file-src",
		TenantID:      "tenant-alpha",
		VolumeID:      "vol-file",
		CipherSuiteID: "plain-test-v1",
	}
	cfg := parasync.DefaultFastCDCConfig(testSeed(4))
	srcCkpt, err := parasync.PublishCheckpoint(ctx, srcDir, store, meta, cfg, parasync.PublishOptions{Workers: 4})
	if err != nil {
		t.Fatal(err)
	}
	meta.ID = "file-dst"
	dstCkpt, err := parasync.PublishCheckpoint(ctx, dstDir, store, meta, cfg, parasync.PublishOptions{Workers: 4})
	if err != nil {
		t.Fatal(err)
	}

	restoreDir := t.TempDir()
	rep, err := parasync.Restore(ctx, store, srcCkpt, srcDir, dstCkpt, restoreDir, parasync.RestoreOptions{Workers: 8})
	if err != nil {
		t.Fatal(err)
	}
	if rep.BytesSavedRatio < 0.85 {
		t.Fatalf("expected >=85%% wire byte reduction on shifted edit, got %.2f%% (wire=%d total=%d)",
			rep.BytesSavedRatio*100, rep.WireBytesTransferred, rep.TotalTargetBytes)
	}
	if rep.VerifiedFiles != len(dstFiles) {
		t.Fatalf("verified files=%d want=%d", rep.VerifiedFiles, len(dstFiles))
	}
	assertTreeEquals(t, restoreDir, dstFiles)
}

func TestDeltaRestoreLiveHTTP_S3Backend(t *testing.T) {
	ctx := context.Background()
	store, srv := openLiveS3Store(t)
	srcDir := t.TempDir()
	dstDir := t.TempDir()

	srcFiles := map[string][]byte{
		"repo/main.go":    makeDeterministicBytes(11, 200<<10),
		"repo/engine.go":  makeDeterministicBytes(12, 200<<10),
		"state/cache.dat": makeDeterministicBytes(13, 300<<10),
	}
	dstFiles := map[string][]byte{
		"repo/main.go":    srcFiles["repo/main.go"],
		"repo/engine.go":  append(append([]byte(nil), srcFiles["repo/engine.go"][:4096]...), append([]byte("shift-edit-payload-0123456789"), srcFiles["repo/engine.go"][4096:]...)...),
		"state/cache.dat": srcFiles["state/cache.dat"],
	}
	writeTree(t, srcDir, srcFiles)
	writeTree(t, dstDir, dstFiles)

	meta := parasync.CheckpointMeta{
		ID:            "s3-src",
		TenantID:      "tenant-hanzo",
		VolumeID:      "vol-s3-live",
		CipherSuiteID: "plain-test-v1",
	}
	cfg := parasync.DefaultFastCDCConfig(testSeed(5))
	srcCkpt, err := parasync.PublishCheckpoint(ctx, srcDir, store, meta, cfg, parasync.PublishOptions{Workers: 4})
	if err != nil {
		t.Fatal(err)
	}
	meta.ID = "s3-dst"
	dstCkpt, err := parasync.PublishCheckpoint(ctx, dstDir, store, meta, cfg, parasync.PublishOptions{Workers: 4})
	if err != nil {
		t.Fatal(err)
	}

	reqBefore := srv.Requests()
	restoreDir := t.TempDir()
	rep, err := parasync.Restore(ctx, store, srcCkpt, srcDir, dstCkpt, restoreDir, parasync.RestoreOptions{Workers: 8})
	if err != nil {
		t.Fatal(err)
	}
	s3Reqs := srv.Requests() - reqBefore
	if s3Reqs == 0 || int64(s3Reqs) != rep.RemoteRequests {
		t.Fatalf("expected live HTTP S3 requests (%d) to match report (%d)", s3Reqs, rep.RemoteRequests)
	}
	if rep.BytesSavedRatio < 0.85 {
		t.Fatalf("expected >=85%% wire byte savings over live S3, got %.2f%%", rep.BytesSavedRatio*100)
	}
	assertTreeEquals(t, restoreDir, dstFiles)
}

func TestDeltaRestoreEncryptedHybridCipher(t *testing.T) {
	ctx := context.Background()
	store, _ := openLiveS3Store(t)

	id, err := age.GenerateHybridIdentity()
	if err != nil {
		t.Fatal(err)
	}
	cipher := vfs.Hybrid(id)

	srcDir := t.TempDir()
	dstDir := t.TempDir()
	srcFiles := map[string][]byte{
		"secret/a.bin": makeDeterministicBytes(21, 96<<10),
		"secret/b.bin": makeDeterministicBytes(22, 96<<10),
	}
	dstFiles := map[string][]byte{
		"secret/a.bin": srcFiles["secret/a.bin"],
		"secret/b.bin": append([]byte("encrypted-delta-prefix"), srcFiles["secret/b.bin"]...),
	}
	writeTree(t, srcDir, srcFiles)
	writeTree(t, dstDir, dstFiles)

	meta := parasync.CheckpointMeta{
		ID:            "enc-src",
		TenantID:      "tenant-pq",
		VolumeID:      "vol-pq-01",
		CipherSuiteID: "age-hybrid-mlkem768-v1",
	}
	cfg := parasync.DefaultFastCDCConfig(testSeed(6))
	srcCkpt, err := parasync.PublishCheckpoint(ctx, srcDir, store, meta, cfg, parasync.PublishOptions{Workers: 4, Cipher: cipher})
	if err != nil {
		t.Fatal(err)
	}
	meta.ID = "enc-dst"
	dstCkpt, err := parasync.PublishCheckpoint(ctx, dstDir, store, meta, cfg, parasync.PublishOptions{Workers: 4, Cipher: cipher})
	if err != nil {
		t.Fatal(err)
	}

	restoreDir := t.TempDir()
	rep, err := parasync.Restore(ctx, store, srcCkpt, srcDir, dstCkpt, restoreDir, parasync.RestoreOptions{
		Workers: 4,
		Cipher:  cipher,
	})
	if err != nil {
		t.Fatal(err)
	}
	if rep.ReusedLocalBytes == 0 || rep.WireBytesTransferred >= rep.TotalTargetBytes {
		t.Fatalf("expected encrypted delta reuse, got reused=%d wire=%d total=%d",
			rep.ReusedLocalBytes, rep.WireBytesTransferred, rep.TotalTargetBytes)
	}
	assertTreeEquals(t, restoreDir, dstFiles)

	// Attempting to open encrypted packs with a different Hybrid identity must fail closed.
	wrongID, err := age.GenerateHybridIdentity()
	if err != nil {
		t.Fatal(err)
	}
	_, err = parasync.Restore(ctx, store, srcCkpt, srcDir, dstCkpt, t.TempDir(), parasync.RestoreOptions{
		Workers: 4,
		Cipher:  vfs.Hybrid(wrongID),
	})
	if !errors.Is(err, parasync.ErrCorruptChunk) {
		t.Fatalf("expected ErrCorruptChunk on wrong decryption key, got %v", err)
	}
}

func TestCrossTenantAndCrossVolumeIsolation(t *testing.T) {
	ctx := context.Background()
	store := openFileStore(t)
	dir := t.TempDir()
	writeTree(t, dir, map[string][]byte{
		"shared.bin": makeDeterministicBytes(33, 64<<10),
	})

	cfg := parasync.DefaultFastCDCConfig(testSeed(8))
	ckptA, err := parasync.PublishCheckpoint(ctx, dir, store, parasync.CheckpointMeta{
		ID: "a", TenantID: "tenant-alpha", VolumeID: "vol-1", CipherSuiteID: "aes-256-gcm-v1",
	}, cfg, parasync.PublishOptions{})
	if err != nil {
		t.Fatal(err)
	}
	ckptOtherTenant, err := parasync.PublishCheckpoint(ctx, dir, store, parasync.CheckpointMeta{
		ID: "b", TenantID: "tenant-beta", VolumeID: "vol-1", CipherSuiteID: "aes-256-gcm-v1",
	}, cfg, parasync.PublishOptions{})
	if err != nil {
		t.Fatal(err)
	}
	ckptOtherVol, err := parasync.PublishCheckpoint(ctx, dir, store, parasync.CheckpointMeta{
		ID: "c", TenantID: "tenant-alpha", VolumeID: "vol-2", CipherSuiteID: "aes-256-gcm-v1",
	}, cfg, parasync.PublishOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if err := parasync.CheckCompatibility(ckptA, ckptOtherTenant); !errors.Is(err, parasync.ErrTenantIsolation) {
		t.Fatalf("cross-tenant must fail with ErrTenantIsolation, got %v", err)
	}
	if _, err := parasync.PlanDelta(ctx, ckptA, ckptOtherTenant, parasync.PlanOptions{}); !errors.Is(err, parasync.ErrTenantIsolation) {
		t.Fatalf("PlanDelta cross-tenant must fail with ErrTenantIsolation, got %v", err)
	}
	if _, err := parasync.Restore(ctx, store, ckptA, dir, ckptOtherVol, t.TempDir(), parasync.RestoreOptions{}); !errors.Is(err, parasync.ErrTenantIsolation) {
		t.Fatalf("Restore cross-volume must fail with ErrTenantIsolation, got %v", err)
	}

	// Verify pack keys are strictly tenant/volume prefixed and disjoint.
	prefA := parasync.TenantScopePrefix("tenant-alpha", "vol-1")
	prefB := parasync.TenantScopePrefix("tenant-beta", "vol-1")
	if prefA == prefB {
		t.Fatalf("tenant prefixes collided: %s", prefA)
	}
	if !strings.HasPrefix(ckptA.Files[0].Chunks[0].PackKey, prefA+"/") {
		t.Fatalf("pack key %q missing tenant prefix %q", ckptA.Files[0].Chunks[0].PackKey, prefA)
	}
}

func TestCompatibilityGuardCipherAndChunkingMismatch(t *testing.T) {
	ctx := context.Background()
	store := openFileStore(t)
	dir := t.TempDir()
	writeTree(t, dir, map[string][]byte{"f.bin": makeDeterministicBytes(44, 64<<10)})

	baseMeta := parasync.CheckpointMeta{
		ID: "m1", TenantID: "tenant-alpha", VolumeID: "vol-1", CipherSuiteID: "cipher-v1",
	}
	cdcA, err := parasync.PublishCheckpoint(ctx, dir, store, baseMeta, parasync.DefaultFastCDCConfig(testSeed(1)), parasync.PublishOptions{})
	if err != nil {
		t.Fatal(err)
	}

	diffCipherMeta := baseMeta
	diffCipherMeta.ID = "m2"
	diffCipherMeta.CipherSuiteID = "cipher-v2"
	diffCipher, err := parasync.PublishCheckpoint(ctx, dir, store, diffCipherMeta, parasync.DefaultFastCDCConfig(testSeed(1)), parasync.PublishOptions{})
	if err != nil {
		t.Fatal(err)
	}

	diffMode, err := parasync.PublishCheckpoint(ctx, dir, store, baseMeta, parasync.DefaultFixedConfig(), parasync.PublishOptions{})
	if err != nil {
		t.Fatal(err)
	}

	diffSeed, err := parasync.PublishCheckpoint(ctx, dir, store, baseMeta, parasync.DefaultFastCDCConfig(testSeed(2)), parasync.PublishOptions{})
	if err != nil {
		t.Fatal(err)
	}

	for name, other := range map[string]*parasync.Checkpoint{
		"cipher_suite": diffCipher,
		"chunk_mode":   diffMode,
		"gear_seed":    diffSeed,
	} {
		if err := parasync.CheckCompatibility(cdcA, other); !errors.Is(err, parasync.ErrIncompatibleCheckpoint) {
			t.Fatalf("%s mismatch: want ErrIncompatibleCheckpoint, got %v", name, err)
		}
	}
}

func TestCorruptionDetectionRemoteBitFlip(t *testing.T) {
	ctx := context.Background()
	store, _ := openLiveS3Store(t)
	srcDir := t.TempDir()
	dstDir := t.TempDir()

	writeTree(t, srcDir, map[string][]byte{"a.bin": makeDeterministicBytes(51, 64<<10)})
	writeTree(t, dstDir, map[string][]byte{
		"a.bin": makeDeterministicBytes(51, 64<<10),
		"b.bin": makeDeterministicBytes(52, 64<<10),
	})

	meta := parasync.CheckpointMeta{ID: "c1", TenantID: "t1", VolumeID: "v1", CipherSuiteID: "plain-v1"}
	cfg := parasync.DefaultFastCDCConfig(testSeed(9))
	srcCkpt, err := parasync.PublishCheckpoint(ctx, srcDir, store, meta, cfg, parasync.PublishOptions{})
	if err != nil {
		t.Fatal(err)
	}
	meta.ID = "c2"
	dstCkpt, err := parasync.PublishCheckpoint(ctx, dstDir, store, meta, cfg, parasync.PublishOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// Flip 1 single bit in the remote pack containing b.bin.
	packKey := dstCkpt.Files[1].Chunks[0].PackKey
	raw, _, err := store.Get(ctx, packKey, backend.Unbounded)
	if err != nil {
		t.Fatal(err)
	}
	raw[dstCkpt.Files[1].Chunks[0].PackOffset+7] ^= 0x01
	if err := store.Put(ctx, packKey, raw); err != nil {
		t.Fatal(err)
	}

	// Pre-populate restoreDir with sentinel file to verify no torn target state on failure.
	restoreDir := t.TempDir()
	sentinel := []byte("untouched-existing-state")
	writeTree(t, restoreDir, map[string][]byte{"a.bin": sentinel})

	_, err = parasync.Restore(ctx, store, srcCkpt, srcDir, dstCkpt, restoreDir, parasync.RestoreOptions{Workers: 4})
	if !errors.Is(err, parasync.ErrCorruptChunk) {
		t.Fatalf("expected ErrCorruptChunk on remote bit flip, got %v", err)
	}
	got, err := os.ReadFile(filepath.Join(restoreDir, "a.bin"))
	if err != nil || !bytes.Equal(got, sentinel) {
		t.Fatalf("destination was modified on corrupt remote restore: %q %v", got, err)
	}
	if _, err := os.Stat(filepath.Join(restoreDir, "b.bin")); !os.IsNotExist(err) {
		t.Fatalf("partial b.bin leaked into destination dir")
	}
}

func TestCorruptionDetectionLocalBitFlip(t *testing.T) {
	ctx := context.Background()
	store := openFileStore(t)
	srcDir := t.TempDir()
	dstDir := t.TempDir()

	writeTree(t, srcDir, map[string][]byte{"keep.bin": makeDeterministicBytes(61, 64<<10)})
	writeTree(t, dstDir, map[string][]byte{
		"keep.bin": makeDeterministicBytes(61, 64<<10),
		"new.bin":  makeDeterministicBytes(62, 32<<10),
	})

	meta := parasync.CheckpointMeta{ID: "l1", TenantID: "t1", VolumeID: "v1", CipherSuiteID: "plain-v1"}
	cfg := parasync.DefaultFastCDCConfig(testSeed(10))
	srcCkpt, err := parasync.PublishCheckpoint(ctx, srcDir, store, meta, cfg, parasync.PublishOptions{})
	if err != nil {
		t.Fatal(err)
	}
	meta.ID = "l2"
	dstCkpt, err := parasync.PublishCheckpoint(ctx, dstDir, store, meta, cfg, parasync.PublishOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// Flip 1 bit in local source file on disk after checkpoint publication.
	localFile := filepath.Join(srcDir, "keep.bin")
	b, err := os.ReadFile(localFile)
	if err != nil {
		t.Fatal(err)
	}
	b[123] ^= 0x80
	if err := os.WriteFile(localFile, b, 0o644); err != nil {
		t.Fatal(err)
	}

	restoreDir := t.TempDir()
	_, err = parasync.Restore(ctx, store, srcCkpt, srcDir, dstCkpt, restoreDir, parasync.RestoreOptions{Workers: 4})
	if !errors.Is(err, parasync.ErrCorruptChunk) {
		t.Fatalf("expected ErrCorruptChunk on local bit flip, got %v", err)
	}
}

func TestCorruptionDetectionManifestRootTamper(t *testing.T) {
	ctx := context.Background()
	store := openFileStore(t)
	dir := t.TempDir()
	writeTree(t, dir, map[string][]byte{"a.bin": makeDeterministicBytes(71, 32<<10)})

	meta := parasync.CheckpointMeta{ID: "r1", TenantID: "t1", VolumeID: "v1", CipherSuiteID: "plain-v1"}
	ckpt, err := parasync.PublishCheckpoint(ctx, dir, store, meta, parasync.DefaultFastCDCConfig(testSeed(11)), parasync.PublishOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// Tamper with RootDigest.
	ckpt.RootDigest[0] ^= 0x01
	_, err = parasync.Restore(ctx, store, nil, "", ckpt, t.TempDir(), parasync.RestoreOptions{SkipDelta: true})
	if !errors.Is(err, parasync.ErrCorruptCheckpoint) {
		t.Fatalf("expected ErrCorruptCheckpoint on tampered root digest, got %v", err)
	}
}

type blockingRangeStore struct {
	backend.Backend
	entered chan struct{}
	calls   atomic.Int32
}

func (b *blockingRangeStore) GetRange(ctx context.Context, key string, off, n int64) ([]byte, error) {
	if b.calls.Add(1) == 1 {
		close(b.entered)
	}
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestCancellationNoTornTargetState(t *testing.T) {
	baseStore := openFileStore(t)
	srcDir := t.TempDir()
	dstDir := t.TempDir()

	writeTree(t, srcDir, map[string][]byte{"a.bin": makeDeterministicBytes(81, 64<<10)})
	writeTree(t, dstDir, map[string][]byte{
		"a.bin": makeDeterministicBytes(81, 64<<10),
		"b.bin": makeDeterministicBytes(82, 128<<10),
		"c.bin": makeDeterministicBytes(83, 128<<10),
	})

	meta := parasync.CheckpointMeta{ID: "can-1", TenantID: "t1", VolumeID: "v1", CipherSuiteID: "plain-v1"}
	cfg := parasync.DefaultFastCDCConfig(testSeed(12))
	srcCkpt, err := parasync.PublishCheckpoint(context.Background(), srcDir, baseStore, meta, cfg, parasync.PublishOptions{})
	if err != nil {
		t.Fatal(err)
	}
	meta.ID = "can-2"
	dstCkpt, err := parasync.PublishCheckpoint(context.Background(), dstDir, baseStore, meta, cfg, parasync.PublishOptions{})
	if err != nil {
		t.Fatal(err)
	}

	restoreDir := t.TempDir()
	sentinel := []byte("pre-existing-valid-content")
	writeTree(t, restoreDir, map[string][]byte{"a.bin": sentinel})

	bs := &blockingRangeStore{Backend: baseStore, entered: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		_, rerr := parasync.Restore(ctx, bs, srcCkpt, srcDir, dstCkpt, restoreDir, parasync.RestoreOptions{Workers: 4})
		done <- rerr
	}()

	select {
	case <-bs.entered:
		cancel()
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for remote fetch to start")
	}

	select {
	case rerr := <-done:
		if !errors.Is(rerr, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", rerr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Restore did not exit promptly after context cancel")
	}

	// Verify destination state is untouched and no staging dirs remain.
	got, err := os.ReadFile(filepath.Join(restoreDir, "a.bin"))
	if err != nil || !bytes.Equal(got, sentinel) {
		t.Fatalf("destination file torn after cancel: %q %v", got, err)
	}
	entries, err := os.ReadDir(restoreDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".parasync-stage-") {
			t.Fatalf("leaked staging directory %s after cancel", e.Name())
		}
	}
}

func TestPathTraversalRejection(t *testing.T) {
	badPaths := []string{
		"",
		"/leading/slash.txt",
		"trailing/slash/",
		"../parent/escape.txt",
		"nested/../../escape.txt",
		"a/./b.txt",
		"has\x00nul.txt",
		`back\slash.txt`,
	}
	for _, p := range badPaths {
		if err := parasync.ValidatePath(p); !errors.Is(err, parasync.ErrInvalidPath) {
			t.Fatalf("ValidatePath(%q): want ErrInvalidPath, got %v", p, err)
		}
	}
}

func TestNegativeRegimeHighChurnAutoFallback(t *testing.T) {
	ctx := context.Background()
	store := openFileStore(t)
	srcDir := t.TempDir()
	dstDir := t.TempDir()

	// 100% churn (models re-compressed / re-encrypted target with zero chunk overlap).
	writeTree(t, srcDir, map[string][]byte{
		"blob1.bin": makeDeterministicBytes(1001, 256<<10),
		"blob2.bin": makeDeterministicBytes(1002, 256<<10),
	})
	writeTree(t, dstDir, map[string][]byte{
		"blob1.bin": makeDeterministicBytes(2001, 256<<10),
		"blob2.bin": makeDeterministicBytes(2002, 256<<10),
	})

	meta := parasync.CheckpointMeta{ID: "hc-1", TenantID: "t1", VolumeID: "v1", CipherSuiteID: "plain-v1"}
	cfg := parasync.DefaultFastCDCConfig(testSeed(13))
	srcCkpt, err := parasync.PublishCheckpoint(ctx, srcDir, store, meta, cfg, parasync.PublishOptions{})
	if err != nil {
		t.Fatal(err)
	}
	meta.ID = "hc-2"
	dstCkpt, err := parasync.PublishCheckpoint(ctx, dstDir, store, meta, cfg, parasync.PublishOptions{})
	if err != nil {
		t.Fatal(err)
	}

	plan, err := parasync.PlanDelta(ctx, srcCkpt, dstCkpt, parasync.PlanOptions{AutoFallbackChurnRatio: 0.85})
	if err != nil {
		t.Fatal(err)
	}
	if plan.ChurnRatio < 0.99 || !plan.FallbackRecommended {
		t.Fatalf("expected ~100%% churn and FallbackRecommended=true, got churn=%.2f fallback=%v",
			plan.ChurnRatio, plan.FallbackRecommended)
	}

	restoreDir := t.TempDir()
	rep, err := parasync.Restore(ctx, store, srcCkpt, srcDir, dstCkpt, restoreDir, parasync.RestoreOptions{
		Workers:            8,
		EnableAutoFallback: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !rep.UsedFullFallback || rep.LocalDiskReadBytes != 0 {
		t.Fatalf("expected auto-fallback with 0 local reads, got fallback=%v localReads=%d",
			rep.UsedFullFallback, rep.LocalDiskReadBytes)
	}
	assertTreeEquals(t, restoreDir, map[string][]byte{
		"blob1.bin": makeDeterministicBytes(2001, 256<<10),
		"blob2.bin": makeDeterministicBytes(2002, 256<<10),
	})
}

func TestNegativeRegimeTinyCheckpoint(t *testing.T) {
	ctx := context.Background()
	store := openFileStore(t)
	srcDir := t.TempDir()
	dstDir := t.TempDir()

	// Tiny checkpoint (< 64 KiB, few small files).
	srcFiles := map[string][]byte{
		"cfg/a.json": []byte(`{"mode":"alpha","v":1}`),
		"cfg/b.json": []byte(`{"mode":"beta","v":1}`),
		"readme.md":  makeDeterministicBytes(3001, 12<<10),
	}
	dstFiles := map[string][]byte{
		"cfg/a.json": []byte(`{"mode":"alpha","v":2}`),
		"cfg/b.json": []byte(`{"mode":"beta","v":1}`),
		"readme.md":  makeDeterministicBytes(3001, 12<<10),
	}
	writeTree(t, srcDir, srcFiles)
	writeTree(t, dstDir, dstFiles)

	meta := parasync.CheckpointMeta{ID: "tiny-1", TenantID: "t1", VolumeID: "v1", CipherSuiteID: "plain-v1"}
	cfg := parasync.DefaultFastCDCConfig(testSeed(14))
	srcCkpt, err := parasync.PublishCheckpoint(ctx, srcDir, store, meta, cfg, parasync.PublishOptions{})
	if err != nil {
		t.Fatal(err)
	}
	meta.ID = "tiny-2"
	dstCkpt, err := parasync.PublishCheckpoint(ctx, dstDir, store, meta, cfg, parasync.PublishOptions{})
	if err != nil {
		t.Fatal(err)
	}

	restoreDir := t.TempDir()
	rep, err := parasync.Restore(ctx, store, srcCkpt, srcDir, dstCkpt, restoreDir, parasync.RestoreOptions{Workers: 8})
	if err != nil {
		t.Fatal(err)
	}
	if rep.TotalTargetBytes >= 64<<10 {
		t.Fatalf("expected tiny checkpoint < 64 KiB, got %d", rep.TotalTargetBytes)
	}
	assertTreeEquals(t, restoreDir, dstFiles)
}

func TestScanLocalDirLiveDeltaRestore(t *testing.T) {
	ctx := context.Background()
	store, _ := openLiveS3Store(t)
	srcDir := t.TempDir()
	dstDir := t.TempDir()

	srcFiles := map[string][]byte{
		"ws/file1.bin": makeDeterministicBytes(4001, 160<<10),
		"ws/file2.bin": makeDeterministicBytes(4002, 160<<10),
	}
	dstFiles := map[string][]byte{
		"ws/file1.bin": srcFiles["ws/file1.bin"],
		"ws/file2.bin": append([]byte("live-scan-shift-edit!"), srcFiles["ws/file2.bin"]...),
	}
	writeTree(t, srcDir, srcFiles)
	writeTree(t, dstDir, dstFiles)

	meta := parasync.CheckpointMeta{ID: "scan-dst", TenantID: "t1", VolumeID: "v1", CipherSuiteID: "plain-v1"}
	cfg := parasync.DefaultFastCDCConfig(testSeed(15))
	dstCkpt, err := parasync.PublishCheckpoint(ctx, dstDir, store, meta, cfg, parasync.PublishOptions{Workers: 4})
	if err != nil {
		t.Fatal(err)
	}

	restoreDir := t.TempDir()
	rep, err := parasync.Restore(ctx, store, nil, srcDir, dstCkpt, restoreDir, parasync.RestoreOptions{
		Workers:      8,
		ScanLocalDir: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if rep.ScanDuration <= 0 || rep.BytesSavedRatio < 0.85 {
		t.Fatalf("expected live scan delta savings >=85%%, got scan=%v saved=%.2f%%",
			rep.ScanDuration, rep.BytesSavedRatio*100)
	}
	assertTreeEquals(t, restoreDir, dstFiles)
}

func TestNegativeRegimeUnrelatedBaseZeroOverlap(t *testing.T) {
	ctx := context.Background()
	store := openFileStore(t)
	srcDir := t.TempDir()
	dstDir := t.TempDir()

	// Completely unrelated file trees under the same (tenant, volume, cipher, chunking).
	writeTree(t, srcDir, map[string][]byte{
		"old-app/a.bin": makeDeterministicBytes(5001, 128<<10),
		"old-app/b.bin": makeDeterministicBytes(5002, 128<<10),
	})
	dstFiles := map[string][]byte{
		"new-app/x.bin": makeDeterministicBytes(6001, 128<<10),
		"new-app/y.bin": makeDeterministicBytes(6002, 128<<10),
	}
	writeTree(t, dstDir, dstFiles)

	meta := parasync.CheckpointMeta{ID: "unrel-src", TenantID: "t1", VolumeID: "v1", CipherSuiteID: "plain-v1"}
	cfg := parasync.DefaultFastCDCConfig(testSeed(16))
	srcCkpt, err := parasync.PublishCheckpoint(ctx, srcDir, store, meta, cfg, parasync.PublishOptions{})
	if err != nil {
		t.Fatal(err)
	}
	meta.ID = "unrel-dst"
	dstCkpt, err := parasync.PublishCheckpoint(ctx, dstDir, store, meta, cfg, parasync.PublishOptions{})
	if err != nil {
		t.Fatal(err)
	}

	restoreDir := t.TempDir()
	rep, err := parasync.Restore(ctx, store, srcCkpt, srcDir, dstCkpt, restoreDir, parasync.RestoreOptions{Workers: 8})
	if err != nil {
		t.Fatal(err)
	}
	if rep.ReusedLocalBytes != 0 || !rep.FallbackRecommended || rep.ChurnRatio < 0.999 {
		t.Fatalf("expected 0%% reuse and FallbackRecommended=true, got reused=%d churn=%.4f fallback=%v",
			rep.ReusedLocalBytes, rep.ChurnRatio, rep.FallbackRecommended)
	}
	assertTreeEquals(t, restoreDir, dstFiles)
}
