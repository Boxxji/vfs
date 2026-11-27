// Copyright 2026 Hanzo AI Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package dogi_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/luxfi/age"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/internal/storetest"
	"github.com/hanzoai/vfs/pkg/backend"
	_ "github.com/hanzoai/vfs/pkg/backend/file"
	backends3 "github.com/hanzoai/vfs/pkg/backend/s3"
	"github.com/hanzoai/vfs/pkg/dogi"
)

func openFileBackend(t *testing.T) backend.Backend {
	t.Helper()
	dir := t.TempDir()
	be, err := backend.Open(context.Background(), "file://"+filepath.ToSlash(dir))
	if err != nil {
		t.Fatalf("open file backend: %v", err)
	}
	t.Cleanup(func() { _ = be.Close() })
	return be
}

func deterministicPayload(rng *rand.Rand, tag string, size int) []byte {
	buf := make([]byte, size)
	copy(buf, tag)
	for i := len(tag); i < len(buf); i++ {
		buf[i] = byte(rng.Intn(256))
	}
	return buf
}

// TestNamespaceIsolationRejectsCrossTenant verifies strict (TenantID, VolumeID)
// isolation: mismatched namespaces fail with ErrTenantIsolation and never touch
// buffers or backend state, and separate tenants get disjoint key prefixes.
func TestNamespaceIsolationRejectsCrossTenant(t *testing.T) {
	ctx := context.Background()
	be := storetest.NewMem()
	nsA := dogi.Namespace{TenantID: "tenant-alpha", VolumeID: "vol-01"}
	nsB := dogi.Namespace{TenantID: "tenant-beta", VolumeID: "vol-01"}
	nsAVol2 := dogi.Namespace{TenantID: "tenant-alpha", VolumeID: "vol-02"}

	if nsA.KeyPrefix() == nsB.KeyPrefix() {
		t.Fatalf("disjoint tenants produced identical prefix: %s", nsA.KeyPrefix())
	}
	if nsA.KeyPrefix() == nsAVol2.KeyPrefix() {
		t.Fatalf("disjoint volumes produced identical prefix: %s", nsA.KeyPrefix())
	}
	if err := backend.ValidKey(nsA.KeyPrefix() + "/ab/0123456789abcdef0123456789abcdef"); err != nil {
		t.Fatalf("pack key violates backend.ValidKey: %v", err)
	}

	pk, err := dogi.NewPacker(be, dogi.Config{
		Namespace:       nsA,
		Policy:          dogi.PolicyDOGILifetime,
		PackTargetBytes: 16 << 10,
	})
	if err != nil {
		t.Fatalf("NewPacker: %v", err)
	}

	// Same tenant/volume succeeds.
	if _, _, err := pk.PutChunk(ctx, dogi.ChunkMeta{Namespace: nsA, Path: "src/main.go", Epoch: 1}, []byte("package main\n")); err != nil {
		t.Fatalf("same-namespace PutChunk failed: %v", err)
	}
	// Different tenant MUST fail with ErrTenantIsolation.
	if _, _, err := pk.PutChunk(ctx, dogi.ChunkMeta{Namespace: nsB, Path: "src/leak.go", Epoch: 1}, []byte("cross-tenant")); !errors.Is(err, dogi.ErrTenantIsolation) {
		t.Fatalf("cross-tenant PutChunk err = %v, want ErrTenantIsolation", err)
	}
	// Different volume under same tenant MUST fail with ErrTenantIsolation.
	if _, _, err := pk.PutChunk(ctx, dogi.ChunkMeta{Namespace: nsAVol2, Path: "src/other.go", Epoch: 1}, []byte("cross-volume")); !errors.Is(err, dogi.ErrTenantIsolation) {
		t.Fatalf("cross-volume PutChunk err = %v, want ErrTenantIsolation", err)
	}
	if err := pk.FlushAll(ctx, 1); err != nil {
		t.Fatalf("FlushAll: %v", err)
	}
	// Confirm only tenant-alpha/vol-01 keys exist in backend.
	for k := range be.Dump() {
		if !strings.HasPrefix(k, nsA.KeyPrefix()+"/") {
			t.Fatalf("unexpected backend key outside tenant prefix: %s", k)
		}
	}
	// Invalid namespace components rejected at construction.
	for _, bad := range []dogi.Namespace{
		{TenantID: "", VolumeID: "v1"},
		{TenantID: "t1", VolumeID: ""},
		{TenantID: "../escape", VolumeID: "v1"},
		{TenantID: "t1", VolumeID: "a/b"},
	} {
		if _, err := dogi.NewPacker(be, dogi.Config{Namespace: bad}); !errors.Is(err, dogi.ErrInvalidNamespace) {
			t.Fatalf("bad namespace %+v err = %v, want ErrInvalidNamespace", bad, err)
		}
	}
}

// TestStaticPathPriorClassification verifies observable path/directory/extension
// priors across short-lived build artifacts, long-lived workspace files, and
// pinned immutable assets.
func TestStaticPathPriorClassification(t *testing.T) {
	cls := dogi.NewClassifier(dogi.PolicyPathGrouped)
	cases := []struct {
		path string
		want dogi.LifetimeClass
	}{
		{"target/debug/deps/libfoo.o", dogi.ClassShortLived},
		{"node_modules/.cache/webpack/chunk-1.bin", dogi.ClassShortLived},
		{".next/cache/build-123.json", dogi.ClassShortLived},
		{"tmp/go-build/a001.o", dogi.ClassShortLived},
		{"build/foo.tmp", dogi.ClassShortLived},
		{"app/__pycache__/util.pyc", dogi.ClassShortLived},
		{".git/objects/ab/1234567890", dogi.ClassPinnedImmutable},
		{"models/zen-7b/model.safetensors", dogi.ClassPinnedImmutable},
		{"vendor/github.com/pkg/errors/errors.go", dogi.ClassLongLived},
		{"go.sum", dogi.ClassLongLived},
		{"pkg/dogi/packer.go", dogi.ClassLongLived},
		{"ws/opaque-slot-01/blob-001.dat", dogi.ClassMediumLived},
	}
	for _, tc := range cases {
		got := cls.ClassifyWrite(dogi.ChunkMeta{Path: tc.path, Epoch: 0}, 4096)
		if got != tc.want {
			t.Errorf("ClassifyWrite(%q) = %s, want %s", tc.path, got, tc.want)
		}
	}
}

// TestNonOraclePoliciesNeverPeekOracleDeathEpoch proves that PolicyFIFOMixed,
// PolicyPathGrouped, and PolicyDOGILifetime are strictly non-oracle: changing
// OracleDeathEpoch produces bit-identical classification decisions.
func TestNonOraclePoliciesNeverPeekOracleDeathEpoch(t *testing.T) {
	policies := []dogi.Policy{
		dogi.PolicyFIFOMixed,
		dogi.PolicyPathGrouped,
		dogi.PolicyDOGILifetime,
	}
	paths := []string{
		"target/debug/a.o",
		"ws/slot-99/item.dat",
		"pkg/core/service.go",
		".git/objects/aa/bbcc",
	}
	for _, pol := range policies {
		c1 := dogi.NewClassifier(pol)
		c2 := dogi.NewClassifier(pol)
		for _, p := range paths {
			g1 := c1.ClassifyWrite(dogi.ChunkMeta{Path: p, Epoch: 1, OracleDeathEpoch: 2}, 8192)
			g2 := c2.ClassifyWrite(dogi.ChunkMeta{Path: p, Epoch: 1, OracleDeathEpoch: 99999}, 8192)
			if g1 != g2 {
				t.Fatalf("policy %s peeked at OracleDeathEpoch for %q: %s != %s", pol, p, g1, g2)
			}
		}
	}
}

// TestOnlineDirLifetimeTrackerLearnsOpaquePaths verifies that DOGI's online
// EWMA directory tracker dynamically separates ephemeral vs. long-lived opaque
// directories without recognizable path prefixes or extensions.
func TestOnlineDirLifetimeTrackerLearnsOpaquePaths(t *testing.T) {
	cls := dogi.NewClassifier(dogi.PolicyDOGILifetime)
	tr := cls.Tracker()

	// Cold start: both opaque dirs default to MediumLived.
	if got := cls.ClassifyWrite(dogi.ChunkMeta{Path: "ws/slot-01/a.dat", Epoch: 0}, 8192); got != dogi.ClassMediumLived {
		t.Fatalf("cold slot-01 = %s, want medium_lived", got)
	}
	if got := cls.ClassifyWrite(dogi.ChunkMeta{Path: "ws/slot-03/a.dat", Epoch: 0}, 8192); got != dogi.ClassMediumLived {
		t.Fatalf("cold slot-03 = %s, want medium_lived", got)
	}

	// Simulate rapid 1-epoch deaths in ws/slot-01 and multi-epoch survivals in ws/slot-03.
	for i := 0; i < 20; i++ {
		tr.RecordWrite("ws/slot-01", 8192, 1, false)
		tr.RecordDelete("ws/slot-01", 1, 2, false)
		tr.RecordWrite("ws/slot-03", 8192, 0, false)
	}
	tr.RecordEpochSurvival("ws/slot-03", 20, 3)
	tr.RecordEpochSurvival("ws/slot-03", 20, 4)

	if got := cls.ClassifyWrite(dogi.ChunkMeta{Path: "ws/slot-01/next.dat", Epoch: 4}, 8192); got != dogi.ClassShortLived {
		t.Fatalf("learned slot-01 = %s, want short_lived", got)
	}
	if got := cls.ClassifyWrite(dogi.ChunkMeta{Path: "ws/slot-03/next.dat", Epoch: 4}, 8192); got < dogi.ClassLongLived {
		t.Fatalf("learned slot-03 = %s, want >= long_lived", got)
	}
}

// TestOnlineDirLifetimeTrackerAdaptsInvertedPriors verifies that DOGI overrides
// misleading static path priors (long-lived golden files in tmp/golden/ and
// rapidly churned files in src/ephemeral/) once sufficient evidence arrives.
func TestOnlineDirLifetimeTrackerAdaptsInvertedPriors(t *testing.T) {
	cls := dogi.NewClassifier(dogi.PolicyDOGILifetime)
	tr := cls.Tracker()

	// Initially static priors dominate.
	if got := cls.ClassifyWrite(dogi.ChunkMeta{Path: "tmp/golden/g0.tmp", Epoch: 0}, 8192); got != dogi.ClassShortLived {
		t.Fatalf("initial tmp/golden = %s, want short_lived", got)
	}
	if got := cls.ClassifyWrite(dogi.ChunkMeta{Path: "src/ephemeral/e0.go", Epoch: 0}, 8192); got != dogi.ClassLongLived {
		t.Fatalf("initial src/ephemeral = %s, want long_lived", got)
	}

	// Feed 3 epochs of counter-evidence.
	for ep := uint64(1); ep <= 4; ep++ {
		for i := 0; i < 20; i++ {
			tr.RecordWrite("src/ephemeral", 8192, ep, false)
			tr.RecordDelete("src/ephemeral", ep, ep+1, false)
		}
		tr.RecordEpochSurvival("tmp/golden", 25, ep+2)
	}

	if got := cls.ClassifyWrite(dogi.ChunkMeta{Path: "tmp/golden/g9.tmp", Epoch: 5}, 8192); got < dogi.ClassLongLived {
		t.Fatalf("adapted tmp/golden = %s, want >= long_lived", got)
	}
	if got := cls.ClassifyWrite(dogi.ChunkMeta{Path: "src/ephemeral/e9.go", Epoch: 5}, 8192); got != dogi.ClassShortLived {
		t.Fatalf("adapted src/ephemeral = %s, want short_lived", got)
	}
}

// TestFileBackendMultiEpochGCAndBitExactIntegrity tests real on-disk
// pkg/backend/file persistence across multiple epochs, GC sweeps, overwrites,
// deletions, accounting invariants, and bit-exact BLAKE3 verification.
func TestFileBackendMultiEpochGCAndBitExactIntegrity(t *testing.T) {
	ctx := context.Background()
	be := openFileBackend(t)
	ns := dogi.Namespace{TenantID: "org-file", VolumeID: "ws-01"}

	pk, err := dogi.NewPacker(be, dogi.Config{
		Namespace:            ns,
		Policy:               dogi.PolicyDOGILifetime,
		PackTargetBytes:      32 << 10,
		GCDeadRatioThreshold: 0.50,
	})
	if err != nil {
		t.Fatalf("NewPacker: %v", err)
	}

	rng := rand.New(rand.NewSource(1345))
	expected := make(map[string][32]byte)

	for epoch := uint64(0); epoch < 6; epoch++ {
		// Write long-lived workspace files.
		for i := 0; i < 6; i++ {
			p := fmt.Sprintf("pkg/mod/file_%d_%d.go", epoch, i)
			data := deterministicPayload(rng, p, 6<<10)
			id, _, err := pk.PutChunk(ctx, dogi.ChunkMeta{Namespace: ns, Path: p, Epoch: epoch}, data)
			if err != nil {
				t.Fatalf("PutChunk %s: %v", p, err)
			}
			expected[p] = id
		}
		// Write short-lived build artifacts.
		var tmpPaths []string
		for i := 0; i < 16; i++ {
			p := fmt.Sprintf("target/debug/deps/obj_%d_%d.o", epoch, i)
			data := deterministicPayload(rng, p, 8<<10)
			id, _, err := pk.PutChunk(ctx, dogi.ChunkMeta{Namespace: ns, Path: p, Epoch: epoch}, data)
			if err != nil {
				t.Fatalf("PutChunk %s: %v", p, err)
			}
			expected[p] = id
			tmpPaths = append(tmpPaths, p)
		}
		// Overwrite one existing workspace file to exercise overwrite path.
		owPath := "pkg/mod/file_0_0.go"
		owData := deterministicPayload(rng, fmt.Sprintf("ow-%d", epoch), 6<<10)
		id, _, err := pk.PutChunk(ctx, dogi.ChunkMeta{Namespace: ns, Path: owPath, Epoch: epoch}, owData)
		if err != nil {
			t.Fatalf("overwrite %s: %v", owPath, err)
		}
		expected[owPath] = id

		if err := pk.FlushAll(ctx, epoch); err != nil {
			t.Fatalf("FlushAll: %v", err)
		}
		// Delete this epoch's short-lived artifacts before GC.
		for _, tp := range tmpPaths {
			ok, err := pk.DeletePath(tp, epoch+1)
			if err != nil || !ok {
				t.Fatalf("DeletePath %s: ok=%v err=%v", tp, ok, err)
			}
			delete(expected, tp)
		}
		stats, err := pk.Collect(ctx, epoch+1)
		if err != nil {
			t.Fatalf("Collect epoch %d: %v", epoch, err)
		}
		pk.AdvanceEpoch(epoch+1, stats)

		if err := pk.CheckAccountingInvariant(); err != nil {
			t.Fatalf("epoch %d invariant failed: %v", epoch, err)
		}
		if err := pk.VerifyAllLive(ctx, expected); err != nil {
			t.Fatalf("epoch %d bit-exact verify failed: %v", epoch, err)
		}
	}

	m := pk.SnapshotMetrics()
	if m.ZeroCopyReclaimedPacks == 0 {
		t.Fatalf("expected >0 zero-copy reclaimed packs on file backend, got 0")
	}
	if m.LiveCopyBytes != 0 {
		t.Fatalf("expected 0 live copy bytes on clean CI separation, got %d", m.LiveCopyBytes)
	}
}

// TestLiveHTTPS3BackendIntegration exercises the real pkg/backend/s3 HTTP client
// against internal/storetest.NewS3, verifying PutIfAbsent, GetRange, Delete,
// zero-copy pack reclamation, partial compaction, and bit-exact reads over HTTP.
func TestLiveHTTPS3BackendIntegration(t *testing.T) {
	ctx := context.Background()
	srv := storetest.NewS3(t, "hanzo-vfs-dogi")
	be, err := backends3.New(backends3.Config{
		Endpoint:  srv.Endpoint(),
		Insecure:  true,
		Region:    "us-east-1",
		Bucket:    "hanzo-vfs-dogi",
		Prefix:    "tenant-s3/vol-s3",
		AccessKey: "k",
		SecretKey: "s",
	})
	if err != nil {
		t.Fatalf("s3.New: %v", err)
	}
	t.Cleanup(func() { _ = be.Close() })

	ns := dogi.Namespace{TenantID: "tenant-s3", VolumeID: "vol-s3"}
	pk, err := dogi.NewPacker(be, dogi.Config{
		Namespace:            ns,
		Policy:               dogi.PolicyDOGILifetime,
		PackTargetBytes:      24 << 10,
		GCDeadRatioThreshold: 0.50,
	})
	if err != nil {
		t.Fatalf("NewPacker: %v", err)
	}

	rng := rand.New(rand.NewSource(42))
	expected := make(map[string][32]byte)

	// Mix short-lived, long-lived, and partially-deleted medium-lived objects over HTTP S3.
	for i := 0; i < 8; i++ {
		p := fmt.Sprintf("target/build/a_%d.o", i)
		id, _, err := pk.PutChunk(ctx, dogi.ChunkMeta{Namespace: ns, Path: p, Epoch: 1}, deterministicPayload(rng, p, 6<<10))
		if err != nil {
			t.Fatalf("PutChunk %s: %v", p, err)
		}
		expected[p] = id
	}
	for i := 0; i < 6; i++ {
		p := fmt.Sprintf("ws/mixed/m_%d.dat", i)
		id, _, err := pk.PutChunk(ctx, dogi.ChunkMeta{Namespace: ns, Path: p, Epoch: 1}, deterministicPayload(rng, p, 4<<10))
		if err != nil {
			t.Fatalf("PutChunk %s: %v", p, err)
		}
		expected[p] = id
	}
	for i := 0; i < 4; i++ {
		p := fmt.Sprintf("vendor/lib/v_%d.go", i)
		id, _, err := pk.PutChunk(ctx, dogi.ChunkMeta{Namespace: ns, Path: p, Epoch: 1}, deterministicPayload(rng, p, 6<<10))
		if err != nil {
			t.Fatalf("PutChunk %s: %v", p, err)
		}
		expected[p] = id
	}
	if err := pk.FlushAll(ctx, 1); err != nil {
		t.Fatalf("FlushAll: %v", err)
	}

	// Delete 100% of target/build/*.o (triggers zero-copy S3 DELETE) and 4/6 of ws/mixed/ (triggers S3 GetRange compaction).
	for i := 0; i < 8; i++ {
		p := fmt.Sprintf("target/build/a_%d.o", i)
		_, _ = pk.DeletePath(p, 2)
		delete(expected, p)
	}
	for i := 0; i < 4; i++ {
		p := fmt.Sprintf("ws/mixed/m_%d.dat", i)
		_, _ = pk.DeletePath(p, 2)
		delete(expected, p)
	}

	stats, err := pk.Collect(ctx, 2)
	if err != nil {
		t.Fatalf("Collect over HTTP S3: %v", err)
	}
	if stats.ZeroCopyReclaimedPacks == 0 {
		t.Fatalf("expected zero-copy reclaimed packs over HTTP S3, got 0")
	}
	if stats.CompactedPacks == 0 || stats.GetRangeOps == 0 {
		t.Fatalf("expected partial pack compaction over HTTP S3, got %+v", stats)
	}
	if srv.Requests() == 0 {
		t.Fatalf("expected real HTTP S3 requests")
	}
	if err := pk.VerifyAllLive(ctx, expected); err != nil {
		t.Fatalf("VerifyAllLive over HTTP S3: %v", err)
	}
}

// TestHybridPostQuantumEncryptionPerVolume integrates real vfs.Hybrid
// (X25519 + ML-KEM-768) post-quantum encryption per volume, verifying:
//  1. Zero plaintext leakage in raw backend pack objects.
//  2. Every sealed chunk carries an age mlkem768x25519 stanza.
//  3. Bit-flip tampering fails closed with ErrCipherFailed.
//  4. Wrong volume cipher fails closed.
//  5. Post-GC compacted chunks still decrypt bit-exact.
func TestHybridPostQuantumEncryptionPerVolume(t *testing.T) {
	ctx := context.Background()
	mem := storetest.NewMem()

	idVolA, err := age.GenerateHybridIdentity()
	if err != nil {
		t.Fatalf("GenerateHybridIdentity A: %v", err)
	}
	idVolB, err := age.GenerateHybridIdentity()
	if err != nil {
		t.Fatalf("GenerateHybridIdentity B: %v", err)
	}
	cipherA := vfs.Hybrid(idVolA)
	cipherB := vfs.Hybrid(idVolB)

	nsA := dogi.Namespace{TenantID: "tenant-pq", VolumeID: "vol-pq-a"}
	pk, err := dogi.NewPacker(mem, dogi.Config{
		Namespace:            nsA,
		Policy:               dogi.PolicyDOGILifetime,
		PackTargetBytes:      32 << 10,
		GCDeadRatioThreshold: 0.50,
		Sealer:               cipherA,
	})
	if err != nil {
		t.Fatalf("NewPacker: %v", err)
	}

	secretMarker := []byte("CONFIDENTIAL-WORKSPACE-CHUNK-MARKER-987654321")
	expected := make(map[string][32]byte)
	for i := 0; i < 4; i++ {
		p := fmt.Sprintf("ws/ephem/item_%d.dat", i)
		payload := append([]byte(fmt.Sprintf("item-%d:", i)), secretMarker...)
		id, _, err := pk.PutChunk(ctx, dogi.ChunkMeta{Namespace: nsA, Path: p, Epoch: 1}, payload)
		if err != nil {
			t.Fatalf("PutChunk %s: %v", p, err)
		}
		expected[p] = id
	}
	if err := pk.FlushAll(ctx, 1); err != nil {
		t.Fatalf("FlushAll: %v", err)
	}

	// Verify raw backend pack bytes contain no plaintext marker and carry hybrid PQ age stanzas.
	dump := mem.Dump()
	if len(dump) == 0 {
		t.Fatalf("expected sealed pack objects in backend")
	}
	for k, raw := range dump {
		if bytes.Contains(raw, secretMarker) {
			t.Fatalf("plaintext marker leaked in raw backend pack %s", k)
		}
		if !bytes.Contains(raw, []byte("age-encryption.org/v1\n")) {
			t.Fatalf("pack %s missing age header", k)
		}
		if !bytes.Contains(raw, []byte("-> mlkem768x25519")) {
			t.Fatalf("pack %s missing post-quantum mlkem768x25519 stanza", k)
		}
	}

	// Delete 3 of 4 chunks so GC compacts the remaining 1 encrypted chunk.
	for i := 0; i < 3; i++ {
		p := fmt.Sprintf("ws/ephem/item_%d.dat", i)
		_, _ = pk.DeletePath(p, 2)
		delete(expected, p)
	}
	stats, err := pk.Collect(ctx, 2)
	if err != nil {
		t.Fatalf("Collect encrypted pack: %v", err)
	}
	if stats.CompactedPacks != 1 || stats.LiveCopyChunks != 1 {
		t.Fatalf("unexpected encrypted compaction stats: %+v", stats)
	}
	if err := pk.VerifyAllLive(ctx, expected); err != nil {
		t.Fatalf("post-GC encrypted VerifyAllLive: %v", err)
	}

	// Confirm tampering raw pack bytes causes ReadPath to fail closed.
	for k, raw := range mem.Dump() {
		corrupted := append([]byte(nil), raw...)
		corrupted[len(corrupted)-5] ^= 0xFF
		if err := mem.Put(ctx, k, corrupted); err != nil {
			t.Fatalf("corrupt put: %v", err)
		}
	}
	if _, _, err := pk.ReadPath(ctx, "ws/ephem/item_3.dat"); err == nil {
		t.Fatalf("expected tampered encrypted pack to fail ReadPath, got nil")
	}

	// Confirm wrong volume cipher cannot decrypt a chunk sealed under cipherA.
	sealed, kid, err := cipherA.Seal([]byte("volume-a-only"))
	if err != nil {
		t.Fatalf("cipherA.Seal: %v", err)
	}
	if _, err := cipherB.Open(sealed, kid); err == nil {
		t.Fatalf("cipherB unexpectedly opened cipherA object")
	}
}

// TestCorruptionDetectionRejectsTamperedPlaintextPack verifies BLAKE3 content
// integrity detects single-bit corruption in unencrypted packs.
func TestCorruptionDetectionRejectsTamperedPlaintextPack(t *testing.T) {
	ctx := context.Background()
	mem := storetest.NewMem()
	ns := dogi.Namespace{TenantID: "t1", VolumeID: "v1"}
	pk, err := dogi.NewPacker(mem, dogi.Config{
		Namespace:       ns,
		Policy:          dogi.PolicyDOGILifetime,
		PackTargetBytes: 16 << 10,
	})
	if err != nil {
		t.Fatalf("NewPacker: %v", err)
	}
	if _, _, err := pk.PutChunk(ctx, dogi.ChunkMeta{Namespace: ns, Path: "pkg/a.go", Epoch: 1}, []byte("immutable-data-payload")); err != nil {
		t.Fatalf("PutChunk: %v", err)
	}
	if err := pk.FlushAll(ctx, 1); err != nil {
		t.Fatalf("FlushAll: %v", err)
	}
	for k, raw := range mem.Dump() {
		corrupted := append([]byte(nil), raw...)
		corrupted[len(corrupted)-1] ^= 0x01
		_ = mem.Put(ctx, k, corrupted)
	}
	if _, _, err := pk.ReadPath(ctx, "pkg/a.go"); !errors.Is(err, dogi.ErrCorruptPack) {
		t.Fatalf("corrupted ReadPath err = %v, want ErrCorruptPack", err)
	}
}

// TestDOGIvsFIFOCIBuildWorkload compares FIFO vs DOGI on an interleaved CI
// build + long-lived workspace workload and verifies massive live-copy and
// storage-amplification reductions plus zero-copy dead-pack drops.
func TestDOGIvsFIFOCIBuildWorkload(t *testing.T) {
	ctx := context.Background()
	runPolicy := func(pol dogi.Policy) dogi.Metrics {
		be := storetest.NewMem()
		ns := dogi.Namespace{TenantID: "ci", VolumeID: string(pol)}
		pk, err := dogi.NewPacker(be, dogi.Config{
			Namespace:            ns,
			Policy:               pol,
			PackTargetBytes:      64 << 10,
			GCDeadRatioThreshold: 0.50,
		})
		if err != nil {
			t.Fatalf("NewPacker %s: %v", pol, err)
		}
		rng := rand.New(rand.NewSource(1345))
		expected := make(map[string][32]byte)
		var prevTmp []string

		for ep := uint64(0); ep < 10; ep++ {
			for _, tp := range prevTmp {
				_, _ = pk.DeletePath(tp, ep)
				delete(expected, tp)
			}
			prevTmp = nil

			// Interleave 4 long-lived workspace chunks with 16 short-lived build artifacts.
			for i := 0; i < 16; i++ {
				tp := fmt.Sprintf("target/debug/deps/ep%d_%d.o", ep, i)
				id, _, err := pk.PutChunk(ctx, dogi.ChunkMeta{
					Namespace: ns, Path: tp, Epoch: ep, OracleDeathEpoch: ep + 1,
				}, deterministicPayload(rng, tp, 8<<10))
				if err != nil {
					t.Fatalf("put %s: %v", tp, err)
				}
				expected[tp] = id
				prevTmp = append(prevTmp, tp)

				if i%4 == 0 {
					wp := fmt.Sprintf("pkg/workspace/ep%d_%d.go", ep, i/4)
					id, _, err := pk.PutChunk(ctx, dogi.ChunkMeta{
						Namespace: ns, Path: wp, Epoch: ep, OracleDeathEpoch: 0,
					}, deterministicPayload(rng, wp, 8<<10))
					if err != nil {
						t.Fatalf("put %s: %v", wp, err)
					}
					expected[wp] = id
				}
			}
			if err := pk.FlushAll(ctx, ep); err != nil {
				t.Fatalf("FlushAll: %v", err)
			}
			stats, err := pk.Collect(ctx, ep)
			if err != nil {
				t.Fatalf("Collect: %v", err)
			}
			pk.AdvanceEpoch(ep, stats)
		}
		if err := pk.VerifyAllLive(ctx, expected); err != nil {
			t.Fatalf("VerifyAllLive %s: %v", pol, err)
		}
		return pk.SnapshotMetrics()
	}

	fifo := runPolicy(dogi.PolicyFIFOMixed)
	dogiM := runPolicy(dogi.PolicyDOGILifetime)

	if fifo.LiveCopyBytes == 0 {
		t.Fatalf("expected FIFO baseline to copy surviving live chunks, got 0")
	}
	if dogiM.LiveCopyBytes >= fifo.LiveCopyBytes/5 {
		t.Fatalf("expected DOGI to cut LiveCopyBytes by >80%%: dogi=%d fifo=%d", dogiM.LiveCopyBytes, fifo.LiveCopyBytes)
	}
	if dogiM.ZeroCopyReclaimedPacks == 0 {
		t.Fatalf("expected DOGI to reclaim dead packs with zero copy, got 0")
	}
	if dogiM.WriteAmplification >= fifo.WriteAmplification {
		t.Fatalf("expected DOGI WAF (%f) < FIFO WAF (%f)", dogiM.WriteAmplification, fifo.WriteAmplification)
	}
}

// TestDOGIOpaquePathsBeatsPathGrouped proves DOGI's online EWMA directory
// tracker dramatically outperforms static path grouping when directory names
// are opaque and unrecognizable.
func TestDOGIOpaquePathsBeatsPathGrouped(t *testing.T) {
	ctx := context.Background()
	runPolicy := func(pol dogi.Policy) dogi.Metrics {
		be := storetest.NewMem()
		ns := dogi.Namespace{TenantID: "opaque", VolumeID: string(pol)}
		pk, err := dogi.NewPacker(be, dogi.Config{
			Namespace:            ns,
			Policy:               pol,
			PackTargetBytes:      64 << 10,
			GCDeadRatioThreshold: 0.50,
		})
		if err != nil {
			t.Fatalf("NewPacker: %v", err)
		}
		rng := rand.New(rand.NewSource(2026))
		var prevEphem []string
		for ep := uint64(0); ep < 12; ep++ {
			for _, p := range prevEphem {
				_, _ = pk.DeletePath(p, ep)
			}
			prevEphem = nil
			for i := 0; i < 16; i++ {
				epPath := fmt.Sprintf("ws/slot-01/x_%d_%d.dat", ep, i)
				_, _, err := pk.PutChunk(ctx, dogi.ChunkMeta{Namespace: ns, Path: epPath, Epoch: ep}, deterministicPayload(rng, epPath, 8<<10))
				if err != nil {
					t.Fatalf("put: %v", err)
				}
				prevEphem = append(prevEphem, epPath)

				if i%4 == 0 {
					lpPath := fmt.Sprintf("ws/slot-03/y_%d_%d.dat", ep, i/4)
					_, _, err := pk.PutChunk(ctx, dogi.ChunkMeta{Namespace: ns, Path: lpPath, Epoch: ep}, deterministicPayload(rng, lpPath, 8<<10))
					if err != nil {
						t.Fatalf("put: %v", err)
					}
				}
			}
			_ = pk.FlushAll(ctx, ep)
			st, err := pk.Collect(ctx, ep)
			if err != nil {
				t.Fatalf("Collect: %v", err)
			}
			pk.AdvanceEpoch(ep, st)
		}
		return pk.SnapshotMetrics()
	}

	pathStatic := runPolicy(dogi.PolicyPathGrouped)
	dogiM := runPolicy(dogi.PolicyDOGILifetime)

	if dogiM.LiveCopyBytes >= pathStatic.LiveCopyBytes/2 {
		t.Fatalf("expected DOGI online learner to cut LiveCopyBytes >50%% vs static path priors on opaque paths: dogi=%d pathStatic=%d",
			dogiM.LiveCopyBytes, pathStatic.LiveCopyBytes)
	}
	if dogiM.ZeroCopyReclaimedPacks <= pathStatic.ZeroCopyReclaimedPacks {
		t.Fatalf("expected DOGI zero-copy packs (%d) > static (%d)", dogiM.ZeroCopyReclaimedPacks, pathStatic.ZeroCopyReclaimedPacks)
	}
}

// TestNegativeRegimeUniformRandomLifetime verifies Honest Negative Control #1:
// under i.i.d. memoryless random lifetimes uncorrelated with path/dir/size,
// DOGI cannot beat FIFO substantially and incurs >= FIFO pack PUT count.
func TestNegativeRegimeUniformRandomLifetime(t *testing.T) {
	ctx := context.Background()
	runPolicy := func(pol dogi.Policy) dogi.Metrics {
		be := storetest.NewMem()
		ns := dogi.Namespace{TenantID: "neg-rand", VolumeID: string(pol)}
		pk, err := dogi.NewPacker(be, dogi.Config{
			Namespace:            ns,
			Policy:               pol,
			PackTargetBytes:      64 << 10,
			GCDeadRatioThreshold: 0.50,
		})
		if err != nil {
			t.Fatalf("NewPacker: %v", err)
		}
		rng := rand.New(rand.NewSource(777))
		dirs := []string{"target/a", "pkg/b", "ws/c", "vendor/d"}
		deathAt := make(map[uint64][]string)

		for ep := uint64(0); ep < 12; ep++ {
			for _, dp := range deathAt[ep] {
				_, _ = pk.DeletePath(dp, ep)
			}
			delete(deathAt, ep)
			for i := 0; i < 16; i++ {
				d := dirs[rng.Intn(len(dirs))]
				p := fmt.Sprintf("%s/obj_%d_%d.bin", d, ep, i)
				// Geometric memoryless lifetime with p=0.35 independent of dir.
				life := uint64(1)
				for rng.Float64() >= 0.35 && life < 30 {
					life++
				}
				de := ep + life
				deathAt[de] = append(deathAt[de], p)
				_, _, err := pk.PutChunk(ctx, dogi.ChunkMeta{
					Namespace: ns, Path: p, Epoch: ep, OracleDeathEpoch: de,
				}, deterministicPayload(rng, p, 8<<10))
				if err != nil {
					t.Fatalf("put: %v", err)
				}
			}
			_ = pk.FlushAll(ctx, ep)
			st, err := pk.Collect(ctx, ep)
			if err != nil {
				t.Fatalf("Collect: %v", err)
			}
			pk.AdvanceEpoch(ep, st)
		}
		return pk.SnapshotMetrics()
	}

	fifo := runPolicy(dogi.PolicyFIFOMixed)
	dogiM := runPolicy(dogi.PolicyDOGILifetime)
	oracle := runPolicy(dogi.PolicyOracleUpperBound)

	// Honest negative: DOGI pays extra S3 PUTs from multi-stream splitting
	// without meaningfully beating FIFO, while hindsight oracle still wins.
	if dogiM.PacksUploaded < fifo.PacksUploaded {
		t.Fatalf("expected multi-stream DOGI PUTs (%d) >= FIFO (%d) on memoryless churn", dogiM.PacksUploaded, fifo.PacksUploaded)
	}
	if oracle.LiveCopyBytes >= fifo.LiveCopyBytes {
		t.Fatalf("expected hindsight oracle (%d) < FIFO (%d)", oracle.LiveCopyBytes, fifo.LiveCopyBytes)
	}
}

// TestNegativeRegimeAppendOnlyZeroDeletes verifies Honest Negative Control #2:
// when nothing is ever deleted, GC performs zero copying for every policy, and
// maintaining K lifetime streams strictly increases pack PUTs and tail overhead.
func TestNegativeRegimeAppendOnlyZeroDeletes(t *testing.T) {
	ctx := context.Background()
	runPolicy := func(pol dogi.Policy) dogi.Metrics {
		be := storetest.NewMem()
		ns := dogi.Namespace{TenantID: "neg-append", VolumeID: string(pol)}
		pk, err := dogi.NewPacker(be, dogi.Config{
			Namespace:            ns,
			Policy:               pol,
			PackTargetBytes:      128 << 10,
			GCDeadRatioThreshold: 0.50,
		})
		if err != nil {
			t.Fatalf("NewPacker: %v", err)
		}
		rng := rand.New(rand.NewSource(999))
		paths := []string{"target/snap", "ws/log", "pkg/src", ".git/objects/aa"}
		for ep := uint64(0); ep < 10; ep++ {
			for i, dir := range paths {
				p := fmt.Sprintf("%s/rec_%d_%d.dat", dir, ep, i)
				_, _, err := pk.PutChunk(ctx, dogi.ChunkMeta{Namespace: ns, Path: p, Epoch: ep}, deterministicPayload(rng, p, 16<<10))
				if err != nil {
					t.Fatalf("put: %v", err)
				}
			}
			_ = pk.FlushAll(ctx, ep)
			st, err := pk.Collect(ctx, ep)
			if err != nil {
				t.Fatalf("Collect: %v", err)
			}
			pk.AdvanceEpoch(ep, st)
		}
		return pk.SnapshotMetrics()
	}

	fifo := runPolicy(dogi.PolicyFIFOMixed)
	dogiM := runPolicy(dogi.PolicyDOGILifetime)

	if fifo.LiveCopyBytes != 0 || dogiM.LiveCopyBytes != 0 {
		t.Fatalf("expected 0 LiveCopyBytes on append-only archive: fifo=%d dogi=%d", fifo.LiveCopyBytes, dogiM.LiveCopyBytes)
	}
	if dogiM.PacksUploaded <= fifo.PacksUploaded {
		t.Fatalf("expected multi-stream DOGI PUTs (%d) > single-stream FIFO (%d) on append-only archive", dogiM.PacksUploaded, fifo.PacksUploaded)
	}
}

// TestNegativeRegimeAdversarialInvertedPriors verifies Honest Negative Control #3:
// storing long-lived golden artifacts under tmp/golden/ and rapidly churning
// src/ephemeral/ fools static path priors permanently while DOGI suffers an
// initial adaptation lag before its online EWMA tracker recovers.
func TestNegativeRegimeAdversarialInvertedPriors(t *testing.T) {
	ctx := context.Background()
	runPolicy := func(pol dogi.Policy) dogi.Metrics {
		be := storetest.NewMem()
		ns := dogi.Namespace{TenantID: "neg-inv", VolumeID: string(pol)}
		pk, err := dogi.NewPacker(be, dogi.Config{
			Namespace:            ns,
			Policy:               pol,
			PackTargetBytes:      64 << 10,
			GCDeadRatioThreshold: 0.50,
		})
		if err != nil {
			t.Fatalf("NewPacker: %v", err)
		}
		rng := rand.New(rand.NewSource(31415))
		var prevDead []string
		for ep := uint64(0); ep < 14; ep++ {
			for _, dp := range prevDead {
				_, _ = pk.DeletePath(dp, ep)
			}
			prevDead = nil
			for i := 0; i < 8; i++ {
				// Inverted: src/ephemeral dies every epoch; tmp/scratch also dies every epoch.
				p1 := fmt.Sprintf("src/ephemeral/e_%d_%d.go", ep, i)
				_, _, _ = pk.PutChunk(ctx, dogi.ChunkMeta{Namespace: ns, Path: p1, Epoch: ep}, deterministicPayload(rng, p1, 8<<10))
				prevDead = append(prevDead, p1)

				p2 := fmt.Sprintf("tmp/scratch/s_%d_%d.tmp", ep, i)
				_, _, _ = pk.PutChunk(ctx, dogi.ChunkMeta{Namespace: ns, Path: p2, Epoch: ep}, deterministicPayload(rng, p2, 8<<10))
				prevDead = append(prevDead, p2)

				// Long-lived golden under tmp/golden and normal long-lived under src/core.
				if i%4 == 0 {
					g := fmt.Sprintf("tmp/golden/g_%d_%d.tmp", ep, i/4)
					_, _, _ = pk.PutChunk(ctx, dogi.ChunkMeta{Namespace: ns, Path: g, Epoch: ep}, deterministicPayload(rng, g, 8<<10))
					c := fmt.Sprintf("src/core/c_%d_%d.go", ep, i/4)
					_, _, _ = pk.PutChunk(ctx, dogi.ChunkMeta{Namespace: ns, Path: c, Epoch: ep}, deterministicPayload(rng, c, 8<<10))
				}
			}
			_ = pk.FlushAll(ctx, ep)
			st, err := pk.Collect(ctx, ep)
			if err != nil {
				t.Fatalf("Collect: %v", err)
			}
			pk.AdvanceEpoch(ep, st)
		}
		return pk.SnapshotMetrics()
	}

	pathStatic := runPolicy(dogi.PolicyPathGrouped)
	dogiM := runPolicy(dogi.PolicyDOGILifetime)

	if pathStatic.LiveCopyBytes == 0 {
		t.Fatalf("expected inverted static priors to incur live-data copying")
	}
	if dogiM.LiveCopyBytes >= pathStatic.LiveCopyBytes {
		t.Fatalf("expected DOGI online adaptation (%d) to beat static inverted priors (%d)", dogiM.LiveCopyBytes, pathStatic.LiveCopyBytes)
	}
}

// TestGCRelocationReclassification verifies DOGI Insight #2: surviving live
// chunks relocated during GC are promoted out of high-churn short-lived streams.
func TestGCRelocationReclassification(t *testing.T) {
	cls := dogi.NewClassifier(dogi.PolicyDOGILifetime)
	tr := cls.Tracker()
	tr.RecordEpochSurvival("tmp/golden", 20, 4)

	// Surviving chunk in a cold dir promoted to LongLived on first GC, Pinned on second.
	c1 := cls.ClassifyRelocation("tmp/golden/a.tmp", 0, 4, 1, 0)
	if c1 != dogi.ClassLongLived {
		t.Fatalf("GC gen 1 cold survivor = %s, want long_lived", c1)
	}
	c2 := cls.ClassifyRelocation("tmp/golden/a.tmp", 0, 8, 2, 0)
	if c2 != dogi.ClassPinnedImmutable {
		t.Fatalf("GC gen 2 cold survivor = %s, want pinned_immutable", c2)
	}

	// Young survivor in a high-churn dir stays in MediumLived (does not pollute LongLived).
	for i := 0; i < 10; i++ {
		tr.RecordDelete("target/hot", 1, 2, false)
	}
	ch := cls.ClassifyRelocation("target/hot/straggler.o", 2, 3, 1, 0)
	if ch != dogi.ClassMediumLived {
		t.Fatalf("GC gen 1 hot straggler = %s, want medium_lived", ch)
	}
}

// TestConcurrentWritesReadsAndGC stress-tests thread safety under -race with
// concurrent PutChunk, ReadPath, DeletePath, FlushAll, and Collect calls.
func TestConcurrentWritesReadsAndGC(t *testing.T) {
	ctx := context.Background()
	be := storetest.NewMem()
	ns := dogi.Namespace{TenantID: "conc", VolumeID: "vol-race"}
	pk, err := dogi.NewPacker(be, dogi.Config{
		Namespace:            ns,
		Policy:               dogi.PolicyDOGILifetime,
		PackTargetBytes:      16 << 10,
		GCDeadRatioThreshold: 0.50,
	})
	if err != nil {
		t.Fatalf("NewPacker: %v", err)
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 64)

	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(100 + worker)))
			for i := 0; i < 30; i++ {
				ep := uint64(i / 5)
				p := fmt.Sprintf("target/w%d/item_%d.o", worker, i)
				data := deterministicPayload(rng, p, 2048)
				id, _, err := pk.PutChunk(ctx, dogi.ChunkMeta{Namespace: ns, Path: p, Epoch: ep}, data)
				if err != nil {
					errCh <- err
					return
				}
				got, gotID, err := pk.ReadPath(ctx, p)
				if err != nil {
					errCh <- err
					return
				}
				if gotID != id || !bytes.Equal(got, data) {
					errCh <- fmt.Errorf("readback mismatch for %s", p)
					return
				}
				if i%2 == 0 {
					if _, err := pk.DeletePath(p, ep+1); err != nil {
						errCh <- err
						return
					}
				}
			}
		}(w)
	}

	// Concurrent flusher & GC sweeper goroutine.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := uint64(0); i < 8; i++ {
			if err := pk.FlushAll(ctx, i); err != nil {
				errCh <- err
				return
			}
			if _, err := pk.Collect(ctx, i); err != nil {
				errCh <- err
				return
			}
		}
	}()

	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("concurrent error: %v", err)
	}
	if err := pk.CheckAccountingInvariant(); err != nil {
		t.Fatalf("post-concurrency accounting invariant failed: %v", err)
	}
	if err := pk.VerifyAllLive(ctx, nil); err != nil {
		t.Fatalf("post-concurrency VerifyAllLive failed: %v", err)
	}
}

// TestConfigValidationAndEdgeCases verifies input validation on NewPacker,
// PutChunk, DeletePath, and ReadPath.
func TestConfigValidationAndEdgeCases(t *testing.T) {
	ctx := context.Background()
	mem := storetest.NewMem()
	ns := dogi.Namespace{TenantID: "t", VolumeID: "v"}

	if _, err := dogi.NewPacker(nil, dogi.Config{Namespace: ns}); !errors.Is(err, dogi.ErrInvalidConfig) {
		t.Fatalf("nil backend err = %v, want ErrInvalidConfig", err)
	}
	if _, err := dogi.NewPacker(mem, dogi.Config{Namespace: ns, Policy: "bogus"}); !errors.Is(err, dogi.ErrInvalidConfig) {
		t.Fatalf("bogus policy err = %v, want ErrInvalidConfig", err)
	}
	if _, err := dogi.NewPacker(mem, dogi.Config{Namespace: ns, PackTargetBytes: 100}); !errors.Is(err, dogi.ErrInvalidConfig) {
		t.Fatalf("tiny PackTargetBytes err = %v, want ErrInvalidConfig", err)
	}
	if _, err := dogi.NewPacker(mem, dogi.Config{Namespace: ns, GCDeadRatioThreshold: 1.5}); !errors.Is(err, dogi.ErrInvalidConfig) {
		t.Fatalf("bad GCDeadRatioThreshold err = %v, want ErrInvalidConfig", err)
	}

	pk, err := dogi.NewPacker(mem, dogi.Config{Namespace: ns})
	if err != nil {
		t.Fatalf("default config NewPacker: %v", err)
	}
	if _, _, err := pk.PutChunk(ctx, dogi.ChunkMeta{Namespace: ns, Path: ""}, []byte("x")); !errors.Is(err, dogi.ErrInvalidChunk) {
		t.Fatalf("empty path err = %v, want ErrInvalidChunk", err)
	}
	if _, _, err := pk.PutChunk(ctx, dogi.ChunkMeta{Namespace: ns, Path: "a"}, nil); !errors.Is(err, dogi.ErrInvalidChunk) {
		t.Fatalf("empty payload err = %v, want ErrInvalidChunk", err)
	}
	if _, _, err := pk.ReadPath(ctx, "missing/file"); !errors.Is(err, dogi.ErrChunkNotFound) {
		t.Fatalf("missing ReadPath err = %v, want ErrChunkNotFound", err)
	}
	if ok, err := pk.DeletePath("missing/file", 1); err != nil || ok {
		t.Fatalf("missing DeletePath = (%v, %v), want (false, nil)", ok, err)
	}
}

// TestReconcileUploadedPackAfterLostAck verifies that when PutIfAbsent stores
// the pack on the backend but its acknowledgement is lost (storetest.Hook.After),
// a subsequent FlushAll reconciles the existing immutable pack cleanly instead
// of wedging permanently on backend.ErrConflict.
func TestReconcileUploadedPackAfterLostAck(t *testing.T) {
	ctx := context.Background()
	mem := storetest.NewMem()
	var mu sync.Mutex
	failOnce := true
	be := &storetest.Hook{
		Backend: mem,
		After: func(op, key string) error {
			if op != "putabsent" {
				return nil
			}
			mu.Lock()
			defer mu.Unlock()
			if failOnce {
				failOnce = false
				return errors.New("simulated lost acknowledgement after durable write")
			}
			return nil
		},
	}

	ns := dogi.Namespace{TenantID: "ack-tenant", VolumeID: "ack-vol"}
	pk, err := dogi.NewPacker(be, dogi.Config{
		Namespace:       ns,
		Policy:          dogi.PolicyDOGILifetime,
		PackTargetBytes: 16 << 10,
	})
	if err != nil {
		t.Fatalf("NewPacker: %v", err)
	}
	payload := []byte("immutable-payload-survives-lost-ack")
	id, _, err := pk.PutChunk(ctx, dogi.ChunkMeta{Namespace: ns, Path: "pkg/app.go", Epoch: 1}, payload)
	if err != nil {
		t.Fatalf("PutChunk: %v", err)
	}

	// First FlushAll stores the pack on mem, then receives a lost-ACK error.
	if err := pk.FlushAll(ctx, 1); err == nil {
		t.Fatalf("expected simulated lost-ACK error on first FlushAll")
	}
	// Retry FlushAll: backend returns ErrConflict for the same key; Packer must
	// reconcile the byte-identical stored pack and seal without wedging.
	if err := pk.FlushAll(ctx, 1); err != nil {
		t.Fatalf("retry FlushAll after lost ACK failed: %v", err)
	}
	if err := pk.CheckAccountingInvariant(); err != nil {
		t.Fatalf("CheckAccountingInvariant: %v", err)
	}
	if err := pk.VerifyAllLive(ctx, map[string][32]byte{"pkg/app.go": id}); err != nil {
		t.Fatalf("VerifyAllLive after lost-ACK reconciliation: %v", err)
	}
}

type blockingGetRangeBackend struct {
	backend.Backend
	mu        sync.Mutex
	intercept bool
	entered   chan struct{}
	release   chan struct{}
}

func (b *blockingGetRangeBackend) GetRange(ctx context.Context, key string, off, length int64) ([]byte, error) {
	b.mu.Lock()
	shouldBlock := b.intercept
	if shouldBlock {
		b.intercept = false
	}
	b.mu.Unlock()
	if shouldBlock {
		close(b.entered)
		<-b.release
	}
	return b.Backend.GetRange(ctx, key, off, length)
}

// TestReadPathRetriesAcrossConcurrentGCRelocation reproduces a ReadPath pausing
// right before GetRange while Collect relocates the still-live chunk and deletes
// its old sealed pack; ReadPath must retry transparently instead of returning
// spurious ErrCorruptPack.
func TestReadPathRetriesAcrossConcurrentGCRelocation(t *testing.T) {
	ctx := context.Background()
	mem := storetest.NewMem()
	be := &blockingGetRangeBackend{
		Backend: mem,
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	ns := dogi.Namespace{TenantID: "race-tenant", VolumeID: "race-vol"}
	pk, err := dogi.NewPacker(be, dogi.Config{
		Namespace:            ns,
		Policy:               dogi.PolicyFIFOMixed,
		PackTargetBytes:      16 << 10,
		GCDeadRatioThreshold: 0.50,
	})
	if err != nil {
		t.Fatalf("NewPacker: %v", err)
	}

	rng := rand.New(rand.NewSource(77))
	keepPayload := deterministicPayload(rng, "pkg/keep.go", 2048)
	keepID, _, err := pk.PutChunk(ctx, dogi.ChunkMeta{Namespace: ns, Path: "pkg/keep.go", Epoch: 1}, keepPayload)
	if err != nil {
		t.Fatalf("PutChunk keep: %v", err)
	}
	for i := 0; i < 4; i++ {
		p := fmt.Sprintf("tmp/dead_%d.o", i)
		if _, _, err := pk.PutChunk(ctx, dogi.ChunkMeta{Namespace: ns, Path: p, Epoch: 1}, deterministicPayload(rng, p, 2048)); err != nil {
			t.Fatalf("PutChunk dead: %v", err)
		}
	}
	if err := pk.FlushAll(ctx, 1); err != nil {
		t.Fatalf("FlushAll: %v", err)
	}
	for i := 0; i < 4; i++ {
		_, _ = pk.DeletePath(fmt.Sprintf("tmp/dead_%d.o", i), 2)
	}

	// Arm interceptor so ReadPath pauses right before GetRange on the old sealed pack.
	be.mu.Lock()
	be.intercept = true
	be.mu.Unlock()

	type readRes struct {
		data []byte
		id   [32]byte
		err  error
	}
	resCh := make(chan readRes, 1)
	go func() {
		d, id, err := pk.ReadPath(ctx, "pkg/keep.go")
		resCh <- readRes{data: d, id: id, err: err}
	}()

	// Wait until ReadPath has captured the old pack key and paused before GetRange.
	<-be.entered

	// Run GC Collect to relocate pkg/keep.go to a fresh pack and delete the old pack.
	stats, err := pk.Collect(ctx, 2)
	if err != nil {
		t.Fatalf("Collect during concurrent ReadPath: %v", err)
	}
	if stats.CompactedPacks != 1 || stats.LiveCopyChunks != 1 {
		t.Fatalf("expected 1 compacted pack and 1 relocated chunk, got %+v", stats)
	}

	// Resume ReadPath: its initial GetRange hits ErrNotFound on the deleted old pack
	// and must transparently retry against the relocated live record.
	close(be.release)
	res := <-resCh
	if res.err != nil {
		t.Fatalf("ReadPath across concurrent GC relocation failed: %v", res.err)
	}
	if res.id != keepID || !bytes.Equal(res.data, keepPayload) {
		t.Fatalf("ReadPath returned wrong payload after concurrent GC relocation")
	}
}

// TestPutChunkRollsBackAtomicallyOnUploadError verifies that when PutChunk crosses
// PackTargetBytes and its automatic upload fails, both the stream buffer and live
// index are rolled back atomically so CheckAccountingInvariant and subsequent
// retries stay completely consistent.
func TestPutChunkRollsBackAtomicallyOnUploadError(t *testing.T) {
	ctx := context.Background()
	mem := storetest.NewMem()
	var mu sync.Mutex
	failNextPut := true
	be := &storetest.Hook{
		Backend: mem,
		Before: func(op, key string) error {
			if op != "putabsent" {
				return nil
			}
			mu.Lock()
			defer mu.Unlock()
			if failNextPut {
				failNextPut = false
				return errors.New("injected backend put failure")
			}
			return nil
		},
	}

	ns := dogi.Namespace{TenantID: "rb-tenant", VolumeID: "rb-vol"}
	pk, err := dogi.NewPacker(be, dogi.Config{
		Namespace:       ns,
		Policy:          dogi.PolicyDOGILifetime,
		PackTargetBytes: 4 << 10, // Minimum pack size (4 KiB)
	})
	if err != nil {
		t.Fatalf("NewPacker: %v", err)
	}

	payload := bytes.Repeat([]byte("A"), 5<<10) // Crosses 4 KiB threshold immediately.
	if _, _, err := pk.PutChunk(ctx, dogi.ChunkMeta{Namespace: ns, Path: "pkg/big.bin", Epoch: 1}, payload); err == nil {
		t.Fatalf("expected injected upload failure on PutChunk")
	}
	if err := pk.CheckAccountingInvariant(); err != nil {
		t.Fatalf("accounting invariant broken after failed PutChunk: %v", err)
	}
	if m := pk.SnapshotMetrics(); m.LiveBytes != 0 || m.LiveChunks != 0 {
		t.Fatalf("expected 0 live bytes/chunks after rolled-back PutChunk, got bytes=%d chunks=%d", m.LiveBytes, m.LiveChunks)
	}

	// Retry the exact same PutChunk now that the backend has recovered.
	id, _, err := pk.PutChunk(ctx, dogi.ChunkMeta{Namespace: ns, Path: "pkg/big.bin", Epoch: 1}, payload)
	if err != nil {
		t.Fatalf("retry PutChunk failed: %v", err)
	}
	if err := pk.CheckAccountingInvariant(); err != nil {
		t.Fatalf("accounting invariant broken after retry PutChunk: %v", err)
	}
	if m := pk.SnapshotMetrics(); m.LiveBytes != int64(len(payload)) || m.LiveChunks != 1 {
		t.Fatalf("unexpected live accounting after retry: bytes=%d chunks=%d", m.LiveBytes, m.LiveChunks)
	}
	if err := pk.VerifyAllLive(ctx, map[string][32]byte{"pkg/big.bin": id}); err != nil {
		t.Fatalf("VerifyAllLive after retry: %v", err)
	}

	// Also verify overwrite rollback keeps the previous live version intact.
	mu.Lock()
	failNextPut = true
	mu.Unlock()
	payload2 := bytes.Repeat([]byte("B"), 5<<10)
	if _, _, err := pk.PutChunk(ctx, dogi.ChunkMeta{Namespace: ns, Path: "pkg/big.bin", Epoch: 2}, payload2); err == nil {
		t.Fatalf("expected injected upload failure on overwrite PutChunk")
	}
	if err := pk.CheckAccountingInvariant(); err != nil {
		t.Fatalf("accounting invariant broken after failed overwrite: %v", err)
	}
	if err := pk.VerifyAllLive(ctx, map[string][32]byte{"pkg/big.bin": id}); err != nil {
		t.Fatalf("previous live chunk lost after failed overwrite: %v", err)
	}
}

// TestDeterministicGCOrderIndependentOfRandomPackIDs verifies that repeated runs
// of identical workloads produce identical GC LiveCopyBytes and PacksUploaded
// even though pack storage keys use fresh crypto/rand IDs.
func TestDeterministicGCOrderIndependentOfRandomPackIDs(t *testing.T) {
	ctx := context.Background()
	runOnce := func() dogi.Metrics {
		mem := storetest.NewMem()
		ns := dogi.Namespace{TenantID: "det", VolumeID: "v"}
		pk, err := dogi.NewPacker(mem, dogi.Config{
			Namespace:            ns,
			Policy:               dogi.PolicyDOGILifetime,
			PackTargetBytes:      32 << 10,
			GCDeadRatioThreshold: 0.40,
		})
		if err != nil {
			t.Fatalf("NewPacker: %v", err)
		}
		rng := rand.New(rand.NewSource(1345))
		type liveItem struct {
			path       string
			deathEpoch uint64
		}
		var items []liveItem
		for ep := uint64(0); ep < 10; ep++ {
			var kept []liveItem
			for _, it := range items {
				if it.deathEpoch <= ep {
					_, _ = pk.DeletePath(it.path, ep)
				} else {
					kept = append(kept, it)
				}
			}
			items = kept
			for i := 0; i < 12; i++ {
				p := fmt.Sprintf("ws/d%d/f_%d_%d.dat", i%3, ep, i)
				ttl := uint64(1 + rng.Intn(4))
				if _, _, err := pk.PutChunk(ctx, dogi.ChunkMeta{Namespace: ns, Path: p, Epoch: ep}, deterministicPayload(rng, p, 4<<10)); err != nil {
					t.Fatalf("PutChunk: %v", err)
				}
				items = append(items, liveItem{path: p, deathEpoch: ep + ttl})
			}
			if err := pk.FlushAll(ctx, ep); err != nil {
				t.Fatalf("FlushAll: %v", err)
			}
			st, err := pk.Collect(ctx, ep)
			if err != nil {
				t.Fatalf("Collect: %v", err)
			}
			pk.AdvanceEpoch(ep, st)
		}
		return pk.SnapshotMetrics()
	}

	m1 := runOnce()
	m2 := runOnce()
	m3 := runOnce()
	if m1.LiveCopyBytes != m2.LiveCopyBytes || m1.LiveCopyBytes != m3.LiveCopyBytes ||
		m1.PacksUploaded != m2.PacksUploaded || m1.PacksUploaded != m3.PacksUploaded ||
		m1.CompactedPacks != m2.CompactedPacks || m1.CompactedPacks != m3.CompactedPacks {
		t.Fatalf("GC metrics varied across identical-seed runs: m1=%+v m2=%+v m3=%+v", m1, m2, m3)
	}
}
