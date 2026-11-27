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

package most_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/url"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/luxfi/age"
	"github.com/zeebo/blake3"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/internal/storetest"
	"github.com/hanzoai/vfs/pkg/backend"
	_ "github.com/hanzoai/vfs/pkg/backend/file"
	"github.com/hanzoai/vfs/pkg/backend/s3"
	"github.com/hanzoai/vfs/pkg/most"
)

func openFileNVMe(t *testing.T) backend.Backend {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "nvme")
	u := (&url.URL{Scheme: "file", Path: dir}).String()
	be, err := backend.Open(context.Background(), u)
	if err != nil {
		t.Fatalf("open file backend: %v", err)
	}
	t.Cleanup(func() { _ = be.Close() })
	return be
}

func openLiveS3(t *testing.T, prefix string) (*storetest.S3, backend.Backend) {
	t.Helper()
	srv := storetest.NewS3(t, "hanzo-vfs-test")
	be, err := s3.New(s3.Config{
		Endpoint:  srv.Endpoint(),
		Insecure:  true,
		Region:    "us-east-1",
		Bucket:    "hanzo-vfs-test",
		Prefix:    prefix,
		AccessKey: "k",
		SecretKey: "s",
	})
	if err != nil {
		t.Fatalf("open s3 backend: %v", err)
	}
	t.Cleanup(func() { _ = be.Close() })
	return srv, be
}

func makeDeterministicBlock(seed uint64, size int) []byte {
	rng := rand.New(rand.NewPCG(seed, seed*7919+1345))
	b := make([]byte, size)
	for i := range b {
		b[i] = byte(rng.Uint32())
	}
	return b
}

func newHybridCipher(t *testing.T) vfs.Cipher {
	t.Helper()
	id, err := age.GenerateHybridIdentity()
	if err != nil {
		t.Fatalf("generate hybrid identity: %v", err)
	}
	return vfs.Hybrid(id)
}

func percentileDuration(samples []time.Duration, p float64) time.Duration {
	if len(samples) == 0 {
		return 0
	}
	cp := make([]time.Duration, len(samples))
	copy(cp, samples)
	slices.Sort(cp)
	idx := int(float64(len(cp)-1) * p)
	if idx < 0 {
		idx = 0
	}
	if idx >= len(cp) {
		idx = len(cp) - 1
	}
	return cp[idx]
}

// TestAllFourRegimesLiveFileAndS3 exercises all 4 tiering regimes over live
// pkg/backend/file (NVMe L2) and live HTTP storetest.NewS3 + pkg/backend/s3 (L3).
func TestAllFourRegimesLiveFileAndS3(t *testing.T) {
	ctx := context.Background()
	regimes := []most.Regime{
		most.RegimeExclusiveMigration,
		most.RegimeNaiveFullRAMPin,
		most.RegimePerAgentPrivateCache,
		most.RegimeMOSTSelectiveMirror,
	}

	const (
		numBlocks = 24
		blockSize = 4096
		namespace = "go-1-27-stdlib"
	)

	payloads := make([][]byte, numBlocks)
	ids := make([]most.BlockID, numBlocks)
	for i := 0; i < numBlocks; i++ {
		payloads[i] = makeDeterministicBlock(uint64(100+i), blockSize)
		id, err := most.NewSharedBlockID(namespace, payloads[i])
		if err != nil {
			t.Fatalf("NewSharedBlockID(%d): %v", i, err)
		}
		ids[i] = id
	}

	for _, regime := range regimes {
		t.Run(string(regime), func(t *testing.T) {
			nvmeBe := openFileNVMe(t)
			srv, s3Be := openLiveS3(t, "most-"+string(regime))

			eng, err := most.NewEngine(most.Config{
				Regime:            regime,
				RAMBudgetBytes:    8 * blockSize,
				AgentCount:        4,
				AllowedNamespaces: []string{namespace},
				NVMe:              most.NVMeConfig{Store: nvmeBe, Channels: 4},
				Remote:            s3Be,
			})
			if err != nil {
				t.Fatalf("NewEngine: %v", err)
			}

			// Publish half warm-NVMe and half cold (S3-only) to verify both L2 & L3 paths.
			for i := 0; i < numBlocks; i++ {
				warm := i%2 == 0
				if err := eng.PutShared(ctx, ids[i], payloads[i], warm); err != nil {
					t.Fatalf("PutShared(%d): %v", i, err)
				}
			}

			// Read repeatedly across 4 agents and assert 100% bit-exact BLAKE3 match.
			for round := 0; round < 4; round++ {
				for i := 0; i < numBlocks; i++ {
					agentID := fmt.Sprintf("agent-%d", i%4)
					got, err := eng.ReadShared(ctx, agentID, ids[i])
					if err != nil {
						t.Fatalf("ReadShared(round=%d, block=%d): %v", round, i, err)
					}
					if !bytes.Equal(got, payloads[i]) {
						t.Fatalf("payload mismatch for block %d", i)
					}
					if blake3.Sum256(got) != ids[i].Digest {
						t.Fatalf("digest mismatch for block %d", i)
					}
				}
			}

			if srv.Requests() == 0 {
				t.Fatalf("expected live HTTP S3 requests > 0")
			}
			m := eng.Metrics()
			if m.Reads != numBlocks*4 {
				t.Fatalf("Reads = %d, want %d", m.Reads, numBlocks*4)
			}
			if m.CorruptionRejects != 0 || m.IsolationRejects != 0 {
				t.Fatalf("unexpected rejects: %+v", m)
			}
		})
	}
}

// TestMOSTZeroDemotionWritebackInvariant proves MOST performs O(1) instant drops
// with 0 NVMe writeback IOPS, whereas exclusive_tier_migration incurs demotion writes.
func TestMOSTZeroDemotionWritebackInvariant(t *testing.T) {
	ctx := context.Background()
	const (
		numBlocks = 64
		blockSize = 4096
		ramBudget = 16 * blockSize
		namespace = "rust-1-85-core"
	)

	payloads := make([][]byte, numBlocks)
	ids := make([]most.BlockID, numBlocks)
	for i := 0; i < numBlocks; i++ {
		payloads[i] = makeDeterministicBlock(uint64(500+i), blockSize)
		id, err := most.NewSharedBlockID(namespace, payloads[i])
		if err != nil {
			t.Fatal(err)
		}
		ids[i] = id
	}

	run := func(regime most.Regime) most.Metrics {
		nvmeBe := openFileNVMe(t)
		_, s3Be := openLiveS3(t, "inv-"+string(regime))
		eng, err := most.NewEngine(most.Config{
			Regime:            regime,
			RAMBudgetBytes:    ramBudget,
			AgentCount:        4,
			AllowedNamespaces: []string{namespace},
			NVMe:              most.NVMeConfig{Store: nvmeBe, Channels: 4},
			Remote:            s3Be,
		})
		if err != nil {
			t.Fatal(err)
		}
		for i := range ids {
			if err := eng.PutShared(ctx, ids[i], payloads[i], true); err != nil {
				t.Fatal(err)
			}
		}

		// Phase 1: heat blocks 0..19, Phase 2: shift hot set to 20..39 to force evictions.
		for rep := 0; rep < 4; rep++ {
			for i := 0; i < 20; i++ {
				if _, err := eng.ReadShared(ctx, "agent-0", ids[i]); err != nil {
					t.Fatal(err)
				}
			}
		}
		for rep := 0; rep < 8; rep++ {
			for i := 20; i < 40; i++ {
				if _, err := eng.ReadShared(ctx, "agent-1", ids[i]); err != nil {
					t.Fatal(err)
				}
			}
		}
		return eng.Metrics()
	}

	mMOST := run(most.RegimeMOSTSelectiveMirror)
	mMig := run(most.RegimeExclusiveMigration)

	if mMOST.NVMeDemotionWrites != 0 || mMOST.NVMeWritebackIOPS != 0 {
		t.Fatalf("MOST must have 0 NVMe demotion/writeback IOPS, got demotions=%d writebacks=%d",
			mMOST.NVMeDemotionWrites, mMOST.NVMeWritebackIOPS)
	}
	if mMOST.MirrorInstantDrops == 0 {
		t.Fatalf("expected MOST MirrorInstantDrops > 0 under hot-set shift, got 0")
	}
	if mMig.NVMeDemotionWrites == 0 || mMig.NVMeWritebackIOPS == 0 {
		t.Fatalf("expected exclusive_tier_migration to incur NVMe demotion writes, got %+v", mMig)
	}
}

// TestMOSTOutperformsMigrationTailLatencyUnderMultiAgentBurst validates that
// MOST cuts p99 read latency vs exclusive_tier_migration under multi-agent Zipf bursts.
func TestMOSTOutperformsMigrationTailLatencyUnderMultiAgentBurst(t *testing.T) {
	ctx := context.Background()
	const (
		numBlocks = 80
		blockSize = 4096
		ramBudget = 16 * blockSize
		agents    = 16
		opsPerAg  = 40
		namespace = "node-22-runtime"
	)

	payloads := make([][]byte, numBlocks)
	ids := make([]most.BlockID, numBlocks)
	for i := 0; i < numBlocks; i++ {
		payloads[i] = makeDeterministicBlock(uint64(900+i), blockSize)
		id, err := most.NewSharedBlockID(namespace, payloads[i])
		if err != nil {
			t.Fatal(err)
		}
		ids[i] = id
	}

	// Build identical deterministic Zipf-like multi-agent trace.
	traces := make([][]int, agents)
	for a := 0; a < agents; a++ {
		rng := rand.New(rand.NewPCG(uint64(1345+a), uint64(99+a)))
		zipf := rand.NewZipf(rng, 1.25, 1.0, uint64(numBlocks-1))
		traces[a] = make([]int, opsPerAg)
		for j := 0; j < opsPerAg; j++ {
			traces[a][j] = int(zipf.Uint64())
		}
	}

	measure := func(regime most.Regime) (p99 time.Duration, m most.Metrics) {
		nvmeBe := openFileNVMe(t)
		_, s3Be := openLiveS3(t, "burst-"+string(regime))
		eng, err := most.NewEngine(most.Config{
			Regime:            regime,
			RAMBudgetBytes:    ramBudget,
			AgentCount:        agents,
			AllowedNamespaces: []string{namespace},
			NVMe: most.NVMeConfig{
				Store:        nvmeBe,
				Channels:     4,
				ReadLatency:  120 * time.Microsecond,
				WriteLatency: 450 * time.Microsecond,
			},
			Remote: s3Be,
		})
		if err != nil {
			t.Fatal(err)
		}
		for i := range ids {
			if err := eng.PutShared(ctx, ids[i], payloads[i], true); err != nil {
				t.Fatal(err)
			}
		}

		latencies := make([]time.Duration, agents*opsPerAg)
		var wg sync.WaitGroup
		for a := 0; a < agents; a++ {
			wg.Add(1)
			go func(agentIdx int) {
				defer wg.Done()
				agentID := fmt.Sprintf("agent-%d", agentIdx)
				base := agentIdx * opsPerAg
				for j, blk := range traces[agentIdx] {
					start := time.Now()
					got, err := eng.ReadShared(ctx, agentID, ids[blk])
					latencies[base+j] = time.Since(start)
					if err != nil {
						t.Errorf("ReadShared(%s, %d): %v", agentID, blk, err)
						return
					}
					if len(got) != blockSize {
						t.Errorf("short read: %d", len(got))
						return
					}
				}
			}(a)
		}
		wg.Wait()
		return percentileDuration(latencies, 0.99), eng.Metrics()
	}

	p99Mig, mMig := measure(most.RegimeExclusiveMigration)
	p99MOST, mMOST := measure(most.RegimeMOSTSelectiveMirror)

	t.Logf("migration p99=%v demotions=%d hit=%.2f%% | MOST p99=%v demotions=%d hit=%.2f%%",
		p99Mig, mMig.NVMeDemotionWrites, mMig.RAMHitRate*100,
		p99MOST, mMOST.NVMeDemotionWrites, mMOST.RAMHitRate*100)

	if p99MOST >= p99Mig {
		t.Fatalf("expected MOST p99 (%v) < exclusive_tier_migration p99 (%v)", p99MOST, p99Mig)
	}
	if mMOST.NVMeDemotionWrites != 0 {
		t.Fatalf("expected MOST 0 demotion writes, got %d", mMOST.NVMeDemotionWrites)
	}
	if mMig.NVMeDemotionWrites == 0 {
		t.Fatalf("expected exclusive_tier_migration demotion writes > 0")
	}
}

// TestPerAgentMemoryScalingAmortization verifies MOST amortizes RAM per agent
// O(hot_set / M) while per_agent_private_cache scales total RAM linearly O(M).
func TestPerAgentMemoryScalingAmortization(t *testing.T) {
	ctx := context.Background()
	const (
		agents    = 16
		numBlocks = 16
		blockSize = 4096
		ramBudget = 16 * blockSize
		namespace = "go-1-27-stdlib"
	)

	payloads := make([][]byte, numBlocks)
	ids := make([]most.BlockID, numBlocks)
	for i := 0; i < numBlocks; i++ {
		payloads[i] = makeDeterministicBlock(uint64(1200+i), blockSize)
		id, err := most.NewSharedBlockID(namespace, payloads[i])
		if err != nil {
			t.Fatal(err)
		}
		ids[i] = id
	}

	run := func(regime most.Regime) most.Metrics {
		nvmeBe := openFileNVMe(t)
		_, s3Be := openLiveS3(t, "scale-"+string(regime))
		eng, err := most.NewEngine(most.Config{
			Regime:            regime,
			RAMBudgetBytes:    ramBudget,
			AgentCount:        agents,
			AllowedNamespaces: []string{namespace},
			NVMe:              most.NVMeConfig{Store: nvmeBe, Channels: 4},
			Remote:            s3Be,
		})
		if err != nil {
			t.Fatal(err)
		}
		for i := range ids {
			if err := eng.PutShared(ctx, ids[i], payloads[i], true); err != nil {
				t.Fatal(err)
			}
		}
		for a := 0; a < agents; a++ {
			agentID := fmt.Sprintf("agent-%d", a)
			for rep := 0; rep < 3; rep++ {
				for i := 0; i < numBlocks; i++ {
					if _, err := eng.ReadShared(ctx, agentID, ids[i]); err != nil {
						t.Fatal(err)
					}
				}
			}
		}
		return eng.Metrics()
	}

	mMOST := run(most.RegimeMOSTSelectiveMirror)
	mPriv := run(most.RegimePerAgentPrivateCache)

	if mPriv.PeakRAMBytes < int64(agents)*ramBudget {
		t.Fatalf("expected per_agent_private_cache peak RAM >= %d, got %d",
			int64(agents)*ramBudget, mPriv.PeakRAMBytes)
	}
	if mMOST.PeakRAMBytes > ramBudget {
		t.Fatalf("MOST exceeded RAM budget: %d > %d", mMOST.PeakRAMBytes, ramBudget)
	}
	if mMOST.RAMPerAgentBytes*4 >= mPriv.RAMPerAgentBytes {
		t.Fatalf("expected MOST RAM/agent (%.1f) to be far lower than per-agent private (%.1f)",
			mMOST.RAMPerAgentBytes, mPriv.RAMPerAgentBytes)
	}
}

// TestNaiveFullPinStrictCapOOM verifies naive_full_ram_pin rejects over-budget
// pins with ErrRAMExceeded when EnforceRAMCap is enabled and reports OverBudget
// when unbounded.
func TestNaiveFullPinStrictCapOOM(t *testing.T) {
	ctx := context.Background()
	const (
		blockSize = 4096
		namespace = "go-1-27-stdlib"
	)

	nvmeBe := openFileNVMe(t)
	_, s3Be := openLiveS3(t, "oom-strict")
	eng, err := most.NewEngine(most.Config{
		Regime:            most.RegimeNaiveFullRAMPin,
		RAMBudgetBytes:    2 * blockSize,
		EnforceRAMCap:     true,
		AgentCount:        4,
		AllowedNamespaces: []string{namespace},
		NVMe:              most.NVMeConfig{Store: nvmeBe, Channels: 2},
		Remote:            s3Be,
	})
	if err != nil {
		t.Fatal(err)
	}

	var ids []most.BlockID
	for i := 0; i < 4; i++ {
		data := makeDeterministicBlock(uint64(2000+i), blockSize)
		id, err := most.NewSharedBlockID(namespace, data)
		if err != nil {
			t.Fatal(err)
		}
		if err := eng.PutShared(ctx, id, data, true); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}

	if _, err := eng.ReadShared(ctx, "a0", ids[0]); err != nil {
		t.Fatalf("block 0 should fit: %v", err)
	}
	if _, err := eng.ReadShared(ctx, "a0", ids[1]); err != nil {
		t.Fatalf("block 1 should fit: %v", err)
	}
	if _, err := eng.ReadShared(ctx, "a0", ids[2]); !errors.Is(err, most.ErrRAMExceeded) {
		t.Fatalf("block 2 err = %v, want ErrRAMExceeded", err)
	}

	// Unbounded pin succeeds but reports OverBudget = true.
	engUnbounded, err := most.NewEngine(most.Config{
		Regime:            most.RegimeNaiveFullRAMPin,
		RAMBudgetBytes:    2 * blockSize,
		EnforceRAMCap:     false,
		AgentCount:        4,
		AllowedNamespaces: []string{namespace},
		NVMe:              most.NVMeConfig{Store: nvmeBe, Channels: 2},
		Remote:            s3Be,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range ids {
		if _, err := engUnbounded.ReadShared(ctx, "a0", id); err != nil {
			t.Fatal(err)
		}
	}
	if !engUnbounded.Metrics().OverBudget {
		t.Fatalf("expected OverBudget=true for unbounded naive_full_ram_pin")
	}
}

// TestCrossTenantIsolationAndSharedMirrorBoundary verifies private workspace
// blocks can never enter the shared mirror or cross tenant boundaries.
func TestCrossTenantIsolationAndSharedMirrorBoundary(t *testing.T) {
	ctx := context.Background()
	nvmeBe := openFileNVMe(t)
	_, s3Be := openLiveS3(t, "iso")

	tenantA := most.Scope{TenantID: "tenant-alpha", VolumeID: "vol-1"}
	tenantB := most.Scope{TenantID: "tenant-beta", VolumeID: "vol-2"}

	eng, err := most.NewEngine(most.Config{
		Regime:            most.RegimeMOSTSelectiveMirror,
		RAMBudgetBytes:    64 << 10,
		AgentCount:        4,
		AllowedNamespaces: []string{"go-1-27-stdlib"},
		NVMe:              most.NVMeConfig{Store: nvmeBe, Channels: 4},
		Remote:            s3Be,
	})
	if err != nil {
		t.Fatal(err)
	}

	privA, err := most.NewPrivateBlockID(tenantA, "workspace-secret-1")
	if err != nil {
		t.Fatal(err)
	}
	secret := []byte("confidential tenant-alpha workspace payload")
	if err := eng.PutPrivate(ctx, tenantA, privA, secret); err != nil {
		t.Fatalf("PutPrivate(tenantA): %v", err)
	}

	// 1. Private block cannot enter shared mirror via PutShared, ReadShared, or AttemptMirrorPrivate.
	if err := eng.PutShared(ctx, privA, secret, true); !errors.Is(err, most.ErrTenantIsolation) {
		t.Fatalf("PutShared(private) = %v, want ErrTenantIsolation", err)
	}
	if _, err := eng.ReadShared(ctx, "agent-0", privA); !errors.Is(err, most.ErrTenantIsolation) {
		t.Fatalf("ReadShared(private) = %v, want ErrTenantIsolation", err)
	}
	if err := eng.AttemptMirrorPrivate(privA, secret); !errors.Is(err, most.ErrTenantIsolation) {
		t.Fatalf("AttemptMirrorPrivate = %v, want ErrTenantIsolation", err)
	}
	if eng.SharedRAMContains(privA.Key) {
		t.Fatalf("private block must NEVER appear in shared RAM mirror")
	}

	// 2. Tenant B cannot read or overwrite Tenant A's private block.
	if _, err := eng.ReadPrivate(ctx, tenantB, privA); !errors.Is(err, most.ErrTenantIsolation) {
		t.Fatalf("cross-tenant ReadPrivate = %v, want ErrTenantIsolation", err)
	}
	if err := eng.PutPrivate(ctx, tenantB, privA, []byte("overwrite")); !errors.Is(err, most.ErrTenantIsolation) {
		t.Fatalf("cross-tenant PutPrivate = %v, want ErrTenantIsolation", err)
	}

	// 3. Tenant A reads its own private block cleanly.
	got, err := eng.ReadPrivate(ctx, tenantA, privA)
	if err != nil {
		t.Fatalf("ReadPrivate(tenantA): %v", err)
	}
	if !bytes.Equal(got, secret) {
		t.Fatalf("private payload mismatch")
	}
	if eng.Metrics().IsolationRejects < 5 {
		t.Fatalf("expected IsolationRejects >= 5, got %d", eng.Metrics().IsolationRejects)
	}
}

// TestNamespaceAllowlistEnforcement verifies only allowlisted toolchain
// namespaces may be stored or read through the shared mirror.
func TestNamespaceAllowlistEnforcement(t *testing.T) {
	ctx := context.Background()
	nvmeBe := openFileNVMe(t)
	_, s3Be := openLiveS3(t, "ns-allow")

	eng, err := most.NewEngine(most.Config{
		Regime:            most.RegimeMOSTSelectiveMirror,
		RAMBudgetBytes:    64 << 10,
		AllowedNamespaces: []string{"go-1-27-stdlib"},
		NVMe:              most.NVMeConfig{Store: nvmeBe, Channels: 2},
		Remote:            s3Be,
	})
	if err != nil {
		t.Fatal(err)
	}

	deniedData := []byte("untrusted-namespace-payload")
	deniedID, err := most.NewSharedBlockID("untrusted-tc", deniedData)
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.PutShared(ctx, deniedID, deniedData, true); !errors.Is(err, most.ErrNamespaceDenied) {
		t.Fatalf("PutShared(denied ns) = %v, want ErrNamespaceDenied", err)
	}
	if _, err := eng.ReadShared(ctx, "agent-0", deniedID); !errors.Is(err, most.ErrNamespaceDenied) {
		t.Fatalf("ReadShared(denied ns) = %v, want ErrNamespaceDenied", err)
	}
}

// TestPrivateWorkspaceHybridPQEncryptionLiveS3 verifies private workspace
// blocks are encrypted per-volume using post-quantum vfs.Hybrid (X25519 + ML-KEM-768)
// over live NVMe (file) and live HTTP S3 backends, with zero plaintext on disk/S3
// and cryptographic isolation between tenants.
func TestPrivateWorkspaceHybridPQEncryptionLiveS3(t *testing.T) {
	ctx := context.Background()
	nvmeBe := openFileNVMe(t)
	_, s3Be := openLiveS3(t, "pq-hybrid")

	scopeA := most.Scope{TenantID: "tenant-a", VolumeID: "vol-a"}
	scopeB := most.Scope{TenantID: "tenant-b", VolumeID: "vol-b"}
	cipherA := newHybridCipher(t)
	cipherB := newHybridCipher(t)

	eng, err := most.NewEngine(most.Config{
		Regime:            most.RegimeMOSTSelectiveMirror,
		RAMBudgetBytes:    64 << 10,
		AllowedNamespaces: []string{"go-1-27-stdlib"},
		NVMe:              most.NVMeConfig{Store: nvmeBe, Channels: 2},
		Remote:            s3Be,
		VolumeCiphers: map[most.Scope]vfs.Cipher{
			scopeA: cipherA,
			scopeB: cipherB,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	plaintext := []byte("SENSITIVE_PRIVATE_WORKSPACE_PAYLOAD_PQ_TEST_1345")
	privA, err := most.NewPrivateBlockID(scopeA, "scratch-chunk-0")
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.PutPrivate(ctx, scopeA, privA, plaintext); err != nil {
		t.Fatalf("PutPrivate: %v", err)
	}

	// Verify both NVMe and live S3 hold PQ hybrid age ciphertext and ZERO plaintext.
	for name, be := range map[string]backend.Backend{"nvme": nvmeBe, "s3": s3Be} {
		raw, _, err := be.Get(ctx, privA.Key, backend.Unbounded)
		if err != nil {
			t.Fatalf("%s raw Get: %v", name, err)
		}
		if bytes.Contains(raw, plaintext) {
			t.Fatalf("%s stored plaintext bytes on backend!", name)
		}
		if !bytes.Contains(raw, []byte("age-encryption.org/v1\n")) {
			t.Fatalf("%s missing age header", name)
		}
		if !bytes.Contains(raw, []byte("-> mlkem768x25519")) {
			t.Fatalf("%s missing post-quantum mlkem768x25519 stanza", name)
		}
	}

	// Tenant A decrypts cleanly from NVMe and also after NVMe eviction (cold S3 fallback).
	got, err := eng.ReadPrivate(ctx, scopeA, privA)
	if err != nil || !bytes.Equal(got, plaintext) {
		t.Fatalf("ReadPrivate(NVMe) = %q, err=%v", got, err)
	}
	if err := nvmeBe.Delete(ctx, privA.Key); err != nil {
		t.Fatal(err)
	}
	gotS3, err := eng.ReadPrivate(ctx, scopeA, privA)
	if err != nil || !bytes.Equal(gotS3, plaintext) {
		t.Fatalf("ReadPrivate(S3 fallback) = %q, err=%v", gotS3, err)
	}

	// Cryptographic isolation: attempting to open Tenant A's raw ciphertext with
	// Tenant B's cipher must fail even if raw bytes were obtained out-of-band.
	rawS3, _, err := s3Be.Get(ctx, privA.Key, backend.Unbounded)
	if err != nil {
		t.Fatal(err)
	}
	engWrongKey, err := most.NewEngine(most.Config{
		Regime:            most.RegimeMOSTSelectiveMirror,
		RAMBudgetBytes:    64 << 10,
		AllowedNamespaces: []string{"go-1-27-stdlib"},
		NVMe:              most.NVMeConfig{Store: openFileNVMe(t), Channels: 2},
		Remote:            s3Be,
		VolumeCiphers:     map[most.Scope]vfs.Cipher{scopeA: cipherB},
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = rawS3
	if _, err := engWrongKey.ReadPrivate(ctx, scopeA, privA); err == nil {
		t.Fatalf("expected decryption failure with wrong tenant PQ hybrid cipher")
	}
}

// TestCorruptionRejectionNVMeAndS3 verifies 1-bit corruption in either NVMe or
// S3 is rejected with ErrCorruptBlock and never enters the RAM mirror.
func TestCorruptionRejectionNVMeAndS3(t *testing.T) {
	ctx := context.Background()
	const namespace = "go-1-27-stdlib"

	nvmeBe := openFileNVMe(t)
	_, s3Be := openLiveS3(t, "corrupt")
	eng, err := most.NewEngine(most.Config{
		Regime:            most.RegimeMOSTSelectiveMirror,
		RAMBudgetBytes:    64 << 10,
		AllowedNamespaces: []string{namespace},
		NVMe:              most.NVMeConfig{Store: nvmeBe, Channels: 2},
		Remote:            s3Be,
	})
	if err != nil {
		t.Fatal(err)
	}

	good := makeDeterministicBlock(333, 4096)
	id, err := most.NewSharedBlockID(namespace, good)
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.PutShared(ctx, id, good, true); err != nil {
		t.Fatal(err)
	}

	// Flip 1 bit on NVMe.
	corrupt := append([]byte(nil), good...)
	corrupt[17] ^= 0x01
	if err := nvmeBe.Put(ctx, id.Key, corrupt); err != nil {
		t.Fatal(err)
	}
	if _, err := eng.ReadShared(ctx, "agent-0", id); !errors.Is(err, most.ErrCorruptBlock) {
		t.Fatalf("NVMe corrupt read err = %v, want ErrCorruptBlock", err)
	}
	if eng.SharedRAMContains(id.Key) {
		t.Fatalf("corrupted NVMe block must never enter RAM mirror")
	}

	// Remove from NVMe and flip 1 bit on S3.
	if err := nvmeBe.Delete(ctx, id.Key); err != nil {
		t.Fatal(err)
	}
	if err := s3Be.Put(ctx, id.Key, corrupt); err != nil {
		t.Fatal(err)
	}
	if _, err := eng.ReadShared(ctx, "agent-0", id); !errors.Is(err, most.ErrCorruptBlock) {
		t.Fatalf("S3 corrupt read err = %v, want ErrCorruptBlock", err)
	}
	if eng.Metrics().CorruptionRejects < 2 {
		t.Fatalf("expected CorruptionRejects >= 2, got %d", eng.Metrics().CorruptionRejects)
	}
}

type blockingBackend struct {
	backend.Backend
	entered   chan struct{}
	release   chan struct{}
	canceled  atomic.Bool
	getCalls  atomic.Int32
	blockOnce sync.Once
}

func (b *blockingBackend) Get(ctx context.Context, key string, max int64) ([]byte, backend.Version, error) {
	b.getCalls.Add(1)
	b.blockOnce.Do(func() {
		close(b.entered)
		select {
		case <-b.release:
		case <-ctx.Done():
			b.canceled.Store(true)
		}
	})
	if err := ctx.Err(); err != nil {
		b.canceled.Store(true)
		return nil, "", err
	}
	return b.Backend.Get(ctx, key, max)
}

// TestSingleFlightCancelIsolation proves that canceling the initiating caller
// of a coalesced single-flight fetch NEVER aborts a live concurrent waiter,
// and that when all waiters cancel, the detached flight cancels cleanly.
func TestSingleFlightCancelIsolation(t *testing.T) {
	const namespace = "go-1-27-stdlib"
	data := makeDeterministicBlock(777, 4096)
	id, err := most.NewSharedBlockID(namespace, data)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("initiator_cancel_does_not_abort_waiter", func(t *testing.T) {
		nvmeBe := openFileNVMe(t)
		_, s3Be := openLiveS3(t, "sf-iso")
		if err := s3Be.Put(context.Background(), id.Key, data); err != nil {
			t.Fatal(err)
		}
		blk := &blockingBackend{
			Backend: s3Be,
			entered: make(chan struct{}),
			release: make(chan struct{}),
		}
		eng, err := most.NewEngine(most.Config{
			Regime:            most.RegimeMOSTSelectiveMirror,
			RAMBudgetBytes:    64 << 10,
			AllowedNamespaces: []string{namespace},
			NVMe:              most.NVMeConfig{Store: nvmeBe, Channels: 2},
			Remote:            blk,
		})
		if err != nil {
			t.Fatal(err)
		}

		ctx1, cancel1 := context.WithCancel(context.Background())
		err1Ch := make(chan error, 1)
		go func() {
			_, err := eng.ReadShared(ctx1, "agent-1", id)
			err1Ch <- err
		}()

		<-blk.entered

		// Launch waiter 2 which coalesces onto the in-flight fetch.
		res2Ch := make(chan []byte, 1)
		err2Ch := make(chan error, 1)
		go func() {
			got, err := eng.ReadShared(context.Background(), "agent-2", id)
			res2Ch <- got
			err2Ch <- err
		}()

		// Give waiter 2 a brief window to join the flight.
		time.Sleep(15 * time.Millisecond)

		// Cancel initiating caller 1.
		cancel1()
		if err1 := <-err1Ch; !errors.Is(err1, context.Canceled) {
			t.Fatalf("caller 1 err = %v, want context.Canceled", err1)
		}

		// Release backend fetch; waiter 2 MUST succeed with bit-exact bytes.
		close(blk.release)
		if err2 := <-err2Ch; err2 != nil {
			t.Fatalf("live waiter 2 failed after initiator cancel: %v", err2)
		}
		if got2 := <-res2Ch; !bytes.Equal(got2, data) {
			t.Fatalf("live waiter 2 received corrupted data")
		}
		if blk.getCalls.Load() != 1 {
			t.Fatalf("expected 1 coalesced S3 GET, got %d", blk.getCalls.Load())
		}
		if eng.Metrics().SingleFlightCoalesced < 1 {
			t.Fatalf("expected SingleFlightCoalesced >= 1")
		}
	})

	t.Run("all_waiters_cancel_cancels_detached_flight", func(t *testing.T) {
		nvmeBe := openFileNVMe(t)
		_, s3Be := openLiveS3(t, "sf-allcancel")
		if err := s3Be.Put(context.Background(), id.Key, data); err != nil {
			t.Fatal(err)
		}
		blk := &blockingBackend{
			Backend: s3Be,
			entered: make(chan struct{}),
			release: make(chan struct{}),
		}
		eng, err := most.NewEngine(most.Config{
			Regime:            most.RegimeMOSTSelectiveMirror,
			RAMBudgetBytes:    64 << 10,
			AllowedNamespaces: []string{namespace},
			NVMe:              most.NVMeConfig{Store: nvmeBe, Channels: 2},
			Remote:            blk,
		})
		if err != nil {
			t.Fatal(err)
		}

		ctx1, cancel1 := context.WithCancel(context.Background())
		err1Ch := make(chan error, 1)
		go func() {
			_, err := eng.ReadShared(ctx1, "agent-1", id)
			err1Ch <- err
		}()
		<-blk.entered
		cancel1()
		if err := <-err1Ch; !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want context.Canceled", err)
		}
		deadline := time.Now().Add(time.Second)
		for !blk.canceled.Load() && time.Now().Before(deadline) {
			time.Sleep(5 * time.Millisecond)
		}
		if !blk.canceled.Load() {
			t.Fatalf("expected detached flight to cancel when all waiters left")
		}
	})
}

// TestDefensiveCopyImmutability ensures mutating returned slices cannot corrupt
// the shared RAM mirror.
func TestDefensiveCopyImmutability(t *testing.T) {
	ctx := context.Background()
	const namespace = "go-1-27-stdlib"
	nvmeBe := openFileNVMe(t)
	_, s3Be := openLiveS3(t, "defcopy")
	eng, err := most.NewEngine(most.Config{
		Regime:            most.RegimeMOSTSelectiveMirror,
		RAMBudgetBytes:    64 << 10,
		AllowedNamespaces: []string{namespace},
		NVMe:              most.NVMeConfig{Store: nvmeBe, Channels: 2},
		Remote:            s3Be,
		Tracker:           most.TrackerConfig{MinHits: 1},
	})
	if err != nil {
		t.Fatal(err)
	}

	orig := makeDeterministicBlock(42, 4096)
	id, err := most.NewSharedBlockID(namespace, orig)
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.PutShared(ctx, id, orig, true); err != nil {
		t.Fatal(err)
	}

	got1, err := eng.ReadShared(ctx, "a0", id)
	if err != nil {
		t.Fatal(err)
	}
	// Mutate caller buffer in place.
	for i := range got1 {
		got1[i] ^= 0xff
	}

	got2, err := eng.ReadShared(ctx, "a1", id)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got2, orig) {
		t.Fatalf("caller mutation corrupted shared RAM mirror!")
	}
}

// TestHotnessTrackerAgingAndScanResistance validates CMS decay, read-dominance
// gating, and scan pollution resistance.
func TestHotnessTrackerAgingAndScanResistance(t *testing.T) {
	tr := most.NewHotnessTracker(most.TrackerConfig{
		Width:            512,
		Depth:            4,
		DecayInterval:    64,
		MinHits:          2,
		MinReadDominance: 0.85,
	})
	if tr.MemoryBytes() <= 0 {
		t.Fatalf("expected positive MemoryBytes")
	}

	hotKey := most.HashKey("tc/go/00/hot")
	coldKey := most.HashKey("tc/go/00/cold")
	writeHeavyKey := most.HashKey("priv/t/v/mut")

	for i := 0; i < 15; i++ {
		tr.ObserveRead(hotKey)
	}
	coldReads, coldRatio := tr.ObserveRead(coldKey)
	if tr.ShouldAdmit(coldReads, coldRatio, hotKey, true, 1.0) {
		t.Fatalf("one-hit cold scan block should NOT evict hot block")
	}

	// Write-heavy key fails read-dominance filter.
	tr.ObserveRead(writeHeavyKey)
	tr.ObserveRead(writeHeavyKey)
	tr.ObserveWrite(writeHeavyKey)
	tr.ObserveWrite(writeHeavyKey)
	whReads, whRatio := tr.ObserveRead(writeHeavyKey)
	if tr.ShouldAdmit(whReads, whRatio, 0, false, 0.2) {
		t.Fatalf("write-heavy block (ratio=%.2f) should fail read-dominance gate", whRatio)
	}

	// Trigger aging decay and confirm hot counter decreases.
	before, _ := tr.Estimate(hotKey)
	for i := 0; i < 128; i++ {
		tr.ObserveRead(most.HashKey(fmt.Sprintf("decay-%d", i)))
	}
	after, _ := tr.Estimate(hotKey)
	if after >= before {
		t.Fatalf("expected hot counter to decay: before=%d after=%d", before, after)
	}
}

// TestMultiAgentConcurrentRaceStress exercises 32 concurrent agents performing
// shared toolchain reads and per-tenant PQ private reads/writes under -race.
func TestMultiAgentConcurrentRaceStress(t *testing.T) {
	ctx := context.Background()
	const (
		agents    = 32
		numShared = 32
		blockSize = 2048
		namespace = "go-1-27-stdlib"
	)

	payloads := make([][]byte, numShared)
	ids := make([]most.BlockID, numShared)
	for i := 0; i < numShared; i++ {
		payloads[i] = makeDeterministicBlock(uint64(3000+i), blockSize)
		id, err := most.NewSharedBlockID(namespace, payloads[i])
		if err != nil {
			t.Fatal(err)
		}
		ids[i] = id
	}

	for _, regime := range []most.Regime{most.RegimeMOSTSelectiveMirror, most.RegimeExclusiveMigration} {
		t.Run(string(regime), func(t *testing.T) {
			nvmeBe := openFileNVMe(t)
			_, s3Be := openLiveS3(t, "stress-"+string(regime))
			ciphers := make(map[most.Scope]vfs.Cipher, 4)
			scopes := make([]most.Scope, 4)
			for i := 0; i < 4; i++ {
				sc := most.Scope{TenantID: fmt.Sprintf("t-%d", i), VolumeID: fmt.Sprintf("v-%d", i)}
				scopes[i] = sc
				ciphers[sc] = newHybridCipher(t)
			}

			eng, err := most.NewEngine(most.Config{
				Regime:            regime,
				RAMBudgetBytes:    10 * blockSize,
				AgentCount:        agents,
				AllowedNamespaces: []string{namespace},
				NVMe:              most.NVMeConfig{Store: nvmeBe, Channels: 4},
				Remote:            s3Be,
				VolumeCiphers:     ciphers,
			})
			if err != nil {
				t.Fatal(err)
			}
			for i := range ids {
				if err := eng.PutShared(ctx, ids[i], payloads[i], true); err != nil {
					t.Fatal(err)
				}
			}

			var wg sync.WaitGroup
			for a := 0; a < agents; a++ {
				wg.Add(1)
				go func(idx int) {
					defer wg.Done()
					agentID := fmt.Sprintf("agent-%d", idx)
					sc := scopes[idx%len(scopes)]
					rng := rand.New(rand.NewPCG(uint64(100+idx), uint64(idx+1)))
					for step := 0; step < 30; step++ {
						blk := int(rng.Uint64() % numShared)
						got, err := eng.ReadShared(ctx, agentID, ids[blk])
						if err != nil {
							t.Errorf("ReadShared: %v", err)
							return
						}
						if blake3.Sum256(got) != ids[blk].Digest {
							t.Errorf("digest mismatch for block %d", blk)
							return
						}
						if step%10 == 0 {
							pid, err := most.NewPrivateBlockID(sc, fmt.Sprintf("a%d-s%d", idx, step))
							if err != nil {
								t.Errorf("NewPrivateBlockID: %v", err)
								return
							}
							privData := []byte(fmt.Sprintf("private-data-%d-%d", idx, step))
							if err := eng.PutPrivate(ctx, sc, pid, privData); err != nil {
								t.Errorf("PutPrivate: %v", err)
								return
							}
							gotPriv, err := eng.ReadPrivate(ctx, sc, pid)
							if err != nil || !bytes.Equal(gotPriv, privData) {
								t.Errorf("ReadPrivate mismatch: err=%v", err)
								return
							}
						}
					}
				}(a)
			}
			wg.Wait()
			if eng.Metrics().CorruptionRejects != 0 {
				t.Fatalf("unexpected corruption rejects: %d", eng.Metrics().CorruptionRejects)
			}
		})
	}
}

// TestNegativeRegime1UniformNoSkewScan confirms that under flat uniform random
// scans (Zipf alpha = 0) with WorkingSet = 4x RAM, selective mirroring has no
// hot subset to exploit, producing a low hit rate and non-zero tracker overhead.
func TestNegativeRegime1UniformNoSkewScan(t *testing.T) {
	ctx := context.Background()
	const (
		numBlocks = 64
		blockSize = 4096
		ramBudget = 16 * blockSize // 25% of working set
		namespace = "go-1-27-stdlib"
	)

	nvmeBe := openFileNVMe(t)
	_, s3Be := openLiveS3(t, "neg1")
	eng, err := most.NewEngine(most.Config{
		Regime:            most.RegimeMOSTSelectiveMirror,
		RAMBudgetBytes:    ramBudget,
		AgentCount:        8,
		AllowedNamespaces: []string{namespace},
		NVMe:              most.NVMeConfig{Store: nvmeBe, Channels: 4},
		Remote:            s3Be,
	})
	if err != nil {
		t.Fatal(err)
	}

	ids := make([]most.BlockID, numBlocks)
	for i := 0; i < numBlocks; i++ {
		data := makeDeterministicBlock(uint64(4000+i), blockSize)
		id, err := most.NewSharedBlockID(namespace, data)
		if err != nil {
			t.Fatal(err)
		}
		ids[i] = id
		if err := eng.PutShared(ctx, id, data, true); err != nil {
			t.Fatal(err)
		}
	}

	rng := rand.New(rand.NewPCG(1345, 1))
	for i := 0; i < 600; i++ {
		blk := int(rng.Uint64() % numBlocks)
		if _, err := eng.ReadShared(ctx, fmt.Sprintf("agent-%d", i%8), ids[blk]); err != nil {
			t.Fatal(err)
		}
	}

	m := eng.Metrics()
	if m.RAMHitRate > 0.35 {
		t.Fatalf("expected low hit rate under uniform scan (WorkingSet=4x RAM), got %.2f", m.RAMHitRate)
	}
	if m.TrackerMetaBytes <= 0 {
		t.Fatalf("expected non-zero TrackerMetaBytes overhead")
	}
}

// TestNegativeRegime2WriteHeavyInvalidationChurn proves why MOST forbids
// mirroring write-heavy mutable data: dual RAM+NVMe mirroring incurs
// invalidation and write-through sync overhead worse than write-back migration.
func TestNegativeRegime2WriteHeavyInvalidationChurn(t *testing.T) {
	ctx := context.Background()
	const (
		numBlocks = 16
		blockSize = 4096
		ramBudget = 16 * blockSize
	)
	sc := most.Scope{TenantID: "study-tenant", VolumeID: "study-vol"}

	// First verify production MOST rejects mutable mirroring and shared mutation.
	prodEng, err := most.NewEngine(most.Config{
		Regime:            most.RegimeMOSTSelectiveMirror,
		RAMBudgetBytes:    ramBudget,
		AllowedNamespaces: []string{"go-1-27-stdlib"},
		NVMe:              most.NVMeConfig{Store: openFileNVMe(t), Channels: 2},
		Remote:            openFileNVMe(t),
	})
	if err != nil {
		t.Fatal(err)
	}
	privID, _ := most.NewPrivateBlockID(sc, "mut-0")
	if err := prodEng.WriteMutable(ctx, sc, privID, []byte("x")); !errors.Is(err, most.ErrTenantIsolation) {
		t.Fatalf("production MOST WriteMutable err = %v, want ErrTenantIsolation", err)
	}
	sharedID, _ := most.NewSharedBlockID("go-1-27-stdlib", []byte("imm"))
	if err := prodEng.WriteMutable(ctx, sc, sharedID, []byte("x")); !errors.Is(err, most.ErrImmutableMutation) {
		t.Fatalf("WriteMutable on shared block err = %v, want ErrImmutableMutation", err)
	}

	// Now compare write-heavy ablation (50% writes / 50% reads) vs write-back migration.
	run := func(regime most.Regime) most.Metrics {
		nvmeBe := openFileNVMe(t)
		_, s3Be := openLiveS3(t, "neg2-"+string(regime))
		eng, err := most.NewEngine(most.Config{
			Regime:                     regime,
			RAMBudgetBytes:             ramBudget,
			AgentCount:                 4,
			AllowedNamespaces:          []string{"go-1-27-stdlib"},
			NVMe:                       most.NVMeConfig{Store: nvmeBe, Channels: 4},
			Remote:                     s3Be,
			AllowMutableMirrorForStudy: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		ids := make([]most.BlockID, numBlocks)
		for i := 0; i < numBlocks; i++ {
			id, err := most.NewPrivateBlockID(sc, fmt.Sprintf("mut-%d", i))
			if err != nil {
				t.Fatal(err)
			}
			ids[i] = id
			if err := eng.SeedMutable(ctx, id, makeDeterministicBlock(uint64(5000+i), blockSize)); err != nil {
				t.Fatal(err)
			}
		}
		// Warm reads first so blocks reside in RAM, then run 50% write / 50% read churn.
		for rep := 0; rep < 2; rep++ {
			for _, id := range ids {
				if _, err := eng.ReadMutable(ctx, sc, id); err != nil {
					t.Fatal(err)
				}
			}
		}
		payload := makeDeterministicBlock(9999, blockSize)
		for step := 0; step < 200; step++ {
			id := ids[step%numBlocks]
			if step%2 == 0 {
				if err := eng.WriteMutable(ctx, sc, id, payload); err != nil {
					t.Fatal(err)
				}
			} else {
				if _, err := eng.ReadMutable(ctx, sc, id); err != nil {
					t.Fatal(err)
				}
			}
		}
		return eng.Metrics()
	}

	mMig := run(most.RegimeExclusiveMigration)
	mMOST := run(most.RegimeMOSTSelectiveMirror)

	if mMOST.MirrorInvalidations == 0 {
		t.Fatalf("expected MirrorInvalidations > 0 in write-heavy ablation")
	}
	if mMOST.NVMeWritebackIOPS <= mMig.NVMeWritebackIOPS {
		t.Fatalf("expected mutable mirroring writeback IOPS (%d) > write-back migration (%d)",
			mMOST.NVMeWritebackIOPS, mMig.NVMeWritebackIOPS)
	}
}

// TestNegativeRegime3SingleAgentAmpleRAM confirms that for M=1 agent with
// RAM >= WorkingSet, selective mirroring saves 0 RAM/agent and adds tracker overhead.
func TestNegativeRegime3SingleAgentAmpleRAM(t *testing.T) {
	ctx := context.Background()
	const (
		numBlocks = 16
		blockSize = 4096
		ramBudget = 32 * blockSize // 2x working set (ample RAM)
		namespace = "go-1-27-stdlib"
	)

	payloads := make([][]byte, numBlocks)
	ids := make([]most.BlockID, numBlocks)
	for i := 0; i < numBlocks; i++ {
		payloads[i] = makeDeterministicBlock(uint64(6000+i), blockSize)
		id, err := most.NewSharedBlockID(namespace, payloads[i])
		if err != nil {
			t.Fatal(err)
		}
		ids[i] = id
	}

	run := func(regime most.Regime) most.Metrics {
		nvmeBe := openFileNVMe(t)
		_, s3Be := openLiveS3(t, "neg3-"+string(regime))
		eng, err := most.NewEngine(most.Config{
			Regime:            regime,
			RAMBudgetBytes:    ramBudget,
			AgentCount:        1,
			AllowedNamespaces: []string{namespace},
			NVMe:              most.NVMeConfig{Store: nvmeBe, Channels: 4},
			Remote:            s3Be,
		})
		if err != nil {
			t.Fatal(err)
		}
		for i := range ids {
			if err := eng.PutShared(ctx, ids[i], payloads[i], true); err != nil {
				t.Fatal(err)
			}
		}
		for rep := 0; rep < 6; rep++ {
			for i := 0; i < numBlocks; i++ {
				if _, err := eng.ReadShared(ctx, "agent-0", ids[i]); err != nil {
					t.Fatal(err)
				}
			}
		}
		return eng.Metrics()
	}

	mPin := run(most.RegimeNaiveFullRAMPin)
	mMig := run(most.RegimeExclusiveMigration)
	mMOST := run(most.RegimeMOSTSelectiveMirror)

	if mMig.NVMeDemotionWrites != 0 {
		t.Fatalf("ample RAM exclusive migration should have 0 demotions, got %d", mMig.NVMeDemotionWrites)
	}
	if mMOST.RAMPerAgentBytes <= mPin.RAMPerAgentBytes {
		t.Fatalf("expected MOST RAM/agent (%.0f) > naive pin (%.0f) due to tracker metadata overhead when M=1 and RAM>=WS",
			mMOST.RAMPerAgentBytes, mPin.RAMPerAgentBytes)
	}
}
