package most

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
)

func TestSharedDataIntegrityAndDigestVerification(t *testing.T) {
	algos := []HashAlgo{AlgoBLAKE3, AlgoSHA256}
	const blockSize = 4096

	for _, algo := range algos {
		for _, pol := range AllPolicies() {
			t.Run(fmt.Sprintf("%s/%s", algo, pol), func(t *testing.T) {
				verify := true
				eng, err := NewEngine(Config{
					Policy:          pol,
					NumAgents:       4,
					BlockSize:       blockSize,
					HostRAMBudget:   32 * blockSize,
					MirrorFraction:  0.25,
					MinMirrorHits:   2,
					Shards:          4,
					HashAlgo:        algo,
					VerifyNVMeReads: &verify,
				})
				if err != nil {
					t.Fatalf("NewEngine: %v", err)
				}
				defer eng.Close()

				payload := makeDeterministicBlock(blockSize, 0x1234, 7)
				key, err := eng.RegisterSharedToolchain("go-1.27", payload)
				if err != nil {
					t.Fatalf("RegisterSharedToolchain: %v", err)
				}

				ctx := AgentContext{AgentID: "a0", TenantID: "t0", VolumeID: "v0"}
				got := make([]byte, blockSize)

				// Multiple reads exercise both NVMe miss and RAM hit paths.
				for i := 0; i < 5; i++ {
					if _, err := eng.ReadShared(ctx, key, got); err != nil {
						t.Fatalf("ReadShared iter %d: %v", i, err)
					}
					if !bytes.Equal(got, payload) {
						t.Fatalf("bit-exact mismatch on iter %d", i)
					}
				}

				// Unattested block must fail closed.
				unattested := SharedToolchainKey{
					ToolchainID: "python-3.12",
					ContentHash: ComputeDigest(algo, []byte("not-registered")),
				}
				if _, err := eng.ReadShared(ctx, unattested, got); !errors.Is(err, ErrUnattestedBlock) {
					t.Fatalf("expected ErrUnattestedBlock, got %v", err)
				}

				// Wrong buffer size must fail cleanly.
				if _, err := eng.ReadShared(ctx, key, make([]byte, blockSize-1)); !errors.Is(err, ErrBlockSizeMismatch) {
					t.Fatalf("expected ErrBlockSizeMismatch, got %v", err)
				}
			})
		}
	}
}

func TestCorruptNVMeSharedBlockRejectedByDigest(t *testing.T) {
	const blockSize = 4096
	verify := true
	eng, err := NewEngine(Config{
		Policy:          PolicyMOSTSelectiveMirror,
		NumAgents:       2,
		BlockSize:       blockSize,
		HostRAMBudget:   16 * blockSize,
		MinMirrorHits:   3,
		HashAlgo:        AlgoBLAKE3,
		VerifyNVMeReads: &verify,
	})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	defer eng.Close()

	payload := makeDeterministicBlock(blockSize, 0xCAFE, 1)
	key, err := eng.RegisterSharedToolchain("rust-1.85", payload)
	if err != nil {
		t.Fatalf("RegisterSharedToolchain: %v", err)
	}

	// Corrupt a byte directly in the backing NVMe file on disk.
	f, err := os.OpenFile(eng.sharedNVMe.Path(), os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("open nvme backing file: %v", err)
	}
	var b [1]byte
	if _, err := f.ReadAt(b[:], 0); err != nil {
		_ = f.Close()
		t.Fatalf("read backing byte: %v", err)
	}
	b[0] ^= 0xff
	if _, err := f.WriteAt(b[:], 0); err != nil {
		_ = f.Close()
		t.Fatalf("corrupt backing byte: %v", err)
	}
	_ = f.Close()

	ctx := AgentContext{AgentID: "a0", TenantID: "t0", VolumeID: "v0"}
	dst := make([]byte, blockSize)
	if _, err := eng.ReadShared(ctx, key, dst); !errors.Is(err, ErrDigestMismatch) {
		t.Fatalf("expected ErrDigestMismatch on corrupted NVMe block, got %v", err)
	}
	if st := eng.Stats(); st.DigestMismatchCount == 0 {
		t.Fatalf("expected DigestMismatchCount > 0, got %+v", st)
	}
}

func TestStrictTenantIsolationZeroLeaks(t *testing.T) {
	const blockSize = 4096

	for _, pol := range AllPolicies() {
		t.Run(pol.String(), func(t *testing.T) {
			eng, err := NewEngine(Config{
				Policy:        pol,
				NumAgents:     4,
				BlockSize:     blockSize,
				HostRAMBudget: 32 * blockSize,
				MinMirrorHits: 2,
				Shards:        4,
			})
			if err != nil {
				t.Fatalf("NewEngine: %v", err)
			}
			defer eng.Close()

			ctxA := AgentContext{AgentID: "agent-a", TenantID: "tenant-alpha", VolumeID: "vol-alpha"}
			ctxB := AgentContext{AgentID: "agent-b", TenantID: "tenant-beta", VolumeID: "vol-beta"}
			ctxEmpty := AgentContext{AgentID: "agent-empty"}

			keyA := PrivateBlockKey{TenantID: ctxA.TenantID, VolumeID: ctxA.VolumeID, BlockID: 42}
			keyAMissing := PrivateBlockKey{TenantID: ctxA.TenantID, VolumeID: ctxA.VolumeID, BlockID: 9999}
			secretA := makeDeterministicBlock(blockSize, 0x5E4EC7, 42)

			if err := eng.RegisterPrivateBlock(ctxA, keyA, secretA); err != nil {
				t.Fatalf("RegisterPrivateBlock tenant A: %v", err)
			}
			// Tenant B cannot register into Tenant A's namespace.
			if err := eng.RegisterPrivateBlock(ctxB, keyA, secretA); !errors.Is(err, ErrTenantAccessDenied) {
				t.Fatalf("expected ErrTenantAccessDenied on cross-tenant register, got %v", err)
			}

			// Warm Tenant A's private block in RAM across repeated reads.
			dst := make([]byte, blockSize)
			for i := 0; i < 4; i++ {
				if _, err := eng.ReadPrivate(ctxA, keyA, dst); err != nil {
					t.Fatalf("ReadPrivate tenant A iter %d: %v", i, err)
				}
				if !bytes.Equal(dst, secretA) {
					t.Fatalf("tenant A private read mismatch")
				}
			}

			// Invariant 1: Private Tenant A block MUST NEVER enter shared RAM tier.
			sharedEntries, unattested := eng.AuditSharedTierIntegrity()
			if sharedEntries != 0 || unattested != 0 {
				t.Fatalf("private block polluted shared tier: sharedEntries=%d unattested=%d", sharedEntries, unattested)
			}

			// Invariant 2: Cross-tenant probe on BOTH existing and missing private keys
			// MUST return identical ErrTenantAccessDenied (zero existence oracle).
			probeBuf := make([]byte, blockSize)
			if _, err := eng.ReadPrivate(ctxB, keyA, probeBuf); !errors.Is(err, ErrTenantAccessDenied) {
				t.Fatalf("expected ErrTenantAccessDenied for Tenant B reading Tenant A existing block, got %v", err)
			}
			if _, err := eng.ReadPrivate(ctxB, keyAMissing, probeBuf); !errors.Is(err, ErrTenantAccessDenied) {
				t.Fatalf("expected ErrTenantAccessDenied for Tenant B reading Tenant A missing block, got %v", err)
			}
			if _, err := eng.ReadPrivate(ctxEmpty, keyA, probeBuf); !errors.Is(err, ErrTenantAccessDenied) {
				t.Fatalf("expected ErrTenantAccessDenied for empty tenant context, got %v", err)
			}
			// Same tenant, wrong volume must also fail closed.
			ctxAWrongVol := AgentContext{AgentID: "agent-a", TenantID: ctxA.TenantID, VolumeID: "vol-other"}
			if _, err := eng.ReadPrivate(ctxAWrongVol, keyA, probeBuf); !errors.Is(err, ErrTenantAccessDenied) {
				t.Fatalf("expected ErrTenantAccessDenied for mismatched volume, got %v", err)
			}

			// Invariant 3: Forging a SharedToolchainKey with Tenant A's private digest
			// MUST fail closed with ErrUnattestedBlock.
			forgedShared := SharedToolchainKey{
				ToolchainID: "private-leak-attempt",
				ContentHash: ComputeDigest(AlgoBLAKE3, secretA),
			}
			if _, err := eng.ReadShared(ctxB, forgedShared, probeBuf); !errors.Is(err, ErrUnattestedBlock) {
				t.Fatalf("expected ErrUnattestedBlock for forged private digest, got %v", err)
			}

			// Confirm zero bytes of secretA were written into probeBuf.
			if bytes.Contains(probeBuf, secretA[:32]) {
				t.Fatalf("cross-tenant probe buffer leaked tenant A bytes")
			}
		})
	}
}

func TestZeroWritebackEvictionVsExclusiveMigration(t *testing.T) {
	const (
		blockSize = 8192
		numBlocks = 64
	)

	runPolicy := func(pol Policy) (Stats, error) {
		eng, err := NewEngine(Config{
			Policy:               pol,
			NumAgents:            4,
			BlockSize:            blockSize,
			HostRAMBudget:        16 * blockSize, // 8 shared + 8 private
			PrivateShareFraction: 0.50,
			MirrorFraction:       0.25, // 4 blocks in MOST shared mirror
			MinMirrorHits:        2,
			Shards:               2,
		})
		if err != nil {
			return Stats{}, err
		}
		defer eng.Close()

		keys := make([]SharedToolchainKey, numBlocks)
		payloads := make([][]byte, numBlocks)
		for i := 0; i < numBlocks; i++ {
			payloads[i] = makeDeterministicBlock(blockSize, 0xE71C7, uint64(i))
			k, err := eng.RegisterSharedToolchain("python-3.12", payloads[i])
			if err != nil {
				return Stats{}, err
			}
			keys[i] = k
		}

		ctx := AgentContext{AgentID: "a0", TenantID: "t0", VolumeID: "v0"}
		buf := make([]byte, blockSize)
		// Touch every block multiple times to force sustained RAM evictions.
		for round := 0; round < 5; round++ {
			for i := 0; i < numBlocks; i++ {
				if _, err := eng.ReadShared(ctx, keys[i], buf); err != nil {
					return Stats{}, fmt.Errorf("read round=%d i=%d: %w", round, i, err)
				}
				if !bytes.Equal(buf, payloads[i]) {
					return Stats{}, fmt.Errorf("data mismatch round=%d i=%d", round, i)
				}
			}
		}
		return eng.Stats(), nil
	}

	mostStats, err := runPolicy(PolicyMOSTSelectiveMirror)
	if err != nil {
		t.Fatalf("MOST run failed: %v", err)
	}
	exclStats, err := runPolicy(PolicyExclusiveMigration)
	if err != nil {
		t.Fatalf("Exclusive run failed: %v", err)
	}

	if mostStats.NVMeEvictionWriteOps != 0 || mostStats.NVMeEvictionWriteByte != 0 {
		t.Fatalf("MOST must have ZERO NVMe eviction writes, got ops=%d bytes=%d",
			mostStats.NVMeEvictionWriteOps, mostStats.NVMeEvictionWriteByte)
	}
	if exclStats.NVMeEvictionWriteOps == 0 || exclStats.NVMeEvictionWriteByte == 0 {
		t.Fatalf("Exclusive migration must incur NVMe eviction writes > 0, got ops=%d bytes=%d",
			exclStats.NVMeEvictionWriteOps, exclStats.NVMeEvictionWriteByte)
	}
	t.Logf("Zero-writeback verified: MOST eviction writes = 0 bytes (evictions=%d), Exclusive migration eviction writes = %d ops (%.2f MiB)",
		mostStats.RAMEvictions, exclStats.NVMeEvictionWriteOps, exclStats.NVMeEvictionWriteMiB)
}

func TestPrimarySkewedBurstMOSTOutperformsBaselines(t *testing.T) {
	spec := DefaultScenarios()[0]
	spec.OpsPerAgent = 600 // 32 * 600 = 19,200 concurrent reads (fast under -race)

	results := make(map[Policy]PolicyResult, 4)
	for _, pol := range AllPolicies() {
		res, err := RunScenarioPolicy(spec, pol)
		if err != nil {
			t.Fatalf("policy %s failed: %v", pol, err)
		}
		if !res.Isolation.Passed() {
			t.Fatalf("policy %s failed tenant isolation audit: %+v", pol, res.Isolation)
		}
		results[pol] = res
		t.Logf("%-34s p50=%6.2fus p99=%8.2fus RAM/ag=%.3fMiB hit=%5.2f%% evictWr=%.2fMiB ops/s=%.0f",
			res.Policy, res.P50LatencyUs, res.P99LatencyUs,
			res.Stats.ResidentRAMPerAgentMiB, res.Stats.RAMHitRatePct,
			res.Stats.NVMeEvictionWriteMiB, res.ThroughputOpsSec)
	}

	priv := results[PolicyPrivatePerAgentLRU]
	excl := results[PolicyExclusiveMigration]
	mostRes := results[PolicyMOSTSelectiveMirror]
	mAll := results[PolicyMirrorAllNegative]

	if mostRes.Stats.NVMeEvictionWriteOps != 0 {
		t.Fatalf("MOST must have 0 NVMe eviction writes, got %d", mostRes.Stats.NVMeEvictionWriteOps)
	}
	if excl.Stats.NVMeEvictionWriteOps == 0 {
		t.Fatalf("Exclusive migration must incur NVMe eviction writes > 0")
	}
	if mostRes.Stats.ResidentRAMPerAgentMiB >= priv.Stats.ResidentRAMPerAgentMiB {
		t.Fatalf("expected MOST ResidentRAM/agent (%.3f MiB) < PrivateLRU (%.3f MiB)",
			mostRes.Stats.ResidentRAMPerAgentMiB, priv.Stats.ResidentRAMPerAgentMiB)
	}
	if mostRes.Stats.ResidentRAMPerAgentMiB >= mAll.Stats.ResidentRAMPerAgentMiB {
		t.Fatalf("expected MOST ResidentRAM/agent (%.3f MiB) < MirrorAll (%.3f MiB)",
			mostRes.Stats.ResidentRAMPerAgentMiB, mAll.Stats.ResidentRAMPerAgentMiB)
	}
	if mostRes.Stats.RAMHitRatePct <= priv.Stats.RAMHitRatePct {
		t.Fatalf("expected MOST RAM hit rate (%.2f%%) > PrivateLRU (%.2f%%)",
			mostRes.Stats.RAMHitRatePct, priv.Stats.RAMHitRatePct)
	}
	if mostRes.P99LatencyUs >= priv.P99LatencyUs {
		t.Fatalf("expected MOST p99 (%.2f us) < PrivateLRU p99 (%.2f us)",
			mostRes.P99LatencyUs, priv.P99LatencyUs)
	}
	if mostRes.P99LatencyUs >= excl.P99LatencyUs {
		t.Fatalf("expected MOST p99 (%.2f us) < ExclusiveMigration p99 (%.2f us)",
			mostRes.P99LatencyUs, excl.P99LatencyUs)
	}
}

func TestNegativeRegimesHonestBoundaries(t *testing.T) {
	scenarios := DefaultScenarios()

	t.Run("uniform_flat_access_no_hot_skew", func(t *testing.T) {
		spec := scenarios[1]
		spec.OpsPerAgent = 400
		mostRes, err := RunScenarioPolicy(spec, PolicyMOSTSelectiveMirror)
		if err != nil {
			t.Fatalf("MOST uniform failed: %v", err)
		}
		exclRes, err := RunScenarioPolicy(spec, PolicyExclusiveMigration)
		if err != nil {
			t.Fatalf("Exclusive uniform failed: %v", err)
		}
		if !mostRes.Isolation.Passed() || !exclRes.Isolation.Passed() {
			t.Fatalf("isolation audit failed")
		}
		// Honest negative result: without hot-set skew and working set >> RAM,
		// selective mirroring cannot achieve high RAM hit rate.
		if mostRes.Stats.RAMHitRatePct > 30.0 {
			t.Fatalf("unexpectedly high RAM hit rate under uniform 4x overflow: %.2f%%", mostRes.Stats.RAMHitRatePct)
		}
		if mostRes.Stats.NVMeEvictionWriteOps != 0 {
			t.Fatalf("MOST must still incur 0 NVMe eviction writes under uniform access, got %d", mostRes.Stats.NVMeEvictionWriteOps)
		}
		if exclRes.Stats.NVMeEvictionWriteOps == 0 {
			t.Fatalf("Exclusive migration must incur NVMe eviction writes under uniform overflow")
		}
		t.Logf("Uniform flat negative verified: MOST hit=%.2f%% p99=%.2fus evictWr=0.00MiB | Exclusive hit=%.2f%% p99=%.2fus evictWr=%.2fMiB",
			mostRes.Stats.RAMHitRatePct, mostRes.P99LatencyUs,
			exclRes.Stats.RAMHitRatePct, exclRes.P99LatencyUs, exclRes.Stats.NVMeEvictionWriteMiB)
	})

	t.Run("mutable_private_workspace_heavy_workload", func(t *testing.T) {
		spec := scenarios[2]
		spec.OpsPerAgent = 450
		privRes, err := RunScenarioPolicy(spec, PolicyPrivatePerAgentLRU)
		if err != nil {
			t.Fatalf("PrivateLRU private-heavy failed: %v", err)
		}
		mostRes, err := RunScenarioPolicy(spec, PolicyMOSTSelectiveMirror)
		if err != nil {
			t.Fatalf("MOST private-heavy failed: %v", err)
		}
		if !privRes.Isolation.Passed() || !mostRes.Isolation.Passed() {
			t.Fatalf("isolation audit failed")
		}
		// Honest boundary: 90% private workload prevents cross-tenant deduplication,
		// so MOST resident RAM/agent rises close to private-LRU (>= 70% of baseline).
		ratio := mostRes.Stats.ResidentRAMPerAgentMiB / privRes.Stats.ResidentRAMPerAgentMiB
		if ratio < 0.70 {
			t.Fatalf("expected private-heavy MOST RAM/agent to approach PrivateLRU (ratio >= 0.70), got %.3f (MOST=%.3f, Priv=%.3f)",
				ratio, mostRes.Stats.ResidentRAMPerAgentMiB, privRes.Stats.ResidentRAMPerAgentMiB)
		}
		t.Logf("Private-heavy boundary verified: MOST RAM/ag=%.3fMiB vs PrivateLRU=%.3fMiB (ratio=%.2f, 0 leaks)",
			mostRes.Stats.ResidentRAMPerAgentMiB, privRes.Stats.ResidentRAMPerAgentMiB, ratio)
	})

	t.Run("mirror_all_ram_thrash_collapse", func(t *testing.T) {
		spec := scenarios[3]
		spec.OpsPerAgent = 500
		mostRes, err := RunScenarioPolicy(spec, PolicyMOSTSelectiveMirror)
		if err != nil {
			t.Fatalf("MOST thrash failed: %v", err)
		}
		mAllRes, err := RunScenarioPolicy(spec, PolicyMirrorAllNegative)
		if err != nil {
			t.Fatalf("MirrorAll thrash failed: %v", err)
		}
		if !mostRes.Isolation.Passed() || !mAllRes.Isolation.Passed() {
			t.Fatalf("isolation audit failed")
		}
		if mostRes.Stats.AdmissionsRejected == 0 {
			t.Fatalf("expected MOST frequency filter to reject cold scan blocks")
		}
		if mostRes.Stats.ResidentRAMPerAgentMiB >= mAllRes.Stats.ResidentRAMPerAgentMiB {
			t.Fatalf("expected MOST RAM/agent (%.3f) < MirrorAll (%.3f)",
				mostRes.Stats.ResidentRAMPerAgentMiB, mAllRes.Stats.ResidentRAMPerAgentMiB)
		}
		if mostRes.P99LatencyUs >= mAllRes.P99LatencyUs {
			t.Fatalf("expected MOST p99 (%.2f us) < MirrorAll thrash p99 (%.2f us)",
				mostRes.P99LatencyUs, mAllRes.P99LatencyUs)
		}
		t.Logf("Mirror-all thrash verified: MOST p99=%.2fus RAM/ag=%.3fMiB rejected=%d | MirrorAll p99=%.2fus RAM/ag=%.3fMiB",
			mostRes.P99LatencyUs, mostRes.Stats.ResidentRAMPerAgentMiB, mostRes.Stats.AdmissionsRejected,
			mAllRes.P99LatencyUs, mAllRes.Stats.ResidentRAMPerAgentMiB)
	})
}

func TestConcurrent32AgentBurstStressRace(t *testing.T) {
	const (
		numAgents  = 32
		blockSize  = 4096
		sharedBlks = 128
		privBlks   = 8
		iters      = 150
	)

	for _, pol := range AllPolicies() {
		t.Run(pol.String(), func(t *testing.T) {
			eng, err := NewEngine(Config{
				Policy:               pol,
				NumAgents:            numAgents,
				BlockSize:            blockSize,
				HostRAMBudget:        128 * blockSize,
				PrivateShareFraction: 0.50,
				MirrorFraction:       0.25,
				MinMirrorHits:        3,
				Shards:               8,
			})
			if err != nil {
				t.Fatalf("NewEngine: %v", err)
			}
			defer eng.Close()

			sharedKeys := make([]SharedToolchainKey, sharedBlks)
			sharedData := make([][]byte, sharedBlks)
			for i := 0; i < sharedBlks; i++ {
				sharedData[i] = makeDeterministicBlock(blockSize, 0xA11CE, uint64(i))
				k, err := eng.RegisterSharedToolchain("node-22", sharedData[i])
				if err != nil {
					t.Fatalf("RegisterSharedToolchain %d: %v", i, err)
				}
				sharedKeys[i] = k
			}

			ctxs := make([]AgentContext, numAgents)
			privKeys := make([][]PrivateBlockKey, numAgents)
			privData := make([][][]byte, numAgents)
			for a := 0; a < numAgents; a++ {
				ctxs[a] = AgentContext{
					AgentID:  fmt.Sprintf("ag-%02d", a),
					TenantID: fmt.Sprintf("tn-%02d", a),
					VolumeID: fmt.Sprintf("vl-%02d", a),
				}
				privKeys[a] = make([]PrivateBlockKey, privBlks)
				privData[a] = make([][]byte, privBlks)
				for b := 0; b < privBlks; b++ {
					pk := PrivateBlockKey{TenantID: ctxs[a].TenantID, VolumeID: ctxs[a].VolumeID, BlockID: uint64(b)}
					privData[a][b] = makeDeterministicBlock(blockSize, uint64(a+100), uint64(b))
					if err := eng.RegisterPrivateBlock(ctxs[a], pk, privData[a][b]); err != nil {
						t.Fatalf("RegisterPrivateBlock a=%d b=%d: %v", a, b, err)
					}
					privKeys[a][b] = pk
				}
			}

			var wg sync.WaitGroup
			errCh := make(chan error, numAgents*2)

			for a := 0; a < numAgents; a++ {
				wg.Add(1)
				go func(agent int) {
					defer wg.Done()
					buf := make([]byte, blockSize)
					for i := 0; i < iters; i++ {
						switch i % 5 {
						case 0, 1, 2:
							idx := (i + agent) % 24 // hot shared set
							if i%15 == 0 {
								idx = (agent*7 + i) % sharedBlks // cold shared read
							}
							if _, err := eng.ReadShared(ctxs[agent], sharedKeys[idx], buf); err != nil {
								errCh <- fmt.Errorf("agent %d ReadShared %d: %w", agent, idx, err)
								return
							}
							if !bytes.Equal(buf, sharedData[idx]) {
								errCh <- fmt.Errorf("agent %d shared data mismatch idx=%d", agent, idx)
								return
							}
						case 3:
							b := i % privBlks
							if _, err := eng.ReadPrivate(ctxs[agent], privKeys[agent][b], buf); err != nil {
								errCh <- fmt.Errorf("agent %d ReadPrivate %d: %w", agent, b, err)
								return
							}
							if !bytes.Equal(buf, privData[agent][b]) {
								errCh <- fmt.Errorf("agent %d private data mismatch b=%d", agent, b)
								return
							}
						case 4:
							// Concurrent cross-tenant adversarial probe must always fail closed.
							victim := (agent + 1) % numAgents
							if _, err := eng.ReadPrivate(ctxs[agent], privKeys[victim][0], buf); !errors.Is(err, ErrTenantAccessDenied) {
								errCh <- fmt.Errorf("agent %d cross-tenant leak on victim %d: err=%v", agent, victim, err)
								return
							}
						}
					}
				}(a)
			}

			wg.Wait()
			close(errCh)
			for err := range errCh {
				t.Fatal(err)
			}

			_, unattested := eng.AuditSharedTierIntegrity()
			if unattested != 0 {
				t.Fatalf("found %d unattested entries in shared tier", unattested)
			}
			st := eng.Stats()
			if st.TenantDeniedCount < uint64(numAgents*(iters/5)) {
				t.Fatalf("expected >= %d tenant denials, got %d", numAgents*(iters/5), st.TenantDeniedCount)
			}
		})
	}
}
