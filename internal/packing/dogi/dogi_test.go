package dogi

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestBitExactSHA256IntegrityAcrossReclaimAndReopen(t *testing.T) {
	dir := t.TempDir()
	policy := NewPolicy(PolicyDOGIAdaptive)
	store, err := NewSlabStore(StoreConfig{
		RootDir:               dir,
		SlabTargetBytes:       64 << 10,
		MaxOpenSlabsPerTenant: 4,
	}, policy)
	if err != nil {
		t.Fatalf("NewSlabStore: %v", err)
	}

	type expectation struct {
		tenant  string
		payload []byte
		digest  [32]byte
		alive   bool
	}
	expected := make(map[uint64]*expectation)

	// Write a mix of ephemeral and long-lived objects across 2 tenants.
	for ep := int64(1); ep <= 6; ep++ {
		for _, tenant := range []string{"tenant-1", "tenant-2"} {
			if err := store.AdvanceEpoch(tenant, ep); err != nil {
				t.Fatalf("AdvanceEpoch: %v", err)
			}
			for i := 0; i < 10; i++ {
				var p string
				if i%2 == 0 {
					p = fmt.Sprintf("target/debug/obj_%d_%d.o", ep, i)
				} else {
					p = fmt.Sprintf("datasets/shard_%d_%d.parquet", ep, i)
				}
				payload := makePayload(uint64(ep*1000+int64(i))+tenantHash64(tenant), 12<<10)
				id, digest, err := store.Put(ObjectHint{
					TenantID: tenant,
					Path:     p,
					Epoch:    ep,
				}, payload)
				if err != nil {
					t.Fatalf("Put: %v", err)
				}
				expected[id] = &expectation{
					tenant:  tenant,
					payload: payload,
					digest:  digest,
					alive:   true,
				}
			}
		}
		// Delete ephemeral objects (odd/even split) and some long-lived objects to trigger
		// both zero-copy clean slab reclaim AND partial-live slab copy migration.
		for id, exp := range expected {
			if exp.alive && (id%2 == 1 || id%7 == 0) {
				if err := store.Delete(exp.tenant, id, ep); err != nil {
					t.Fatalf("Delete %d: %v", id, err)
				}
				exp.alive = false
			}
		}
		if err := store.FlushAll(); err != nil {
			t.Fatalf("FlushAll: %v", err)
		}
		if _, err := store.ReclaimDeadSlabs(0.30); err != nil {
			t.Fatalf("ReclaimDeadSlabs: %v", err)
		}
	}

	if err := store.VerifyTenantIsolation(); err != nil {
		t.Fatalf("VerifyTenantIsolation: %v", err)
	}
	if err := store.SaveIndex(); err != nil {
		t.Fatalf("SaveIndex: %v", err)
	}

	// Reopen from real on-disk index + .pack files and verify EVERY live object bit-for-bit.
	reopened, err := ReopenSlabStore(StoreConfig{
		RootDir:               dir,
		SlabTargetBytes:       64 << 10,
		MaxOpenSlabsPerTenant: 4,
	}, NewPolicy(PolicyDOGIAdaptive))
	if err != nil {
		t.Fatalf("ReopenSlabStore: %v", err)
	}

	verified := 0
	var sampleLiveID uint64
	var sampleTenant string
	for id, exp := range expected {
		got, err := reopened.Get(exp.tenant, id)
		if !exp.alive {
			if !errors.Is(err, ErrNotFound) {
				t.Fatalf("deleted obj %d: expected ErrNotFound, got %v", id, err)
			}
			continue
		}
		if err != nil {
			t.Fatalf("reopened.Get(%d): %v", id, err)
		}
		if !bytes.Equal(got, exp.payload) {
			t.Fatalf("reopened obj %d payload mismatch", id)
		}
		if sha256.Sum256(got) != exp.digest {
			t.Fatalf("reopened obj %d sha256 mismatch", id)
		}
		verified++
		sampleLiveID = id
		sampleTenant = exp.tenant
	}
	if verified == 0 {
		t.Fatalf("expected >0 surviving objects to verify")
	}

	// Bit-flip corruption check: corrupt a byte inside the live slab file and
	// verify Get rejects it with ErrCorruptChecksum.
	reopened.mu.Lock()
	loc := reopened.objects[sampleLiveID]
	slabPath := reopened.sealedByID[loc.SlabID].Path
	flipOffset := loc.Offset + recordHeaderSize + 10
	reopened.mu.Unlock()

	raw, err := os.ReadFile(slabPath)
	if err != nil {
		t.Fatalf("ReadFile slab: %v", err)
	}
	raw[flipOffset] ^= 0xFF
	if err := os.WriteFile(slabPath, raw, 0o644); err != nil {
		t.Fatalf("WriteFile corrupted slab: %v", err)
	}
	if _, err := reopened.Get(sampleTenant, sampleLiveID); !errors.Is(err, ErrCorruptChecksum) {
		t.Fatalf("expected ErrCorruptChecksum after bit flip in %s, got %v", filepath.Base(slabPath), err)
	}
}

func TestDOGIReducesLiveCopyAndWAFOverBaseline(t *testing.T) {
	ops := GenerateWorkload(WorkloadAIAgentBuildChurn, 42)
	const slabTarget = 256 << 10
	const maxOpenPerTenant = 4

	results := make(map[PolicyKind]RunResult, len(AllPolicies))
	for _, p := range AllPolicies {
		res, err := RunWorkload(WorkloadAIAgentBuildChurn, ops, p, slabTarget, maxOpenPerTenant)
		if err != nil {
			t.Fatalf("RunWorkload(%s): %v", p, err)
		}
		if !res.TenantIsolationVerified {
			t.Fatalf("policy %s failed tenant isolation", p)
		}
		if res.Metrics.IntegrityErrors != 0 {
			t.Fatalf("policy %s had %d integrity errors", p, res.Metrics.IntegrityErrors)
		}
		if res.LiveObjectsVerified == 0 {
			t.Fatalf("policy %s verified 0 live objects", p)
		}
		// 2 tenants * maxOpenPerTenant = 8 open slabs max.
		if res.Metrics.PeakOpenSlabs > 2*maxOpenPerTenant {
			t.Fatalf("policy %s exceeded bounded open slab cap: got %d > %d",
				p, res.Metrics.PeakOpenSlabs, 2*maxOpenPerTenant)
		}
		results[p] = res
	}

	base := results[PolicyMixedFIFO].Metrics
	sizeB := results[PolicySizeBucket].Metrics
	statP := results[PolicyStaticPath].Metrics
	dogiM := results[PolicyDOGIAdaptive].Metrics

	t.Logf("mixed_fifo:   live_copied=%dB waf=%.3f zc_rate=%.3f space_amp=%.3f",
		base.LiveBytesCopied, base.WAF, base.ZeroCopyReclaimRate, base.FinalSpaceAmp)
	t.Logf("size_bucket:  live_copied=%dB waf=%.3f zc_rate=%.3f space_amp=%.3f",
		sizeB.LiveBytesCopied, sizeB.WAF, sizeB.ZeroCopyReclaimRate, sizeB.FinalSpaceAmp)
	t.Logf("static_path:  live_copied=%dB waf=%.3f zc_rate=%.3f space_amp=%.3f",
		statP.LiveBytesCopied, statP.WAF, statP.ZeroCopyReclaimRate, statP.FinalSpaceAmp)
	t.Logf("dogi_adapt:   live_copied=%dB waf=%.3f zc_rate=%.3f space_amp=%.3f",
		dogiM.LiveBytesCopied, dogiM.WAF, dogiM.ZeroCopyReclaimRate, dogiM.FinalSpaceAmp)

	// DOGI must reduce live-data copying by at least 50% vs mixed FIFO baseline
	// and also beat both size-bucket and static-path heuristics.
	if dogiM.LiveBytesCopied*2 >= base.LiveBytesCopied {
		t.Fatalf("expected DOGI live bytes copied (%d) < 50%% of mixed FIFO (%d)",
			dogiM.LiveBytesCopied, base.LiveBytesCopied)
	}
	if dogiM.LiveBytesCopied >= sizeB.LiveBytesCopied {
		t.Fatalf("expected DOGI live bytes copied (%d) < size bucket (%d)",
			dogiM.LiveBytesCopied, sizeB.LiveBytesCopied)
	}
	if dogiM.LiveBytesCopied >= statP.LiveBytesCopied {
		t.Fatalf("expected DOGI live bytes copied (%d) < static path (%d)",
			dogiM.LiveBytesCopied, statP.LiveBytesCopied)
	}
	if dogiM.WAF >= base.WAF {
		t.Fatalf("expected DOGI WAF (%.3f) < mixed FIFO WAF (%.3f)", dogiM.WAF, base.WAF)
	}
	if dogiM.ZeroCopyReclaimRate <= base.ZeroCopyReclaimRate {
		t.Fatalf("expected DOGI zero-copy reclaim rate (%.3f) > mixed FIFO (%.3f)",
			dogiM.ZeroCopyReclaimRate, base.ZeroCopyReclaimRate)
	}
}

func TestOnlineClassifierTemporalCausality(t *testing.T) {
	// Two identical classifiers at epoch 0 must classify an opaque path
	// identically; future events cannot influence past decisions, and
	// out-of-order epochs must be rejected.
	cA := NewOnlineSurvivalClassifier(DefaultOnlineClassifierConfig())
	cB := NewOnlineSurvivalClassifier(DefaultOnlineClassifierConfig())

	hint0 := ObjectHint{
		TenantID: "tenant-causal",
		Path:     "opaque_dir/stage/item_001.bin",
		Size:     16 << 10,
		Epoch:    1,
	}
	// Static prior for .bin is ClassLongLived before any deletes are observed.
	pA0 := cA.Classify(hint0)
	pB0 := cB.Classify(hint0)
	if pA0 != ClassLongLived || pA0 != pB0 {
		t.Fatalf("expected identical cold-start prior ClassLongLived, got %v and %v", pA0, pB0)
	}

	// Record writes at epoch 1 in both, then diverge ONLY in future epoch 2.
	for i := uint64(1); i <= 10; i++ {
		if err := cA.ObserveWrite("tenant-causal", i, hint0.Path, 1); err != nil {
			t.Fatalf("cA ObserveWrite: %v", err)
		}
		if err := cB.ObserveWrite("tenant-causal", i, hint0.Path, 1); err != nil {
			t.Fatalf("cB ObserveWrite: %v", err)
		}
	}
	// At epoch 1 (before any deletes have occurred), both still classify identically.
	if cA.Classify(hint0) != cB.Classify(hint0) {
		t.Fatalf("causal violation: divergence before observations occurred")
	}

	// In cA, delete all 10 objects at epoch 2 (short-horizon deaths); in cB,
	// advance to epoch 10 with 0 deletes (long survivals).
	for i := uint64(1); i <= 10; i++ {
		if err := cA.ObserveDelete("tenant-causal", i, 2); err != nil {
			t.Fatalf("cA ObserveDelete: %v", err)
		}
	}
	if err := cB.AdvanceEpoch("tenant-causal", 10); err != nil {
		t.Fatalf("cB AdvanceEpoch: %v", err)
	}

	hintFuture := ObjectHint{
		TenantID: "tenant-causal",
		Path:     "opaque_dir/stage/item_999.bin",
		Size:     16 << 10,
		Epoch:    10,
	}
	if got := cA.Classify(hintFuture); got != ClassEphemeral {
		t.Fatalf("expected cA to adapt to ClassEphemeral after 10 short deaths, got %v", got)
	}
	if got := cB.Classify(hintFuture); got != ClassLongLived {
		t.Fatalf("expected cB to remain ClassLongLived after 10 long survivals, got %v", got)
	}

	// Non-monotonic epoch (going backward in time) must fail explicitly.
	if err := cA.ObserveWrite("tenant-causal", 999, hint0.Path, 1); !errors.Is(err, ErrNonMonotonicEpoch) {
		t.Fatalf("expected ErrNonMonotonicEpoch when epoch regresses, got %v", err)
	}
}

func TestMultiTenantSlabIsolationConcurrent(t *testing.T) {
	dir := t.TempDir()
	policy := NewPolicy(PolicyDOGIAdaptive)
	store, err := NewSlabStore(StoreConfig{
		RootDir:               dir,
		SlabTargetBytes:       64 << 10,
		MaxOpenSlabsPerTenant: 4,
	}, policy)
	if err != nil {
		t.Fatalf("NewSlabStore: %v", err)
	}

	// Verify classifier isolation: tenant-0 deletes many items in shared path
	// "work/shared_name/blob.bin", while tenant-1 writes long-lived items there.
	tenants := []string{"tenant-0", "tenant-1", "tenant-2", "tenant-3"}
	type writtenItem struct {
		tenant  string
		id      uint64
		payload []byte
	}

	var (
		wg    sync.WaitGroup
		mu    sync.Mutex
		items []writtenItem
		errs  []error
	)

	recordErr := func(e error) {
		mu.Lock()
		errs = append(errs, e)
		mu.Unlock()
	}

	for ti, tenant := range tenants {
		wg.Add(1)
		go func(idx int, tid string) {
			defer wg.Done()
			for ep := int64(1); ep <= 8; ep++ {
				if err := store.AdvanceEpoch(tid, ep); err != nil {
					recordErr(err)
					return
				}
				var epIDs []uint64
				for j := 0; j < 6; j++ {
					p := fmt.Sprintf("work/shared_name/blob_%d_%d.bin", ep, j)
					if idx >= 2 && j%2 == 0 {
						p = fmt.Sprintf("target/cache/obj_%d_%d.o", ep, j)
					}
					data := makePayload(uint64(idx*10000+int(ep)*100+j), 14<<10)
					id, _, err := store.Put(ObjectHint{
						TenantID: tid,
						Path:     p,
						Epoch:    ep,
					}, data)
					if err != nil {
						recordErr(err)
						return
					}
					epIDs = append(epIDs, id)
					if idx != 0 {
						mu.Lock()
						items = append(items, writtenItem{tenant: tid, id: id, payload: data})
						mu.Unlock()
					}
				}
				// tenant-0 aggressively deletes every object in work/shared_name/
				// within the same epoch; tenant-1 keeps all of them alive.
				if idx == 0 {
					for _, id := range epIDs {
						if err := store.Delete(tid, id, ep); err != nil {
							recordErr(err)
							return
						}
					}
				}
				if ep%3 == 0 {
					if _, err := store.ReclaimDeadSlabs(0.50); err != nil {
						recordErr(err)
						return
					}
				}
			}
		}(ti, tenant)
	}
	wg.Wait()

	if len(errs) > 0 {
		t.Fatalf("concurrent errors: %v", errs[0])
	}
	if err := store.FlushAll(); err != nil {
		t.Fatalf("FlushAll: %v", err)
	}
	if err := store.VerifyTenantIsolation(); err != nil {
		t.Fatalf("VerifyTenantIsolation: %v", err)
	}

	// Verify per-tenant classifier isolation: tenant-0 adapted to ClassEphemeral,
	// while tenant-1 still classifies the exact same path as ClassLongLived!
	dogiPol := policy.(*dogiAdaptivePolicy)
	sharedPath := "work/shared_name/check.bin"
	gotT0 := dogiPol.Classifier().Classify(ObjectHint{TenantID: "tenant-0", Path: sharedPath, Epoch: 9})
	gotT1 := dogiPol.Classifier().Classify(ObjectHint{TenantID: "tenant-1", Path: sharedPath, Epoch: 9})
	if gotT0 != ClassEphemeral {
		t.Fatalf("expected tenant-0 to classify %s as ephemeral, got %v", sharedPath, gotT0)
	}
	if gotT1 != ClassLongLived {
		t.Fatalf("expected tenant-1 to stay long_lived for %s (isolated from tenant-0), got %v", sharedPath, gotT1)
	}

	// Verify every surviving object reads back cleanly for its owner and fails
	// with ErrTenantMismatch for any other tenant.
	for _, it := range items {
		got, err := store.Get(it.tenant, it.id)
		if err != nil {
			t.Fatalf("owner Get(%s, %d): %v", it.tenant, it.id, err)
		}
		if !bytes.Equal(got, it.payload) {
			t.Fatalf("payload mismatch for (%s, %d)", it.tenant, it.id)
		}
		other := "tenant-0"
		if it.tenant == other {
			other = "tenant-1"
		}
		if _, err := store.Get(other, it.id); !errors.Is(err, ErrTenantMismatch) {
			t.Fatalf("cross-tenant Get(%s, %d): expected ErrTenantMismatch, got %v", other, it.id, err)
		}
		if err := store.Delete(other, it.id, 10); !errors.Is(err, ErrTenantMismatch) {
			t.Fatalf("cross-tenant Delete(%s, %d): expected ErrTenantMismatch, got %v", other, it.id, err)
		}
	}
}

func TestNegativeRegimeUniformRandomDelete(t *testing.T) {
	ops := GenerateWorkload(WorkloadUniformRandomDelete, 99)
	baseRes, err := RunWorkload(WorkloadUniformRandomDelete, ops, PolicyMixedFIFO, 256<<10, 4)
	if err != nil {
		t.Fatalf("baseline: %v", err)
	}
	dogiRes, err := RunWorkload(WorkloadUniformRandomDelete, ops, PolicyDOGIAdaptive, 256<<10, 4)
	if err != nil {
		t.Fatalf("dogi: %v", err)
	}

	// Honest negative boundary: when every object has an identical memoryless
	// lifetime distribution (zero lifetime skew), DOGI should NOT achieve a
	// large reduction in live-data copying, and uses >= baseline open slabs.
	baseCopy := float64(baseRes.Metrics.LiveBytesCopied)
	dogiCopy := float64(dogiRes.Metrics.LiveBytesCopied)
	reduction := 0.0
	if baseCopy > 0 {
		reduction = (baseCopy - dogiCopy) / baseCopy
	}
	t.Logf("uniform_random_delete: base_copy=%dB dogi_copy=%dB reduction=%.2f%% base_open=%d dogi_open=%d",
		baseRes.Metrics.LiveBytesCopied, dogiRes.Metrics.LiveBytesCopied, reduction*100,
		baseRes.Metrics.PeakOpenSlabs, dogiRes.Metrics.PeakOpenSlabs)

	if reduction > 0.20 {
		t.Fatalf("unexpectedly large reduction (%.2f%%) under zero-skew memoryless deletes", reduction*100)
	}
	if dogiRes.Metrics.PeakOpenSlabs < baseRes.Metrics.PeakOpenSlabs {
		t.Fatalf("expected DOGI open slabs (%d) >= mixed FIFO (%d)",
			dogiRes.Metrics.PeakOpenSlabs, baseRes.Metrics.PeakOpenSlabs)
	}
}

func TestNegativeRegimeOverFragmented64Buckets(t *testing.T) {
	ops := GenerateWorkload(WorkloadOverFragmentedUnderfill, 77)
	dogiRes, err := RunWorkload(WorkloadOverFragmentedUnderfill, ops, PolicyDOGIAdaptive, 256<<10, 4)
	if err != nil {
		t.Fatalf("dogi: %v", err)
	}
	// Run over-fragmented policy with an uncapped 64-slab pool to expose open-slab
	// RAM overhead and periodic flush underfill directly.
	overRes, err := RunWorkload(WorkloadOverFragmentedUnderfill, ops, PolicyOverFragmented, 256<<10, 64)
	if err != nil {
		t.Fatalf("over_fragmented: %v", err)
	}

	t.Logf("over_fragmented_underfill: dogi(fill=%.3f slabs=%d openRAM=%dB) vs 64bucket(fill=%.3f slabs=%d openRAM=%dB)",
		dogiRes.Metrics.AvgSlabFillRatio, dogiRes.Metrics.SlabsSealedTotal, dogiRes.Metrics.PeakOpenSlabBytes,
		overRes.Metrics.AvgSlabFillRatio, overRes.Metrics.SlabsSealedTotal, overRes.Metrics.PeakOpenSlabBytes)

	if overRes.Metrics.AvgSlabFillRatio >= dogiRes.Metrics.AvgSlabFillRatio {
		t.Fatalf("expected 64-bucket AvgSlabFillRatio (%.3f) < 3-class DOGI (%.3f)",
			overRes.Metrics.AvgSlabFillRatio, dogiRes.Metrics.AvgSlabFillRatio)
	}
	if overRes.Metrics.SlabsSealedTotal <= dogiRes.Metrics.SlabsSealedTotal {
		t.Fatalf("expected 64-bucket SlabsSealedTotal (%d) > 3-class DOGI (%d)",
			overRes.Metrics.SlabsSealedTotal, dogiRes.Metrics.SlabsSealedTotal)
	}
	if overRes.Metrics.PeakOpenSlabBytes <= dogiRes.Metrics.PeakOpenSlabBytes {
		t.Fatalf("expected 64-bucket PeakOpenSlabBytes (%d) > 3-class DOGI (%d)",
			overRes.Metrics.PeakOpenSlabBytes, dogiRes.Metrics.PeakOpenSlabBytes)
	}
}

func TestBoundaryRegimeAdversarialColdStartConvergence(t *testing.T) {
	ops := GenerateWorkload(WorkloadAdversarialColdStart, 123)
	statRes, err := RunWorkload(WorkloadAdversarialColdStart, ops, PolicyStaticPath, 256<<10, 4)
	if err != nil {
		t.Fatalf("static_path: %v", err)
	}
	dogiRes, err := RunWorkload(WorkloadAdversarialColdStart, ops, PolicyDOGIAdaptive, 256<<10, 4)
	if err != nil {
		t.Fatalf("dogi: %v", err)
	}

	t.Logf("adversarial_cold_start: static_misclass=%d/%d live_copy=%dB | dogi_misclass=%d/%d conv_deletes=%d conv_epoch=%d post_acc=%.3f live_copy=%dB",
		statRes.ColdStartMisclassified, statRes.DeceptiveObjectsTotal, statRes.Metrics.LiveBytesCopied,
		dogiRes.ColdStartMisclassified, dogiRes.DeceptiveObjectsTotal, dogiRes.ConvergenceDeleteCount,
		dogiRes.ConvergenceEpoch, dogiRes.PostConvergenceAccuracy, dogiRes.Metrics.LiveBytesCopied)

	// Honest cold-start boundary: before MinObservations deletes occur, DOGI
	// MUST misclassify the deceptive .bin ephemeral paths (zero look-ahead!).
	if dogiRes.ColdStartMisclassified == 0 {
		t.Fatalf("expected non-zero cold-start misclassification before causal convergence")
	}
	if dogiRes.ConvergenceDeleteCount < DefaultOnlineClassifierConfig().MinObservations {
		t.Fatalf("expected convergence delete count (%d) >= MinObservations (%d)",
			dogiRes.ConvergenceDeleteCount, DefaultOnlineClassifierConfig().MinObservations)
	}
	if dogiRes.PostConvergenceAccuracy < 0.95 {
		t.Fatalf("expected post-convergence accuracy >= 0.95, got %.3f", dogiRes.PostConvergenceAccuracy)
	}
	// Static path never converges on deceptive .bin paths, so DOGI copies far less live data.
	if dogiRes.Metrics.LiveBytesCopied >= statRes.Metrics.LiveBytesCopied {
		t.Fatalf("expected DOGI live copied (%d) < static path (%d) on adversarial opaque paths",
			dogiRes.Metrics.LiveBytesCopied, statRes.Metrics.LiveBytesCopied)
	}
}
