package dogi

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"time"
)

// WorkloadKind identifies a reproducible benchmark or negative/boundary regime.
type WorkloadKind string

const (
	WorkloadAIAgentBuildChurn       WorkloadKind = "ai_agent_build_churn"
	WorkloadUniformRandomDelete     WorkloadKind = "uniform_random_delete_no_lifetime_skew"
	WorkloadOverFragmentedUnderfill WorkloadKind = "over_fragmented_64_bucket_underfill"
	WorkloadAdversarialColdStart    WorkloadKind = "adversarial_renamed_ephemeral_cold_start"
)

// OpKind is an operation in a causal workload trace.
type OpKind uint8

const (
	OpWrite OpKind = iota
	OpDelete
	OpAdvanceEpoch
	OpFlush
	OpReclaim
)

// WorkloadOp is a single deterministic operation in a reproducible trace.
type WorkloadOp struct {
	Kind             OpKind
	TenantID         string
	TraceObjID       int // dense trace-level object index mapped to Store objID at run time
	Path             string
	Size             int
	Epoch            int64
	PayloadSeed      uint64
	TrueLifetime     LifetimeClass // ground-truth label for offline evaluation ONLY; never passed to policy!
	DeceptiveOpaque  bool          // true for adversarial opaque-named ephemeral objects
	ReclaimThreshold float64
}

// RunResult captures all real-disk metrics, /proc/self/io counters, and
// optional convergence diagnostics for one (Workload, Policy) execution.
type RunResult struct {
	Workload                WorkloadKind `json:"workload"`
	Policy                  PolicyKind   `json:"policy"`
	DurationMS              float64      `json:"duration_ms"`
	Metrics                 StoreMetrics `json:"metrics"`
	ProcIO                  ProcIO       `json:"proc_io"`
	LiveObjectsVerified     int          `json:"live_objects_verified"`
	TenantIsolationVerified bool         `json:"tenant_isolation_verified"`
	ColdStartMisclassified  int          `json:"cold_start_misclassified,omitempty"`
	ConvergenceDeleteCount  int          `json:"convergence_delete_count,omitempty"`
	ConvergenceEpoch        int64        `json:"convergence_epoch,omitempty"`
	PostConvergenceAccuracy float64      `json:"post_convergence_accuracy,omitempty"`
	PreConvergenceAccuracy  float64      `json:"pre_convergence_accuracy,omitempty"`
	DeceptiveObjectsTotal   int          `json:"deceptive_objects_total,omitempty"`
}

// GenerateWorkload builds a deterministic, reproducible causal trace for the
// requested WorkloadKind.
func GenerateWorkload(kind WorkloadKind, seed int64) []WorkloadOp {
	switch kind {
	case WorkloadAIAgentBuildChurn:
		return genAIAgentBuildChurn(seed)
	case WorkloadUniformRandomDelete:
		return genUniformRandomDelete(seed)
	case WorkloadOverFragmentedUnderfill:
		return genOverFragmentedUnderfill(seed)
	case WorkloadAdversarialColdStart:
		return genAdversarialColdStart(seed)
	default:
		return genAIAgentBuildChurn(seed)
	}
}

func makePayload(seed uint64, size int) []byte {
	buf := make([]byte, size)
	// Fast deterministic PRNG fill (xorshift64*) so every byte is distinct
	// and verifiable via SHA-256 without spending excessive CPU in rand.
	x := seed ^ 0x9e3779b97f4a7c15
	if x == 0 {
		x = 0x2545f4914f6cdd1d
	}
	i := 0
	for ; i+8 <= size; i += 8 {
		x ^= x >> 12
		x ^= x << 25
		x ^= x >> 27
		v := x * 0x2545f4914f6cdd1d
		binary.LittleEndian.PutUint64(buf[i:i+8], v)
	}
	for ; i < size; i++ {
		x ^= x >> 12
		x ^= x << 25
		x ^= x >> 27
		buf[i] = byte(x * 0x2545f4914f6cdd1d)
	}
	return buf
}

// genAIAgentBuildChurn simulates 2 tenants running iterative AI coding agent
// compile/test/clean cycles over 36 epochs:
//   - Long-lived workspace chunks (~25% of writes, ~96% survival): pinned
//     datasets, model weights, immutable assets, committed git packfiles.
//   - Medium-lived active source edits (~15% of writes, 10-20 epoch retention):
//     Go/TS/Python source files, manifests, rolling logs.
//   - Short-lived standard build artifacts (~35% of writes, die in 1-2 epochs):
//     target/debug/deps/*.o, node_modules/.cache/*.tmp, dist/*.js, __pycache__/*.pyc.
//   - Short-lived opaque/misnamed scratch artifacts (~25% of writes, die in 1-2 epochs):
//     work/stage_a/blob_*.dat, scratch_pool/run_1/chunk_*.bin — look like medium
//     or long-lived .bin files to static heuristics, testing online adaptation.
func genAIAgentBuildChurn(seed int64) []WorkloadOp {
	rng := rand.New(rand.NewSource(seed))
	tenants := []string{"tenant-alpha", "tenant-beta"}
	const epochs = 36

	type pendingDelete struct {
		tenantID   string
		traceObjID int
	}
	deletesByEpoch := make(map[int64][]pendingDelete)

	var ops []WorkloadOp
	nextTraceID := 1

	for ep := int64(1); ep <= epochs; ep++ {
		for _, t := range tenants {
			ops = append(ops, WorkloadOp{
				Kind:     OpAdvanceEpoch,
				TenantID: t,
				Epoch:    ep,
			})
		}

		// Execute scheduled deaths for this epoch (e.g. build clean / cache purge).
		for _, d := range deletesByEpoch[ep] {
			ops = append(ops, WorkloadOp{
				Kind:       OpDelete,
				TenantID:   d.tenantID,
				TraceObjID: d.traceObjID,
				Epoch:      ep,
			})
		}
		delete(deletesByEpoch, ep)

		// Generate interleaved writes across all 4 workspace object categories.
		for _, t := range tenants {
			const writesPerTenantEpoch = 32
			for w := 0; w < writesPerTenantEpoch; w++ {
				id := nextTraceID
				nextTraceID++
				r := rng.Float64()

				var (
					objPath   string
					size      int
					trueClass LifetimeClass
					deceptive bool
					deathEp   int64
				)

				switch {
				case r < 0.24:
					// Long-lived workspace chunks: weights, datasets, assets, git packs.
					trueClass = ClassLongLived
					size = (12 + rng.Intn(40)) << 10 // 12-52 KiB (spans small & large!)
					switch w % 4 {
					case 0:
						objPath = fmt.Sprintf("datasets/eval/shard_%04d.parquet", id)
					case 1:
						objPath = fmt.Sprintf("weights/lora/layer_%04d.safetensors", id)
					case 2:
						objPath = fmt.Sprintf("assets/static/img_%04d.png", id)
					default:
						objPath = fmt.Sprintf(".git/objects/pack/pack_%04d.pack", id)
					}
					// ~4% rare deletion far in the future.
					if rng.Float64() < 0.04 && ep+18 <= epochs {
						deathEp = ep + 18
					}

				case r < 0.39:
					// Medium-lived active source edits and logs.
					trueClass = ClassMedium
					size = (8 + rng.Intn(32)) << 10 // 8-40 KiB
					switch w % 3 {
					case 0:
						objPath = fmt.Sprintf("src/engine/module_%04d.go", id)
					case 1:
						objPath = fmt.Sprintf("apps/web/component_%04d.tsx", id)
					default:
						objPath = fmt.Sprintf("logs/agent/session_%04d.log", id)
					}
					if rng.Float64() < 0.55 {
						deathEp = ep + 10 + int64(rng.Intn(8))
					}

				case r < 0.74:
					// Standard short-lived build artifacts.
					trueClass = ClassEphemeral
					size = (10 + rng.Intn(42)) << 10 // 10-52 KiB (deliberately overlaps long-lived sizes!)
					switch w % 4 {
					case 0:
						objPath = fmt.Sprintf("target/debug/deps/crate_%04d.o", id)
					case 1:
						objPath = fmt.Sprintf("node_modules/.cache/babel/chunk_%04d.tmp", id)
					case 2:
						objPath = fmt.Sprintf("dist/assets/bundle_%04d.js", id)
					default:
						objPath = fmt.Sprintf("__pycache__/mod_%04d.pyc", id)
					}
					deathEp = ep + 1 + int64(rng.Intn(2)) // dies in 1-2 epochs

				default:
					// Opaque/misnamed short-lived build intermediates (deceptive .bin/.dat!).
					trueClass = ClassEphemeral
					deceptive = true
					size = (10 + rng.Intn(42)) << 10 // 10-52 KiB
					if w%2 == 0 {
						objPath = fmt.Sprintf("work/stage_alpha/blob_%04d.bin", id)
					} else {
						objPath = fmt.Sprintf("scratch_pool/job_runner/item_%04d.dat", id)
					}
					deathEp = ep + 1 + int64(rng.Intn(2)) // dies in 1-2 epochs
				}

				ops = append(ops, WorkloadOp{
					Kind:            OpWrite,
					TenantID:        t,
					TraceObjID:      id,
					Path:            objPath,
					Size:            size,
					Epoch:           ep,
					PayloadSeed:     uint64(seed)*1000003 + uint64(id),
					TrueLifetime:    trueClass,
					DeceptiveOpaque: deceptive,
				})
				if deathEp > 0 && deathEp <= epochs {
					deletesByEpoch[deathEp] = append(deletesByEpoch[deathEp], pendingDelete{
						tenantID:   t,
						traceObjID: id,
					})
				}
			}
		}

		// Periodic reclaim pass every 3 epochs (fixed downstream consumer).
		if ep%3 == 0 {
			ops = append(ops, WorkloadOp{
				Kind:             OpReclaim,
				Epoch:            ep,
				ReclaimThreshold: 0.45,
			})
		}
	}

	// Final flush & reclaim pass to measure clean end-of-workload state.
	ops = append(ops,
		WorkloadOp{Kind: OpFlush, Epoch: epochs},
		WorkloadOp{Kind: OpReclaim, Epoch: epochs, ReclaimThreshold: 0.45},
	)
	return ops
}

// genUniformRandomDelete generates Negative Regime 1: every object has an
// identical memoryless random deletion probability regardless of path,
// directory, or size (zero lifetime skew).
func genUniformRandomDelete(seed int64) []WorkloadOp {
	rng := rand.New(rand.NewSource(seed))
	tenant := "tenant-uniform"
	const epochs = 24
	const writesPerEpoch = 36

	type pendingDelete struct{ id int }
	deletesByEpoch := make(map[int64][]pendingDelete)

	paths := []string{
		"target/debug/obj_%04d.o",
		"datasets/chunk_%04d.parquet",
		"src/app/file_%04d.go",
		"work/stage/blob_%04d.bin",
	}

	var ops []WorkloadOp
	nextID := 1
	for ep := int64(1); ep <= epochs; ep++ {
		ops = append(ops, WorkloadOp{Kind: OpAdvanceEpoch, TenantID: tenant, Epoch: ep})
		for _, d := range deletesByEpoch[ep] {
			ops = append(ops, WorkloadOp{
				Kind:       OpDelete,
				TenantID:   tenant,
				TraceObjID: d.id,
				Epoch:      ep,
			})
		}
		delete(deletesByEpoch, ep)

		for w := 0; w < writesPerEpoch; w++ {
			id := nextID
			nextID++
			pat := paths[w%len(paths)]
			size := (12 + rng.Intn(32)) << 10
			ops = append(ops, WorkloadOp{
				Kind:         OpWrite,
				TenantID:     tenant,
				TraceObjID:   id,
				Path:         fmt.Sprintf(pat, id),
				Size:         size,
				Epoch:        ep,
				PayloadSeed:  uint64(seed)*7919 + uint64(id),
				TrueLifetime: ClassMedium,
			})
			// Identical memoryless geometric death delay for ALL objects (p=0.18/epoch).
			delay := int64(1)
			for rng.Float64() >= 0.18 && ep+delay <= epochs {
				delay++
			}
			if ep+delay <= epochs {
				deletesByEpoch[ep+delay] = append(deletesByEpoch[ep+delay], pendingDelete{id: id})
			}
		}

		if ep%3 == 0 {
			ops = append(ops, WorkloadOp{Kind: OpReclaim, Epoch: ep, ReclaimThreshold: 0.45})
		}
	}
	ops = append(ops,
		WorkloadOp{Kind: OpFlush, Epoch: epochs},
		WorkloadOp{Kind: OpReclaim, Epoch: epochs, ReclaimThreshold: 0.45},
	)
	return ops
}

// genOverFragmentedUnderfill generates Negative Regime 2: a moderate-rate
// workspace workload with periodic generation flushes (modeling hanzoai/vfs
// Flush boundaries). With 64 ultra-fine buckets, slabs are repeatedly flushed
// partially empty, degrading slab fill ratio and inflating open-slab RAM and
// slab count compared to 3 coarse lifetime classes.
func genOverFragmentedUnderfill(seed int64) []WorkloadOp {
	rng := rand.New(rand.NewSource(seed))
	tenant := "tenant-moderate"
	const epochs = 18
	const writesPerEpoch = 24

	type pendingDelete struct{ id int }
	deletesByEpoch := make(map[int64][]pendingDelete)

	var ops []WorkloadOp
	nextID := 1
	for ep := int64(1); ep <= epochs; ep++ {
		ops = append(ops, WorkloadOp{Kind: OpAdvanceEpoch, TenantID: tenant, Epoch: ep})
		for _, d := range deletesByEpoch[ep] {
			ops = append(ops, WorkloadOp{
				Kind:       OpDelete,
				TenantID:   tenant,
				TraceObjID: d.id,
				Epoch:      ep,
			})
		}
		delete(deletesByEpoch, ep)

		for w := 0; w < writesPerEpoch; w++ {
			id := nextID
			nextID++
			size := (10 + rng.Intn(22)) << 10
			var objPath string
			var deathEp int64
			if w%2 == 0 {
				objPath = fmt.Sprintf("target/mod_%02d/unit_%04d.o", w%12, id)
				deathEp = ep + 1
			} else {
				objPath = fmt.Sprintf("datasets/part_%02d/block_%04d.parquet", w%12, id)
			}
			ops = append(ops, WorkloadOp{
				Kind:        OpWrite,
				TenantID:    tenant,
				TraceObjID:  id,
				Path:        objPath,
				Size:        size,
				Epoch:       ep,
				PayloadSeed: uint64(seed)*3571 + uint64(id),
			})
			if deathEp > 0 && deathEp <= epochs {
				deletesByEpoch[deathEp] = append(deletesByEpoch[deathEp], pendingDelete{id: id})
			}
		}

		// Generation-boundary flush every 2 epochs (like vfs Flush / Bound/2).
		if ep%2 == 0 {
			ops = append(ops,
				WorkloadOp{Kind: OpFlush, Epoch: ep},
				WorkloadOp{Kind: OpReclaim, Epoch: ep, ReclaimThreshold: 0.45},
			)
		}
	}
	return ops
}

// genAdversarialColdStart generates Boundary Regime 3: ephemeral build temp
// objects written under newly coined deceptive paths with `.bin` and `.parquet`
// extensions alongside genuine long-lived `.parquet` files. Explicitly tests
// and measures the cold-start misclassification window before online causal
// survival feedback converges.
func genAdversarialColdStart(seed int64) []WorkloadOp {
	rng := rand.New(rand.NewSource(seed))
	tenant := "tenant-adversarial"
	const epochs = 20

	type pendingDelete struct{ id int }
	deletesByEpoch := make(map[int64][]pendingDelete)

	var ops []WorkloadOp
	nextID := 1
	for ep := int64(1); ep <= epochs; ep++ {
		ops = append(ops, WorkloadOp{Kind: OpAdvanceEpoch, TenantID: tenant, Epoch: ep})
		for _, d := range deletesByEpoch[ep] {
			ops = append(ops, WorkloadOp{
				Kind:       OpDelete,
				TenantID:   tenant,
				TraceObjID: d.id,
				Epoch:      ep,
			})
		}
		delete(deletesByEpoch, ep)

		// 12 deceptive ephemeral writes per epoch + 6 genuine long-lived writes.
		for w := 0; w < 18; w++ {
			id := nextID
			nextID++
			size := (16 + rng.Intn(24)) << 10
			if w < 12 {
				// Deceptive path: looks like long-lived .bin / .parquet!
				objPath := fmt.Sprintf("opaque_drop/ephemeral_stage/item_%04d.bin", id)
				ops = append(ops, WorkloadOp{
					Kind:            OpWrite,
					TenantID:        tenant,
					TraceObjID:      id,
					Path:            objPath,
					Size:            size,
					Epoch:           ep,
					PayloadSeed:     uint64(seed)*6151 + uint64(id),
					TrueLifetime:    ClassEphemeral,
					DeceptiveOpaque: true,
				})
				if ep+1 <= epochs {
					deletesByEpoch[ep+1] = append(deletesByEpoch[ep+1], pendingDelete{id: id})
				}
			} else {
				objPath := fmt.Sprintf("datasets/gold_corpus/shard_%04d.parquet", id)
				ops = append(ops, WorkloadOp{
					Kind:         OpWrite,
					TenantID:     tenant,
					TraceObjID:   id,
					Path:         objPath,
					Size:         size,
					Epoch:        ep,
					PayloadSeed:  uint64(seed)*6151 + uint64(id),
					TrueLifetime: ClassLongLived,
				})
			}
		}

		if ep%2 == 0 {
			ops = append(ops, WorkloadOp{Kind: OpReclaim, Epoch: ep, ReclaimThreshold: 0.45})
		}
	}
	ops = append(ops,
		WorkloadOp{Kind: OpFlush, Epoch: epochs},
		WorkloadOp{Kind: OpReclaim, Epoch: epochs, ReclaimThreshold: 0.45},
	)
	return ops
}

// RunWorkload executes a deterministic trace against a fresh real-disk
// SlabStore in an isolated os.MkdirTemp directory, measures real /proc/self/io
// deltas, audits tenant isolation, and verifies bit-exact SHA-256 read-back
// for every surviving live object.
func RunWorkload(kind WorkloadKind, ops []WorkloadOp, policyKind PolicyKind, slabTargetBytes, maxOpenSlabsPerTenant int) (RunResult, error) {
	tmpDir, err := os.MkdirTemp("", fmt.Sprintf("dogi-%s-*", policyKind))
	if err != nil {
		return RunResult{}, fmt.Errorf("dogi: mkdirtemp: %w", err)
	}
	defer os.RemoveAll(tmpDir)

	policy := NewPolicy(policyKind)
	store, err := NewSlabStore(StoreConfig{
		RootDir:               tmpDir,
		SlabTargetBytes:       slabTargetBytes,
		MaxOpenSlabsPerTenant: maxOpenSlabsPerTenant,
	}, policy)
	if err != nil {
		return RunResult{}, err
	}

	type liveRecord struct {
		tenantID string
		objID    uint64
		digest   [32]byte
	}
	traceToLive := make(map[int]liveRecord)

	// Cold-start convergence tracking for deceptive opaque ephemeral objects.
	var (
		deceptiveTotal   int
		deceptiveDeletes int
		converged        bool
		convDeleteCount  int
		convEpoch        int64
		coldMisclass     int
		preTotal         int
		preCorrect       int
		postTotal        int
		postCorrect      int
	)
	deceptiveTraceIDs := make(map[int]bool)

	ioStart := ReadProcIO()
	t0 := time.Now()

	for _, op := range ops {
		switch op.Kind {
		case OpAdvanceEpoch:
			if err := store.AdvanceEpoch(op.TenantID, op.Epoch); err != nil {
				return RunResult{}, err
			}

		case OpWrite:
			hint := ObjectHint{
				TenantID: op.TenantID,
				Path:     op.Path,
				Size:     int64(op.Size),
				Epoch:    op.Epoch,
			}
			// Measure causal classification decision BEFORE inserting the object
			// for policies that actually classify objects by lifetime.
			if op.DeceptiveOpaque && (policyKind == PolicyStaticPath || policyKind == PolicyDOGIAdaptive) {
				deceptiveTotal++
				deceptiveTraceIDs[op.TraceObjID] = true
				pred := policy.AssignBucket(hint)
				isCorrect := (pred == int(ClassEphemeral))
				if !converged {
					if isCorrect {
						converged = true
						convDeleteCount = deceptiveDeletes
						convEpoch = op.Epoch
						postTotal++
						postCorrect++
					} else {
						coldMisclass++
						preTotal++
					}
				} else {
					postTotal++
					if isCorrect {
						postCorrect++
					}
				}
			}

			payload := makePayload(op.PayloadSeed, op.Size)
			objID, digest, err := store.Put(hint, payload)
			if err != nil {
				return RunResult{}, err
			}
			traceToLive[op.TraceObjID] = liveRecord{
				tenantID: op.TenantID,
				objID:    objID,
				digest:   digest,
			}

		case OpDelete:
			rec, ok := traceToLive[op.TraceObjID]
			if !ok {
				continue
			}
			if err := store.Delete(rec.tenantID, rec.objID, op.Epoch); err != nil {
				return RunResult{}, err
			}
			delete(traceToLive, op.TraceObjID)
			if deceptiveTraceIDs[op.TraceObjID] {
				deceptiveDeletes++
			}

		case OpFlush:
			if err := store.FlushAll(); err != nil {
				return RunResult{}, err
			}

		case OpReclaim:
			if _, err := store.ReclaimDeadSlabs(op.ReclaimThreshold); err != nil {
				return RunResult{}, err
			}
		}
	}

	// Verify bit-exact SHA-256 read-back of EVERY surviving object from real disk.
	verified := 0
	for _, rec := range traceToLive {
		got, err := store.Get(rec.tenantID, rec.objID)
		if err != nil {
			return RunResult{}, fmt.Errorf("dogi: final verify obj %d: %w", rec.objID, err)
		}
		if sha256.Sum256(got) != rec.digest {
			return RunResult{}, fmt.Errorf("dogi: final sha256 mismatch obj %d", rec.objID)
		}
		verified++
	}

	elapsed := time.Since(t0)
	ioEnd := ReadProcIO()

	isoErr := store.VerifyTenantIsolation()
	if isoErr != nil {
		return RunResult{}, isoErr
	}

	res := RunResult{
		Workload:                kind,
		Policy:                  policyKind,
		DurationMS:              float64(elapsed.Microseconds()) / 1000.0,
		Metrics:                 store.Metrics(),
		ProcIO:                  ioEnd.Sub(ioStart),
		LiveObjectsVerified:     verified,
		TenantIsolationVerified: isoErr == nil,
		ColdStartMisclassified:  coldMisclass,
		ConvergenceDeleteCount:  convDeleteCount,
		ConvergenceEpoch:        convEpoch,
		DeceptiveObjectsTotal:   deceptiveTotal,
	}
	if preTotal > 0 {
		res.PreConvergenceAccuracy = float64(preCorrect) / float64(preTotal)
	}
	if postTotal > 0 {
		res.PostConvergenceAccuracy = float64(postCorrect) / float64(postTotal)
	}
	return res, nil
}
