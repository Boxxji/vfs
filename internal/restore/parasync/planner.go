package parasync

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

// Strategy selects the restore algorithm evaluated in the matched comparison.
type Strategy string

const (
	// StrategyFullSequential downloads 100% of target ciphertext chunks sequentially (W=1).
	StrategyFullSequential Strategy = "full_checkpoint_sequential"

	// StrategyFullParallel downloads 100% of target ciphertext chunks in parallel (W workers).
	StrategyFullParallel Strategy = "full_checkpoint_parallel"

	// StrategyFixedChunkSequentialDelta diffs fixed 64 KiB manifests sequentially,
	// reuses unchanged local blocks, and downloads changed chunks sequentially (W=1).
	StrategyFixedChunkSequentialDelta Strategy = "fixed_chunk_sequential_delta"

	// StrategyParaSyncParallelDelta runs ParaSync-inspired parallel sharded
	// manifest diffing, work-stealing parallel ciphertext transfer & AEAD
	// verification, and overlapped parallel local chunk reuse.
	StrategyParaSyncParallelDelta Strategy = "parasync_parallel_delta"
)

// AllStrategies returns the 4 matched restore strategies in canonical order.
func AllStrategies() []Strategy {
	return []Strategy{
		StrategyFullSequential,
		StrategyFullParallel,
		StrategyFixedChunkSequentialDelta,
		StrategyParaSyncParallelDelta,
	}
}

// FallbackReason explains why a delta strategy deterministically fell back to
// full checkpoint download.
const (
	FallbackNone                  = ""
	FallbackEmptyBase             = "empty_base_checkpoint"
	FallbackIncompatibleTenant    = "incompatible_tenant"
	FallbackIncompatibleKeyEpoch  = "incompatible_key_epoch"
	FallbackIncompatibleChunkMode = "incompatible_chunking_mode"
)

// RestoreOptions configures planning and execution of a checkpoint restore.
type RestoreOptions struct {
	Strategy           Strategy
	Workers            int  // worker concurrency for parallel strategies (default 8)
	DiffShards         int  // shard parallelism for ParaSync chunk-index diff (default 4)
	VerifyVolumeSHA256 bool // compute whole-volume SHA-256 after restore
}

func (o RestoreOptions) normalized() RestoreOptions {
	if o.Strategy == "" {
		o.Strategy = StrategyParaSyncParallelDelta
	}
	if o.Workers <= 0 {
		o.Workers = 8
	}
	if o.Strategy == StrategyFullSequential || o.Strategy == StrategyFixedChunkSequentialDelta {
		o.Workers = 1
	}
	if o.DiffShards <= 0 {
		o.DiffShards = 4
	}
	return o
}

type localSource struct {
	relPath      string
	offset       int64
	length       int
	cipherDigest Digest
}

type chunkDest struct {
	fileIdx int
	offset  int64
}

type localCopyTask struct {
	srcPath         string
	srcOffset       int64
	dstFileIdx      int
	dstOffset       int64
	length          int
	expectedChunkID Digest
	fallbackRef     ChunkRef
}

type remoteFetchTask struct {
	ref     ChunkRef
	dests   []chunkDest
	fileIdx int // primary file index for clustered initial deque placement
}

// RestorePlan captures the exact diff and transfer schedule produced before
// network execution.
type RestorePlan struct {
	Strategy                 Strategy      `json:"strategy"`
	EffectiveWorkers         int           `json:"effective_workers"`
	DiffShardsUsed           int           `json:"diff_shards_used"`
	FallbackApplied          bool          `json:"fallback_applied"`
	FallbackReason           string        `json:"fallback_reason"`
	TotalChunks              int           `json:"total_chunks"`
	ReusedLocalChunks        int           `json:"reused_local_chunks"`
	RemoteFetchChunks        int           `json:"remote_fetch_chunks"`
	UniqueRemoteObjects      int           `json:"unique_remote_objects"`
	ReusedLocalBytes         int64         `json:"reused_local_bytes"`
	PlannedRemotePlainBytes  int64         `json:"planned_remote_plain_bytes"`
	PlannedRemoteCipherBytes int64         `json:"planned_remote_cipher_bytes"`
	PlanDuration             time.Duration `json:"plan_duration_ns"`

	localTasks  []localCopyTask
	remoteTasks []remoteFetchTask
}

// RestoreResult captures measured wall time, CPU time, network transfer,
// concurrency telemetry, and bit-exact verification for a completed restore.
type RestoreResult struct {
	Plan                     RestorePlan   `json:"plan"`
	WallTime                 time.Duration `json:"wall_time_ns"`
	DiffTime                 time.Duration `json:"diff_time_ns"`
	TransferAndApplyTime     time.Duration `json:"transfer_and_apply_time_ns"`
	CPUTime                  time.Duration `json:"cpu_time_ns"`
	TransferredBytes         int64         `json:"transferred_bytes"`
	HTTPGetCount             int64         `json:"http_get_count"`
	TotalRestoredBytes       int64         `json:"total_restored_bytes"`
	EffectiveThroughputMiBps float64       `json:"effective_throughput_mib_s"`
	RestoredVolumeSHA256     string        `json:"restored_volume_sha256"`
	ExpectedVolumeSHA256     string        `json:"expected_volume_sha256"`
	BitExact                 bool          `json:"bit_exact"`
	StealCount               int64         `json:"steal_count"`
	LocalBitrotRepairs       int64         `json:"local_bitrot_repairs"`
}

// PlanRestore executes Stage 1 (chunk-index diffing) between base and target
// checkpoints according to opts.Strategy, enforcing strict tenant and key-epoch
// isolation.
func PlanRestore(base, target *Checkpoint, baseDir string, opts RestoreOptions) (*RestorePlan, error) {
	if target == nil {
		return nil, errors.New("parasync: target checkpoint is nil")
	}
	opts = opts.normalized()
	start := time.Now()

	plan := &RestorePlan{
		Strategy:         opts.Strategy,
		EffectiveWorkers: opts.Workers,
		TotalChunks:      target.TotalChunks,
	}

	// Non-delta full strategies always schedule 100% of target chunks for remote fetch.
	if opts.Strategy == StrategyFullSequential || opts.Strategy == StrategyFullParallel {
		plan.DiffShardsUsed = 1
		buildFullRemotePlan(target, plan)
		plan.PlanDuration = nonNegDuration(time.Since(start))
		return plan, nil
	}

	// Delta compatibility checks: fail closed to full restore if incompatible.
	if reason := checkDeltaCompatibility(base, target, baseDir); reason != FallbackNone {
		plan.FallbackApplied = true
		plan.FallbackReason = reason
		plan.DiffShardsUsed = 1
		buildFullRemotePlan(target, plan)
		plan.PlanDuration = nonNegDuration(time.Since(start))
		return plan, nil
	}

	if opts.Strategy == StrategyFixedChunkSequentialDelta {
		plan.DiffShardsUsed = 1
		buildSequentialDeltaPlan(base, target, plan)
		plan.PlanDuration = nonNegDuration(time.Since(start))
		return plan, nil
	}

	// StrategyParaSyncParallelDelta: parallel sharded chunk-index diff.
	plan.DiffShardsUsed = opts.DiffShards
	buildParallelDeltaPlan(base, target, opts.DiffShards, plan)
	plan.PlanDuration = nonNegDuration(time.Since(start))
	return plan, nil
}

func nonNegDuration(d time.Duration) time.Duration {
	if d < 0 {
		return 0
	}
	return d
}

func checkDeltaCompatibility(base, target *Checkpoint, baseDir string) string {
	if base == nil || len(base.Files) == 0 || baseDir == "" {
		return FallbackEmptyBase
	}
	if base.TenantID != target.TenantID {
		return FallbackIncompatibleTenant
	}
	if base.KeyEpoch != target.KeyEpoch {
		return FallbackIncompatibleKeyEpoch
	}
	if base.ChunkingMode != target.ChunkingMode {
		return FallbackIncompatibleChunkMode
	}
	return FallbackNone
}

func buildFullRemotePlan(target *Checkpoint, plan *RestorePlan) {
	remoteIndex := make(map[Digest]int, target.UniqueChunks)
	tasks := make([]remoteFetchTask, 0, target.UniqueChunks)

	for fi, f := range target.Files {
		for _, c := range f.Chunks {
			plan.RemoteFetchChunks++
			plan.PlannedRemotePlainBytes += int64(c.PlainSize)
			if idx, ok := remoteIndex[c.CipherDigest]; ok {
				tasks[idx].dests = append(tasks[idx].dests, chunkDest{fileIdx: fi, offset: c.Offset})
				continue
			}
			remoteIndex[c.CipherDigest] = len(tasks)
			tasks = append(tasks, remoteFetchTask{
				ref:     c,
				dests:   []chunkDest{{fileIdx: fi, offset: c.Offset}},
				fileIdx: fi,
			})
			plan.PlannedRemoteCipherBytes += int64(c.CipherSize)
		}
	}
	plan.UniqueRemoteObjects = len(tasks)
	plan.remoteTasks = tasks
}

func buildSequentialDeltaPlan(base, target *Checkpoint, plan *RestorePlan) {
	byCipher := make(map[Digest]localSource, base.UniqueChunks)
	byChunkID := make(map[Digest]localSource, base.UniqueChunks)
	for _, f := range base.Files {
		for _, c := range f.Chunks {
			src := localSource{
				relPath:      f.Path,
				offset:       c.Offset,
				length:       c.PlainSize,
				cipherDigest: c.CipherDigest,
			}
			if _, ok := byCipher[c.CipherDigest]; !ok {
				byCipher[c.CipherDigest] = src
			}
			if _, ok := byChunkID[c.ChunkID]; !ok {
				byChunkID[c.ChunkID] = src
			}
		}
	}

	remoteIndex := make(map[Digest]int)
	var remotes []remoteFetchTask
	var locals []localCopyTask

	for fi, f := range target.Files {
		for _, c := range f.Chunks {
			src, found := byCipher[c.CipherDigest]
			if !found {
				src, found = byChunkID[c.ChunkID]
			}
			if found && src.length == c.PlainSize {
				locals = append(locals, localCopyTask{
					srcPath:         src.relPath,
					srcOffset:       src.offset,
					dstFileIdx:      fi,
					dstOffset:       c.Offset,
					length:          c.PlainSize,
					expectedChunkID: c.ChunkID,
					fallbackRef:     c,
				})
				plan.ReusedLocalChunks++
				plan.ReusedLocalBytes += int64(c.PlainSize)
				continue
			}

			plan.RemoteFetchChunks++
			plan.PlannedRemotePlainBytes += int64(c.PlainSize)
			if idx, ok := remoteIndex[c.CipherDigest]; ok {
				remotes[idx].dests = append(remotes[idx].dests, chunkDest{fileIdx: fi, offset: c.Offset})
				continue
			}
			remoteIndex[c.CipherDigest] = len(remotes)
			remotes = append(remotes, remoteFetchTask{
				ref:     c,
				dests:   []chunkDest{{fileIdx: fi, offset: c.Offset}},
				fileIdx: fi,
			})
			plan.PlannedRemoteCipherBytes += int64(c.CipherSize)
		}
	}

	plan.UniqueRemoteObjects = len(remotes)
	plan.localTasks = locals
	plan.remoteTasks = remotes
}

type chunkIndexShard struct {
	mu        sync.Mutex
	byCipher  map[Digest]localSource
	byChunkID map[Digest]localSource
}

// buildParallelDeltaPlan implements ParaSync Stage 1:
//  1. Builds a sharded read-only base chunk index in parallel across S shards.
//  2. Partitions target files across S diff workers in parallel to classify
//     unchanged local-reusable chunks vs missing remote ciphertext chunks.
//  3. Deduplicates unique remote ciphertext objects across shard results.
func buildParallelDeltaPlan(base, target *Checkpoint, shards int, plan *RestorePlan) {
	if shards < 1 {
		shards = 1
	}
	if shards > len(target.Files) && len(target.Files) > 0 {
		shards = len(target.Files)
	}

	// Step 1: Build sharded base index in parallel.
	indexShards := make([]chunkIndexShard, shards)
	perShardCap := (base.UniqueChunks / shards) + 16
	for i := range indexShards {
		indexShards[i].byCipher = make(map[Digest]localSource, perShardCap)
		indexShards[i].byChunkID = make(map[Digest]localSource, perShardCap)
	}

	var indexWG sync.WaitGroup
	baseFiles := base.Files
	for s := 0; s < shards; s++ {
		startIdx := (len(baseFiles) * s) / shards
		endIdx := (len(baseFiles) * (s + 1)) / shards
		if startIdx >= endIdx {
			continue
		}
		indexWG.Add(1)
		go func(files []FileEntry) {
			defer indexWG.Done()
			for _, f := range files {
				for _, c := range f.Chunks {
					src := localSource{
						relPath:      f.Path,
						offset:       c.Offset,
						length:       c.PlainSize,
						cipherDigest: c.CipherDigest,
					}
					cs := int(c.CipherDigest[0]) % shards
					indexShards[cs].mu.Lock()
					if _, ok := indexShards[cs].byCipher[c.CipherDigest]; !ok {
						indexShards[cs].byCipher[c.CipherDigest] = src
					}
					indexShards[cs].mu.Unlock()

					ks := int(c.ChunkID[0]) % shards
					indexShards[ks].mu.Lock()
					if _, ok := indexShards[ks].byChunkID[c.ChunkID]; !ok {
						indexShards[ks].byChunkID[c.ChunkID] = src
					}
					indexShards[ks].mu.Unlock()
				}
			}
		}(baseFiles[startIdx:endIdx])
	}
	indexWG.Wait()

	// Step 2: Parallel sharded diff across target file ranges.
	type rawRemote struct {
		ref     ChunkRef
		fileIdx int
		offset  int64
	}
	type shardDiffOut struct {
		locals          []localCopyTask
		remotes         []rawRemote
		reusedBytes     int64
		remotePlainByte int64
	}

	outs := make([]shardDiffOut, shards)
	var diffWG sync.WaitGroup
	targetFiles := target.Files
	for s := 0; s < shards; s++ {
		startIdx := (len(targetFiles) * s) / shards
		endIdx := (len(targetFiles) * (s + 1)) / shards
		if startIdx >= endIdx {
			continue
		}
		diffWG.Add(1)
		go func(shardID, lo, hi int) {
			defer diffWG.Done()
			var out shardDiffOut
			for fi := lo; fi < hi; fi++ {
				f := targetFiles[fi]
				for _, c := range f.Chunks {
					cs := int(c.CipherDigest[0]) % shards
					src, found := indexShards[cs].byCipher[c.CipherDigest]
					if !found {
						ks := int(c.ChunkID[0]) % shards
						src, found = indexShards[ks].byChunkID[c.ChunkID]
					}
					if found && src.length == c.PlainSize {
						out.locals = append(out.locals, localCopyTask{
							srcPath:         src.relPath,
							srcOffset:       src.offset,
							dstFileIdx:      fi,
							dstOffset:       c.Offset,
							length:          c.PlainSize,
							expectedChunkID: c.ChunkID,
							fallbackRef:     c,
						})
						out.reusedBytes += int64(c.PlainSize)
						continue
					}
					out.remotes = append(out.remotes, rawRemote{
						ref:     c,
						fileIdx: fi,
						offset:  c.Offset,
					})
					out.remotePlainByte += int64(c.PlainSize)
				}
			}
			outs[shardID] = out
		}(s, startIdx, endIdx)
	}
	diffWG.Wait()

	// Step 3: Merge shard outputs and deduplicate unique remote ciphertext fetches.
	totalLocals := 0
	totalRemotes := 0
	for i := range outs {
		totalLocals += len(outs[i].locals)
		totalRemotes += len(outs[i].remotes)
		plan.ReusedLocalBytes += outs[i].reusedBytes
		plan.PlannedRemotePlainBytes += outs[i].remotePlainByte
	}
	plan.ReusedLocalChunks = totalLocals
	plan.RemoteFetchChunks = totalRemotes

	locals := make([]localCopyTask, 0, totalLocals)
	remoteIndex := make(map[Digest]int, totalRemotes)
	remotes := make([]remoteFetchTask, 0, totalRemotes)

	for i := range outs {
		locals = append(locals, outs[i].locals...)
		for _, rr := range outs[i].remotes {
			if idx, ok := remoteIndex[rr.ref.CipherDigest]; ok {
				remotes[idx].dests = append(remotes[idx].dests, chunkDest{fileIdx: rr.fileIdx, offset: rr.offset})
				continue
			}
			remoteIndex[rr.ref.CipherDigest] = len(remotes)
			remotes = append(remotes, remoteFetchTask{
				ref:     rr.ref,
				dests:   []chunkDest{{fileIdx: rr.fileIdx, offset: rr.offset}},
				fileIdx: rr.fileIdx,
			})
			plan.PlannedRemoteCipherBytes += int64(rr.ref.CipherSize)
		}
	}

	plan.UniqueRemoteObjects = len(remotes)
	plan.localTasks = locals
	plan.remoteTasks = remotes
}

// workStealingDeque is a bounded-overhead mutex-protected deque supporting LIFO
// owner pop (cache locality) and FIFO thief steal (load balancing under skew).
type workStealingDeque struct {
	mu    sync.Mutex
	items []remoteFetchTask
}

func (d *workStealingDeque) push(t remoteFetchTask) {
	d.mu.Lock()
	d.items = append(d.items, t)
	d.mu.Unlock()
}

func (d *workStealingDeque) popLocal() (remoteFetchTask, bool) {
	d.mu.Lock()
	n := len(d.items)
	if n == 0 {
		d.mu.Unlock()
		return remoteFetchTask{}, false
	}
	t := d.items[n-1]
	d.items = d.items[:n-1]
	d.mu.Unlock()
	return t, true
}

func (d *workStealingDeque) steal() (remoteFetchTask, bool) {
	d.mu.Lock()
	if len(d.items) == 0 {
		d.mu.Unlock()
		return remoteFetchTask{}, false
	}
	t := d.items[0]
	d.items = d.items[1:]
	d.mu.Unlock()
	return t, true
}

// ExecuteRestore runs the full restore pipeline (Stage 1 Plan + Stage 2 Transfer
// & Verify + Stage 3 Materialize) from baseDir + store into targetDir.
func ExecuteRestore(
	ctx context.Context,
	store ObjectStore,
	key *TenantKey,
	base *Checkpoint,
	target *Checkpoint,
	baseDir string,
	targetDir string,
	opts RestoreOptions,
) (*RestoreResult, error) {
	if target == nil {
		return nil, errors.New("parasync: target checkpoint is nil")
	}
	if key == nil {
		return nil, errors.New("parasync: tenant key is nil")
	}
	if target.TenantID != key.TenantID || target.KeyEpoch != key.Epoch {
		return nil, fmt.Errorf("parasync: tenant key (%s, epoch %d) does not match target checkpoint (%s, epoch %d)",
			key.TenantID, key.Epoch, target.TenantID, target.KeyEpoch)
	}

	opts = opts.normalized()
	beforeStats := store.Stats()
	cpuStart := processCPUTime()
	wallStart := time.Now()

	// Stage 1: Plan / Diff
	plan, err := PlanRestore(base, target, baseDir, opts)
	if err != nil {
		return nil, err
	}
	diffTime := plan.PlanDuration

	transferStart := time.Now()

	// Pre-create target directory tree and pre-size all target files so
	// concurrent WriteAt calls on disjoint chunk ranges are lock-free.
	if err := os.MkdirAll(targetDir, 0o755); err != nil {
		return nil, fmt.Errorf("parasync: mkdir target %s: %w", targetDir, err)
	}
	dstFiles := make([]*os.File, len(target.Files))
	closeDst := func() {
		for _, f := range dstFiles {
			if f != nil {
				_ = f.Close()
			}
		}
	}
	for i, fe := range target.Files {
		full := filepath.Join(targetDir, filepath.FromSlash(fe.Path))
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			closeDst()
			return nil, fmt.Errorf("parasync: mkdir parent for %s: %w", fe.Path, err)
		}
		mode := os.FileMode(fe.Mode)
		if mode == 0 {
			mode = 0o644
		}
		f, err := os.OpenFile(full, os.O_CREATE|os.O_RDWR|os.O_TRUNC, mode)
		if err != nil {
			closeDst()
			return nil, fmt.Errorf("parasync: create %s: %w", fe.Path, err)
		}
		if fe.Size > 0 {
			if err := f.Truncate(fe.Size); err != nil {
				_ = f.Close()
				closeDst()
				return nil, fmt.Errorf("parasync: truncate %s: %w", fe.Path, err)
			}
		}
		dstFiles[i] = f
	}

	// Open read-only handles for base files referenced by local reuse tasks.
	srcFiles := make(map[string]*os.File)
	closeSrc := func() {
		for _, f := range srcFiles {
			if f != nil {
				_ = f.Close()
			}
		}
	}
	for _, lt := range plan.localTasks {
		if _, exists := srcFiles[lt.srcPath]; exists {
			continue
		}
		full := filepath.Join(baseDir, filepath.FromSlash(lt.srcPath))
		f, err := os.Open(full)
		if err != nil {
			// If a base file is missing locally, leave nil so the local copy
			// worker transparently repairs via remote fetch.
			srcFiles[lt.srcPath] = nil
			continue
		}
		srcFiles[lt.srcPath] = f
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var firstErr error
	var errOnce sync.Once
	setErr := func(e error) {
		if e == nil {
			return
		}
		errOnce.Do(func() {
			firstErr = e
			cancel()
		})
	}

	var stealCount atomic.Int64
	var localBitrotRepairs atomic.Int64

	// Helper to fetch, verify, decrypt, and write a single remote chunk.
	executeRemoteTask := func(task remoteFetchTask) error {
		if err := runCtx.Err(); err != nil {
			return err
		}
		ciphertext, err := store.Get(runCtx, task.ref.ObjectKey)
		if err != nil {
			return fmt.Errorf("parasync: fetch %s: %w", task.ref.ObjectKey, err)
		}
		plain, err := key.OpenChunk(task.ref.ChunkID, task.ref.CipherDigest, ciphertext)
		if err != nil {
			return fmt.Errorf("parasync: verify/open %s: %w", task.ref.ObjectKey, err)
		}
		if len(plain) != task.ref.PlainSize {
			return fmt.Errorf("%w: chunk %s size %d want %d", ErrPlaintextMismatch, task.ref.ChunkID.String()[:12], len(plain), task.ref.PlainSize)
		}
		for _, d := range task.dests {
			n, err := dstFiles[d.fileIdx].WriteAt(plain, d.offset)
			if err != nil {
				return fmt.Errorf("parasync: writeAt file %d@%d: %w", d.fileIdx, d.offset, err)
			}
			if n != len(plain) {
				return fmt.Errorf("parasync: short writeAt file %d@%d (%d/%d)", d.fileIdx, d.offset, n, len(plain))
			}
		}
		return nil
	}

	// Helper to copy a local chunk with keyed BLAKE3 integrity verification
	// (automatically repairing from S3 if local base data is missing/corrupt).
	executeLocalTask := func(task localCopyTask) error {
		if err := runCtx.Err(); err != nil {
			return err
		}
		sf := srcFiles[task.srcPath]
		if sf != nil {
			buf := make([]byte, task.length)
			n, err := sf.ReadAt(buf, task.srcOffset)
			if err == nil && n == task.length && key.ChunkID(buf) == task.expectedChunkID {
				wn, werr := dstFiles[task.dstFileIdx].WriteAt(buf, task.dstOffset)
				if werr != nil {
					return fmt.Errorf("parasync: local writeAt file %d@%d: %w", task.dstFileIdx, task.dstOffset, werr)
				}
				if wn != task.length {
					return fmt.Errorf("parasync: short local writeAt (%d/%d)", wn, task.length)
				}
				return nil
			}
		}
		// Local base chunk missing or failed integrity check: transparently repair from S3.
		localBitrotRepairs.Add(1)
		return executeRemoteTask(remoteFetchTask{
			ref:     task.fallbackRef,
			dests:   []chunkDest{{fileIdx: task.dstFileIdx, offset: task.dstOffset}},
			fileIdx: task.dstFileIdx,
		})
	}

	workers := opts.Workers
	if workers == 1 {
		// Sequential execution for StrategyFullSequential & StrategyFixedChunkSequentialDelta.
		for _, lt := range plan.localTasks {
			if err := executeLocalTask(lt); err != nil {
				setErr(err)
				break
			}
		}
		if firstErr == nil {
			for _, rt := range plan.remoteTasks {
				if err := executeRemoteTask(rt); err != nil {
					setErr(err)
					break
				}
			}
		}
	} else {
		// Parallel execution: overlap local chunk copy workers with work-stealing
		// remote fetch + verify + decrypt workers.
		var wg sync.WaitGroup

		// 1. Local reuse workers (parallel disk-to-disk copy + BLAKE3 check).
		if len(plan.localTasks) > 0 {
			localWorkers := workers
			if localWorkers > 4 {
				localWorkers = 4
			}
			if localWorkers > len(plan.localTasks) {
				localWorkers = len(plan.localTasks)
			}
			var nextLocal atomic.Int64
			for w := 0; w < localWorkers; w++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for {
						idx := int(nextLocal.Add(1)) - 1
						if idx >= len(plan.localTasks) {
							return
						}
						if err := executeLocalTask(plan.localTasks[idx]); err != nil {
							setErr(err)
							return
						}
					}
				}()
			}
		}

		// 2. Remote transfer & verify workers.
		if len(plan.remoteTasks) > 0 {
			if opts.Strategy == StrategyParaSyncParallelDelta {
				// Work-stealing deques: tasks are initially assigned by fileIdx
				// modulo workers so skewed churn triggers authentic work stealing.
				deques := make([]workStealingDeque, workers)
				for _, rt := range plan.remoteTasks {
					owner := rt.fileIdx % workers
					deques[owner].push(rt)
				}
				for w := 0; w < workers; w++ {
					wg.Add(1)
					go func(workerID int) {
						defer wg.Done()
						for {
							if runCtx.Err() != nil {
								return
							}
							task, ok := deques[workerID].popLocal()
							if !ok {
								// Attempt to steal from victim deques.
								for step := 1; step < workers; step++ {
									victim := (workerID + step) % workers
									if stolen, hit := deques[victim].steal(); hit {
										stealCount.Add(1)
										task = stolen
										ok = true
										break
									}
								}
							}
							if !ok {
								return
							}
							if err := executeRemoteTask(task); err != nil {
								setErr(err)
								return
							}
						}
					}(w)
				}
			} else {
				// Standard parallel worker pool for StrategyFullParallel.
				var nextRemote atomic.Int64
				for w := 0; w < workers; w++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						for {
							if runCtx.Err() != nil {
								return
							}
							idx := int(nextRemote.Add(1)) - 1
							if idx >= len(plan.remoteTasks) {
								return
							}
							if err := executeRemoteTask(plan.remoteTasks[idx]); err != nil {
								setErr(err)
								return
							}
						}
					}()
				}
			}
		}

		wg.Wait()
	}

	closeSrc()
	closeDst()

	if firstErr != nil {
		return nil, firstErr
	}

	transferTime := nonNegDuration(time.Since(transferStart))
	wallTime := nonNegDuration(time.Since(wallStart))
	cpuTime := nonNegDuration(processCPUTime() - cpuStart)
	afterStats := store.Stats()

	transferredBytes := afterStats.BytesDownloaded - beforeStats.BytesDownloaded
	httpGets := afterStats.GetCount - beforeStats.GetCount

	effThroughput := 0.0
	if wallTime > 0 {
		effThroughput = (float64(target.TotalPlainBytes) / (1024.0 * 1024.0)) / wallTime.Seconds()
	}

	res := &RestoreResult{
		Plan:                     *plan,
		WallTime:                 wallTime,
		DiffTime:                 diffTime,
		TransferAndApplyTime:     transferTime,
		CPUTime:                  cpuTime,
		TransferredBytes:         transferredBytes,
		HTTPGetCount:             httpGets,
		TotalRestoredBytes:       target.TotalPlainBytes,
		EffectiveThroughputMiBps: effThroughput,
		ExpectedVolumeSHA256:     target.VolumeSHA256,
		StealCount:               stealCount.Load(),
		LocalBitrotRepairs:       localBitrotRepairs.Load(),
	}

	if opts.VerifyVolumeSHA256 {
		gotSHA, _, err := ComputeVolumeSHA256(targetDir)
		if err != nil {
			return nil, fmt.Errorf("parasync: verify restored volume sha256: %w", err)
		}
		res.RestoredVolumeSHA256 = gotSHA
		res.BitExact = gotSHA == target.VolumeSHA256
	}

	return res, nil
}
