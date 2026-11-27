// Copyright 2026 Hanzo AI Inc.
// SPDX-License-Identifier: Apache-2.0

package parasync

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hanzoai/vfs/pkg/backend"
	"github.com/zeebo/blake3"
)

// RestoreOptions configures the 3-stage concurrent ParaSync restore pipeline.
type RestoreOptions struct {
	// Workers sets concurrency P for diffing, remote fetching, local reuse
	// verification, and offset-partitioned pwrite assembly. Default: 8.
	Workers int

	// SkipDelta disables local chunk reuse and performs a 100% remote restore
	// (used for the matched full_restore_baseline regime).
	SkipDelta bool

	// ScanLocalDir re-chunks sourceDir on the fly using target.Chunking before
	// planning the delta (models an unindexed local directory and accounts for
	// live CDC scanning CPU/IO). If source is also provided, compatibility and
	// tenant isolation are still checked first.
	ScanLocalDir bool

	// EnableAutoFallback switches automatically to PlanFullRestore when the
	// planned churn ratio meets or exceeds PlanOptions.AutoFallbackChurnRatio.
	EnableAutoFallback bool

	// Cipher optionally decrypts sealed remote pack chunks.
	Cipher Cipher

	// SyncFiles calls fsync on staged files before atomic rename commit.
	SyncFiles bool

	// PlanOptions forwards diff-shard and range-coalescing parameters.
	PlanOptions PlanOptions
}

func (o RestoreOptions) normalized() RestoreOptions {
	if o.Workers <= 0 {
		o.Workers = 8
	}
	if o.PlanOptions.Workers <= 0 {
		o.PlanOptions.Workers = o.Workers
	}
	return o
}

// RestoreReport captures exact wall time, CPU time, network transfer, local
// I/O, and cryptographic verification metrics for a completed restore.
type RestoreReport struct {
	PlanDuration      time.Duration `json:"plan_duration_ns"`
	ScanDuration      time.Duration `json:"scan_duration_ns"`
	PipelineDuration  time.Duration `json:"pipeline_duration_ns"`
	VerifyDuration    time.Duration `json:"verify_duration_ns"`
	CommitDuration    time.Duration `json:"commit_duration_ns"`
	TotalWallDuration time.Duration `json:"total_wall_duration_ns"`

	CPUUserDuration  time.Duration `json:"cpu_user_duration_ns"`
	CPUSysDuration   time.Duration `json:"cpu_sys_duration_ns"`
	CPUTotalDuration time.Duration `json:"cpu_total_duration_ns"`

	TotalTargetBytes        int64   `json:"total_target_bytes"`
	ReusedLocalBytes        int64   `json:"reused_local_bytes"`
	RemotePayloadBytes      int64   `json:"remote_payload_bytes"`
	UniqueRemoteBytes       int64   `json:"unique_remote_bytes"`
	WireBytesTransferred    int64   `json:"wire_bytes_transferred"`
	CoalescedGapBytes       int64   `json:"coalesced_gap_bytes"`
	LocalDiskReadBytes      int64   `json:"local_disk_read_bytes"`
	LocalDiskWriteBytes     int64   `json:"local_disk_write_bytes"`
	RemoteRequests          int64   `json:"remote_requests"`
	SingleFlightDedupChunks int     `json:"single_flight_dedup_chunks"`
	SingleFlightDedupBytes  int64   `json:"single_flight_dedup_bytes"`
	BytesSavedRatio         float64 `json:"bytes_saved_ratio"`
	ChurnRatio              float64 `json:"churn_ratio"`
	FallbackRecommended     bool    `json:"fallback_recommended"`
	UsedFullFallback        bool    `json:"used_full_fallback"`
	VerifiedFiles           int     `json:"verified_files"`
	VerifiedChunks          int64   `json:"verified_chunks"`
}

// stagedTargetPool manages concurrent offset-partitioned WriteAt (pwrite)
// access to staged files while bounding open file descriptors.
type stagedTargetPool struct {
	paths     []string
	sizes     []int64
	keepOpen  bool
	openFiles []*os.File
	mu        sync.Mutex
	closed    bool
}

const maxAlwaysOpenStagedFiles = 512

func newStagedTargetPool(stageDir string, files []FileEntry) (*stagedTargetPool, error) {
	paths := make([]string, len(files))
	sizes := make([]int64, len(files))
	openFiles := make([]*os.File, len(files))
	openedCount := 0

	cleanup := func() {
		for _, f := range openFiles {
			if f != nil {
				_ = f.Close()
			}
		}
	}

	// Prefer keeping multi-chunk files open to avoid repeated open/close per
	// pwrite, while strictly bounding total simultaneously open target FDs.
	for i := range files {
		relOS := filepath.FromSlash(files[i].Path)
		full := filepath.Join(stageDir, relOS)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			cleanup()
			return nil, fmt.Errorf("parasync: mkdir staged parent for %s: %w", files[i].Path, err)
		}
		f, err := os.OpenFile(full, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o600)
		if err != nil {
			cleanup()
			return nil, fmt.Errorf("parasync: create staged file %s: %w", files[i].Path, err)
		}
		if files[i].Size > 0 {
			if err := f.Truncate(files[i].Size); err != nil {
				_ = f.Close()
				cleanup()
				return nil, fmt.Errorf("parasync: pre-size staged file %s: %w", files[i].Path, err)
			}
		}
		paths[i] = full
		sizes[i] = files[i].Size
		keep := openedCount < maxAlwaysOpenStagedFiles && (len(files) <= maxAlwaysOpenStagedFiles || len(files[i].Chunks) > 1)
		if keep {
			openFiles[i] = f
			openedCount++
		} else {
			if err := f.Close(); err != nil {
				cleanup()
				return nil, err
			}
		}
	}

	return &stagedTargetPool{
		paths:     paths,
		sizes:     sizes,
		keepOpen:  openedCount > 0,
		openFiles: openFiles,
	}, nil
}

func (p *stagedTargetPool) writeAt(fileIdx int, data []byte, off int64) error {
	if len(data) == 0 {
		return nil
	}
	if fileIdx < 0 || fileIdx >= len(p.paths) {
		return fmt.Errorf("parasync: invalid target file index %d", fileIdx)
	}
	if off < 0 || off+int64(len(data)) > p.sizes[fileIdx] {
		return fmt.Errorf("%w: write out of bounds for staged file %d (%d+%d > %d)",
			ErrCorruptCheckpoint, fileIdx, off, len(data), p.sizes[fileIdx])
	}
	if f := p.openFiles[fileIdx]; f != nil {
		// POSIX pwrite(2) is thread-safe on disjoint offsets of the same fd.
		n, err := f.WriteAt(data, off)
		if err != nil {
			return err
		}
		if n != len(data) {
			return io.ErrShortWrite
		}
		return nil
	}
	f, err := os.OpenFile(p.paths[fileIdx], os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	n, werr := f.WriteAt(data, off)
	cerr := f.Close()
	if werr != nil {
		return werr
	}
	if n != len(data) {
		return io.ErrShortWrite
	}
	return cerr
}

func (p *stagedTargetPool) closeAll() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil
	}
	p.closed = true
	var firstErr error
	for i, f := range p.openFiles {
		if f != nil {
			if err := f.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
			p.openFiles[i] = nil
		}
	}
	return firstErr
}

type writeJob struct {
	data    []byte
	targets []WriteTarget
}

type localReuseGroup struct {
	sourcePath string
	ops        []LocalReuseOp
}

// groupLocalReuseOps batches local reuse operations by source file (splitting
// large files into <=64-op chunks) to bound concurrent source FDs while keeping
// multi-worker parallelism balanced.
func groupLocalReuseOps(ops []LocalReuseOp) []localReuseGroup {
	if len(ops) == 0 {
		return nil
	}
	byFile := make(map[string][]LocalReuseOp)
	var order []string
	for _, op := range ops {
		if _, ok := byFile[op.SourcePath]; !ok {
			order = append(order, op.SourcePath)
		}
		byFile[op.SourcePath] = append(byFile[op.SourcePath], op)
	}
	const maxOpsPerGroup = 64
	var groups []localReuseGroup
	for _, sp := range order {
		list := byFile[sp]
		for start := 0; start < len(list); start += maxOpsPerGroup {
			end := start + maxOpsPerGroup
			if end > len(list) {
				end = len(list)
			}
			groups = append(groups, localReuseGroup{
				sourcePath: sp,
				ops:        list[start:end],
			})
		}
	}
	return groups
}

// Restore reconstructs target into destDir using the 3-stage concurrent
// ParaSync pipeline:
//
//	Stage 1: Compatibility check & work-balanced parallel delta planning
//	Stage 2: Parallel coalesced S3/backend Range-GET + local chunk read, both
//	         with strict BLAKE3-256 verification and single-flight remote dedup
//	Stage 3: Parallel offset-partitioned pwrite assembly into a private staging
//	         directory, followed by full-file BLAKE3 verification and atomic
//	         rename commit.
//
// On any error, corruption, or context cancellation, the staging directory is
// removed and existing files in destDir remain untouched.
func Restore(
	ctx context.Context,
	store backend.Backend,
	source *Checkpoint,
	sourceDir string,
	target *Checkpoint,
	destDir string,
	opts RestoreOptions,
) (*RestoreReport, error) {
	totalStart := time.Now()
	cpuUserStart, cpuSysStart := getProcessCPUTime()

	if store == nil {
		return nil, fmt.Errorf("parasync: nil backend store")
	}
	if target == nil {
		return nil, fmt.Errorf("%w: nil target checkpoint", ErrIncompatibleCheckpoint)
	}
	if destDir == "" {
		return nil, fmt.Errorf("parasync: empty destination directory")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	opts = opts.normalized()
	if err := target.ValidateStructure(); err != nil {
		return nil, err
	}

	// Stage 1: Compatibility check, optional local scan, and parallel delta plan.
	var (
		plan             *DeltaPlan
		scanDur          time.Duration
		planDur          time.Duration
		usedFullFallback bool
		err              error
	)

	planStart := time.Now()
	if opts.SkipDelta {
		plan, err = PlanFullRestore(ctx, target, opts.PlanOptions)
		if err != nil {
			return nil, err
		}
	} else {
		if source != nil {
			if err := CheckCompatibility(source, target); err != nil {
				return nil, err
			}
		}
		effectiveSource := source
		if opts.ScanLocalDir {
			if sourceDir == "" {
				return nil, fmt.Errorf("parasync: sourceDir required when ScanLocalDir is enabled")
			}
			scanStart := time.Now()
			meta := CheckpointMeta{
				ID:            "local-scan",
				TenantID:      target.TenantID,
				VolumeID:      target.VolumeID,
				CipherSuiteID: target.CipherSuiteID,
			}
			effectiveSource, err = BuildLocalCheckpoint(ctx, sourceDir, meta, target.Chunking, opts.Workers)
			if err != nil {
				return nil, err
			}
			scanDur = time.Since(scanStart)
		}
		if effectiveSource == nil {
			return nil, fmt.Errorf("%w: source checkpoint required when SkipDelta is false", ErrIncompatibleCheckpoint)
		}
		plan, err = PlanDelta(ctx, effectiveSource, target, opts.PlanOptions)
		if err != nil {
			return nil, err
		}
		if opts.EnableAutoFallback && plan.FallbackRecommended {
			usedFullFallback = true
			plan, err = PlanFullRestore(ctx, target, opts.PlanOptions)
			if err != nil {
				return nil, err
			}
			plan.FallbackRecommended = true
		}
	}
	planDur = time.Since(planStart)

	if len(plan.ReuseOps) > 0 && sourceDir == "" {
		return nil, fmt.Errorf("parasync: sourceDir required to reuse %d local chunks", len(plan.ReuseOps))
	}

	// Prepare atomic staging directory on the destination filesystem.
	if err := os.MkdirAll(destDir, 0o755); err != nil {
		return nil, fmt.Errorf("parasync: create destination dir: %w", err)
	}
	stageDir, err := os.MkdirTemp(destDir, ".parasync-stage-*")
	if err != nil {
		return nil, fmt.Errorf("parasync: create staging dir: %w", err)
	}
	defer func() {
		_ = os.RemoveAll(stageDir)
	}()

	pool, err := newStagedTargetPool(stageDir, target.Files)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = pool.closeAll()
	}()

	// Stage 2 (Remote Fetch + Local Reuse Verify) -> Stage 3 (Parallel Pwrite).
	pipeStart := time.Now()
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		firstErr        error
		errOnce         sync.Once
		wireBytes       atomic.Int64
		remoteReqs      atomic.Int64
		localReadBytes  atomic.Int64
		localWriteBytes atomic.Int64
		verifiedChunks  atomic.Int64
	)
	setErr := func(e error) {
		if e == nil {
			return
		}
		errOnce.Do(func() {
			firstErr = e
			cancel()
		})
	}

	writeCh := make(chan writeJob, opts.Workers*4)
	var writerWG sync.WaitGroup
	for w := 0; w < opts.Workers; w++ {
		writerWG.Add(1)
		go func() {
			defer writerWG.Done()
			for job := range writeCh {
				if runCtx.Err() != nil {
					setErr(runCtx.Err())
					return
				}
				for _, wt := range job.targets {
					if runCtx.Err() != nil {
						setErr(runCtx.Err())
						return
					}
					if err := pool.writeAt(wt.FileIdx, job.data, wt.Offset); err != nil {
						setErr(fmt.Errorf("parasync: pwrite file %d at %d: %w", wt.FileIdx, wt.Offset, err))
						return
					}
					localWriteBytes.Add(int64(len(job.data)))
				}
			}
		}()
	}

	sendWrite := func(job writeJob) bool {
		select {
		case writeCh <- job:
			return true
		case <-runCtx.Done():
			setErr(runCtx.Err())
			return false
		}
	}

	var producerWG sync.WaitGroup

	// Stage 2a: Parallel coalesced remote GetRange fetch + decrypt + BLAKE3 verify.
	if len(plan.FetchBatches) > 0 {
		batchCh := make(chan FetchBatch, len(plan.FetchBatches))
		for _, b := range plan.FetchBatches {
			batchCh <- b
		}
		close(batchCh)

		nFetch := opts.Workers
		if nFetch > len(plan.FetchBatches) {
			nFetch = len(plan.FetchBatches)
		}
		for w := 0; w < nFetch; w++ {
			producerWG.Add(1)
			go func() {
				defer producerWG.Done()
				for batch := range batchCh {
					if runCtx.Err() != nil {
						setErr(runCtx.Err())
						return
					}
					raw, err := store.GetRange(runCtx, batch.PackKey, batch.Offset, batch.Length)
					if err != nil {
						setErr(fmt.Errorf("parasync: remote GetRange %s [%d+%d]: %w",
							batch.PackKey, batch.Offset, batch.Length, err))
						return
					}
					remoteReqs.Add(1)
					wireBytes.Add(int64(len(raw)))
					if int64(len(raw)) != batch.Length {
						setErr(fmt.Errorf("%w: short remote range %s (%d != %d)",
							ErrCorruptChunk, batch.PackKey, len(raw), batch.Length))
						return
					}
					for _, rcf := range batch.Chunks {
						if runCtx.Err() != nil {
							setErr(runCtx.Err())
							return
						}
						if rcf.BatchOffset < 0 || rcf.BatchOffset+rcf.PackLength > int64(len(raw)) {
							setErr(fmt.Errorf("%w: chunk slice out of batch bounds in %s",
								ErrCorruptChunk, batch.PackKey))
							return
						}
						frame := raw[rcf.BatchOffset : rcf.BatchOffset+rcf.PackLength]
						plain, err := openChunkFrame(opts.Cipher, frame)
						if err != nil {
							setErr(err)
							return
						}
						if int64(len(plain)) != rcf.PlainLength {
							setErr(fmt.Errorf("%w: remote chunk length mismatch (%d != %d)",
								ErrCorruptChunk, len(plain), rcf.PlainLength))
							return
						}
						if got := SumDigest(plain); got != rcf.Digest {
							setErr(fmt.Errorf("%w: remote chunk %s digest mismatch (got %s)",
								ErrCorruptChunk, rcf.Digest.Hex(), got.Hex()))
							return
						}
						verifiedChunks.Add(1)
						// Copy slice out of batch buffer so batch buffer can be GC'd promptly.
						out := make([]byte, len(plain))
						copy(out, plain)
						if !sendWrite(writeJob{data: out, targets: rcf.Targets}) {
							return
						}
					}
				}
			}()
		}
	}

	// Stage 2b: Parallel local reuse read + BLAKE3 verify (overlapped with S3 fetch!).
	localGroups := groupLocalReuseOps(plan.ReuseOps)
	if len(localGroups) > 0 {
		groupCh := make(chan localReuseGroup, len(localGroups))
		for _, g := range localGroups {
			groupCh <- g
		}
		close(groupCh)

		nLocal := opts.Workers
		if nLocal > len(localGroups) {
			nLocal = len(localGroups)
		}
		for w := 0; w < nLocal; w++ {
			producerWG.Add(1)
			go func() {
				defer producerWG.Done()
				for grp := range groupCh {
					if runCtx.Err() != nil {
						setErr(runCtx.Err())
						return
					}
					srcPath := filepath.Join(sourceDir, filepath.FromSlash(grp.sourcePath))
					sf, err := os.Open(srcPath)
					if err != nil {
						setErr(fmt.Errorf("parasync: open local source %s: %w", grp.sourcePath, err))
						return
					}
					for _, op := range grp.ops {
						if runCtx.Err() != nil {
							_ = sf.Close()
							setErr(runCtx.Err())
							return
						}
						buf := make([]byte, op.Length)
						n, rerr := sf.ReadAt(buf, op.SourceOffset)
						if rerr != nil && !(rerr == io.EOF && int64(n) == op.Length) {
							_ = sf.Close()
							setErr(fmt.Errorf("parasync: read local chunk %s@%d: %w", grp.sourcePath, op.SourceOffset, rerr))
							return
						}
						if int64(n) != op.Length {
							_ = sf.Close()
							setErr(fmt.Errorf("%w: short local chunk read %s@%d (%d != %d)",
								ErrCorruptChunk, grp.sourcePath, op.SourceOffset, n, op.Length))
							return
						}
						localReadBytes.Add(op.Length)
						if got := SumDigest(buf); got != op.Digest {
							_ = sf.Close()
							setErr(fmt.Errorf("%w: local chunk %s@%d digest mismatch (got %s, want %s)",
								ErrCorruptChunk, grp.sourcePath, op.SourceOffset, got.Hex(), op.Digest.Hex()))
							return
						}
						verifiedChunks.Add(1)
						if !sendWrite(writeJob{
							data: buf,
							targets: []WriteTarget{{
								FileIdx: op.TargetFileIdx,
								Offset:  op.TargetOffset,
							}},
						}) {
							_ = sf.Close()
							return
						}
					}
					_ = sf.Close()
				}
			}()
		}
	}

	producerWG.Wait()
	close(writeCh)
	writerWG.Wait()

	if err := pool.closeAll(); err != nil {
		setErr(err)
	}
	pipeDur := time.Since(pipeStart)

	if firstErr != nil {
		return nil, firstErr
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// Full-file BLAKE3 verification before atomic rename commit.
	verifyStart := time.Now()
	if err := verifyStagedFiles(ctx, pool.paths, target.Files, opts.Workers, opts.SyncFiles); err != nil {
		return nil, err
	}
	verifyDur := time.Since(verifyStart)

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// Atomic, symlink-confined transactional commit + obsolete-file reconciliation.
	commitStart := time.Now()
	if err := commitStagedTree(ctx, destDir, pool.paths, target.Files); err != nil {
		return nil, err
	}
	commitDur := time.Since(commitStart)

	cpuUserEnd, cpuSysEnd := getProcessCPUTime()
	cpuUser := cpuUserEnd - cpuUserStart
	cpuSys := cpuSysEnd - cpuSysStart
	if cpuUser < 0 {
		cpuUser = 0
	}
	if cpuSys < 0 {
		cpuSys = 0
	}

	wire := wireBytes.Load()
	var savedRatio float64
	if plan.TotalTargetBytes > 0 {
		savedRatio = 1.0 - float64(wire)/float64(plan.TotalTargetBytes)
	}

	totalWall := time.Since(totalStart)
	if totalWall < 0 {
		totalWall = planDur + pipeDur + verifyDur + commitDur
	}
	if planDur < 0 {
		planDur = 0
	}
	if scanDur < 0 {
		scanDur = 0
	}
	if pipeDur < 0 {
		pipeDur = 0
	}
	if verifyDur < 0 {
		verifyDur = 0
	}
	if commitDur < 0 {
		commitDur = 0
	}

	return &RestoreReport{
		PlanDuration:            planDur,
		ScanDuration:            scanDur,
		PipelineDuration:        pipeDur,
		VerifyDuration:          verifyDur,
		CommitDuration:          commitDur,
		TotalWallDuration:       totalWall,
		CPUUserDuration:         cpuUser,
		CPUSysDuration:          cpuSys,
		CPUTotalDuration:        cpuUser + cpuSys,
		TotalTargetBytes:        plan.TotalTargetBytes,
		ReusedLocalBytes:        plan.ReuseLocalBytes,
		RemotePayloadBytes:      plan.RemotePayloadBytes,
		UniqueRemoteBytes:       plan.UniqueRemoteBytes,
		WireBytesTransferred:    wire,
		CoalescedGapBytes:       plan.CoalescedGapBytes,
		LocalDiskReadBytes:      localReadBytes.Load(),
		LocalDiskWriteBytes:     localWriteBytes.Load(),
		RemoteRequests:          remoteReqs.Load(),
		SingleFlightDedupChunks: plan.SingleFlightDedupChunks,
		SingleFlightDedupBytes:  plan.SingleFlightDedupBytes,
		BytesSavedRatio:         savedRatio,
		ChurnRatio:              plan.ChurnRatio,
		FallbackRecommended:     plan.FallbackRecommended,
		UsedFullFallback:        usedFullFallback,
		VerifiedFiles:           len(target.Files),
		VerifiedChunks:          verifiedChunks.Load(),
	}, nil
}

func verifyStagedFiles(ctx context.Context, stagedPaths []string, files []FileEntry, workers int, syncFiles bool) error {
	if len(files) == 0 {
		return nil
	}
	if workers <= 0 {
		workers = runtime.NumCPU()
		if workers < 1 {
			workers = 1
		}
	}
	if workers > len(files) {
		workers = len(files)
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	jobs := make(chan int, len(files))
	for i := range files {
		jobs <- i
	}
	close(jobs)

	var (
		wg       sync.WaitGroup
		firstErr error
		errOnce  sync.Once
	)
	setErr := func(e error) {
		errOnce.Do(func() {
			firstErr = e
			cancel()
		})
	}

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			buf := make([]byte, 64<<10)
			for idx := range jobs {
				if runCtx.Err() != nil {
					setErr(runCtx.Err())
					return
				}
				f, err := os.Open(stagedPaths[idx])
				if err != nil {
					setErr(fmt.Errorf("parasync: verify open %s: %w", files[idx].Path, err))
					return
				}
				h := blake3.New()
				var total int64
				for {
					n, rerr := f.Read(buf)
					if n > 0 {
						_, _ = h.Write(buf[:n])
						total += int64(n)
					}
					if rerr == io.EOF {
						break
					}
					if rerr != nil {
						_ = f.Close()
						setErr(fmt.Errorf("parasync: verify read %s: %w", files[idx].Path, rerr))
						return
					}
				}
				if syncFiles {
					if serr := f.Sync(); serr != nil {
						_ = f.Close()
						setErr(fmt.Errorf("parasync: sync staged %s: %w", files[idx].Path, serr))
						return
					}
				}
				_ = f.Close()
				if total != files[idx].Size {
					setErr(fmt.Errorf("%w: verified size mismatch for %s (%d != %d)",
						ErrCorruptCheckpoint, files[idx].Path, total, files[idx].Size))
					return
				}
				var got Digest
				copy(got[:], h.Sum(nil))
				if got != files[idx].FileDigest {
					setErr(fmt.Errorf("%w: verified file digest mismatch for %s (got %s, want %s)",
						ErrCorruptCheckpoint, files[idx].Path, got.Hex(), files[idx].FileDigest.Hex()))
					return
				}
			}
		}()
	}
	wg.Wait()
	return firstErr
}

func isParasyncInternalDir(name string) bool {
	return strings.HasPrefix(name, ".parasync-stage-") || strings.HasPrefix(name, ".parasync-backup-")
}

// commitStagedTree atomically commits all verified staged files into destDir
// while enforcing symlink confinement (never traversing symlinks beneath destDir),
// rolling back all changes on error or cancellation, and reconciling obsolete
// files absent from the target checkpoint.
func commitStagedTree(ctx context.Context, destDir string, stagedPaths []string, files []FileEntry) error {
	fi, err := os.Lstat(destDir)
	if err != nil {
		return fmt.Errorf("parasync: lstat destDir: %w", err)
	}
	if fi.Mode()&os.ModeSymlink != 0 || !fi.IsDir() {
		return fmt.Errorf("%w: destination root %q must be a real directory", ErrInvalidPath, destDir)
	}

	targetSet := make(map[string]struct{}, len(files))
	targetAncestors := make(map[string]struct{}, len(files))

	// Phase 1: Pre-flight symlink confinement and path-type conflict validation
	// before mutating any existing destination file.
	for i := range files {
		if err := ctx.Err(); err != nil {
			return err
		}
		rel := files[i].Path
		targetSet[rel] = struct{}{}
		segs := strings.Split(rel, "/")
		cur := destDir
		for si, seg := range segs {
			cur = filepath.Join(cur, seg)
			isLeaf := si == len(segs)-1
			if !isLeaf {
				targetAncestors[strings.Join(segs[:si+1], "/")] = struct{}{}
			}
			info, lerr := os.Lstat(cur)
			if lerr != nil {
				if os.IsNotExist(lerr) {
					break
				}
				return fmt.Errorf("parasync: lstat %q: %w", rel, lerr)
			}
			if info.Mode()&os.ModeSymlink != 0 {
				return fmt.Errorf("%w: symlink traversal blocked at %q", ErrInvalidPath, strings.Join(segs[:si+1], "/"))
			}
			if !isLeaf && !info.IsDir() {
				return fmt.Errorf("%w: non-directory ancestor %q blocks %q", ErrInvalidPath, strings.Join(segs[:si+1], "/"), rel)
			}
			if isLeaf && info.IsDir() {
				return fmt.Errorf("parasync: destination %q is an existing directory", rel)
			}
		}
	}

	// Phase 2: Collect existing non-target regular files to reconcile (never following symlinks).
	var obsoleteFiles []string
	var candidateDirs []string
	var walkErr error
	var scanDir func(absDir, relPrefix string)
	scanDir = func(absDir, relPrefix string) {
		if walkErr != nil {
			return
		}
		entries, rerr := os.ReadDir(absDir)
		if rerr != nil {
			walkErr = rerr
			return
		}
		for _, de := range entries {
			name := de.Name()
			if relPrefix == "" && isParasyncInternalDir(name) {
				continue
			}
			rel := name
			if relPrefix != "" {
				rel = relPrefix + "/" + name
			}
			abs := filepath.Join(absDir, name)
			info, lerr := os.Lstat(abs)
			if lerr != nil {
				walkErr = lerr
				return
			}
			if info.Mode()&os.ModeSymlink != 0 {
				// Do not traverse symlinks; leave untouched unless targeted (already blocked above).
				continue
			}
			if info.IsDir() {
				scanDir(abs, rel)
				if _, keep := targetAncestors[rel]; !keep {
					candidateDirs = append(candidateDirs, rel)
				}
				continue
			}
			if _, keep := targetSet[rel]; !keep {
				obsoleteFiles = append(obsoleteFiles, rel)
			}
		}
	}
	scanDir(destDir, "")
	if walkErr != nil {
		return fmt.Errorf("parasync: scan destination for reconciliation: %w", walkErr)
	}

	// Phase 3: Transactional backup, install, and obsolete removal with full rollback on error.
	backupDir, err := os.MkdirTemp(destDir, ".parasync-backup-*")
	if err != nil {
		return fmt.Errorf("parasync: create backup dir: %w", err)
	}
	defer os.RemoveAll(backupDir)

	type movedBackup struct {
		origAbs   string
		backupAbs string
	}
	var backups []movedBackup
	var newlyPlaced []string
	var createdDirs []string
	backupSeq := 0

	rollback := func() {
		for i := len(newlyPlaced) - 1; i >= 0; i-- {
			_ = os.Remove(newlyPlaced[i])
		}
		for i := len(backups) - 1; i >= 0; i-- {
			_ = os.Rename(backups[i].backupAbs, backups[i].origAbs)
		}
		for i := len(createdDirs) - 1; i >= 0; i-- {
			_ = os.Remove(createdDirs[i])
		}
	}

	moveToBackup := func(origAbs string) error {
		backupSeq++
		bakAbs := filepath.Join(backupDir, fmt.Sprintf("bak-%06d", backupSeq))
		if err := os.Rename(origAbs, bakAbs); err != nil {
			return err
		}
		backups = append(backups, movedBackup{origAbs: origAbs, backupAbs: bakAbs})
		return nil
	}

	// Step A: Move obsolete files aside into backupDir so rollback restores them cleanly.
	for _, rel := range obsoleteFiles {
		if err := ctx.Err(); err != nil {
			rollback()
			return err
		}
		if err := moveToBackup(filepath.Join(destDir, filepath.FromSlash(rel))); err != nil {
			rollback()
			return fmt.Errorf("parasync: remove obsolete %s: %w", rel, err)
		}
	}

	// Step B: Create missing parent directories safely (no symlinks) and swap staged files in.
	for i := range files {
		if err := ctx.Err(); err != nil {
			rollback()
			return err
		}
		segs := strings.Split(files[i].Path, "/")
		cur := destDir
		for si := 0; si < len(segs)-1; si++ {
			cur = filepath.Join(cur, segs[si])
			info, lerr := os.Lstat(cur)
			if lerr != nil {
				if !os.IsNotExist(lerr) {
					rollback()
					return fmt.Errorf("parasync: lstat parent %s: %w", files[i].Path, lerr)
				}
				if mkErr := os.Mkdir(cur, 0o755); mkErr != nil {
					rollback()
					return fmt.Errorf("parasync: mkdir parent %s: %w", files[i].Path, mkErr)
				}
				createdDirs = append(createdDirs, cur)
				continue
			}
			if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
				rollback()
				return fmt.Errorf("%w: unsafe parent during commit %q", ErrInvalidPath, files[i].Path)
			}
		}

		finalPath := filepath.Join(destDir, filepath.FromSlash(files[i].Path))
		hadExisting := false
		if info, lerr := os.Lstat(finalPath); lerr == nil {
			if info.Mode()&os.ModeSymlink != 0 || info.IsDir() {
				rollback()
				return fmt.Errorf("%w: unsafe target during commit %q", ErrInvalidPath, files[i].Path)
			}
			if err := moveToBackup(finalPath); err != nil {
				rollback()
				return fmt.Errorf("parasync: backup existing %s: %w", files[i].Path, err)
			}
			hadExisting = true
		} else if !os.IsNotExist(lerr) {
			rollback()
			return fmt.Errorf("parasync: lstat target %s: %w", files[i].Path, lerr)
		}

		if err := os.Rename(stagedPaths[i], finalPath); err != nil {
			rollback()
			return fmt.Errorf("parasync: atomic rename %s: %w", files[i].Path, err)
		}
		if !hadExisting {
			newlyPlaced = append(newlyPlaced, finalPath)
		}
		if files[i].Mode != 0 {
			_ = os.Chmod(finalPath, fs.FileMode(files[i].Mode))
		}
	}

	// Step C: Prune now-empty obsolete directories (deepest first).
	sort.Slice(candidateDirs, func(i, j int) bool {
		return len(candidateDirs[i]) > len(candidateDirs[j])
	})
	for _, rel := range candidateDirs {
		_ = os.Remove(filepath.Join(destDir, filepath.FromSlash(rel)))
	}
	return nil
}
