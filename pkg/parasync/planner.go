// Copyright 2026 Hanzo AI Inc.
// SPDX-License-Identifier: Apache-2.0

package parasync

import (
	"context"
	"fmt"
	"runtime"
	"sort"
	"strings"
	"sync"
)

// Default range coalescing bounds for remote S3/backend GetRange batching.
const (
	DefaultMaxCoalesceGap  int64 = 8 << 10 // 8 KiB max gap bridged between adjacent changed chunks
	DefaultMaxCoalesceSpan int64 = 1 << 20 // 1 MiB max coalesced GetRange span
	diffTaskMaxChunks            = 64      // split large files into ~1 MiB diff tasks for balanced shards
)

// PlanOptions controls parallel delta planning and remote range coalescing.
type PlanOptions struct {
	// Workers sets the degree of parallelism for shard diffing (default: NumCPU).
	Workers int

	// MaxCoalesceGap is the maximum byte gap between two changed chunks inside
	// the same remote pack that will be merged into a single GetRange request.
	// Negative disables coalescing entirely; zero uses DefaultMaxCoalesceGap.
	MaxCoalesceGap int64

	// MaxCoalesceSpan caps the byte length of a single coalesced GetRange batch.
	// Zero uses DefaultMaxCoalesceSpan.
	MaxCoalesceSpan int64

	// AutoFallbackChurnRatio is the optional churn threshold in (0, 1] above
	// which the planner marks FallbackRecommended = true. Default 0.85.
	AutoFallbackChurnRatio float64

	// DisableGapBridging forces gap=0 (strictly contiguous ranges only).
	DisableGapBridging bool
}

func (o PlanOptions) normalized() PlanOptions {
	if o.Workers <= 0 {
		o.Workers = runtime.NumCPU()
		if o.Workers < 1 {
			o.Workers = 1
		}
	}
	if o.DisableGapBridging {
		o.MaxCoalesceGap = 0
	} else if o.MaxCoalesceGap == 0 {
		o.MaxCoalesceGap = DefaultMaxCoalesceGap
	} else if o.MaxCoalesceGap < 0 {
		o.MaxCoalesceGap = -1
	}
	if o.MaxCoalesceSpan <= 0 {
		o.MaxCoalesceSpan = DefaultMaxCoalesceSpan
	}
	if o.AutoFallbackChurnRatio <= 0 {
		o.AutoFallbackChurnRatio = 0.85
	}
	return o
}

// LocalReuseOp directs Stage 3 to copy a verified chunk from the local
// SourceCheckpoint directory into the staged target file.
type LocalReuseOp struct {
	TargetFileIdx int
	TargetOffset  int64
	Length        int64
	Digest        Digest
	SourcePath    string
	SourceOffset  int64
}

// WriteTarget identifies one destination (file, offset) for a fetched remote chunk.
type WriteTarget struct {
	FileIdx int
	Offset  int64
}

// RemoteChunkFetch describes one unique remote chunk within a FetchBatch and
// all target file offsets where its verified plaintext must be written.
type RemoteChunkFetch struct {
	Digest      Digest
	PlainLength int64
	PackOffset  int64 // absolute offset inside FetchBatch.PackKey
	PackLength  int64 // encoded length inside pack (plaintext or sealed frame)
	BatchOffset int64 // relative offset inside FetchBatch payload
	Targets     []WriteTarget
}

// FetchBatch is a single gap-coalesced GetRange request against a remote pack.
type FetchBatch struct {
	PackKey string
	Offset  int64
	Length  int64
	Chunks  []RemoteChunkFetch
}

// DeltaPlan is the immutable execution plan produced by PlanDelta or
// PlanFullRestore.
type DeltaPlan struct {
	TenantID      string
	VolumeID      string
	CipherSuiteID string
	SourceID      string
	TargetID      string
	Chunking      ChunkConfig

	ReuseOps     []LocalReuseOp
	FetchBatches []FetchBatch

	TotalTargetBytes        int64
	ReuseLocalBytes         int64
	RemotePayloadBytes      int64 // logical plaintext bytes of changed chunks (before dedup)
	UniqueRemoteBytes       int64 // deduplicated remote plaintext bytes
	CoalescedWireBytes      int64 // actual bytes requested over backend GetRange (incl. bridged gaps)
	CoalescedGapBytes       int64 // extra bytes transferred due to gap bridging
	ReuseChunkCount         int
	RemoteChunkRefCount     int
	UniqueRemoteChunkCount  int
	SingleFlightDedupChunks int
	SingleFlightDedupBytes  int64
	ShardLoads              []int64
	ChurnRatio              float64
	FallbackRecommended     bool
	IsFullRestore           bool
}

// CheckCompatibility verifies that source and target checkpoints belong to the
// exact same (TenantID, VolumeID) namespace, share the same CipherSuiteID and
// ChunkConfig, and have valid manifests and root digests.
//
// Cross-tenant or cross-volume attempts always fail closed with ErrTenantIsolation.
func CheckCompatibility(source, target *Checkpoint) error {
	if source == nil || target == nil {
		return fmt.Errorf("%w: both source and target checkpoints are required", ErrIncompatibleCheckpoint)
	}
	if source.TenantID == "" || source.VolumeID == "" || target.TenantID == "" || target.VolumeID == "" {
		return fmt.Errorf("%w: tenant_id and volume_id must be non-empty", ErrIncompatibleCheckpoint)
	}
	if source.TenantID != target.TenantID || source.VolumeID != target.VolumeID {
		return fmt.Errorf("%w: source=(%s/%s) target=(%s/%s)",
			ErrTenantIsolation, source.TenantID, source.VolumeID, target.TenantID, target.VolumeID)
	}
	if source.CipherSuiteID != target.CipherSuiteID {
		return fmt.Errorf("%w: cipher suite mismatch (%q != %q)",
			ErrIncompatibleCheckpoint, source.CipherSuiteID, target.CipherSuiteID)
	}
	if !source.Chunking.Equal(target.Chunking) {
		return fmt.Errorf("%w: chunking scheme mismatch (%s != %s)",
			ErrIncompatibleCheckpoint, source.Chunking.Mode, target.Chunking.Mode)
	}
	if err := source.ValidateStructure(); err != nil {
		return fmt.Errorf("source checkpoint invalid: %w", err)
	}
	if err := target.ValidateStructure(); err != nil {
		return fmt.Errorf("target checkpoint invalid: %w", err)
	}
	return nil
}

type localChunkSource struct {
	filePath string
	offset   int64
	length   int64
}

type diffTask struct {
	fileIdx    int
	chunkStart int
	chunkEnd   int
	bytes      int64
}

type remoteRef struct {
	fileIdx    int
	fileOffset int64
	chunk      ChunkRef
}

// PlanDelta computes a work-balanced parallel delta plan from source (locally
// available) to target (stored in backend). It enforces strict tenant/volume
// isolation and compatibility before indexing any chunks.
func PlanDelta(ctx context.Context, source, target *Checkpoint, opts PlanOptions) (*DeltaPlan, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := CheckCompatibility(source, target); err != nil {
		return nil, err
	}
	opts = opts.normalized()

	// Build a local chunk digest index strictly scoped to this single
	// (TenantID, VolumeID) source checkpoint.
	localIdx := make(map[Digest]localChunkSource, source.TotalChunks())
	for i := range source.Files {
		f := &source.Files[i]
		for j := range f.Chunks {
			ch := &f.Chunks[j]
			if _, exists := localIdx[ch.Digest]; !exists {
				localIdx[ch.Digest] = localChunkSource{
					filePath: f.Path,
					offset:   ch.Offset,
					length:   ch.Length,
				}
			}
		}
	}

	return buildPlan(ctx, source.ID, target, localIdx, opts, false)
}

// PlanFullRestore computes a 100% remote restore plan for target without
// reading any local source checkpoint. Remote chunks are still single-flight
// deduplicated and coalesced into efficient GetRange batches.
func PlanFullRestore(ctx context.Context, target *Checkpoint, opts PlanOptions) (*DeltaPlan, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := target.ValidateStructure(); err != nil {
		return nil, err
	}
	opts = opts.normalized()
	return buildPlan(ctx, "", target, nil, opts, true)
}

func buildPlan(
	ctx context.Context,
	sourceID string,
	target *Checkpoint,
	localIdx map[Digest]localChunkSource,
	opts PlanOptions,
	fullRestore bool,
) (*DeltaPlan, error) {
	// Partition target files/extents into byte-balanced diff tasks so a single
	// large file does not create straggler shards. Choose task granularity
	// dynamically (~totalBytes / (4 * Workers), clamped to [32 KiB, 512 KiB]).
	var totalBytes int64
	for fi := range target.Files {
		totalBytes += target.Files[fi].Size
	}
	targetTaskBytes := totalBytes / int64(opts.Workers*4)
	const (
		minTaskBytes int64 = 32 << 10
		maxTaskBytes int64 = 512 << 10
	)
	if targetTaskBytes < minTaskBytes {
		targetTaskBytes = minTaskBytes
	}
	if targetTaskBytes > maxTaskBytes {
		targetTaskBytes = maxTaskBytes
	}

	var tasks []diffTask
	for fi := range target.Files {
		f := &target.Files[fi]
		nChunks := len(f.Chunks)
		for start := 0; start < nChunks; {
			end := start
			var taskBytes int64
			for end < nChunks {
				nextLen := f.Chunks[end].Length
				if end > start && taskBytes+nextLen > targetTaskBytes {
					break
				}
				taskBytes += nextLen
				end++
				if end-start >= diffTaskMaxChunks {
					break
				}
			}
			tasks = append(tasks, diffTask{
				fileIdx:    fi,
				chunkStart: start,
				chunkEnd:   end,
				bytes:      taskBytes,
			})
			start = end
		}
	}

	// Longest-Processing-Time-First (LPT) greedy assignment across P shards.
	sort.Slice(tasks, func(i, j int) bool {
		if tasks[i].bytes != tasks[j].bytes {
			return tasks[i].bytes > tasks[j].bytes
		}
		if tasks[i].fileIdx != tasks[j].fileIdx {
			return tasks[i].fileIdx < tasks[j].fileIdx
		}
		return tasks[i].chunkStart < tasks[j].chunkStart
	})

	nShards := opts.Workers
	if nShards > len(tasks) && len(tasks) > 0 {
		nShards = len(tasks)
	}
	if nShards < 1 {
		nShards = 1
	}

	shards := make([][]diffTask, nShards)
	shardLoads := make([]int64, nShards)
	for _, t := range tasks {
		minIdx := 0
		for s := 1; s < nShards; s++ {
			if shardLoads[s] < shardLoads[minIdx] {
				minIdx = s
			}
		}
		shards[minIdx] = append(shards[minIdx], t)
		shardLoads[minIdx] += t.bytes
	}

	type shardOut struct {
		reuse  []LocalReuseOp
		remote []remoteRef
		err    error
	}
	outs := make([]shardOut, nShards)

	scopePrefix := TenantScopePrefix(target.TenantID, target.VolumeID) + "/"
	var wg sync.WaitGroup
	for s := 0; s < nShards; s++ {
		wg.Add(1)
		go func(shardIdx int) {
			defer wg.Done()
			var reuse []LocalReuseOp
			var remote []remoteRef
			for _, task := range shards[shardIdx] {
				if err := ctx.Err(); err != nil {
					outs[shardIdx].err = err
					return
				}
				f := &target.Files[task.fileIdx]
				for ci := task.chunkStart; ci < task.chunkEnd; ci++ {
					ch := f.Chunks[ci]
					if !fullRestore && localIdx != nil {
						if src, ok := localIdx[ch.Digest]; ok && src.length == ch.Length {
							reuse = append(reuse, LocalReuseOp{
								TargetFileIdx: task.fileIdx,
								TargetOffset:  ch.Offset,
								Length:        ch.Length,
								Digest:        ch.Digest,
								SourcePath:    src.filePath,
								SourceOffset:  src.offset,
							})
							continue
						}
					}
					if ch.PackKey == "" || ch.PackLength <= 0 {
						outs[shardIdx].err = fmt.Errorf("%w: missing remote pack location for chunk %s in %q",
							ErrCorruptCheckpoint, ch.Digest.Hex(), f.Path)
						return
					}
					if !strings.HasPrefix(ch.PackKey, scopePrefix) {
						outs[shardIdx].err = fmt.Errorf("%w: remote pack key %q outside target namespace %s",
							ErrTenantIsolation, ch.PackKey, scopePrefix)
						return
					}
					remote = append(remote, remoteRef{
						fileIdx:    task.fileIdx,
						fileOffset: ch.Offset,
						chunk:      ch,
					})
				}
			}
			outs[shardIdx] = shardOut{reuse: reuse, remote: remote}
		}(s)
	}
	wg.Wait()

	var (
		allReuse       []LocalReuseOp
		reuseBytes     int64
		remotePayload  int64
		remoteRefCount int
	)
	uniqueByDigest := make(map[Digest]*RemoteChunkFetch)
	var uniqueOrder []Digest

	for s := range outs {
		if outs[s].err != nil {
			return nil, outs[s].err
		}
		for _, op := range outs[s].reuse {
			allReuse = append(allReuse, op)
			reuseBytes += op.Length
		}
		for _, ref := range outs[s].remote {
			remoteRefCount++
			remotePayload += ref.chunk.Length
			existing, ok := uniqueByDigest[ref.chunk.Digest]
			if ok {
				existing.Targets = append(existing.Targets, WriteTarget{
					FileIdx: ref.fileIdx,
					Offset:  ref.fileOffset,
				})
				continue
			}
			entry := &RemoteChunkFetch{
				Digest:      ref.chunk.Digest,
				PlainLength: ref.chunk.Length,
				PackOffset:  ref.chunk.PackOffset,
				PackLength:  ref.chunk.PackLength,
				Targets: []WriteTarget{{
					FileIdx: ref.fileIdx,
					Offset:  ref.fileOffset,
				}},
			}
			uniqueByDigest[ref.chunk.Digest] = entry
			uniqueOrder = append(uniqueOrder, ref.chunk.Digest)
		}
	}

	// Group unique remote chunks by PackKey for gap-coalesced GetRange batching.
	byPack := make(map[string][]*RemoteChunkFetch)
	// Recover PackKey per unique digest cleanly.
	packKeyByDigest := make(map[Digest]string, len(uniqueByDigest))
	for s := range outs {
		for _, ref := range outs[s].remote {
			if _, ok := packKeyByDigest[ref.chunk.Digest]; !ok {
				packKeyByDigest[ref.chunk.Digest] = ref.chunk.PackKey
			}
		}
	}

	var uniqueRemoteBytes int64
	for _, d := range uniqueOrder {
		rcf := uniqueByDigest[d]
		uniqueRemoteBytes += rcf.PlainLength
		pk := packKeyByDigest[d]
		byPack[pk] = append(byPack[pk], rcf)
	}

	packKeys := make([]string, 0, len(byPack))
	for pk := range byPack {
		packKeys = append(packKeys, pk)
	}
	sort.Strings(packKeys)

	var (
		batches            []FetchBatch
		coalescedWireBytes int64
		coalescedGapBytes  int64
	)

	for _, pk := range packKeys {
		list := byPack[pk]
		sort.Slice(list, func(i, j int) bool {
			if list[i].PackOffset != list[j].PackOffset {
				return list[i].PackOffset < list[j].PackOffset
			}
			return list[i].PackLength < list[j].PackLength
		})

		var cur FetchBatch
		var curEnd int64
		hasCur := false

		flush := func() {
			if !hasCur {
				return
			}
			cur.Length = curEnd - cur.Offset
			var sumChunkPackBytes int64
			for i := range cur.Chunks {
				cur.Chunks[i].BatchOffset = cur.Chunks[i].PackOffset - cur.Offset
				sumChunkPackBytes += cur.Chunks[i].PackLength
			}
			if cur.Length > sumChunkPackBytes {
				coalescedGapBytes += cur.Length - sumChunkPackBytes
			}
			coalescedWireBytes += cur.Length
			batches = append(batches, cur)
			hasCur = false
		}

		for _, item := range list {
			itemEnd := item.PackOffset + item.PackLength
			if !hasCur {
				cur = FetchBatch{
					PackKey: pk,
					Offset:  item.PackOffset,
					Chunks:  []RemoteChunkFetch{*item},
				}
				curEnd = itemEnd
				hasCur = true
				continue
			}

			gap := item.PackOffset - curEnd
			newEnd := itemEnd
			if curEnd > newEnd {
				newEnd = curEnd
			}
			mergedSpan := newEnd - cur.Offset

			canCoalesce := opts.MaxCoalesceGap >= 0 &&
				gap >= 0 &&
				gap <= opts.MaxCoalesceGap &&
				mergedSpan <= opts.MaxCoalesceSpan

			if canCoalesce {
				cur.Chunks = append(cur.Chunks, *item)
				curEnd = newEnd
			} else {
				flush()
				cur = FetchBatch{
					PackKey: pk,
					Offset:  item.PackOffset,
					Chunks:  []RemoteChunkFetch{*item},
				}
				curEnd = itemEnd
				hasCur = true
			}
		}
		flush()
	}

	// Deterministic ordering of ReuseOps by (TargetFileIdx, TargetOffset).
	sort.Slice(allReuse, func(i, j int) bool {
		if allReuse[i].TargetFileIdx != allReuse[j].TargetFileIdx {
			return allReuse[i].TargetFileIdx < allReuse[j].TargetFileIdx
		}
		return allReuse[i].TargetOffset < allReuse[j].TargetOffset
	})

	var churnRatio float64
	if totalBytes > 0 {
		churnRatio = float64(remotePayload) / float64(totalBytes)
	}
	dedupChunks := remoteRefCount - len(uniqueOrder)
	dedupBytes := remotePayload - uniqueRemoteBytes
	if dedupChunks < 0 {
		dedupChunks = 0
	}
	if dedupBytes < 0 {
		dedupBytes = 0
	}

	plan := &DeltaPlan{
		TenantID:                target.TenantID,
		VolumeID:                target.VolumeID,
		CipherSuiteID:           target.CipherSuiteID,
		SourceID:                sourceID,
		TargetID:                target.ID,
		Chunking:                target.Chunking,
		ReuseOps:                allReuse,
		FetchBatches:            batches,
		TotalTargetBytes:        totalBytes,
		ReuseLocalBytes:         reuseBytes,
		RemotePayloadBytes:      remotePayload,
		UniqueRemoteBytes:       uniqueRemoteBytes,
		CoalescedWireBytes:      coalescedWireBytes,
		CoalescedGapBytes:       coalescedGapBytes,
		ReuseChunkCount:         len(allReuse),
		RemoteChunkRefCount:     remoteRefCount,
		UniqueRemoteChunkCount:  len(uniqueOrder),
		SingleFlightDedupChunks: dedupChunks,
		SingleFlightDedupBytes:  dedupBytes,
		ShardLoads:              shardLoads,
		ChurnRatio:              churnRatio,
		FallbackRecommended:     !fullRestore && totalBytes > 0 && churnRatio >= opts.AutoFallbackChurnRatio,
		IsFullRestore:           fullRestore,
	}
	return plan, nil
}
