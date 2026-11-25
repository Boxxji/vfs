// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package aiturbo

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"
)

const (
	// DefaultGroupTargetBytes is the default AITURBO super-object size target (4 MiB),
	// balancing S3 request coalescing against multi-stream parallelism and partial-read locality.
	DefaultGroupTargetBytes int64 = 4 << 20

	// DefaultLargePassthroughBytes is the size threshold above which an already-large
	// tensor bypasses coalescing buffers and uploads directly as a standalone immutable group.
	DefaultLargePassthroughBytes int64 = 4 << 20

	// DefaultWorkers bounds concurrent upload/download HTTP streams per planner.
	DefaultWorkers = 8

	// DefaultMaxRangeGapBytes merges nearby requested ranges within the same super-object
	// when the gap between them is at most 64 KiB, trading tiny bounded read-ahead for fewer GETs.
	DefaultMaxRangeGapBytes int64 = 64 << 10

	// DefaultMaxMergedRangeBytes caps any single merged HTTP Range GET at 4 MiB so
	// large selective restores retain parallel stream concurrency.
	DefaultMaxMergedRangeBytes int64 = 4 << 20

	// OverCoalescedGroupTargetBytes is the intentionally oversized 256 MiB group target
	// used by the over_coalesced_giant_group_negative control.
	OverCoalescedGroupTargetBytes int64 = 256 << 20
)

// PlanConfig configures how the grouped checkpoint planner packs, uploads, and restores.
type PlanConfig struct {
	Strategy                    Strategy
	GroupTargetBytes            int64
	LargeObjectPassthroughBytes int64
	Workers                     int
	MaxRangeGapBytes            int64
	MaxMergedRangeBytes         int64
	UseRangeReads               bool
}

// DefaultConfigForStrategy returns the canonical matched configuration for each
// of the five benchmarked checkpoint IO strategies.
func DefaultConfigForStrategy(s Strategy) PlanConfig {
	switch s {
	case StrategyPerObjectSequential:
		return PlanConfig{
			Strategy:                    s,
			GroupTargetBytes:            1,
			LargeObjectPassthroughBytes: 1,
			Workers:                     1,
			MaxRangeGapBytes:            0,
			MaxMergedRangeBytes:         DefaultMaxMergedRangeBytes,
			UseRangeReads:               false,
		}
	case StrategyPerObjectParallel:
		return PlanConfig{
			Strategy:                    s,
			GroupTargetBytes:            1,
			LargeObjectPassthroughBytes: 1,
			Workers:                     DefaultWorkers,
			MaxRangeGapBytes:            0,
			MaxMergedRangeBytes:         DefaultMaxMergedRangeBytes,
			UseRangeReads:               false,
		}
	case StrategyNaiveTarball:
		return PlanConfig{
			Strategy:                    s,
			GroupTargetBytes:            math.MaxInt64,
			LargeObjectPassthroughBytes: math.MaxInt64,
			Workers:                     1,
			MaxRangeGapBytes:            0,
			MaxMergedRangeBytes:         math.MaxInt64,
			UseRangeReads:               false,
		}
	case StrategyOverCoalesced:
		return PlanConfig{
			Strategy:                    s,
			GroupTargetBytes:            OverCoalescedGroupTargetBytes,
			LargeObjectPassthroughBytes: OverCoalescedGroupTargetBytes,
			Workers:                     DefaultWorkers,
			MaxRangeGapBytes:            0,
			MaxMergedRangeBytes:         OverCoalescedGroupTargetBytes,
			UseRangeReads:               false,
		}
	case StrategyAITurboGrouped:
		fallthrough
	default:
		return PlanConfig{
			Strategy:                    StrategyAITurboGrouped,
			GroupTargetBytes:            DefaultGroupTargetBytes,
			LargeObjectPassthroughBytes: DefaultLargePassthroughBytes,
			Workers:                     DefaultWorkers,
			MaxRangeGapBytes:            DefaultMaxRangeGapBytes,
			MaxMergedRangeBytes:         DefaultMaxMergedRangeBytes,
			UseRangeReads:               true,
		}
	}
}

// Validate checks configuration invariants and returns a normalized copy.
func (c PlanConfig) Validate() (PlanConfig, error) {
	if c.Strategy == "" {
		c.Strategy = StrategyAITurboGrouped
	}
	if c.Workers <= 0 {
		c.Workers = 1
	}
	if c.GroupTargetBytes <= 0 {
		return c, fmt.Errorf("%w: GroupTargetBytes must be > 0", ErrInvalidConfig)
	}
	if c.LargeObjectPassthroughBytes <= 0 {
		c.LargeObjectPassthroughBytes = c.GroupTargetBytes
	}
	if c.MaxRangeGapBytes < 0 {
		return c, fmt.Errorf("%w: MaxRangeGapBytes must be >= 0", ErrInvalidConfig)
	}
	if c.MaxMergedRangeBytes <= 0 {
		c.MaxMergedRangeBytes = DefaultMaxMergedRangeBytes
	}
	return c, nil
}

// PlannedGroup is a single immutable super-object scheduled for upload.
type PlannedGroup struct {
	Key        string
	Data       []byte
	SHA256Hex  string
	Entries    []GroupManifestEntry
	PackedCopy bool // true if an intermediate coalescing buffer was allocated
}

// UploadPlan is the deterministic result of planning a checkpoint's group layout.
type UploadPlan struct {
	TenantID     string
	CheckpointID string
	Strategy     Strategy
	Groups       []PlannedGroup
	Entries      []GroupManifestEntry
	TotalBytes   int64
	PackingBytes int64
}

// RangeSpan represents a coalesced byte range [Start, End) within a group object.
type RangeSpan struct {
	GroupKey string
	Start    int64
	End      int64
	Entries  []GroupManifestEntry
}

// Length returns the byte length of the coalesced span.
func (r RangeSpan) Length() int64 { return r.End - r.Start }

// Planner executes grouped checkpoint uploads, atomic conditional Head commits,
// and coalesced Range-GET restores over an ObjectStore.
type Planner struct {
	store ObjectStore
	cfg   PlanConfig
}

// NewPlanner constructs a grouped checkpoint IO planner over store.
func NewPlanner(store ObjectStore, cfg PlanConfig) (*Planner, error) {
	if store == nil {
		return nil, fmt.Errorf("%w: ObjectStore is required", ErrInvalidConfig)
	}
	norm, err := cfg.Validate()
	if err != nil {
		return nil, err
	}
	return &Planner{store: store, cfg: norm}, nil
}

// Config returns the planner's normalized PlanConfig.
func (p *Planner) Config() PlanConfig { return p.cfg }

// PlanGroups deterministically groups a checkpoint's files into tenant-isolated
// immutable super-objects according to the configured Strategy.
func (p *Planner) PlanGroups(ckpt *Checkpoint) (*UploadPlan, error) {
	if ckpt == nil {
		return nil, fmt.Errorf("%w: nil checkpoint", ErrInvalidConfig)
	}
	if err := ValidateTenantID(ckpt.TenantID); err != nil {
		return nil, err
	}
	if ckpt.ID == "" {
		return nil, fmt.Errorf("%w: checkpoint ID is required", ErrInvalidConfig)
	}

	// Copy and deterministically sort by path so files in the same directory or
	// tensor layer stay adjacent inside super-objects for Range-GET locality.
	files := make([]CheckpointFile, len(ckpt.Files))
	seen := make(map[string]struct{}, len(ckpt.Files))
	var totalBytes int64
	for i, f := range ckpt.Files {
		if f.TenantID != "" && f.TenantID != ckpt.TenantID {
			return nil, fmt.Errorf("%w: file %q belongs to tenant %q, checkpoint tenant is %q",
				ErrTenantViolation, f.Path, f.TenantID, ckpt.TenantID)
		}
		if err := ValidateRelativePath(f.Path); err != nil {
			return nil, err
		}
		if _, dup := seen[f.Path]; dup {
			return nil, fmt.Errorf("%w: duplicate file path %q", ErrInvalidConfig, f.Path)
		}
		seen[f.Path] = struct{}{}
		if f.SHA256 == ([32]byte{}) {
			f.SHA256 = sha256.Sum256(f.Data)
		}
		f.TenantID = ckpt.TenantID
		files[i] = f
		totalBytes += int64(len(f.Data))
	}
	sort.Slice(files, func(i, j int) bool {
		return files[i].Path < files[j].Path
	})

	plan := &UploadPlan{
		TenantID:     ckpt.TenantID,
		CheckpointID: ckpt.ID,
		Strategy:     p.cfg.Strategy,
		TotalBytes:   totalBytes,
	}

	// Per-object baselines: every file is its own single-entry object (no coalescing).
	if p.cfg.Strategy == StrategyPerObjectSequential || p.cfg.Strategy == StrategyPerObjectParallel {
		plan.Groups = make([]PlannedGroup, 0, len(files))
		plan.Entries = make([]GroupManifestEntry, 0, len(files))
		for _, f := range files {
			g, err := buildStandaloneGroup(ckpt.TenantID, f)
			if err != nil {
				return nil, err
			}
			plan.Groups = append(plan.Groups, g)
			plan.Entries = append(plan.Entries, g.Entries...)
		}
		return plan, nil
	}

	// Grouped strategies (AITURBO, naive tarball monolith, over-coalesced negative control):
	// First partition files into deterministic batches (metadata only, O(N)),
	// then assemble super-object buffers in parallel across bounded workers.
	type batchSpec struct {
		standalone bool
		files      []CheckpointFile
		totalBytes int64
	}
	var specs []batchSpec
	var openBatch []CheckpointFile
	var openBytes int64

	flushBatch := func() {
		if len(openBatch) == 0 {
			return
		}
		cp := make([]CheckpointFile, len(openBatch))
		copy(cp, openBatch)
		specs = append(specs, batchSpec{
			standalone: len(cp) == 1 && p.cfg.Strategy == StrategyAITurboGrouped,
			files:      cp,
			totalBytes: openBytes,
		})
		openBatch = openBatch[:0]
		openBytes = 0
	}

	for _, f := range files {
		sz := int64(len(f.Data))
		if sz >= p.cfg.LargeObjectPassthroughBytes {
			specs = append(specs, batchSpec{
				standalone: true,
				files:      []CheckpointFile{f},
				totalBytes: sz,
			})
			continue
		}
		// If adding this file would exceed GroupTargetBytes and we already have
		// staged files, seal the current super-object first.
		if len(openBatch) > 0 && openBytes+sz > p.cfg.GroupTargetBytes {
			flushBatch()
		}
		openBatch = append(openBatch, f)
		openBytes += sz
		if openBytes >= p.cfg.GroupTargetBytes {
			flushBatch()
		}
	}
	flushBatch()

	groups := make([]PlannedGroup, len(specs))
	workers := min(p.cfg.Workers, len(specs))
	if workers <= 1 {
		for i, sp := range specs {
			var (
				g   PlannedGroup
				err error
			)
			if sp.standalone {
				g, err = buildStandaloneGroup(ckpt.TenantID, sp.files[0])
			} else {
				g, err = buildCoalescedGroup(ckpt.TenantID, sp.files, sp.totalBytes)
			}
			if err != nil {
				return nil, err
			}
			groups[i] = g
		}
	} else {
		idxCh := make(chan int, len(specs))
		for i := range specs {
			idxCh <- i
		}
		close(idxCh)

		var (
			wg       sync.WaitGroup
			errOnce  sync.Once
			firstErr error
		)
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := range idxCh {
					sp := specs[i]
					var (
						g   PlannedGroup
						err error
					)
					if sp.standalone {
						g, err = buildStandaloneGroup(ckpt.TenantID, sp.files[0])
					} else {
						g, err = buildCoalescedGroup(ckpt.TenantID, sp.files, sp.totalBytes)
					}
					if err != nil {
						errOnce.Do(func() { firstErr = err })
						return
					}
					groups[i] = g
				}
			}()
		}
		wg.Wait()
		if firstErr != nil {
			return nil, firstErr
		}
	}

	plan.Groups = groups
	plan.Entries = make([]GroupManifestEntry, 0, len(files))
	for _, g := range groups {
		plan.Entries = append(plan.Entries, g.Entries...)
		if g.PackedCopy {
			plan.PackingBytes += int64(len(g.Data))
		}
	}

	sort.Slice(plan.Entries, func(i, j int) bool {
		return plan.Entries[i].Path < plan.Entries[j].Path
	})
	return plan, nil
}

func buildStandaloneGroup(tenantID string, f CheckpointFile) (PlannedGroup, error) {
	digestHex := hex.EncodeToString(f.SHA256[:])
	key, err := TenantGroupKey(tenantID, digestHex)
	if err != nil {
		return PlannedGroup{}, err
	}
	entry := GroupManifestEntry{
		Path:      f.Path,
		GroupKey:  key,
		Offset:    0,
		Length:    int64(len(f.Data)),
		SHA256Hex: digestHex,
		GroupSize: int64(len(f.Data)),
	}
	return PlannedGroup{
		Key:        key,
		Data:       f.Data,
		SHA256Hex:  digestHex,
		Entries:    []GroupManifestEntry{entry},
		PackedCopy: false,
	}, nil
}

func buildCoalescedGroup(tenantID string, batch []CheckpointFile, totalBytes int64) (PlannedGroup, error) {
	buf := make([]byte, 0, totalBytes)
	entries := make([]GroupManifestEntry, 0, len(batch))
	// Compute a deterministic content-addressed Merkle digest over the ordered
	// (offset, length, member SHA-256) tuples rather than re-hashing raw payload
	// bytes; each entry's raw bytes are independently SHA-256 verified on restore.
	merkle := sha256.New()
	_, _ = fmt.Fprintf(merkle, "aiturbo-group-v1|tenant=%s|count=%d\n", tenantID, len(batch))
	for _, f := range batch {
		off := int64(len(buf))
		buf = append(buf, f.Data...)
		fHex := hex.EncodeToString(f.SHA256[:])
		_, _ = fmt.Fprintf(merkle, "%d|%d|%s\n", off, len(f.Data), fHex)
		entries = append(entries, GroupManifestEntry{
			Path:      f.Path,
			Offset:    off,
			Length:    int64(len(f.Data)),
			SHA256Hex: fHex,
			GroupSize: totalBytes,
		})
	}
	digestHex := hex.EncodeToString(merkle.Sum(nil))
	key, err := TenantGroupKey(tenantID, digestHex)
	if err != nil {
		return PlannedGroup{}, err
	}
	for i := range entries {
		entries[i].GroupKey = key
		entries[i].GroupSize = int64(len(buf))
	}
	return PlannedGroup{
		Key:        key,
		Data:       buf,
		SHA256Hex:  digestHex,
		Entries:    entries,
		PackedCopy: true,
	}, nil
}

// ReadHead fetches and decodes the tenant's current HeadRecord and its ETag.
func (p *Planner) ReadHead(ctx context.Context, tenantID string) (HeadRecord, string, error) {
	headKey, err := TenantHeadKey(tenantID)
	if err != nil {
		return HeadRecord{}, "", err
	}
	raw, etag, err := p.store.Get(ctx, headKey)
	if err != nil {
		return HeadRecord{}, "", err
	}
	var head HeadRecord
	if err := json.Unmarshal(raw, &head); err != nil {
		return HeadRecord{}, "", fmt.Errorf("aiturbo: decode head: %w", err)
	}
	if head.TenantID != tenantID {
		return HeadRecord{}, "", fmt.Errorf("%w: head tenant %q != requested %q",
			ErrTenantViolation, head.TenantID, tenantID)
	}
	if !KeyBelongsToTenant(tenantID, head.ManifestKey) {
		return HeadRecord{}, "", fmt.Errorf("%w: manifest key %q escapes tenant %q",
			ErrTenantViolation, head.ManifestKey, tenantID)
	}
	return head, etag, nil
}

// CommitCheckpoint publishes a checkpoint using strict three-phase barrier
// ordering:
//  1. Upload all immutable group super-objects (bounded parallel workers).
//  2. Upload the immutable content-addressed CheckpointManifest.
//  3. Atomically publish Head via conditional PUT:
//     - expectedPrevETag == "" -> PutIfAbsent (If-None-Match: *)
//     - expectedPrevETag != "" -> PutIfMatch  (If-Match: expectedPrevETag)
//
// If any data or manifest upload fails, Head is never touched. If a concurrent
// writer wins the Head race, ErrHeadConflict (HTTP 412) is returned with zero
// silent overwrite and zero torn checkpoint state.
func (p *Planner) CommitCheckpoint(ctx context.Context, ckpt *Checkpoint, expectedPrevETag string, nextGeneration uint64) (*CommitResult, error) {
	start := time.Now()
	before := p.store.Metrics()

	plan, err := p.PlanGroups(ckpt)
	if err != nil {
		return nil, err
	}

	// Deduplicate identical immutable content-addressed group objects within one commit.
	uniqueGroups := make([]PlannedGroup, 0, len(plan.Groups))
	seenGroup := make(map[string]struct{}, len(plan.Groups))
	for _, g := range plan.Groups {
		if _, dup := seenGroup[g.Key]; dup {
			continue
		}
		seenGroup[g.Key] = struct{}{}
		uniqueGroups = append(uniqueGroups, g)
	}

	// Phase 1: Bounded parallel upload of all immutable group super-objects.
	if err := p.uploadGroups(ctx, uniqueGroups); err != nil {
		return nil, fmt.Errorf("aiturbo: phase-1 group upload failed (head untouched): %w", err)
	}

	// Phase 2: Build and upload immutable content-addressed manifest.
	groupInfos := make([]GroupObjectInfo, len(uniqueGroups))
	for i, g := range uniqueGroups {
		groupInfos[i] = GroupObjectInfo{
			Key:        g.Key,
			Size:       int64(len(g.Data)),
			SHA256Hex:  g.SHA256Hex,
			EntryCount: len(g.Entries),
		}
	}
	manifest := CheckpointManifest{
		Version:      1,
		CheckpointID: ckpt.ID,
		TenantID:     ckpt.TenantID,
		Strategy:     p.cfg.Strategy,
		Generation:   nextGeneration,
		CreatedAt:    time.Now().UTC(),
		TotalBytes:   plan.TotalBytes,
		FileCount:    len(plan.Entries),
		Groups:       groupInfos,
		Entries:      plan.Entries,
	}
	manifest.ManifestSHA256 = ComputeManifestDigest(&manifest)
	manifestBytes, err := json.Marshal(manifest)
	if err != nil {
		return nil, fmt.Errorf("aiturbo: encode manifest: %w", err)
	}
	manifestSum := sha256.Sum256(manifestBytes)
	manifestKey, err := TenantManifestKey(ckpt.TenantID, hex.EncodeToString(manifestSum[:]))
	if err != nil {
		return nil, err
	}
	if _, err := p.store.Put(ctx, manifestKey, manifestBytes); err != nil {
		return nil, fmt.Errorf("aiturbo: phase-2 manifest upload failed (head untouched): %w", err)
	}

	// Phase 3: Atomic conditional Head PUT barrier.
	headKey, err := TenantHeadKey(ckpt.TenantID)
	if err != nil {
		return nil, err
	}
	head := HeadRecord{
		Version:        1,
		TenantID:       ckpt.TenantID,
		Generation:     nextGeneration,
		CheckpointID:   ckpt.ID,
		Strategy:       p.cfg.Strategy,
		ManifestKey:    manifestKey,
		ManifestSHA256: manifest.ManifestSHA256,
		TotalBytes:     plan.TotalBytes,
		FileCount:      len(plan.Entries),
		CommittedAt:    time.Now().UTC(),
	}
	headBytes, err := json.Marshal(head)
	if err != nil {
		return nil, fmt.Errorf("aiturbo: encode head: %w", err)
	}

	var newETag string
	if expectedPrevETag == "" {
		newETag, err = p.store.PutIfAbsent(ctx, headKey, headBytes)
	} else {
		newETag, err = p.store.PutIfMatch(ctx, headKey, headBytes, expectedPrevETag)
	}
	if err != nil {
		return nil, err
	}

	after := p.store.Metrics()
	return &CommitResult{
		Head:         head,
		HeadETag:     newETag,
		ManifestKey:  manifestKey,
		GroupCount:   len(uniqueGroups),
		TotalBytes:   plan.TotalBytes,
		Duration:     time.Since(start),
		StoreMetrics: after.Sub(before),
		PackingBytes: plan.PackingBytes,
	}, nil
}

func (p *Planner) uploadGroups(ctx context.Context, groups []PlannedGroup) error {
	if len(groups) == 0 {
		return nil
	}
	// Schedule largest super-objects first (LPT) so multi-MiB weight tensors
	// start at t=0 and smaller super-objects backfill trailing worker slots.
	ordered := make([]PlannedGroup, len(groups))
	copy(ordered, groups)
	if p.cfg.Workers > 1 {
		sort.SliceStable(ordered, func(i, j int) bool {
			return len(ordered[i].Data) > len(ordered[j].Data)
		})
	}

	workers := min(p.cfg.Workers, len(ordered))
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	jobs := make(chan PlannedGroup, len(ordered))
	for _, g := range ordered {
		jobs <- g
	}
	close(jobs)

	var (
		wg       sync.WaitGroup
		errOnce  sync.Once
		firstErr error
	)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for g := range jobs {
				if ctx.Err() != nil {
					return
				}
				if _, err := p.store.Put(ctx, g.Key, g.Data); err != nil {
					errOnce.Do(func() {
						firstErr = err
						cancel()
					})
					return
				}
			}
		}()
	}
	wg.Wait()
	return firstErr
}

// MergeRanges coalesces sorted requested entries within each super-object into
// bounded HTTP Range spans when adjacent entries are separated by at most
// maxGapBytes and the merged span stays within maxSpanBytes.
func MergeRanges(entries []GroupManifestEntry, maxGapBytes, maxSpanBytes int64) []RangeSpan {
	if len(entries) == 0 {
		return nil
	}
	byGroup := make(map[string][]GroupManifestEntry)
	groupOrder := make([]string, 0)
	for _, e := range entries {
		if e.Length == 0 {
			continue
		}
		if _, ok := byGroup[e.GroupKey]; !ok {
			groupOrder = append(groupOrder, e.GroupKey)
		}
		byGroup[e.GroupKey] = append(byGroup[e.GroupKey], e)
	}
	sort.Strings(groupOrder)

	var spans []RangeSpan
	for _, gk := range groupOrder {
		list := byGroup[gk]
		sort.Slice(list, func(i, j int) bool {
			if list[i].Offset == list[j].Offset {
				return list[i].Path < list[j].Path
			}
			return list[i].Offset < list[j].Offset
		})
		cur := RangeSpan{
			GroupKey: gk,
			Start:    list[0].Offset,
			End:      list[0].Offset + list[0].Length,
			Entries:  []GroupManifestEntry{list[0]},
		}
		for _, e := range list[1:] {
			eEnd := e.Offset + e.Length
			gap := e.Offset - cur.End
			mergedEnd := max(cur.End, eEnd)
			if gap <= maxGapBytes && (mergedEnd-cur.Start) <= maxSpanBytes {
				cur.End = mergedEnd
				cur.Entries = append(cur.Entries, e)
			} else {
				spans = append(spans, cur)
				cur = RangeSpan{
					GroupKey: gk,
					Start:    e.Offset,
					End:      eEnd,
					Entries:  []GroupManifestEntry{e},
				}
			}
		}
		spans = append(spans, cur)
	}
	return spans
}

// RestoreCheckpoint restores all files from the tenant's latest committed Head
// and verifies bit-exact SHA-256 integrity for every file.
func (p *Planner) RestoreCheckpoint(ctx context.Context, tenantID string) (*RestoreResult, error) {
	return p.restore(ctx, tenantID, nil, p.cfg.UseRangeReads)
}

// RestoreSubset restores only the requested paths from the tenant's latest
// committed Head, using coalesced HTTP Range GETs when enabled by PlanConfig.
func (p *Planner) RestoreSubset(ctx context.Context, tenantID string, paths []string) (*RestoreResult, error) {
	return p.restore(ctx, tenantID, paths, p.cfg.UseRangeReads)
}

// RestoreSubsetWithRangeMode restores the requested paths while explicitly
// overriding UseRangeReads, allowing controlled A/B read-amplification checks
// against the exact same committed checkpoint.
func (p *Planner) RestoreSubsetWithRangeMode(ctx context.Context, tenantID string, paths []string, useRangeReads bool) (*RestoreResult, error) {
	return p.restore(ctx, tenantID, paths, useRangeReads)
}

func (p *Planner) restore(ctx context.Context, tenantID string, subset []string, useRangeReads bool) (*RestoreResult, error) {
	start := time.Now()
	before := p.store.Metrics()

	head, _, err := p.ReadHead(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	manifestRaw, _, err := p.store.Get(ctx, head.ManifestKey)
	if err != nil {
		return nil, fmt.Errorf("aiturbo: fetch manifest %s: %w", head.ManifestKey, err)
	}
	var manifest CheckpointManifest
	if err := json.Unmarshal(manifestRaw, &manifest); err != nil {
		return nil, fmt.Errorf("aiturbo: decode manifest: %w", err)
	}
	if manifest.TenantID != tenantID {
		return nil, fmt.Errorf("%w: manifest tenant %q != requested %q",
			ErrTenantViolation, manifest.TenantID, tenantID)
	}
	if got := ComputeManifestDigest(&manifest); got != head.ManifestSHA256 || got != manifest.ManifestSHA256 {
		return nil, fmt.Errorf("%w: manifest digest mismatch", ErrIntegrityMismatch)
	}

	entryByPath := make(map[string]GroupManifestEntry, len(manifest.Entries))
	for _, e := range manifest.Entries {
		if !KeyBelongsToTenant(tenantID, e.GroupKey) {
			return nil, fmt.Errorf("%w: group key %q escapes tenant %q",
				ErrTenantViolation, e.GroupKey, tenantID)
		}
		entryByPath[e.Path] = e
	}

	var targetEntries []GroupManifestEntry
	if len(subset) == 0 {
		targetEntries = manifest.Entries
	} else {
		targetEntries = make([]GroupManifestEntry, 0, len(subset))
		for _, pth := range subset {
			e, ok := entryByPath[pth]
			if !ok {
				return nil, fmt.Errorf("%w: path %q not in checkpoint %s", ErrNotFound, pth, manifest.CheckpointID)
			}
			targetEntries = append(targetEntries, e)
		}
	}

	files := make(map[string][]byte, len(targetEntries))
	var requestedBytes int64
	var nonEmpty []GroupManifestEntry
	for _, e := range targetEntries {
		requestedBytes += e.Length
		if e.Length == 0 {
			emptySum := sha256.Sum256(nil)
			if hex.EncodeToString(emptySum[:]) != e.SHA256Hex {
				return nil, fmt.Errorf("%w: zero-byte file %q checksum mismatch", ErrIntegrityMismatch, e.Path)
			}
			files[e.Path] = []byte{}
			continue
		}
		nonEmpty = append(nonEmpty, e)
	}

	// Build fetch tasks: full-group fetches for full restores or non-range mode,
	// and coalesced HTTP Range GET spans for selective range-enabled restores.
	type fetchTask struct {
		groupKey string
		fullGet  bool
		start    int64
		end      int64
		entries  []GroupManifestEntry
	}

	var tasks []fetchTask
	isFullRestore := len(subset) == 0
	if isFullRestore || !useRangeReads {
		byGroup := make(map[string][]GroupManifestEntry)
		order := make([]string, 0)
		for _, e := range nonEmpty {
			if _, ok := byGroup[e.GroupKey]; !ok {
				order = append(order, e.GroupKey)
			}
			byGroup[e.GroupKey] = append(byGroup[e.GroupKey], e)
		}
		sort.Strings(order)
		for _, gk := range order {
			tasks = append(tasks, fetchTask{
				groupKey: gk,
				fullGet:  true,
				entries:  byGroup[gk],
			})
		}
	} else {
		spans := MergeRanges(nonEmpty, p.cfg.MaxRangeGapBytes, p.cfg.MaxMergedRangeBytes)
		for _, sp := range spans {
			// If a span covers the entire group object, issue a plain GET.
			full := len(sp.Entries) > 0 && sp.Start == 0 && sp.End == sp.Entries[0].GroupSize
			tasks = append(tasks, fetchTask{
				groupKey: sp.GroupKey,
				fullGet:  full,
				start:    sp.Start,
				end:      sp.End,
				entries:  sp.Entries,
			})
		}
	}

	if p.cfg.Workers > 1 && len(tasks) > 1 {
		taskSize := func(t fetchTask) int64 {
			if !t.fullGet {
				return t.end - t.start
			}
			if len(t.entries) > 0 {
				return t.entries[0].GroupSize
			}
			return 0
		}
		sort.SliceStable(tasks, func(i, j int) bool {
			return taskSize(tasks[i]) > taskSize(tasks[j])
		})
	}

	beforeData := p.store.Metrics()

	if len(tasks) > 0 {
		workers := min(p.cfg.Workers, len(tasks))
		runCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		jobCh := make(chan fetchTask, len(tasks))
		for _, t := range tasks {
			jobCh <- t
		}
		close(jobCh)

		var (
			wg       sync.WaitGroup
			mu       sync.Mutex
			errOnce  sync.Once
			firstErr error
		)

		for i := 0; i < workers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for task := range jobCh {
					if runCtx.Err() != nil {
						return
					}
					var (
						payload []byte
						baseOff int64
						err     error
					)
					if task.fullGet {
						payload, _, err = p.store.Get(runCtx, task.groupKey)
						baseOff = 0
					} else {
						payload, err = p.store.GetRange(runCtx, task.groupKey, task.start, task.end-task.start)
						baseOff = task.start
					}
					if err != nil {
						errOnce.Do(func() {
							firstErr = err
							cancel()
						})
						return
					}

					localFiles := make(map[string][]byte, len(task.entries))
					for _, e := range task.entries {
						relStart := e.Offset - baseOff
						relEnd := relStart + e.Length
						if relStart < 0 || relEnd > int64(len(payload)) {
							errOnce.Do(func() {
								firstErr = fmt.Errorf("%w: entry %q slice [%d:%d] out of range (%d)",
									ErrIntegrityMismatch, e.Path, relStart, relEnd, len(payload))
								cancel()
							})
							return
						}
						chunk := make([]byte, e.Length)
						copy(chunk, payload[relStart:relEnd])
						sum := sha256.Sum256(chunk)
						if hex.EncodeToString(sum[:]) != e.SHA256Hex {
							errOnce.Do(func() {
								firstErr = fmt.Errorf("%w: file %q SHA-256 mismatch", ErrIntegrityMismatch, e.Path)
								cancel()
							})
							return
						}
						localFiles[e.Path] = chunk
					}

					mu.Lock()
					for k, v := range localFiles {
						files[k] = v
					}
					mu.Unlock()
				}
			}()
		}
		wg.Wait()
		if firstErr != nil {
			return nil, firstErr
		}
	}

	after := p.store.Metrics()
	delta := after.Sub(before)
	dataDelta := after.Sub(beforeData)
	readAmp := 1.0
	dataReadAmp := 1.0
	if requestedBytes > 0 {
		readAmp = float64(delta.BytesDownloaded) / float64(requestedBytes)
		dataReadAmp = float64(dataDelta.BytesDownloaded) / float64(requestedBytes)
	}

	return &RestoreResult{
		Head:                head,
		Files:               files,
		RequestedBytes:      requestedBytes,
		DataBytesDownloaded: dataDelta.BytesDownloaded,
		DownloadedBytes:     delta.BytesDownloaded,
		DataReadAmp:         dataReadAmp,
		ReadAmp:             readAmp,
		Duration:            time.Since(start),
		StoreMetrics:        delta,
	}, nil
}

// ErrIsHeadConflict reports whether err wraps ErrHeadConflict.
func ErrIsHeadConflict(err error) bool {
	return errors.Is(err, ErrHeadConflict)
}
