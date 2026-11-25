// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package aiturbo

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// AWSS3StandardPutPer1k matches pkg/cost.Catalog[BackendAWSS3Std].PutPer1k ($0.005 / 1k PUTs).
	AWSS3StandardPutPer1k = 0.005
	// AWSS3StandardGetPer1k matches pkg/cost.Catalog[BackendAWSS3Std].GetPer1k ($0.0004 / 1k GETs).
	AWSS3StandardGetPer1k = 0.0004

	bytesPerMiB = 1024.0 * 1024.0
	bytesPerGiB = 1024.0 * 1024.0 * 1024.0
)

// splitMix64 is a fast deterministic PRNG for generating realistic incompressible
// tensor/checkpoint payloads without external dependencies.
type splitMix64 uint64

func (s *splitMix64) next() uint64 {
	*s += 0x9e3779b97f4a7c15
	z := uint64(*s)
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

func fillDeterministicBytes(rng *splitMix64, size int) []byte {
	buf := make([]byte, size)
	i := 0
	for ; i+8 <= size; i += 8 {
		binary.LittleEndian.PutUint64(buf[i:i+8], rng.next())
	}
	if i < size {
		v := rng.next()
		for ; i < size; i++ {
			buf[i] = byte(v)
			v >>= 8
		}
	}
	return buf
}

// MixedWorkloadSpec controls the file-size mix of a synthetic ML / agent checkpoint.
type MixedWorkloadSpec struct {
	Seed            uint64
	TenantID        string
	CheckpointID    string
	SmallFileCount  int   // 4 KiB..64 KiB optimizer/config/index/agent-state files
	MediumFileCount int   // 256 KiB..1.5 MiB adapter/layer shard files
	LargeFileCount  int   // 4 MiB..8 MiB base weight tensor shards
	SmallMinBytes   int64 // default 4 KiB
	SmallMaxBytes   int64 // default 64 KiB
	MediumMinBytes  int64 // default 256 KiB
	MediumMaxBytes  int64 // default 1536 KiB
	LargeMinBytes   int64 // default 4 MiB
	LargeMaxBytes   int64 // default 8 MiB
}

// DefaultMixedWorkloadSpec returns a realistic mixed ML + agent-workspace
// checkpoint profile (~272 files, ~50 MiB total).
func DefaultMixedWorkloadSpec(tenantID, checkpointID string, seed uint64) MixedWorkloadSpec {
	return MixedWorkloadSpec{
		Seed:            seed,
		TenantID:        tenantID,
		CheckpointID:    checkpointID,
		SmallFileCount:  240,
		MediumFileCount: 28,
		LargeFileCount:  4,
		SmallMinBytes:   4 << 10,
		SmallMaxBytes:   64 << 10,
		MediumMinBytes:  256 << 10,
		MediumMaxBytes:  1536 << 10,
		LargeMinBytes:   4 << 20,
		LargeMaxBytes:   8 << 20,
	}
}

func pickSize(rng *splitMix64, minB, maxB int64) int {
	if maxB <= minB {
		return int(minB)
	}
	span := uint64(maxB - minB + 1)
	return int(minB + int64(rng.next()%span))
}

// GenerateMixedCheckpoint builds a deterministic mixed AI-model / agent-workspace
// checkpoint containing small metadata/optimizer files, medium layer shards,
// and large weight tensors, plus a fixed 16 KiB sentinel config file for exact
// single-file selective-restore testing.
func GenerateMixedCheckpoint(spec MixedWorkloadSpec) *Checkpoint {
	rng := splitMix64(spec.Seed)
	if rng == 0 {
		rng = 0x1344a17b0c0ffee
	}
	files := make([]CheckpointFile, 0, spec.SmallFileCount+spec.MediumFileCount+spec.LargeFileCount)
	var total int64

	addFile := func(p string, size int) {
		data := fillDeterministicBytes(&rng, size)
		files = append(files, CheckpointFile{
			Path:     p,
			TenantID: spec.TenantID,
			Data:     data,
			SHA256:   sha256.Sum256(data),
		})
		total += int64(size)
	}

	// Deterministic 16 KiB sentinel config file (index 0 in config/) for exact
	// single-file selective restore benchmarks.
	if spec.SmallFileCount > 0 {
		addFile("config/checkpoint_index.json", 16<<10)
	}
	for i := 1; i < spec.SmallFileCount; i++ {
		sz := pickSize(&rng, spec.SmallMinBytes, spec.SmallMaxBytes)
		var p string
		switch i % 4 {
		case 0:
			p = fmt.Sprintf("agent/state/step-%04d.json", i)
		case 1:
			p = fmt.Sprintf("optimizer/moment/param-%04d.bin", i)
		case 2:
			p = fmt.Sprintf("tokenizer/vocab/shard-%04d.dat", i)
		default:
			p = fmt.Sprintf("metadata/index/block-%04d.idx", i)
		}
		addFile(p, sz)
	}

	for i := 0; i < spec.MediumFileCount; i++ {
		sz := pickSize(&rng, spec.MediumMinBytes, spec.MediumMaxBytes)
		p := fmt.Sprintf("layers/block-%02d/adapter-weights.safetensors", i)
		addFile(p, sz)
	}

	for i := 0; i < spec.LargeFileCount; i++ {
		sz := pickSize(&rng, spec.LargeMinBytes, spec.LargeMaxBytes)
		p := fmt.Sprintf("weights/model-%02d-of-%02d.safetensors", i+1, spec.LargeFileCount)
		addFile(p, sz)
	}

	return &Checkpoint{
		ID:         spec.CheckpointID,
		TenantID:   spec.TenantID,
		Files:      files,
		TotalBytes: total,
	}
}

// GenerateLargeTensorOnlyCheckpoint builds a checkpoint consisting exclusively
// of already-large tensors (e.g. 16 MiB each) to test the documented zero-gain
// boundary regime where grouping adds no S3 request reduction.
func GenerateLargeTensorOnlyCheckpoint(tenantID, checkpointID string, count int, tensorSizeBytes int64, seed uint64) *Checkpoint {
	rng := splitMix64(seed)
	if rng == 0 {
		rng = 0x51a9e7e501234567
	}
	files := make([]CheckpointFile, 0, count)
	var total int64
	for i := 0; i < count; i++ {
		data := fillDeterministicBytes(&rng, int(tensorSizeBytes))
		files = append(files, CheckpointFile{
			Path:     fmt.Sprintf("weights/large-shard-%02d.safetensors", i),
			TenantID: tenantID,
			Data:     data,
			SHA256:   sha256.Sum256(data),
		})
		total += tensorSizeBytes
	}
	return &Checkpoint{
		ID:         checkpointID,
		TenantID:   tenantID,
		Files:      files,
		TotalBytes: total,
	}
}

// BenchmarkConfig parameterizes the reproducible loopback HTTP S3 benchmark suite.
type BenchmarkConfig struct {
	Seed                          uint64        `json:"seed"`
	Iterations                    int           `json:"iterations"`
	RequestRTT                    time.Duration `json:"request_rtt_ns"`
	RequestRTTMs                  float64       `json:"request_rtt_ms"`
	PerStreamBandwidthBytesPerSec int64         `json:"per_stream_bandwidth_bytes_per_sec"`
	MaxConcurrentRequests         int           `json:"max_concurrent_requests"`
	Workers                       int           `json:"workers"`
	AITurboGroupTargetBytes       int64         `json:"aiturbo_group_target_bytes"`
	CASRacers                     int           `json:"cas_racers"`
}

// DefaultBenchmarkConfig returns the canonical benchmark configuration:
// realistic intra-AZ S3 RTT (2 ms), 150 MiB/s per-TCP-stream ceiling, W=8 workers,
// 4 MiB AITURBO super-object target, and 5 measured iterations per strategy.
func DefaultBenchmarkConfig() BenchmarkConfig {
	return BenchmarkConfig{
		Seed:                          20260401,
		Iterations:                    5,
		RequestRTT:                    2 * time.Millisecond,
		RequestRTTMs:                  2.0,
		PerStreamBandwidthBytesPerSec: 150 << 20,
		MaxConcurrentRequests:         64,
		Workers:                       DefaultWorkers,
		AITurboGroupTargetBytes:       DefaultGroupTargetBytes,
		CASRacers:                     16,
	}
}

// StrategyScore holds real measured metrics for one strategy across iterations.
type StrategyScore struct {
	Strategy               Strategy `json:"strategy"`
	Workers                int      `json:"workers"`
	GroupTargetBytes       int64    `json:"group_target_bytes"`
	GroupCount             int      `json:"group_count"`
	CheckpointBytes        int64    `json:"checkpoint_bytes"`
	FileCount              int      `json:"file_count"`
	UploadMBps             float64  `json:"upload_mb_per_sec"`
	DownloadMBps           float64  `json:"download_mb_per_sec"`
	UploadPutRequests      int64    `json:"upload_put_requests"`
	FullRestoreGetRequests int64    `json:"full_restore_get_requests"`
	PutsPerGiB             float64  `json:"puts_per_gib"`
	GetsPerGiB             float64  `json:"gets_per_gib"`
	TotalRequestsPerGiB    float64  `json:"total_requests_per_gib"`
	PutCostPer1kUSD        float64  `json:"put_cost_per_1k_usd"`
	GetCostPer1kUSD        float64  `json:"get_cost_per_1k_usd"`
	EstApiCostPer100GiBUSD float64  `json:"est_api_cost_per_100_gib_usd"`
	CommitP50Ms            float64  `json:"commit_p50_ms"`
	CommitP99Ms            float64  `json:"commit_p99_ms"`
	FullRestoreP50Ms       float64  `json:"full_restore_p50_ms"`
	SelectiveRequestedB    int64    `json:"selective_requested_bytes"`
	SelectiveDataDownB     int64    `json:"selective_data_downloaded_bytes"`
	SelectiveTotalDownB    int64    `json:"selective_total_downloaded_bytes"`
	SelectiveDataReadAmp   float64  `json:"selective_data_read_amp"`
	SelectiveTotalReadAmp  float64  `json:"selective_total_read_amp"`
	SelectiveLatencyMs     float64  `json:"selective_latency_ms"`
	SelectiveGetRequests   int64    `json:"selective_get_requests"`
	PackingBufferBytes     int64    `json:"packing_buffer_bytes"`
	SHA256BitExact         bool     `json:"sha256_bit_exact"`
	HeadCASVerified        bool     `json:"head_cas_verified"`
}

// SelectiveReadAmpNegative captures negative regime #1: single 16 KiB file
// restore without HTTP Range vs with HTTP Range on the exact same grouped checkpoint.
type SelectiveReadAmpNegative struct {
	TargetPath           string  `json:"target_path"`
	TargetBytes          int64   `json:"target_bytes"`
	GroupSizeBytes       int64   `json:"group_size_bytes"`
	FullGroupDataBytes   int64   `json:"full_group_data_bytes"`
	FullGroupDataReadAmp float64 `json:"full_group_data_read_amp"`
	FullGroupLatencyMs   float64 `json:"full_group_latency_ms"`
	RangeGetDataBytes    int64   `json:"range_get_data_bytes"`
	RangeGetDataReadAmp  float64 `json:"range_get_data_read_amp"`
	RangeGetLatencyMs    float64 `json:"range_get_latency_ms"`
	RangeGetRequests     int64   `json:"range_get_requests"`
	FullGroupGetRequests int64   `json:"full_group_get_requests"`
	BytesSavedRatio      float64 `json:"bytes_saved_ratio"`
	SHA256BitExactBoth   bool    `json:"sha256_bit_exact_both"`
}

// LargeTensorOnlyNegative captures negative regime #2: checkpoint consisting
// solely of 16 MiB+ tensors where grouping provides ~0 request reduction.
type LargeTensorOnlyNegative struct {
	FileCount                   int     `json:"file_count"`
	TensorSizeBytes             int64   `json:"tensor_size_bytes"`
	TotalBytes                  int64   `json:"total_bytes"`
	PerObjectParallelPutsPerGiB float64 `json:"per_object_parallel_puts_per_gib"`
	AITurboPutsPerGiB           float64 `json:"aiturbo_puts_per_gib"`
	ForcedPackPutsPerGiB        float64 `json:"forced_pack_puts_per_gib"`
	AITurboPassthroughCopyBytes int64   `json:"aiturbo_passthrough_copy_bytes"`
	ForcedPackCopyBytes         int64   `json:"forced_pack_copy_bytes"`
	RequestReductionPct         float64 `json:"request_reduction_pct"`
	PerObjectUploadMBps         float64 `json:"per_object_upload_mb_per_sec"`
	AITurboUploadMBps           float64 `json:"aiturbo_upload_mb_per_sec"`
	ForcedPackUploadMBps        float64 `json:"forced_pack_upload_mb_per_sec"`
}

// HeadCASConflictNegative captures negative/boundary regime #3: concurrent
// writers racing on the same volume Head with conditional If-Match / If-None-Match.
type HeadCASConflictNegative struct {
	ConcurrentWriters      int    `json:"concurrent_writers"`
	InitialCreateWinners   int    `json:"initial_create_winners"`
	InitialCreateConflicts int    `json:"initial_create_conflicts"`
	UpdateRaceWinners      int    `json:"update_race_winners"`
	UpdateRaceConflicts    int    `json:"update_race_conflicts"`
	RetrySucceeded         bool   `json:"retry_succeeded"`
	FinalGeneration        uint64 `json:"final_generation"`
	TornReadsObserved      int    `json:"torn_reads_observed"`
	SilentOverwrites       int    `json:"silent_overwrites"`
}

// NegativeRegimes bundles all three documented honest negative & boundary regimes.
type NegativeRegimes struct {
	SelectiveSingleFileReadAmp SelectiveReadAmpNegative `json:"selective_single_file_restore_read_amp"`
	AlreadyLargeTensorOnly     LargeTensorOnlyNegative  `json:"already_large_tensor_only_checkpoint"`
	ConcurrentHeadCASConflict  HeadCASConflictNegative  `json:"concurrent_head_cas_conflict_retry"`
}

// SuiteReport is the complete reproducible JSON/Markdown benchmark output.
type SuiteReport struct {
	TimestampUTC    string          `json:"timestamp_utc"`
	Config          BenchmarkConfig `json:"config"`
	CheckpointFiles int             `json:"checkpoint_files"`
	CheckpointBytes int64           `json:"checkpoint_bytes"`
	Scores          []StrategyScore `json:"scores"`
	NegativeRegimes NegativeRegimes `json:"negative_regimes"`
}

// VerifyCheckpointExact checks that restored contains every expected file
// with bit-exact byte content and matching SHA-256.
func VerifyCheckpointExact(expected *Checkpoint, restored map[string][]byte) bool {
	if len(restored) != len(expected.Files) {
		return false
	}
	for _, f := range expected.Files {
		got, ok := restored[f.Path]
		if !ok || !bytes.Equal(got, f.Data) {
			return false
		}
		if sha256.Sum256(got) != f.SHA256 {
			return false
		}
	}
	return true
}

func percentileMs(samples []float64, p float64) float64 {
	if len(samples) == 0 {
		return 0
	}
	cp := make([]float64, len(samples))
	copy(cp, samples)
	sort.Float64s(cp)
	if len(cp) == 1 {
		return cp[0]
	}
	idx := int(math.Ceil(p/100.0*float64(len(cp)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(cp) {
		idx = len(cp) - 1
	}
	return cp[idx]
}

func medianFloat64(samples []float64) float64 {
	return percentileMs(samples, 50)
}

// RunHeadCASRace exercises N concurrent writers racing on initial Head creation
// (If-None-Match: *) and on a subsequent generation update (If-Match: prevETag),
// followed by a clean rebase/retry by a losing writer.
func RunHeadCASRace(ctx context.Context, srv *LoopbackS3Server, tenantID string, racers int) (HeadCASConflictNegative, error) {
	if racers < 2 {
		racers = 2
	}
	out := HeadCASConflictNegative{ConcurrentWriters: racers}

	// Build small distinct checkpoints for each racer so any torn/mixed read is
	// immediately distinguishable.
	ckpts := make([]*Checkpoint, racers)
	for i := 0; i < racers; i++ {
		rng := splitMix64(uint64(1000 + i))
		files := make([]CheckpointFile, 4)
		var total int64
		for j := 0; j < 4; j++ {
			data := fillDeterministicBytes(&rng, 4096)
			files[j] = CheckpointFile{
				Path:     fmt.Sprintf("racer-%02d/part-%d.bin", i, j),
				TenantID: tenantID,
				Data:     data,
				SHA256:   sha256.Sum256(data),
			}
			total += int64(len(data))
		}
		ckpts[i] = &Checkpoint{
			ID:         fmt.Sprintf("ckpt-gen1-racer-%02d", i),
			TenantID:   tenantID,
			Files:      files,
			TotalBytes: total,
		}
	}

	// Round 1: N goroutines race on initial Head creation (If-None-Match: *).
	var (
		startGate  = make(chan struct{})
		wg         sync.WaitGroup
		winCount   atomic.Int64
		confCount  atomic.Int64
		otherErrMu sync.Mutex
		otherErr   error
		winnerETag string
		winnerMu   sync.Mutex
	)
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			planner, err := NewPlanner(srv.Client(), DefaultConfigForStrategy(StrategyAITurboGrouped))
			if err != nil {
				otherErrMu.Lock()
				otherErr = err
				otherErrMu.Unlock()
				return
			}
			<-startGate
			res, err := planner.CommitCheckpoint(ctx, ckpts[idx], "", 1)
			if err == nil {
				winCount.Add(1)
				winnerMu.Lock()
				winnerETag = res.HeadETag
				winnerMu.Unlock()
				return
			}
			if ErrIsHeadConflict(err) {
				confCount.Add(1)
				return
			}
			otherErrMu.Lock()
			otherErr = err
			otherErrMu.Unlock()
		}(i)
	}
	close(startGate)
	wg.Wait()
	if otherErr != nil {
		return out, otherErr
	}
	out.InitialCreateWinners = int(winCount.Load())
	out.InitialCreateConflicts = int(confCount.Load())

	// Verify restored Head matches exactly one whole checkpoint (0 torn reads).
	verifier, err := NewPlanner(srv.Client(), DefaultConfigForStrategy(StrategyAITurboGrouped))
	if err != nil {
		return out, err
	}
	res1, err := verifier.RestoreCheckpoint(ctx, tenantID)
	if err != nil {
		return out, err
	}
	matchedGen1 := false
	for _, c := range ckpts {
		if c.ID == res1.Head.CheckpointID && VerifyCheckpointExact(c, res1.Files) {
			matchedGen1 = true
			break
		}
	}
	if !matchedGen1 {
		out.TornReadsObserved++
	}

	// Round 2: N goroutines race on updating Head from winnerETag -> generation 2 (If-Match).
	ckpts2 := make([]*Checkpoint, racers)
	for i := 0; i < racers; i++ {
		rng := splitMix64(uint64(2000 + i))
		data := fillDeterministicBytes(&rng, 8192)
		ckpts2[i] = &Checkpoint{
			ID:       fmt.Sprintf("ckpt-gen2-racer-%02d", i),
			TenantID: tenantID,
			Files: []CheckpointFile{{
				Path:     fmt.Sprintf("gen2/racer-%02d.bin", i),
				TenantID: tenantID,
				Data:     data,
				SHA256:   sha256.Sum256(data),
			}},
			TotalBytes: int64(len(data)),
		}
	}

	var (
		startGate2 = make(chan struct{})
		win2       atomic.Int64
		conf2      atomic.Int64
	)
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			planner, err := NewPlanner(srv.Client(), DefaultConfigForStrategy(StrategyAITurboGrouped))
			if err != nil {
				otherErrMu.Lock()
				otherErr = err
				otherErrMu.Unlock()
				return
			}
			<-startGate2
			_, err = planner.CommitCheckpoint(ctx, ckpts2[idx], winnerETag, 2)
			if err == nil {
				win2.Add(1)
				return
			}
			if ErrIsHeadConflict(err) {
				conf2.Add(1)
				return
			}
			otherErrMu.Lock()
			otherErr = err
			otherErrMu.Unlock()
		}(i)
	}
	close(startGate2)
	wg.Wait()
	if otherErr != nil {
		return out, otherErr
	}
	out.UpdateRaceWinners = int(win2.Load())
	out.UpdateRaceConflicts = int(conf2.Load())

	// Verify generation 2 is intact and not torn.
	head2, etag2, err := verifier.ReadHead(ctx, tenantID)
	if err != nil {
		return out, err
	}
	res2, err := verifier.RestoreCheckpoint(ctx, tenantID)
	if err != nil {
		return out, err
	}
	matchedGen2 := false
	for _, c := range ckpts2 {
		if c.ID == res2.Head.CheckpointID && VerifyCheckpointExact(c, res2.Files) {
			matchedGen2 = true
			break
		}
	}
	if !matchedGen2 {
		out.TornReadsObserved++
	}

	// Rebase & retry: a former loser reads the latest Head ETag and cleanly commits generation 3.
	rng3 := splitMix64(3000)
	retryData := fillDeterministicBytes(&rng3, 8192)
	retryCkpt := &Checkpoint{
		ID:       "ckpt-gen3-rebased-retry",
		TenantID: tenantID,
		Files: []CheckpointFile{{
			Path:     "gen3/rebased.bin",
			TenantID: tenantID,
			Data:     retryData,
			SHA256:   sha256.Sum256(retryData),
		}},
		TotalBytes: int64(len(retryData)),
	}
	res3, err := verifier.CommitCheckpoint(ctx, retryCkpt, etag2, head2.Generation+1)
	if err != nil {
		return out, err
	}
	out.RetrySucceeded = true
	out.FinalGeneration = res3.Head.Generation
	if out.InitialCreateWinners != 1 || out.UpdateRaceWinners != 1 {
		out.SilentOverwrites = (out.InitialCreateWinners - 1) + (out.UpdateRaceWinners - 1)
	}
	return out, nil
}

// RunBenchmarkSuite executes all 5 matched strategies and all 3 documented
// honest negative/boundary regimes over a real loopback HTTP S3 server.
func RunBenchmarkSuite(ctx context.Context, cfg BenchmarkConfig) (*SuiteReport, error) {
	if cfg.Iterations <= 0 {
		cfg.Iterations = 3
	}
	if cfg.Workers <= 0 {
		cfg.Workers = DefaultWorkers
	}
	if cfg.AITurboGroupTargetBytes <= 0 {
		cfg.AITurboGroupTargetBytes = DefaultGroupTargetBytes
	}
	if cfg.CASRacers < 2 {
		cfg.CASRacers = 16
	}
	cfg.RequestRTTMs = float64(cfg.RequestRTT) / float64(time.Millisecond)

	// Shared deterministic mixed checkpoint used identically across all 5 strategies.
	baseSpec := DefaultMixedWorkloadSpec("tenant-bench", "ckpt-mixed-v1", cfg.Seed)
	baseCkpt := GenerateMixedCheckpoint(baseSpec)
	selectivePath := "config/checkpoint_index.json"

	scores := make([]StrategyScore, 0, len(AllStrategies))

	for idx, strat := range AllStrategies {
		srv := NewLoopbackS3Server(LoopbackS3Config{
			Bucket:                        "hanzo-vfs-bench",
			RequestRTT:                    cfg.RequestRTT,
			PerStreamBandwidthBytesPerSec: cfg.PerStreamBandwidthBytesPerSec,
			MaxConcurrentRequests:         cfg.MaxConcurrentRequests,
		})

		planCfg := DefaultConfigForStrategy(strat)
		if strat == StrategyPerObjectParallel || strat == StrategyAITurboGrouped || strat == StrategyOverCoalesced {
			planCfg.Workers = cfg.Workers
		}
		if strat == StrategyAITurboGrouped {
			planCfg.GroupTargetBytes = cfg.AITurboGroupTargetBytes
			planCfg.LargeObjectPassthroughBytes = cfg.AITurboGroupTargetBytes
		}

		var (
			commitLatMs   = make([]float64, 0, cfg.Iterations)
			restoreLatMs  = make([]float64, 0, cfg.Iterations)
			uploadMBps    = make([]float64, 0, cfg.Iterations)
			downloadMBps  = make([]float64, 0, cfg.Iterations)
			selLatMs      = make([]float64, 0, cfg.Iterations)
			lastCommit    *CommitResult
			lastFull      *RestoreResult
			lastSelective *RestoreResult
			allExact      = true
			casVerified   = true
		)

		for iter := 0; iter < cfg.Iterations; iter++ {
			tenantID := fmt.Sprintf("tenant-s%d-i%d", idx, iter)
			ckpt := cloneCheckpointForTenant(baseCkpt, tenantID, fmt.Sprintf("%s-iter-%d", baseCkpt.ID, iter))

			planner, err := NewPlanner(srv.Client(), planCfg)
			if err != nil {
				srv.Close()
				return nil, err
			}

			cRes, err := planner.CommitCheckpoint(ctx, ckpt, "", 1)
			if err != nil {
				srv.Close()
				return nil, fmt.Errorf("strategy %s iter %d commit: %w", strat, iter, err)
			}
			// Verify strict conditional Head PUT rejects stale If-None-Match on the first iteration.
			if iter == 0 {
				if _, conflictErr := planner.CommitCheckpoint(ctx, ckpt, "", 2); !ErrIsHeadConflict(conflictErr) {
					casVerified = false
				}
			}

			fRes, err := planner.RestoreCheckpoint(ctx, tenantID)
			if err != nil {
				srv.Close()
				return nil, fmt.Errorf("strategy %s iter %d full restore: %w", strat, iter, err)
			}
			if !VerifyCheckpointExact(ckpt, fRes.Files) {
				allExact = false
			}

			sRes, err := planner.RestoreSubset(ctx, tenantID, []string{selectivePath})
			if err != nil {
				srv.Close()
				return nil, fmt.Errorf("strategy %s iter %d selective restore: %w", strat, iter, err)
			}
			gotSel, ok := sRes.Files[selectivePath]
			if !ok || !bytes.Equal(gotSel, ckpt.Files[0].Data) {
				allExact = false
			}

			commitMs := float64(cRes.Duration) / float64(time.Millisecond)
			fullMs := float64(fRes.Duration) / float64(time.Millisecond)
			selMs := float64(sRes.Duration) / float64(time.Millisecond)

			commitLatMs = append(commitLatMs, commitMs)
			restoreLatMs = append(restoreLatMs, fullMs)
			selLatMs = append(selLatMs, selMs)

			upSec := cRes.Duration.Seconds()
			dnSec := fRes.Duration.Seconds()
			if upSec > 0 {
				uploadMBps = append(uploadMBps, (float64(ckpt.TotalBytes)/bytesPerMiB)/upSec)
			}
			if dnSec > 0 {
				downloadMBps = append(downloadMBps, (float64(ckpt.TotalBytes)/bytesPerMiB)/dnSec)
			}
			lastCommit = cRes
			lastFull = fRes
			lastSelective = sRes
		}

		srv.Close()

		gib := float64(baseCkpt.TotalBytes) / bytesPerGiB
		puts := lastCommit.StoreMetrics.TotalPutRequests()
		gets := lastFull.StoreMetrics.TotalGetRequests()
		putsPerGiB := float64(puts) / gib
		getsPerGiB := float64(gets) / gib
		// Estimated S3 API cost for 100 GiB of checkpoint write + full read cycle.
		apiCost100GiB := (putsPerGiB*100.0/1000.0)*AWSS3StandardPutPer1k +
			(getsPerGiB*100.0/1000.0)*AWSS3StandardGetPer1k

		scores = append(scores, StrategyScore{
			Strategy:               strat,
			Workers:                planCfg.Workers,
			GroupTargetBytes:       planCfg.GroupTargetBytes,
			GroupCount:             lastCommit.GroupCount,
			CheckpointBytes:        baseCkpt.TotalBytes,
			FileCount:              len(baseCkpt.Files),
			UploadMBps:             round2(medianFloat64(uploadMBps)),
			DownloadMBps:           round2(medianFloat64(downloadMBps)),
			UploadPutRequests:      puts,
			FullRestoreGetRequests: gets,
			PutsPerGiB:             round2(putsPerGiB),
			GetsPerGiB:             round2(getsPerGiB),
			TotalRequestsPerGiB:    round2(putsPerGiB + getsPerGiB),
			PutCostPer1kUSD:        AWSS3StandardPutPer1k,
			GetCostPer1kUSD:        AWSS3StandardGetPer1k,
			EstApiCostPer100GiBUSD: round4(apiCost100GiB),
			CommitP50Ms:            round2(percentileMs(commitLatMs, 50)),
			CommitP99Ms:            round2(percentileMs(commitLatMs, 99)),
			FullRestoreP50Ms:       round2(percentileMs(restoreLatMs, 50)),
			SelectiveRequestedB:    lastSelective.RequestedBytes,
			SelectiveDataDownB:     lastSelective.DataBytesDownloaded,
			SelectiveTotalDownB:    lastSelective.DownloadedBytes,
			SelectiveDataReadAmp:   round2(lastSelective.DataReadAmp),
			SelectiveTotalReadAmp:  round2(lastSelective.ReadAmp),
			SelectiveLatencyMs:     round2(medianFloat64(selLatMs)),
			SelectiveGetRequests:   lastSelective.StoreMetrics.TotalGetRequests(),
			PackingBufferBytes:     lastCommit.PackingBytes,
			SHA256BitExact:         allExact,
			HeadCASVerified:        casVerified,
		})
	}

	// Execute the 3 Documented Honest Negative & Boundary Regimes.
	neg, err := runNegativeRegimes(ctx, cfg, baseCkpt, selectivePath)
	if err != nil {
		return nil, err
	}

	return &SuiteReport{
		TimestampUTC:    time.Now().UTC().Format(time.RFC3339),
		Config:          cfg,
		CheckpointFiles: len(baseCkpt.Files),
		CheckpointBytes: baseCkpt.TotalBytes,
		Scores:          scores,
		NegativeRegimes: neg,
	}, nil
}

func runNegativeRegimes(ctx context.Context, cfg BenchmarkConfig, baseCkpt *Checkpoint, selectivePath string) (NegativeRegimes, error) {
	var out NegativeRegimes

	// Negative Regime #1: selective_single_file_restore_read_amp
	{
		srv := NewLoopbackS3Server(LoopbackS3Config{
			Bucket:                        "hanzo-vfs-neg1",
			RequestRTT:                    cfg.RequestRTT,
			PerStreamBandwidthBytesPerSec: cfg.PerStreamBandwidthBytesPerSec,
			MaxConcurrentRequests:         cfg.MaxConcurrentRequests,
		})
		defer srv.Close()

		planCfg := DefaultConfigForStrategy(StrategyAITurboGrouped)
		planCfg.Workers = cfg.Workers
		planCfg.GroupTargetBytes = cfg.AITurboGroupTargetBytes
		planCfg.LargeObjectPassthroughBytes = cfg.AITurboGroupTargetBytes

		planner, err := NewPlanner(srv.Client(), planCfg)
		if err != nil {
			return out, err
		}
		ckpt := cloneCheckpointForTenant(baseCkpt, "tenant-neg1", "ckpt-neg1")
		if _, err := planner.CommitCheckpoint(ctx, ckpt, "", 1); err != nil {
			return out, err
		}
		plan, err := planner.PlanGroups(ckpt)
		if err != nil {
			return out, err
		}
		var groupSize int64
		for _, e := range plan.Entries {
			if e.Path == selectivePath {
				groupSize = e.GroupSize
				break
			}
		}

		fullGrpRes, err := planner.RestoreSubsetWithRangeMode(ctx, "tenant-neg1", []string{selectivePath}, false)
		if err != nil {
			return out, err
		}
		rangeRes, err := planner.RestoreSubsetWithRangeMode(ctx, "tenant-neg1", []string{selectivePath}, true)
		if err != nil {
			return out, err
		}
		exact := bytes.Equal(fullGrpRes.Files[selectivePath], ckpt.Files[0].Data) &&
			bytes.Equal(rangeRes.Files[selectivePath], ckpt.Files[0].Data)

		savedRatio := 1.0
		if rangeRes.DataBytesDownloaded > 0 {
			savedRatio = float64(fullGrpRes.DataBytesDownloaded) / float64(rangeRes.DataBytesDownloaded)
		}
		out.SelectiveSingleFileReadAmp = SelectiveReadAmpNegative{
			TargetPath:           selectivePath,
			TargetBytes:          rangeRes.RequestedBytes,
			GroupSizeBytes:       groupSize,
			FullGroupDataBytes:   fullGrpRes.DataBytesDownloaded,
			FullGroupDataReadAmp: round2(fullGrpRes.DataReadAmp),
			FullGroupLatencyMs:   round2(float64(fullGrpRes.Duration) / float64(time.Millisecond)),
			RangeGetDataBytes:    rangeRes.DataBytesDownloaded,
			RangeGetDataReadAmp:  round2(rangeRes.DataReadAmp),
			RangeGetLatencyMs:    round2(float64(rangeRes.Duration) / float64(time.Millisecond)),
			RangeGetRequests:     rangeRes.StoreMetrics.TotalGetRequests(),
			FullGroupGetRequests: fullGrpRes.StoreMetrics.TotalGetRequests(),
			BytesSavedRatio:      round2(savedRatio),
			SHA256BitExactBoth:   exact,
		}
	}

	// Negative Regime #2: already_large_tensor_only_checkpoint
	{
		srv := NewLoopbackS3Server(LoopbackS3Config{
			Bucket:                        "hanzo-vfs-neg2",
			RequestRTT:                    cfg.RequestRTT,
			PerStreamBandwidthBytesPerSec: cfg.PerStreamBandwidthBytesPerSec,
			MaxConcurrentRequests:         cfg.MaxConcurrentRequests,
		})
		defer srv.Close()

		largeCkpt := GenerateLargeTensorOnlyCheckpoint("tenant-large-a", "ckpt-large-only", 4, 16<<20, cfg.Seed+99)
		gib := float64(largeCkpt.TotalBytes) / bytesPerGiB

		perObjCfg := DefaultConfigForStrategy(StrategyPerObjectParallel)
		perObjCfg.Workers = cfg.Workers
		perObjPlanner, err := NewPlanner(srv.Client(), perObjCfg)
		if err != nil {
			return out, err
		}
		perObjRes, err := perObjPlanner.CommitCheckpoint(ctx, largeCkpt, "", 1)
		if err != nil {
			return out, err
		}

		aiCfg := DefaultConfigForStrategy(StrategyAITurboGrouped)
		aiCfg.Workers = cfg.Workers
		aiCfg.GroupTargetBytes = cfg.AITurboGroupTargetBytes
		aiCfg.LargeObjectPassthroughBytes = cfg.AITurboGroupTargetBytes
		aiPlanner, err := NewPlanner(srv.Client(), aiCfg)
		if err != nil {
			return out, err
		}
		aiCkpt := cloneCheckpointForTenant(largeCkpt, "tenant-large-b", "ckpt-large-only")
		aiRes, err := aiPlanner.CommitCheckpoint(ctx, aiCkpt, "", 1)
		if err != nil {
			return out, err
		}

		// Forced packing (passthrough disabled at 256 MiB) shows the buffer memory
		// copy penalty and parallelism loss when blindly packing already-large tensors.
		forcedCfg := DefaultConfigForStrategy(StrategyOverCoalesced)
		forcedCfg.Workers = cfg.Workers
		forcedPlanner, err := NewPlanner(srv.Client(), forcedCfg)
		if err != nil {
			return out, err
		}
		forcedCkpt := cloneCheckpointForTenant(largeCkpt, "tenant-large-c", "ckpt-large-only")
		forcedRes, err := forcedPlanner.CommitCheckpoint(ctx, forcedCkpt, "", 1)
		if err != nil {
			return out, err
		}

		perObjPutsGiB := float64(perObjRes.StoreMetrics.TotalPutRequests()) / gib
		aiPutsGiB := float64(aiRes.StoreMetrics.TotalPutRequests()) / gib
		forcedPutsGiB := float64(forcedRes.StoreMetrics.TotalPutRequests()) / gib
		reductionPct := 0.0
		if perObjPutsGiB > 0 {
			reductionPct = (perObjPutsGiB - aiPutsGiB) / perObjPutsGiB * 100.0
		}

		out.AlreadyLargeTensorOnly = LargeTensorOnlyNegative{
			FileCount:                   len(largeCkpt.Files),
			TensorSizeBytes:             16 << 20,
			TotalBytes:                  largeCkpt.TotalBytes,
			PerObjectParallelPutsPerGiB: round2(perObjPutsGiB),
			AITurboPutsPerGiB:           round2(aiPutsGiB),
			ForcedPackPutsPerGiB:        round2(forcedPutsGiB),
			AITurboPassthroughCopyBytes: aiRes.PackingBytes,
			ForcedPackCopyBytes:         forcedRes.PackingBytes,
			RequestReductionPct:         round2(reductionPct),
			PerObjectUploadMBps:         round2((float64(largeCkpt.TotalBytes) / bytesPerMiB) / perObjRes.Duration.Seconds()),
			AITurboUploadMBps:           round2((float64(largeCkpt.TotalBytes) / bytesPerMiB) / aiRes.Duration.Seconds()),
			ForcedPackUploadMBps:        round2((float64(largeCkpt.TotalBytes) / bytesPerMiB) / forcedRes.Duration.Seconds()),
		}
	}

	// Negative Regime #3: concurrent_head_cas_conflict_retry
	{
		srv := NewLoopbackS3Server(LoopbackS3Config{
			Bucket:                "hanzo-vfs-neg3",
			RequestRTT:            cfg.RequestRTT,
			MaxConcurrentRequests: cfg.MaxConcurrentRequests,
		})
		defer srv.Close()

		casRes, err := RunHeadCASRace(ctx, srv, "tenant-cas-race", cfg.CASRacers)
		if err != nil {
			return out, err
		}
		out.ConcurrentHeadCASConflict = casRes
	}

	return out, nil
}

func cloneCheckpointForTenant(src *Checkpoint, tenantID, ckptID string) *Checkpoint {
	files := make([]CheckpointFile, len(src.Files))
	for i, f := range src.Files {
		f.TenantID = tenantID
		files[i] = f
	}
	return &Checkpoint{
		ID:         ckptID,
		TenantID:   tenantID,
		Files:      files,
		TotalBytes: src.TotalBytes,
	}
}

func round2(v float64) float64 {
	return math.Round(v*100) / 100
}

func round4(v float64) float64 {
	return math.Round(v*10000) / 10000
}
