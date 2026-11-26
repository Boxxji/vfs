// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

// Command aiturbo-bench runs reproducible matched-baseline evaluations of
// pkg/aiturbo grouped IO planning for bulk checkpoint upload and download
// (hanzoai/vfs#48, HIP-1345, AITURBO FAST 2026).
package main

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hanzoai/vfs/pkg/aiturbo"
	"github.com/hanzoai/vfs/pkg/backend"
	_ "github.com/hanzoai/vfs/pkg/backend/file"
	s3be "github.com/hanzoai/vfs/pkg/backend/s3"
)

// SuiteConfig configures the benchmark suite execution.
type SuiteConfig struct {
	Seed           int64
	RepoRoot       string
	OutDir         string
	PutRTT         time.Duration
	GetRTT         time.Duration
	PerStreamMiBS  float64
	AggregateMiBS  float64
	Concurrency    int
	TargetGroupMiB int
	MaxGapKiB      int
	ScaleDownForCI bool // when true (used in unit tests), uses smaller byte sizes and shorter RTTs
}

// SuiteReport is the complete machine-readable output emitted to results.json.
type SuiteReport struct {
	SchemaVersion    int                     `json:"schema_version"`
	GeneratedAtUTC   string                  `json:"generated_at_utc"`
	GitCommit        string                  `json:"git_commit"`
	GoVersion        string                  `json:"go_version"`
	GOOS             string                  `json:"goos"`
	GOARCH           string                  `json:"goarch"`
	NumCPU           int                     `json:"num_cpu"`
	Seed             int64                   `json:"seed"`
	S3Model          S3ModelSummary          `json:"s3_model"`
	WorkloadSummary  WorkloadSummary         `json:"workload_summary"`
	MatchedRegimes   []*aiturbo.RegimeResult `json:"matched_regimes"`
	GroupSizeSweep   []GroupSizeSweepPoint   `json:"group_size_sweep"`
	RealFileBackend  []*aiturbo.RegimeResult `json:"real_file_backend"`
	RealHanzoS3HTTP  []*aiturbo.RegimeResult `json:"real_hanzo_s3_http"`
	ConsistencyAudit ConsistencyAuditResult  `json:"consistency_audit"`
	NegativeRegimes  NegativeRegimesResult   `json:"negative_regimes"`
}

type S3ModelSummary struct {
	PutRTTMs             float64 `json:"put_rtt_ms"`
	GetRTTMs             float64 `json:"get_rtt_ms"`
	PerStreamMiBPerSec   float64 `json:"per_stream_mib_per_sec"`
	AggregateMiBPerSec   float64 `json:"aggregate_mib_per_sec"`
	Concurrency          int     `json:"concurrency"`
	TargetGroupBytes     int64   `json:"target_group_bytes"`
	LargeObjectThreshold int64   `json:"large_object_threshold"`
	MaxGapBytes          int64   `json:"max_gap_bytes"`
}

type WorkloadSummary struct {
	TotalEntries        int     `json:"total_entries"`
	RepoGroundedEntries int     `json:"repo_grounded_entries"`
	SmallEntries        int     `json:"small_entries"`
	MediumEntries       int     `json:"medium_entries"`
	LargeEntries        int     `json:"large_entries"`
	TotalLogicalBytes   int64   `json:"total_logical_bytes"`
	TotalLogicalMiB     float64 `json:"total_logical_mib"`
	SelectiveEntries    int     `json:"selective_entries"`
	SelectiveBytes      int64   `json:"selective_bytes"`
	SelectivePctOfTotal float64 `json:"selective_pct_of_total"`
}

type GroupSizeSweepPoint struct {
	TargetGroupMiB     int     `json:"target_group_mib"`
	UploadMiBPerSec    float64 `json:"upload_mib_per_sec"`
	UploadPUTs         int64   `json:"upload_puts"`
	UploadPUTsPerGiB   float64 `json:"upload_puts_per_gib"`
	DownloadMiBPerSec  float64 `json:"download_mib_per_sec"`
	DownloadGETs       int64   `json:"download_gets"`
	DownloadGETsPerGiB float64 `json:"download_gets_per_gib"`
	SelectiveGETs      int64   `json:"selective_gets"`
	SelectiveReadAmp   float64 `json:"selective_read_amplification"`
}

type ConsistencyAuditResult struct {
	MidUploadFaultInjected   bool `json:"mid_upload_fault_injected"`
	HeadPublishedOnFault     bool `json:"head_published_on_fault"`
	TornReadsObserved        int  `json:"torn_reads_observed"`
	AbortedObjectsCleanedUp  int  `json:"aborted_objects_cleaned_up"`
	BLAKE3CorruptionDetected bool `json:"blake3_corruption_detected"`
	MultiTenantNamespaces    int  `json:"multi_tenant_namespaces"`
	CrossTenantGroups        int  `json:"cross_tenant_groups"`
	SharedKeysAcrossTenants  int  `json:"shared_keys_across_tenants"`
}

type NegativeRegimesResult struct {
	AllLargeShards           AllLargeShardsResult           `json:"all_large_shards"`
	SparseRandomOversizedGap SparseRandomOversizedGapResult `json:"sparse_random_oversized_gap"`
	TinySingleFile           TinySingleFileResult           `json:"tiny_single_file"`
}

type AllLargeShardsResult struct {
	ShardCount        int                   `json:"shard_count"`
	ShardSizeMiB      int                   `json:"shard_size_mib"`
	NaiveConcurrent   *aiturbo.RegimeResult `json:"naive_concurrent"`
	AITurboDirect     *aiturbo.RegimeResult `json:"aiturbo_direct_bypass"`
	PackedGroupCount  int                   `json:"packed_group_count"`
	DirectBypassCount int                   `json:"direct_bypass_count"`
	PUTReductionPct   float64               `json:"put_reduction_pct"`
}

type SparseRandomOversizedGapResult struct {
	TotalFiles            int     `json:"total_files"`
	SelectedFiles         int     `json:"selected_files"`
	SelectedPct           float64 `json:"selected_pct"`
	RequestedBytes        int64   `json:"requested_bytes"`
	OversizedMaxGapKiB    int64   `json:"oversized_max_gap_kib"`
	OversizedFetchedBytes int64   `json:"oversized_fetched_bytes"`
	OversizedReadAmp      float64 `json:"oversized_read_amplification"`
	OversizedGETs         int64   `json:"oversized_gets"`
	OversizedDurationMs   float64 `json:"oversized_duration_ms"`
	TightMaxGapKiB        int64   `json:"tight_max_gap_kib"`
	TightFetchedBytes     int64   `json:"tight_fetched_bytes"`
	TightReadAmp          float64 `json:"tight_read_amplification"`
	TightGETs             int64   `json:"tight_gets"`
	TightDurationMs       float64 `json:"tight_duration_ms"`
	NaivePerObjectGETs    int64   `json:"naive_per_object_gets"`
	NaivePerObjectReadAmp float64 `json:"naive_per_object_read_amplification"`
	NaivePerObjectDurMs   float64 `json:"naive_per_object_duration_ms"`
}

type TinySingleFileResult struct {
	FileBytes       int64                 `json:"file_bytes"`
	NaiveSerial     *aiturbo.RegimeResult `json:"naive_serial"`
	AITurboGrouped  *aiturbo.RegimeResult `json:"aiturbo_grouped"`
	PUTReductionPct float64               `json:"put_reduction_pct"`
}

func main() {
	cfg := SuiteConfig{}
	flag.Int64Var(&cfg.Seed, "seed", 1345, "deterministic PRNG seed")
	flag.StringVar(&cfg.RepoRoot, "repo-root", ".", "repository root for grounding real small checkpoint files")
	flag.StringVar(&cfg.OutDir, "out-dir", "docs/research/hip-1345-aiturbo-grouped-io", "output directory for results.json and REPORT.md")
	flag.DurationVar(&cfg.PutRTT, "put-rtt", 10*time.Millisecond, "simulated S3 PUT first-byte RTT")
	flag.DurationVar(&cfg.GetRTT, "get-rtt", 8*time.Millisecond, "simulated S3 GET first-byte RTT")
	flag.Float64Var(&cfg.PerStreamMiBS, "per-stream-mibs", 100.0, "simulated per-connection S3 stream cap (MiB/s)")
	flag.Float64Var(&cfg.AggregateMiBS, "agg-mibs", 500.0, "simulated aggregate link cap (MiB/s)")
	flag.IntVar(&cfg.Concurrency, "concurrency", 8, "worker pool parallelism P")
	flag.IntVar(&cfg.TargetGroupMiB, "target-group-mib", 8, "default AITURBO TargetGroupBytes in MiB")
	flag.IntVar(&cfg.MaxGapKiB, "max-gap-kib", 64, "default AITURBO MaxGapBytes in KiB")
	flag.Parse()

	ctx := context.Background()
	report, err := runSuite(ctx, cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "aiturbo-bench failed: %v\n", err)
		os.Exit(1)
	}

	if cfg.OutDir != "" {
		if err := os.MkdirAll(cfg.OutDir, 0o755); err != nil {
			fmt.Fprintf(os.Stderr, "mkdir %s: %v\n", cfg.OutDir, err)
			os.Exit(1)
		}
		rawJSON, err := json.MarshalIndent(report, "", "  ")
		if err != nil {
			fmt.Fprintf(os.Stderr, "marshal results.json: %v\n", err)
			os.Exit(1)
		}
		jsonPath := filepath.Join(cfg.OutDir, "results.json")
		if err := os.WriteFile(jsonPath, append(rawJSON, '\n'), 0o644); err != nil {
			fmt.Fprintf(os.Stderr, "write %s: %v\n", jsonPath, err)
			os.Exit(1)
		}
		md := renderMarkdownReport(report)
		mdPath := filepath.Join(cfg.OutDir, "REPORT.md")
		if err := os.WriteFile(mdPath, []byte(md), 0o644); err != nil {
			fmt.Fprintf(os.Stderr, "write %s: %v\n", mdPath, err)
			os.Exit(1)
		}
		fmt.Printf("Wrote %s and %s\n", jsonPath, mdPath)
	}

	printTerminalSummary(report)
}

func runSuite(ctx context.Context, sc SuiteConfig) (*SuiteReport, error) {
	ns := aiturbo.Namespace{TenantID: "hanzo-research", VolumeID: "zen-ckpt-vol-01"}
	entries, selective, ws := buildMixedAICheckpoint(sc.RepoRoot, ns, sc.Seed, sc.ScaleDownForCI)

	targetGroupBytes := int64(sc.TargetGroupMiB) << 20
	if sc.ScaleDownForCI {
		targetGroupBytes = 1 << 20
	}
	maxGapBytes := int64(sc.MaxGapKiB) << 10

	aiCfg := aiturbo.DefaultConfig()
	aiCfg.TargetGroupBytes = targetGroupBytes
	aiCfg.LargeObjectThreshold = targetGroupBytes
	aiCfg.MaxGapBytes = maxGapBytes
	aiCfg.MaxCoalescedRangeBytes = max(targetGroupBytes, 16<<20)
	aiCfg.Concurrency = sc.Concurrency

	const mib = 1024.0 * 1024.0
	s3Model := aiturbo.S3ModelConfig{
		PutRTT:               sc.PutRTT,
		GetRTT:               sc.GetRTT,
		PerStreamBytesPerSec: sc.PerStreamMiBS * mib,
		AggregateBytesPerSec: sc.AggregateMiBS * mib,
	}

	report := &SuiteReport{
		SchemaVersion:  1,
		GeneratedAtUTC: time.Now().UTC().Format(time.RFC3339),
		GitCommit:      detectGitCommit(sc.RepoRoot),
		GoVersion:      runtime.Version(),
		GOOS:           runtime.GOOS,
		GOARCH:         runtime.GOARCH,
		NumCPU:         runtime.NumCPU(),
		Seed:           sc.Seed,
		S3Model: S3ModelSummary{
			PutRTTMs:             float64(sc.PutRTT) / float64(time.Millisecond),
			GetRTTMs:             float64(sc.GetRTT) / float64(time.Millisecond),
			PerStreamMiBPerSec:   sc.PerStreamMiBS,
			AggregateMiBPerSec:   sc.AggregateMiBS,
			Concurrency:          sc.Concurrency,
			TargetGroupBytes:     aiCfg.TargetGroupBytes,
			LargeObjectThreshold: aiCfg.LargeObjectThreshold,
			MaxGapBytes:          aiCfg.MaxGapBytes,
		},
		WorkloadSummary: ws,
	}

	// 1. Primary 4-regime matched comparison under realistic S3 RTT/bandwidth model.
	regimes := []aiturbo.Regime{
		aiturbo.RegimeNaivePerObjectSerial,
		aiturbo.RegimeNaivePerObjectConcurrent,
		aiturbo.RegimeNaiveSingleTarBlob,
		aiturbo.RegimeAITurboGroupedIO,
	}
	for _, r := range regimes {
		be := aiturbo.NewInstrumentedBackend(aiturbo.NewMemoryBackend(), s3Model)
		res, err := aiturbo.RunMatchedRegime(ctx, be, r, ns, "ckpt-primary-"+string(r), entries, selective, aiCfg)
		if err != nil {
			return nil, fmt.Errorf("primary regime %s: %w", r, err)
		}
		report.MatchedRegimes = append(report.MatchedRegimes, res)
	}

	// 2. TargetGroupBytes sweep {4 MiB, 8 MiB, 16 MiB} (or {1, 2, 4} in CI scale-down).
	sweepMiBs := []int{4, 8, 16}
	if sc.ScaleDownForCI {
		sweepMiBs = []int{1, 2}
	}
	for _, gm := range sweepMiBs {
		sweepCfg := aiCfg
		sweepCfg.TargetGroupBytes = int64(gm) << 20
		sweepCfg.LargeObjectThreshold = int64(gm) << 20
		sweepCfg.MaxCoalescedRangeBytes = max(sweepCfg.TargetGroupBytes, 16<<20)
		be := aiturbo.NewInstrumentedBackend(aiturbo.NewMemoryBackend(), s3Model)
		res, err := aiturbo.RunMatchedRegime(ctx, be, aiturbo.RegimeAITurboGroupedIO, ns, fmt.Sprintf("ckpt-sweep-%dmib", gm), entries, selective, sweepCfg)
		if err != nil {
			return nil, fmt.Errorf("sweep %dMiB: %w", gm, err)
		}
		report.GroupSizeSweep = append(report.GroupSizeSweep, GroupSizeSweepPoint{
			TargetGroupMiB:     gm,
			UploadMiBPerSec:    res.UploadMiBPerSec,
			UploadPUTs:         res.UploadMetrics.TotalWriteRequests(),
			UploadPUTsPerGiB:   res.UploadPUTsPerGiB,
			DownloadMiBPerSec:  res.DownloadMiBPerSec,
			DownloadGETs:       res.DownloadMetrics.TotalReadRequests(),
			DownloadGETsPerGiB: res.DownloadGETsPerGiB,
			SelectiveGETs:      res.SelectiveGETs,
			SelectiveReadAmp:   res.SelectiveReadAmp,
		})
	}

	// 3. Real local file:// backend grounding (zero simulated sleep, real OS syscalls).
	fileRegimes := []aiturbo.Regime{
		aiturbo.RegimeNaivePerObjectConcurrent,
		aiturbo.RegimeNaiveSingleTarBlob,
		aiturbo.RegimeAITurboGroupedIO,
	}
	for _, r := range fileRegimes {
		tmpDir, err := os.MkdirTemp("", "aiturbo-file-bench-*")
		if err != nil {
			return nil, err
		}
		fBe, err := backend.Open(ctx, "file://"+tmpDir)
		if err != nil {
			_ = os.RemoveAll(tmpDir)
			return nil, err
		}
		be := aiturbo.NewInstrumentedBackend(fBe, aiturbo.S3ModelConfig{})
		res, runErr := aiturbo.RunMatchedRegime(ctx, be, r, ns, "ckpt-file-"+string(r), entries, selective, aiCfg)
		_ = fBe.Close()
		_ = os.RemoveAll(tmpDir)
		if runErr != nil {
			return nil, fmt.Errorf("real file backend %s: %w", r, runErr)
		}
		report.RealFileBackend = append(report.RealFileBackend, res)
	}

	// 4. Real localhost Hanzo S3 HTTP protocol grounding via pkg/backend/s3 (github.com/hanzos3/go).
	httpRegimes := []aiturbo.Regime{
		aiturbo.RegimeNaivePerObjectConcurrent,
		aiturbo.RegimeAITurboGroupedIO,
	}
	for _, r := range httpRegimes {
		srv := newLocalS3HTTPServer("aiturbo-bench")
		rawS3, err := s3be.New(s3be.Config{
			Endpoint:  srv.Endpoint(),
			Insecure:  true,
			Region:    "us-east-1",
			Bucket:    "aiturbo-bench",
			Prefix:    "vfs/bench",
			AccessKey: "bench-ak",
			SecretKey: "bench-sk",
		})
		if err != nil {
			srv.Close()
			return nil, err
		}
		be := aiturbo.NewInstrumentedBackend(rawS3, aiturbo.S3ModelConfig{})
		res, runErr := aiturbo.RunMatchedRegime(ctx, be, r, ns, "ckpt-s3http-"+string(r), entries, selective, aiCfg)
		_ = rawS3.Close()
		srv.Close()
		if runErr != nil {
			return nil, fmt.Errorf("real Hanzo S3 HTTP %s: %w", r, runErr)
		}
		report.RealHanzoS3HTTP = append(report.RealHanzoS3HTTP, res)
	}

	// 5. Consistency, BLAKE3 Integrity, and Multi-Tenant Isolation Audit.
	audit, err := runConsistencyAndIsolationAudit(ctx, ns, entries, selective, aiCfg)
	if err != nil {
		return nil, fmt.Errorf("consistency audit: %w", err)
	}
	report.ConsistencyAudit = audit

	// 6. Three Honest Negative / Boundary Regimes.
	neg, err := runNegativeRegimes(ctx, ns, s3Model, aiCfg, sc.Seed, sc.ScaleDownForCI)
	if err != nil {
		return nil, fmt.Errorf("negative regimes: %w", err)
	}
	report.NegativeRegimes = neg

	return report, nil
}

func buildMixedAICheckpoint(repoRoot string, ns aiturbo.Namespace, seed int64, scaleDown bool) ([]aiturbo.Entry, []string, WorkloadSummary) {
	rng := rand.New(rand.NewSource(seed))
	entries := make([]aiturbo.Entry, 0, 260)
	ws := WorkloadSummary{}

	// Ground small metadata/config files in real files from hanzoai/vfs repo when available.
	maxRepoFiles := 72
	if scaleDown {
		maxRepoFiles = 16
	}
	repoFiles := collectRealRepoFiles(repoRoot, maxRepoFiles)
	for _, rf := range repoFiles {
		entries = append(entries, aiturbo.Entry{
			TenantID: ns.TenantID,
			VolumeID: ns.VolumeID,
			Path:     "repo_meta/" + rf.relPath,
			Data:     rf.data,
		})
		ws.RepoGroundedEntries++
		ws.SmallEntries++
		ws.TotalLogicalBytes += int64(len(rf.data))
	}

	// Additional deterministic small tokenizer / optimizer / LoRA metadata files (4 KiB - 48 KiB).
	synthSmallCount := 128
	medCount := 48
	largeCount := 4
	largeSize := 8 << 20
	if scaleDown {
		synthSmallCount = 24
		medCount = 8
		largeCount = 2
		largeSize = 1 << 20
	}

	var selective []string
	var selBytes int64

	for i := 0; i < synthSmallCount; i++ {
		sz := (4 + rng.Intn(45)) << 10 // 4 KiB..48 KiB
		buf := make([]byte, sz)
		_, _ = rng.Read(buf)
		layer := i / 16
		p := fmt.Sprintf("layers/layer-%02d/meta-%03d.bin", layer, i%16)
		entries = append(entries, aiturbo.Entry{
			TenantID: ns.TenantID,
			VolumeID: ns.VolumeID,
			Path:     p,
			Data:     buf,
		})
		ws.SmallEntries++
		ws.TotalLogicalBytes += int64(sz)
		// Include layer-00 small files in selective ~5% restore.
		if layer == 0 {
			selective = append(selective, p)
			selBytes += int64(sz)
		}
	}

	// Medium layer tensors (128 KiB - 512 KiB).
	for i := 0; i < medCount; i++ {
		sz := (128 + rng.Intn(385)) << 10 // 128 KiB..512 KiB
		if scaleDown {
			sz = 96 << 10
		}
		buf := make([]byte, sz)
		_, _ = rng.Read(buf)
		layer := i / 8
		p := fmt.Sprintf("layers/layer-%02d/tensor-%02d.bin", layer, i%8)
		entries = append(entries, aiturbo.Entry{
			TenantID: ns.TenantID,
			VolumeID: ns.VolumeID,
			Path:     p,
			Data:     buf,
		})
		ws.MediumEntries++
		ws.TotalLogicalBytes += int64(sz)
		if layer == 0 {
			selective = append(selective, p)
			selBytes += int64(sz)
		}
	}

	// Large base-weight shards (8 MiB each in full mode, 1 MiB in CI scale-down).
	for i := 0; i < largeCount; i++ {
		buf := make([]byte, largeSize)
		_, _ = rng.Read(buf)
		p := fmt.Sprintf("weights/model-shard-%02d.safetensors", i)
		entries = append(entries, aiturbo.Entry{
			TenantID: ns.TenantID,
			VolumeID: ns.VolumeID,
			Path:     p,
			Data:     buf,
		})
		ws.LargeEntries++
		ws.TotalLogicalBytes += int64(largeSize)
	}

	ws.TotalEntries = len(entries)
	ws.TotalLogicalMiB = float64(ws.TotalLogicalBytes) / (1024.0 * 1024.0)
	ws.SelectiveEntries = len(selective)
	ws.SelectiveBytes = selBytes
	if ws.TotalLogicalBytes > 0 {
		ws.SelectivePctOfTotal = 100.0 * float64(selBytes) / float64(ws.TotalLogicalBytes)
	}
	return entries, selective, ws
}

type repoFile struct {
	relPath string
	data    []byte
}

func collectRealRepoFiles(repoRoot string, limit int) []repoFile {
	if repoRoot == "" || limit <= 0 {
		return nil
	}
	var candidates []string
	_ = filepath.WalkDir(repoRoot, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		name := d.Name()
		if d.IsDir() {
			if name == ".git" || name == "docs" || name == "testdata" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(name, ".go") && !strings.HasSuffix(name, ".md") {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return nil
		}
		if info.Size() >= 1024 && info.Size() <= 64<<10 {
			candidates = append(candidates, p)
		}
		return nil
	})
	sort.Strings(candidates)
	out := make([]repoFile, 0, min(limit, len(candidates)))
	for _, abs := range candidates {
		if len(out) >= limit {
			break
		}
		rel, err := filepath.Rel(repoRoot, abs)
		if err != nil {
			continue
		}
		rel = filepath.ToSlash(rel)
		if _, err := aiturbo.ValidateEntryPath("repo_meta/" + rel); err != nil {
			continue
		}
		data, err := os.ReadFile(abs)
		if err != nil || len(data) == 0 {
			continue
		}
		out = append(out, repoFile{relPath: rel, data: data})
	}
	return out
}

func runConsistencyAndIsolationAudit(ctx context.Context, ns aiturbo.Namespace, entries []aiturbo.Entry, selective []string, cfg aiturbo.Config) (ConsistencyAuditResult, error) {
	var out ConsistencyAuditResult

	// 1. Mid-upload failure injection -> zero torn reads.
	mem := aiturbo.NewMemoryBackend()
	faultBe := aiturbo.NewInstrumentedBackend(mem, aiturbo.S3ModelConfig{
		FailOnPutCall: 3,
	})
	eng := aiturbo.NewEngine(faultBe)
	upRes, err := eng.UploadCheckpoint(ctx, ns, "ckpt-audit-fault", entries, cfg)
	if !errors.Is(err, aiturbo.ErrInjectedFailure) {
		return out, fmt.Errorf("expected injected failure, got %v", err)
	}
	out.MidUploadFaultInjected = true
	out.HeadPublishedOnFault = upRes.HeadPublished
	out.AbortedObjectsCleanedUp = upRes.CleanedUpObjects

	faultBe.SetModelConfig(aiturbo.S3ModelConfig{})
	if _, _, err := eng.LoadManifest(ctx, ns, "ckpt-audit-fault", cfg); !errors.Is(err, backend.ErrNotFound) {
		out.TornReadsObserved++
	}

	// 2. Single-bit BLAKE3 corruption detection.
	cleanRes, err := eng.UploadCheckpoint(ctx, ns, "ckpt-audit-corrupt", entries, cfg)
	if err != nil {
		return out, err
	}
	var targetKey string
	for _, ent := range cleanRes.Manifest.Entries {
		if len(selective) > 0 && ent.Path == selective[0] {
			targetKey = ent.ObjectKey
			break
		}
	}
	faultBe.SetModelConfig(aiturbo.S3ModelConfig{CorruptOnReadKey: targetKey})
	if _, err := eng.DownloadCheckpoint(ctx, ns, "ckpt-audit-corrupt", selective, cfg); errors.Is(err, aiturbo.ErrCorruptData) {
		out.BLAKE3CorruptionDetected = true
	}

	// 3. Multi-tenant co-residency audit (4 tenants x 2 volumes = 8 namespaces).
	shared := make([]byte, 32<<10)
	for i := range shared {
		shared[i] = byte(i * 31)
	}
	mtEntries := make([]aiturbo.Entry, 0, 8*10)
	for t := 0; t < 4; t++ {
		for v := 0; v < 2; v++ {
			for f := 0; f < 10; f++ {
				mtEntries = append(mtEntries, aiturbo.Entry{
					TenantID: fmt.Sprintf("tenant-%d", t),
					VolumeID: fmt.Sprintf("vol-%d", v),
					Path:     fmt.Sprintf("ckpt/item-%02d.bin", f),
					Data:     shared,
				})
			}
		}
	}
	plan, err := aiturbo.PlanUpload(mtEntries, cfg)
	if err != nil {
		return out, err
	}
	out.MultiTenantNamespaces = plan.TenantNamespaceCount
	keyOwners := make(map[string]aiturbo.Namespace, len(plan.Groups))
	for _, g := range plan.Groups {
		if prev, dup := keyOwners[g.ObjectKey]; dup && prev != g.Namespace {
			out.SharedKeysAcrossTenants++
		}
		keyOwners[g.ObjectKey] = g.Namespace
		if !g.Namespace.OwnsKey(g.ObjectKey) {
			out.CrossTenantGroups++
		}
	}
	return out, nil
}

func runNegativeRegimes(ctx context.Context, ns aiturbo.Namespace, s3Model aiturbo.S3ModelConfig, baseCfg aiturbo.Config, seed int64, scaleDown bool) (NegativeRegimesResult, error) {
	var out NegativeRegimesResult
	rng := rand.New(rand.NewSource(seed + 999))

	// Negative Regime 1: All-Large-Shard Checkpoint (8 x 8 MiB shards, 0 small files).
	shardCount := 8
	shardMiB := 8
	if scaleDown {
		shardCount = 4
		shardMiB = 1
	}
	largeEntries := make([]aiturbo.Entry, shardCount)
	for i := range largeEntries {
		b := make([]byte, shardMiB<<20)
		_, _ = rng.Read(b)
		largeEntries[i] = aiturbo.Entry{
			TenantID: ns.TenantID,
			VolumeID: ns.VolumeID,
			Path:     fmt.Sprintf("weights/shard-%02d.safetensors", i),
			Data:     b,
		}
	}
	largeCfg := baseCfg
	largeCfg.TargetGroupBytes = int64(shardMiB) << 20
	largeCfg.LargeObjectThreshold = int64(shardMiB) << 20
	largePlan, err := aiturbo.PlanUpload(largeEntries, largeCfg)
	if err != nil {
		return out, err
	}
	beNaiveLarge := aiturbo.NewInstrumentedBackend(aiturbo.NewMemoryBackend(), s3Model)
	naiveLargeRes, err := aiturbo.RunMatchedRegime(ctx, beNaiveLarge, aiturbo.RegimeNaivePerObjectConcurrent, ns, "neg-large-naive", largeEntries, nil, largeCfg)
	if err != nil {
		return out, err
	}
	beAILarge := aiturbo.NewInstrumentedBackend(aiturbo.NewMemoryBackend(), s3Model)
	aiLargeRes, err := aiturbo.RunMatchedRegime(ctx, beAILarge, aiturbo.RegimeAITurboGroupedIO, ns, "neg-large-ai", largeEntries, nil, largeCfg)
	if err != nil {
		return out, err
	}
	naivePUTs := naiveLargeRes.UploadMetrics.TotalWriteRequests()
	aiPUTs := aiLargeRes.UploadMetrics.TotalWriteRequests()
	putRed := 0.0
	if naivePUTs > 0 {
		putRed = 100.0 * float64(naivePUTs-aiPUTs) / float64(naivePUTs)
	}
	out.AllLargeShards = AllLargeShardsResult{
		ShardCount:        shardCount,
		ShardSizeMiB:      shardMiB,
		NaiveConcurrent:   naiveLargeRes,
		AITurboDirect:     aiLargeRes,
		PackedGroupCount:  largePlan.PackedGroupCount,
		DirectBypassCount: largePlan.DirectObjectCount,
		PUTReductionPct:   putRed,
	}

	// Negative Regime 2: 1% Sparse Random Point-Reads across dispersed packed offsets
	// with oversized MaxGapBytes (4 MiB) vs tight MaxGapBytes (64 KiB) vs direct GETs.
	totalSparseFiles := 200
	chunkSize := 32 << 10 // 32 KiB x 200 = 6.25 MiB packed into one 8 MiB group
	if scaleDown {
		totalSparseFiles = 80
		chunkSize = 12 << 10
	}
	sparseEntries := make([]aiturbo.Entry, totalSparseFiles)
	for i := range sparseEntries {
		b := make([]byte, chunkSize)
		_, _ = rng.Read(b)
		sparseEntries[i] = aiturbo.Entry{
			TenantID: ns.TenantID,
			VolumeID: ns.VolumeID,
			Path:     fmt.Sprintf("sparse/item-%03d.bin", i),
			Data:     b,
		}
	}
	// Select 1% (2 dispersed items near start and end of the group).
	selSparse := []string{
		sparseEntries[2].Path,
		sparseEntries[totalSparseFiles-3].Path,
	}
	sparseCfg := baseCfg
	sparseCfg.TargetGroupBytes = 8 << 20
	sparseCfg.LargeObjectThreshold = 8 << 20

	beSparseAI := aiturbo.NewInstrumentedBackend(aiturbo.NewMemoryBackend(), s3Model)
	engSparse := aiturbo.NewEngine(beSparseAI)
	if _, err := engSparse.UploadCheckpoint(ctx, ns, "neg-sparse-ai", sparseEntries, sparseCfg); err != nil {
		return out, err
	}

	wideCfg := sparseCfg
	wideCfg.MaxGapBytes = 8 << 20 // intentionally oversized gap threshold
	wideCfg.MaxCoalescedRangeBytes = 16 << 20
	beSparseAI.ResetMetrics()
	wideRes, err := engSparse.DownloadCheckpoint(ctx, ns, "neg-sparse-ai", selSparse, wideCfg)
	if err != nil {
		return out, err
	}
	wideMetrics := beSparseAI.Metrics()

	tightCfg := sparseCfg
	tightCfg.MaxGapBytes = 64 << 10
	beSparseAI.ResetMetrics()
	tightRes, err := engSparse.DownloadCheckpoint(ctx, ns, "neg-sparse-ai", selSparse, tightCfg)
	if err != nil {
		return out, err
	}
	tightMetrics := beSparseAI.Metrics()

	beSparseNaive := aiturbo.NewInstrumentedBackend(aiturbo.NewMemoryBackend(), s3Model)
	naiveSparseRes, err := aiturbo.RunMatchedRegime(ctx, beSparseNaive, aiturbo.RegimeNaivePerObjectConcurrent, ns, "neg-sparse-naive", sparseEntries, selSparse, sparseCfg)
	if err != nil {
		return out, err
	}

	out.SparseRandomOversizedGap = SparseRandomOversizedGapResult{
		TotalFiles:            totalSparseFiles,
		SelectedFiles:         len(selSparse),
		SelectedPct:           100.0 * float64(len(selSparse)) / float64(totalSparseFiles),
		RequestedBytes:        wideRes.Plan.TotalRequestedBytes,
		OversizedMaxGapKiB:    wideCfg.MaxGapBytes >> 10,
		OversizedFetchedBytes: wideRes.Plan.TotalFetchedBytes,
		OversizedReadAmp:      wideRes.Plan.ReadAmplification,
		OversizedGETs:         wideMetrics.TotalReadRequests(),
		OversizedDurationMs:   float64(wideRes.Duration) / float64(time.Millisecond),
		TightMaxGapKiB:        tightCfg.MaxGapBytes >> 10,
		TightFetchedBytes:     tightRes.Plan.TotalFetchedBytes,
		TightReadAmp:          tightRes.Plan.ReadAmplification,
		TightGETs:             tightMetrics.TotalReadRequests(),
		TightDurationMs:       float64(tightRes.Duration) / float64(time.Millisecond),
		NaivePerObjectGETs:    naiveSparseRes.SelectiveGETs,
		NaivePerObjectReadAmp: naiveSparseRes.SelectiveReadAmp,
		NaivePerObjectDurMs:   float64(naiveSparseRes.SelectiveDuration) / float64(time.Millisecond),
	}

	// Negative Regime 3: Tiny 1-File / 10 KiB Checkpoint.
	tinyBuf := make([]byte, 10<<10)
	_, _ = rng.Read(tinyBuf)
	tinyEntries := []aiturbo.Entry{{
		TenantID: ns.TenantID,
		VolumeID: ns.VolumeID,
		Path:     "config.json",
		Data:     tinyBuf,
	}}
	beTinyNaive := aiturbo.NewInstrumentedBackend(aiturbo.NewMemoryBackend(), s3Model)
	tinyNaiveRes, err := aiturbo.RunMatchedRegime(ctx, beTinyNaive, aiturbo.RegimeNaivePerObjectSerial, ns, "neg-tiny-naive", tinyEntries, nil, baseCfg)
	if err != nil {
		return out, err
	}
	beTinyAI := aiturbo.NewInstrumentedBackend(aiturbo.NewMemoryBackend(), s3Model)
	tinyAIRes, err := aiturbo.RunMatchedRegime(ctx, beTinyAI, aiturbo.RegimeAITurboGroupedIO, ns, "neg-tiny-ai", tinyEntries, nil, baseCfg)
	if err != nil {
		return out, err
	}
	tinyNaivePUTs := tinyNaiveRes.UploadMetrics.TotalWriteRequests()
	tinyAIPUTs := tinyAIRes.UploadMetrics.TotalWriteRequests()
	tinyRed := 0.0
	if tinyNaivePUTs > 0 {
		tinyRed = 100.0 * float64(tinyNaivePUTs-tinyAIPUTs) / float64(tinyNaivePUTs)
	}
	out.TinySingleFile = TinySingleFileResult{
		FileBytes:       int64(len(tinyBuf)),
		NaiveSerial:     tinyNaiveRes,
		AITurboGrouped:  tinyAIRes,
		PUTReductionPct: tinyRed,
	}

	return out, nil
}

func detectGitCommit(repoRoot string) string {
	cmd := exec.Command("git", "rev-parse", "--short", "HEAD")
	cmd.Dir = repoRoot
	out, err := cmd.Output()
	if err != nil {
		return "unknown"
	}
	return strings.TrimSpace(string(out))
}

func printTerminalSummary(r *SuiteReport) {
	fmt.Printf("\n=== AITURBO Grouped IO Benchmark (seed=%d, logical=%.2f MiB, files=%d) ===\n",
		r.Seed, r.WorkloadSummary.TotalLogicalMiB, r.WorkloadSummary.TotalEntries)
	fmt.Printf("%-30s %10s %10s %10s %10s %9s %7s\n",
		"Regime", "Up MiB/s", "PUTs/GiB", "Dn MiB/s", "GETs/GiB", "Sel5% ms", "ReadAmp")
	for _, m := range r.MatchedRegimes {
		fmt.Printf("%-30s %10.2f %10.1f %10.2f %10.1f %9.1f %6.2fx\n",
			m.Regime, m.UploadMiBPerSec, m.UploadPUTsPerGiB, m.DownloadMiBPerSec, m.DownloadGETsPerGiB,
			float64(m.SelectiveDuration)/float64(time.Millisecond), m.SelectiveReadAmp)
	}
	fmt.Printf("\nConsistency Audit: fault_head_published=%v torn_reads=%d blake3_corrupt_caught=%v cross_tenant_groups=%d\n",
		r.ConsistencyAudit.HeadPublishedOnFault, r.ConsistencyAudit.TornReadsObserved,
		r.ConsistencyAudit.BLAKE3CorruptionDetected, r.ConsistencyAudit.CrossTenantGroups)
}

// localS3HTTPServer is a minimal path-style S3 HTTP server for grounding
// pkg/backend/s3 over real loopback TCP/HTTP outside testing.T.
type localS3HTTPServer struct {
	bucket string
	srv    *httptest.Server
	mu     sync.Mutex
	objs   map[string][]byte
	mod    map[string]time.Time
}

func newLocalS3HTTPServer(bucket string) *localS3HTTPServer {
	s := &localS3HTTPServer{
		bucket: bucket,
		objs:   make(map[string][]byte),
		mod:    make(map[string]time.Time),
	}
	s.srv = httptest.NewServer(http.HandlerFunc(s.serve))
	return s
}

func (s *localS3HTTPServer) Endpoint() string { return strings.TrimPrefix(s.srv.URL, "http://") }
func (s *localS3HTTPServer) Close()           { s.srv.Close() }

var crlfBytes = []byte("\r\n")

func readS3Body(r *http.Request) ([]byte, error) {
	if !strings.Contains(r.Header.Get("Content-Encoding"), "aws-chunked") &&
		!strings.HasPrefix(r.Header.Get("X-Amz-Content-Sha256"), "STREAMING-") {
		return io.ReadAll(r.Body)
	}
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}
	out := make([]byte, 0, len(raw))
	pos := 0
	for pos < len(raw) {
		nl := bytes.Index(raw[pos:], crlfBytes)
		if nl < 0 {
			break
		}
		header := string(raw[pos : pos+nl])
		sizeHex, _, _ := strings.Cut(strings.TrimSpace(header), ";")
		n, err := strconv.ParseInt(sizeHex, 16, 64)
		if err != nil {
			return nil, err
		}
		pos += nl + 2
		if n == 0 {
			return out, nil
		}
		if pos+int(n) > len(raw) {
			return nil, io.ErrUnexpectedEOF
		}
		out = append(out, raw[pos:pos+int(n)]...)
		pos += int(n) + 2 // skip trailing CRLF
	}
	return out, nil
}

func (s *localS3HTTPServer) serve(w http.ResponseWriter, r *http.Request) {
	p := strings.TrimPrefix(r.URL.Path, "/")
	bucket, key, _ := strings.Cut(p, "/")
	if bucket != s.bucket {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	if key == "" {
		if r.URL.Query().Has("location") {
			fmt.Fprint(w, `<?xml version="1.0" encoding="UTF-8"?><LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></LocationConstraint>`)
			return
		}
		w.WriteHeader(http.StatusOK)
		return
	}

	switch r.Method {
	case http.MethodPut:
		data, err := readS3Body(r)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		sum := md5.Sum(data)
		etag := `"` + hex.EncodeToString(sum[:]) + `"`
		now := time.Now().UTC()
		s.mu.Lock()
		cur, exists := s.objs[key]
		if r.Header.Get("If-None-Match") == "*" && exists {
			s.mu.Unlock()
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusPreconditionFailed)
			fmt.Fprint(w, `<Error><Code>PreconditionFailed</Code></Error>`)
			return
		}
		if im := r.Header.Get("If-Match"); im != "" {
			curSum := md5.Sum(cur)
			curETag := `"` + hex.EncodeToString(curSum[:]) + `"`
			if !exists || (im != curETag && im != strings.Trim(curETag, `"`)) {
				s.mu.Unlock()
				w.Header().Set("Content-Type", "application/xml")
				w.WriteHeader(http.StatusPreconditionFailed)
				fmt.Fprint(w, `<Error><Code>PreconditionFailed</Code></Error>`)
				return
			}
		}
		s.objs[key] = data
		s.mod[key] = now
		s.mu.Unlock()
		w.Header().Set("ETag", etag)
		w.WriteHeader(http.StatusOK)

	case http.MethodGet, http.MethodHead:
		s.mu.Lock()
		data, exists := s.objs[key]
		mod := s.mod[key]
		if exists {
			data = append([]byte(nil), data...)
		}
		s.mu.Unlock()
		if !exists {
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusNotFound)
			if r.Method == http.MethodGet {
				fmt.Fprint(w, `<Error><Code>NoSuchKey</Code></Error>`)
			}
			return
		}
		sum := md5.Sum(data)
		w.Header().Set("ETag", `"`+hex.EncodeToString(sum[:])+`"`)
		w.Header().Set("Last-Modified", mod.UTC().Format(http.TimeFormat))
		if rng := r.Header.Get("Range"); strings.HasPrefix(rng, "bytes=") {
			parts := strings.SplitN(strings.TrimPrefix(rng, "bytes="), "-", 2)
			if len(parts) == 2 {
				lo, err1 := strconv.ParseInt(parts[0], 10, 64)
				hi, err2 := strconv.ParseInt(parts[1], 10, 64)
				if err1 == nil && err2 == nil && lo >= 0 && hi >= lo && hi < int64(len(data)) {
					slice := data[lo : hi+1]
					w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", lo, hi, len(data)))
					w.Header().Set("Content-Length", strconv.Itoa(len(slice)))
					w.WriteHeader(http.StatusPartialContent)
					if r.Method == http.MethodGet {
						_, _ = w.Write(slice)
					}
					return
				}
			}
			w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
			return
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		w.WriteHeader(http.StatusOK)
		if r.Method == http.MethodGet {
			_, _ = w.Write(data)
		}

	case http.MethodDelete:
		s.mu.Lock()
		delete(s.objs, key)
		delete(s.mod, key)
		s.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)

	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}
