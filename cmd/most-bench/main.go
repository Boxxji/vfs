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

// Command most-bench executes the reproducible HIP-1345 multi-agent shared
// toolchain benchmark comparing MOST selective hot-data mirroring against
// exclusive cache-tier migration, naive full RAM pinning, and unshared
// per-agent private caches over live pkg/backend/file (NVMe L2) and live HTTP
// pkg/backend/s3 (Hanzo S3 L3).
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"math/rand/v2"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/luxfi/age"
	"github.com/zeebo/blake3"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/pkg/backend"
	_ "github.com/hanzoai/vfs/pkg/backend/file"
	"github.com/hanzoai/vfs/pkg/backend/s3"
	"github.com/hanzoai/vfs/pkg/most"
)

type benchConfig struct {
	Seed           uint64
	RepoRoot       string
	OutDir         string
	BlockSize      int
	CorpusBlocks   int
	RAMBlocks      int
	OpsPerRun      int
	AgentCounts    []int
	NVMeChannels   int
	NVMeReadLatUS  int
	NVMeWriteLatUS int
}

type environmentInfo struct {
	GOOS           string  `json:"goos"`
	GOARCH         string  `json:"goarch"`
	NumCPU         int     `json:"num_cpu"`
	GoVersion      string  `json:"go_version"`
	Seed           uint64  `json:"seed"`
	BlockSizeBytes int     `json:"block_size_bytes"`
	CorpusBlocks   int     `json:"corpus_blocks"`
	RAMBlocks      int     `json:"ram_blocks"`
	RAMBudgetMiB   float64 `json:"ram_budget_mib"`
	OpsPerRun      int     `json:"ops_per_run"`
	NVMeChannels   int     `json:"nvme_channels"`
	NVMeReadLatUS  int     `json:"nvme_read_latency_us"`
	NVMeWriteLatUS int     `json:"nvme_write_latency_us"`
	TimestampUTC   string  `json:"timestamp_utc"`
}

type backendBaseline struct {
	NVMeDirectReadP50US  float64 `json:"nvme_direct_read_p50_us"`
	NVMeDirectReadP99US  float64 `json:"nvme_direct_read_p99_us"`
	NVMeDirectWriteP50US float64 `json:"nvme_direct_write_p50_us"`
	NVMeDirectWriteP99US float64 `json:"nvme_direct_write_p99_us"`
	LiveS3GetP50US       float64 `json:"live_s3_get_p50_us"`
	LiveS3GetP99US       float64 `json:"live_s3_get_p99_us"`
}

type runResult struct {
	Scenario              string      `json:"scenario"`
	Regime                most.Regime `json:"regime"`
	AgentCount            int         `json:"agent_count"`
	TotalOps              int         `json:"total_ops"`
	ElapsedMS             float64     `json:"elapsed_ms"`
	P50US                 float64     `json:"p50_us"`
	P95US                 float64     `json:"p95_us"`
	P99US                 float64     `json:"p99_us"`
	MeanUS                float64     `json:"mean_us"`
	MaxUS                 float64     `json:"max_us"`
	TotalRAMBytes         int64       `json:"total_ram_bytes"`
	TotalRAMMiB           float64     `json:"total_ram_mib"`
	TrackerMetaBytes      int64       `json:"tracker_meta_bytes"`
	TrackerMetaKiB        float64     `json:"tracker_meta_kib"`
	RAMPerAgentBytes      float64     `json:"ram_per_agent_bytes"`
	RAMPerAgentMiB        float64     `json:"ram_per_agent_mib"`
	RAMBudgetMiB          float64     `json:"ram_budget_mib"`
	OverBudget            bool        `json:"over_budget"`
	RAMHitRatePct         float64     `json:"ram_hit_rate_pct"`
	RAMHits               uint64      `json:"ram_hits"`
	NVMeHits              uint64      `json:"nvme_hits"`
	S3Fetches             uint64      `json:"s3_fetches"`
	SingleFlightCoalesced uint64      `json:"single_flight_coalesced"`
	MirrorAdmissions      uint64      `json:"mirror_admissions"`
	MirrorRejections      uint64      `json:"mirror_rejections"`
	MirrorInstantDrops    uint64      `json:"mirror_instant_drops"`
	MirrorInvalidations   uint64      `json:"mirror_invalidations"`
	NVMeReadIOPS          uint64      `json:"nvme_read_iops"`
	NVMeDemotionWrites    uint64      `json:"nvme_demotion_writes"`
	NVMeWritebackIOPS     uint64      `json:"nvme_writeback_iops"`
	NVMeWritebackBytes    uint64      `json:"nvme_writeback_bytes"`
	AggregateReadMiBS     float64     `json:"aggregate_read_mib_s"`
	OpsPerSec             float64     `json:"ops_per_sec"`
	BLAKE3VerifiedReads   uint64      `json:"blake3_verified_reads"`
	CorruptionRejects     uint64      `json:"corruption_rejects"`
	IsolationRejects      uint64      `json:"isolation_rejects"`
}

type coldStartResult struct {
	AgentCount            int     `json:"agent_count"`
	UniqueHotBlocks       int     `json:"unique_hot_blocks"`
	TotalReads            int     `json:"total_reads"`
	SimulatedS3RTTMS      float64 `json:"simulated_s3_rtt_ms"`
	LiveS3HTTPRequests    int64   `json:"live_s3_http_requests"`
	SingleFlightCoalesced uint64  `json:"single_flight_coalesced"`
	P50MS                 float64 `json:"p50_ms"`
	P99MS                 float64 `json:"p99_ms"`
	BLAKE3VerifiedReads   uint64  `json:"blake3_verified_reads"`
}

type pqSecurityResult struct {
	CipherSuite       string  `json:"cipher_suite"`
	Samples           int     `json:"samples"`
	BlockSizeBytes    int     `json:"block_size_bytes"`
	SealP50US         float64 `json:"seal_p50_us"`
	OpenP50US         float64 `json:"open_p50_us"`
	PlaintextLeaks    int     `json:"plaintext_leaks"`
	CrossTenantDenied bool    `json:"cross_tenant_denied"`
}

type benchReport struct {
	HIP             string                 `json:"hip"`
	Issue           string                 `json:"issue"`
	PaperRef        string                 `json:"paper_ref"`
	Environment     environmentInfo        `json:"environment"`
	Corpus          corpusStats            `json:"corpus"`
	Baseline        backendBaseline        `json:"backend_baseline"`
	ScalingSweep    []runResult            `json:"scaling_sweep"`
	ColdStartBurst  coldStartResult        `json:"cold_start_burst"`
	NegativeRegimes map[string][]runResult `json:"negative_regimes"`
	PQSecurityCheck pqSecurityResult       `json:"pq_security_check"`
}

func main() {
	cfg := benchConfig{
		AgentCounts:    []int{1, 4, 16, 32},
		NVMeChannels:   4,
		NVMeReadLatUS:  80,
		NVMeWriteLatUS: 320,
	}
	fs := flag.NewFlagSet("most-bench", flag.ExitOnError)
	fs.Uint64Var(&cfg.Seed, "seed", 1345, "deterministic RNG seed")
	fs.StringVar(&cfg.RepoRoot, "repo-root", ".", "repository root used to build real-file toolchain corpus")
	fs.StringVar(&cfg.OutDir, "out-dir", "docs/research/hip-1345-most-selective-mirroring", "output directory for results.json")
	fs.IntVar(&cfg.BlockSize, "block-size", 16384, "fixed toolchain block size in bytes")
	fs.IntVar(&cfg.CorpusBlocks, "corpus-blocks", 512, "number of blocks in shared toolchain corpus")
	fs.IntVar(&cfg.RAMBlocks, "ram-blocks", 96, "shared RAM budget in blocks for mirror/migration")
	fs.IntVar(&cfg.OpsPerRun, "ops", 2400, "total read operations per regime run")
	_ = fs.Parse(os.Args[1:])

	report, err := runBench(context.Background(), cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "most-bench failed: %v\n", err)
		os.Exit(1)
	}

	if err := writeResults(cfg.OutDir, report); err != nil {
		fmt.Fprintf(os.Stderr, "write results failed: %v\n", err)
		os.Exit(1)
	}
	printSummary(report)
}

func runBench(ctx context.Context, cfg benchConfig) (*benchReport, error) {
	blocks, cStats, err := buildToolchainCorpus(cfg.RepoRoot, cfg.Seed, cfg.BlockSize, cfg.CorpusBlocks)
	if err != nil {
		return nil, fmt.Errorf("build corpus: %w", err)
	}

	ramBudget := int64(cfg.RAMBlocks) * int64(cfg.BlockSize)
	env := environmentInfo{
		GOOS:           runtime.GOOS,
		GOARCH:         runtime.GOARCH,
		NumCPU:         runtime.NumCPU(),
		GoVersion:      runtime.Version(),
		Seed:           cfg.Seed,
		BlockSizeBytes: cfg.BlockSize,
		CorpusBlocks:   cfg.CorpusBlocks,
		RAMBlocks:      cfg.RAMBlocks,
		RAMBudgetMiB:   round4(float64(ramBudget) / (1024 * 1024)),
		OpsPerRun:      cfg.OpsPerRun,
		NVMeChannels:   cfg.NVMeChannels,
		NVMeReadLatUS:  cfg.NVMeReadLatUS,
		NVMeWriteLatUS: cfg.NVMeWriteLatUS,
		TimestampUTC:   time.Now().UTC().Format(time.RFC3339),
	}

	// Single live localhost HTTP S3 server holds the canonical L3 toolchain objects.
	s3Srv := newLocalS3Server("hanzo-most-bench")
	defer s3Srv.Close()
	s3Be, err := s3.New(s3.Config{
		Endpoint:  s3Srv.Endpoint(),
		Insecure:  true,
		Region:    "us-east-1",
		Bucket:    "hanzo-most-bench",
		Prefix:    "hip-1345",
		AccessKey: "k",
		SecretKey: "s",
	})
	if err != nil {
		return nil, fmt.Errorf("open live s3 backend: %w", err)
	}
	defer func() { _ = s3Be.Close() }()

	// Pre-populate L3 Hanzo S3 with all immutable shared toolchain blocks once.
	for _, b := range blocks {
		if _, err := s3Be.PutIfAbsent(ctx, b.ID.Key, b.Data); err != nil {
			return nil, fmt.Errorf("seed s3 %s: %w", b.ID.Key, err)
		}
	}

	// Measure raw local pkg/backend/file NVMe and live HTTP pkg/backend/s3 baselines.
	base, err := measureBackendBaseline(ctx, blocks, s3Be)
	if err != nil {
		return nil, fmt.Errorf("baseline: %w", err)
	}

	regimes := []most.Regime{
		most.RegimeExclusiveMigration,
		most.RegimeNaiveFullRAMPin,
		most.RegimePerAgentPrivateCache,
		most.RegimeMOSTSelectiveMirror,
	}

	// Pre-seed one canonical pkg/backend/file NVMe template directory once, then
	// hardlink its read-only files into each isolated per-run NVMe directory on
	// the same mount (pkg/backend/file overwrites via atomic temp+rename and
	// deletes via unlink, so hardlinked fixture files are strictly isolated).
	benchScratchRoot, err := os.MkdirTemp("", "most-bench-root-*")
	if err != nil {
		return nil, fmt.Errorf("create scratch root: %w", err)
	}
	defer func() { _ = os.RemoveAll(benchScratchRoot) }()

	templateNVMeDir := filepath.Join(benchScratchRoot, "template-nvme")
	templateNVMeBe, err := openFileBackendAt(templateNVMeDir)
	if err != nil {
		return nil, fmt.Errorf("open template nvme: %w", err)
	}
	for _, b := range blocks {
		if err := templateNVMeBe.Put(ctx, b.ID.Key, b.Data); err != nil {
			_ = templateNVMeBe.Close()
			return nil, fmt.Errorf("seed template nvme %s: %w", b.ID.Key, err)
		}
	}
	_ = templateNVMeBe.Close()

	// Experiment A: Multi-agent scaling sweep M in {1, 4, 16, 32}.
	var scaling []runResult
	runIdx := 0
	for _, m := range cfg.AgentCounts {
		traces := buildZipfTraces(cfg.Seed, m, cfg.OpsPerRun, len(blocks), 1.18)
		for _, regime := range regimes {
			runIdx++
			runDir := filepath.Join(benchScratchRoot, fmt.Sprintf("run-%d", runIdx))
			res, err := executeSharedReadRun(ctx, "zipf_shared_toolchain", regime, m, ramBudget, blocks, cStats.Namespaces, s3Be, traces, cfg, templateNVMeDir, runDir)
			if err != nil {
				return nil, fmt.Errorf("scaling M=%d regime=%s: %w", m, regime, err)
			}
			scaling = append(scaling, res)
		}
	}

	// Experiment B: Cold-start multi-agent burst (M=16, empty NVMe -> live S3).
	coldRes, err := executeColdStartBurst(ctx, cfg, blocks, cStats.Namespaces, s3Srv, s3Be, ramBudget)
	if err != nil {
		return nil, fmt.Errorf("cold start burst: %w", err)
	}

	// Experiment C: 3 Explicit Honest Negative Regimes.
	neg := make(map[string][]runResult, 3)

	// Negative Regime 1: Uniform / Flat-Hotness Scan (Zipf alpha = 0, WorkingSet > RAM).
	neg1Agents := 16
	if len(cfg.AgentCounts) > 0 && cfg.AgentCounts[len(cfg.AgentCounts)-1] < neg1Agents {
		neg1Agents = cfg.AgentCounts[len(cfg.AgentCounts)-1]
	}
	uniTraces := buildUniformTraces(cfg.Seed+101, neg1Agents, cfg.OpsPerRun, len(blocks))
	for _, regime := range []most.Regime{most.RegimeExclusiveMigration, most.RegimeMOSTSelectiveMirror} {
		runIdx++
		runDir := filepath.Join(benchScratchRoot, fmt.Sprintf("run-%d", runIdx))
		res, err := executeSharedReadRun(ctx, "neg_uniform_no_skew_scan", regime, neg1Agents, ramBudget, blocks, cStats.Namespaces, s3Be, uniTraces, cfg, templateNVMeDir, runDir)
		if err != nil {
			return nil, fmt.Errorf("neg1 %s: %w", regime, err)
		}
		neg["neg_uniform_no_skew_scan"] = append(neg["neg_uniform_no_skew_scan"], res)
	}

	// Negative Regime 2: Write-Heavy Mutable Workload (50% writes / 50% reads).
	neg2Res, err := executeWriteHeavyNegative(ctx, cfg, s3Be)
	if err != nil {
		return nil, fmt.Errorf("neg2: %w", err)
	}
	neg["neg_write_heavy_invalidation_churn"] = neg2Res

	// Negative Regime 3: Single-Agent (M=1) / Ample-RAM Regime (RAM >= WorkingSet).
	ampleBlocks := min(64, len(blocks))
	ampleSubset := blocks[:ampleBlocks]
	ampleBudget := int64(ampleBlocks*2) * int64(cfg.BlockSize)
	ampleTraces := buildZipfTraces(cfg.Seed+303, 1, min(cfg.OpsPerRun, 1200), ampleBlocks, 1.18)
	for _, regime := range regimes {
		runIdx++
		runDir := filepath.Join(benchScratchRoot, fmt.Sprintf("run-%d", runIdx))
		res, err := executeSharedReadRun(ctx, "neg_single_agent_ample_ram", regime, 1, ampleBudget, ampleSubset, cStats.Namespaces, s3Be, ampleTraces, cfg, templateNVMeDir, runDir)
		if err != nil {
			return nil, fmt.Errorf("neg3 %s: %w", regime, err)
		}
		neg["neg_single_agent_ample_ram"] = append(neg["neg_single_agent_ample_ram"], res)
	}

	// Experiment D: Post-quantum vfs.Hybrid security & overhead check.
	pqRes, err := executePQSecurityCheck(ctx, cfg, s3Be)
	if err != nil {
		return nil, fmt.Errorf("pq security check: %w", err)
	}

	return &benchReport{
		HIP:             "HIP-1345",
		Issue:           "hanzoai/vfs#50",
		PaperRef:        "MOST, FAST 2026 (https://www.usenix.org/conference/fast26/presentation/tu)",
		Environment:     env,
		Corpus:          cStats,
		Baseline:        base,
		ScalingSweep:    scaling,
		ColdStartBurst:  coldRes,
		NegativeRegimes: neg,
		PQSecurityCheck: pqRes,
	}, nil
}

func openFileBackendAt(dir string) (backend.Backend, error) {
	u := (&url.URL{Scheme: "file", Path: dir}).String()
	return backend.Open(context.Background(), u)
}

func cloneNVMeFixture(templateDir, dstDir string, blocks []corpusBlock) error {
	if err := os.MkdirAll(dstDir, 0o755); err != nil {
		return err
	}
	madeDirs := make(map[string]struct{}, 64)
	for _, b := range blocks {
		rel := filepath.FromSlash(b.ID.Key)
		src := filepath.Join(templateDir, rel)
		dst := filepath.Join(dstDir, rel)
		parent := filepath.Dir(dst)
		if _, ok := madeDirs[parent]; !ok {
			if err := os.MkdirAll(parent, 0o755); err != nil {
				return err
			}
			madeDirs[parent] = struct{}{}
		}
		if err := os.Link(src, dst); err != nil {
			// Fallback to copy if cross-device.
			raw, rerr := os.ReadFile(src)
			if rerr != nil {
				return rerr
			}
			if werr := os.WriteFile(dst, raw, 0o644); werr != nil {
				return werr
			}
		}
	}
	return nil
}

func openTempFileBackend() (backend.Backend, func(), error) {
	dir, err := os.MkdirTemp("", "most-nvme-*")
	if err != nil {
		return nil, nil, err
	}
	be, err := openFileBackendAt(dir)
	if err != nil {
		_ = os.RemoveAll(dir)
		return nil, nil, err
	}
	cleanup := func() {
		_ = be.Close()
		_ = os.RemoveAll(dir)
	}
	return be, cleanup, nil
}

func measureBackendBaseline(ctx context.Context, blocks []corpusBlock, s3Be backend.Backend) (backendBaseline, error) {
	nvmeBe, cleanup, err := openTempFileBackend()
	if err != nil {
		return backendBaseline{}, err
	}
	defer cleanup()

	n := min(64, len(blocks))
	writeLats := make([]time.Duration, n)
	readLats := make([]time.Duration, n)
	s3Lats := make([]time.Duration, n)

	for i := 0; i < n; i++ {
		t0 := time.Now()
		if err := nvmeBe.Put(ctx, blocks[i].ID.Key, blocks[i].Data); err != nil {
			return backendBaseline{}, err
		}
		writeLats[i] = time.Since(t0)
	}
	for i := 0; i < n; i++ {
		t0 := time.Now()
		got, _, err := nvmeBe.Get(ctx, blocks[i].ID.Key, backend.Unbounded)
		readLats[i] = time.Since(t0)
		if err != nil || blake3.Sum256(got) != blocks[i].ID.Digest {
			return backendBaseline{}, fmt.Errorf("nvme baseline read verify failed: %v", err)
		}
	}
	for i := 0; i < n; i++ {
		t0 := time.Now()
		got, _, err := s3Be.Get(ctx, blocks[i].ID.Key, backend.Unbounded)
		s3Lats[i] = time.Since(t0)
		if err != nil || blake3.Sum256(got) != blocks[i].ID.Digest {
			return backendBaseline{}, fmt.Errorf("s3 baseline read verify failed: %v", err)
		}
	}

	return backendBaseline{
		NVMeDirectReadP50US:  durUS(percentile(readLats, 0.50)),
		NVMeDirectReadP99US:  durUS(percentile(readLats, 0.99)),
		NVMeDirectWriteP50US: durUS(percentile(writeLats, 0.50)),
		NVMeDirectWriteP99US: durUS(percentile(writeLats, 0.99)),
		LiveS3GetP50US:       durUS(percentile(s3Lats, 0.50)),
		LiveS3GetP99US:       durUS(percentile(s3Lats, 0.99)),
	}, nil
}

func buildZipfTraces(seed uint64, agents, totalOps, numBlocks int, s float64) [][]int {
	opsPerAgent := totalOps / agents
	if opsPerAgent < 1 {
		opsPerAgent = 1
	}
	traces := make([][]int, agents)
	for a := 0; a < agents; a++ {
		rng := rand.New(rand.NewPCG(seed+uint64(a)*31+7, seed^uint64(a+1)*0x9e3779b97f4a7c15))
		zipf := rand.NewZipf(rng, s, 1.0, uint64(numBlocks-1))
		traces[a] = make([]int, opsPerAgent)
		for j := 0; j < opsPerAgent; j++ {
			traces[a][j] = int(zipf.Uint64())
		}
	}
	return traces
}

func buildUniformTraces(seed uint64, agents, totalOps, numBlocks int) [][]int {
	opsPerAgent := totalOps / agents
	if opsPerAgent < 1 {
		opsPerAgent = 1
	}
	traces := make([][]int, agents)
	for a := 0; a < agents; a++ {
		rng := rand.New(rand.NewPCG(seed+uint64(a)*53+11, seed^uint64(a+1)*0xbf58476d1ce4e5b9))
		traces[a] = make([]int, opsPerAgent)
		for j := 0; j < opsPerAgent; j++ {
			traces[a][j] = int(rng.Uint64() % uint64(numBlocks))
		}
	}
	return traces
}

func executeSharedReadRun(
	ctx context.Context,
	scenario string,
	regime most.Regime,
	agents int,
	ramBudget int64,
	blocks []corpusBlock,
	namespaces []string,
	s3Be backend.Backend,
	traces [][]int,
	cfg benchConfig,
	templateNVMeDir string,
	runNVMeDir string,
) (runResult, error) {
	if err := cloneNVMeFixture(templateNVMeDir, runNVMeDir, blocks); err != nil {
		return runResult{}, err
	}
	defer func() { _ = os.RemoveAll(runNVMeDir) }()

	nvmeBe, err := openFileBackendAt(runNVMeDir)
	if err != nil {
		return runResult{}, err
	}
	defer func() { _ = nvmeBe.Close() }()

	eng, err := most.NewEngine(most.Config{
		Regime:            regime,
		RAMBudgetBytes:    ramBudget,
		AgentCount:        agents,
		AllowedNamespaces: namespaces,
		NVMe: most.NVMeConfig{
			Store:        nvmeBe,
			Channels:     cfg.NVMeChannels,
			ReadLatency:  time.Duration(cfg.NVMeReadLatUS) * time.Microsecond,
			WriteLatency: time.Duration(cfg.NVMeWriteLatUS) * time.Microsecond,
		},
		Remote: s3Be,
	})
	if err != nil {
		return runResult{}, err
	}

	totalOps := 0
	for _, tr := range traces {
		totalOps += len(tr)
	}
	latencies := make([]time.Duration, totalOps)
	var verified atomic.Uint64
	var firstErr atomic.Value

	startAll := time.Now()
	var wg sync.WaitGroup
	offset := 0
	for a := 0; a < agents; a++ {
		tr := traces[a]
		base := offset
		offset += len(tr)
		wg.Add(1)
		go func(agentIdx, startIdx int, seq []int) {
			defer wg.Done()
			agentID := fmt.Sprintf("agent-%d", agentIdx)
			for j, blkIdx := range seq {
				blk := blocks[blkIdx]
				t0 := time.Now()
				got, err := eng.ReadShared(ctx, agentID, blk.ID)
				latencies[startIdx+j] = time.Since(t0)
				if err != nil {
					firstErr.CompareAndSwap(nil, err)
					return
				}
				if blake3.Sum256(got) != blk.ID.Digest {
					firstErr.CompareAndSwap(nil, fmt.Errorf("blake3 verification failed for block %d", blkIdx))
					return
				}
				verified.Add(1)
			}
		}(a, base, tr)
	}
	wg.Wait()
	elapsed := time.Since(startAll)

	if v := firstErr.Load(); v != nil {
		return runResult{}, v.(error)
	}

	m := eng.Metrics()
	totalRAM := m.PeakRAMBytes + m.TrackerMetaBytes
	readBytes := float64(verified.Load()) * float64(cfg.BlockSize)
	elapsedSec := elapsed.Seconds()
	if elapsedSec <= 0 {
		elapsedSec = 1e-9
	}

	return runResult{
		Scenario:              scenario,
		Regime:                regime,
		AgentCount:            agents,
		TotalOps:              totalOps,
		ElapsedMS:             round4(float64(elapsed.Microseconds()) / 1000.0),
		P50US:                 durUS(percentile(latencies, 0.50)),
		P95US:                 durUS(percentile(latencies, 0.95)),
		P99US:                 durUS(percentile(latencies, 0.99)),
		MeanUS:                durUS(meanDuration(latencies)),
		MaxUS:                 durUS(percentile(latencies, 1.0)),
		TotalRAMBytes:         totalRAM,
		TotalRAMMiB:           round4(float64(totalRAM) / (1024 * 1024)),
		TrackerMetaBytes:      m.TrackerMetaBytes,
		TrackerMetaKiB:        round4(float64(m.TrackerMetaBytes) / 1024.0),
		RAMPerAgentBytes:      round4(m.RAMPerAgentBytes),
		RAMPerAgentMiB:        round4(m.RAMPerAgentBytes / (1024 * 1024)),
		RAMBudgetMiB:          round4(float64(ramBudget) / (1024 * 1024)),
		OverBudget:            m.OverBudget,
		RAMHitRatePct:         round4(m.RAMHitRate * 100.0),
		RAMHits:               m.RAMHits,
		NVMeHits:              m.NVMeHits,
		S3Fetches:             m.S3Fetches,
		SingleFlightCoalesced: m.SingleFlightCoalesced,
		MirrorAdmissions:      m.MirrorAdmissions,
		MirrorRejections:      m.MirrorRejections,
		MirrorInstantDrops:    m.MirrorInstantDrops,
		MirrorInvalidations:   m.MirrorInvalidations,
		NVMeReadIOPS:          m.NVMeReadIOPS,
		NVMeDemotionWrites:    m.NVMeDemotionWrites,
		NVMeWritebackIOPS:     m.NVMeWritebackIOPS,
		NVMeWritebackBytes:    m.NVMeWritebackBytes,
		AggregateReadMiBS:     round4((readBytes / (1024 * 1024)) / elapsedSec),
		OpsPerSec:             round4(float64(totalOps) / elapsedSec),
		BLAKE3VerifiedReads:   verified.Load(),
		CorruptionRejects:     m.CorruptionRejects,
		IsolationRejects:      m.IsolationRejects,
	}, nil
}

func executeColdStartBurst(
	ctx context.Context,
	cfg benchConfig,
	blocks []corpusBlock,
	namespaces []string,
	s3Srv *localS3Server,
	s3Be backend.Backend,
	ramBudget int64,
) (coldStartResult, error) {
	nvmeBe, cleanup, err := openTempFileBackend()
	if err != nil {
		return coldStartResult{}, err
	}
	defer cleanup()

	const (
		agents    = 16
		hotBlocks = 24
	)
	nHot := min(hotBlocks, len(blocks))
	simRTT := 2 * time.Millisecond

	eng, err := most.NewEngine(most.Config{
		Regime:            most.RegimeMOSTSelectiveMirror,
		RAMBudgetBytes:    ramBudget,
		AgentCount:        agents,
		AllowedNamespaces: namespaces,
		NVMe: most.NVMeConfig{
			Store:        nvmeBe,
			Channels:     cfg.NVMeChannels,
			ReadLatency:  time.Duration(cfg.NVMeReadLatUS) * time.Microsecond,
			WriteLatency: time.Duration(cfg.NVMeWriteLatUS) * time.Microsecond,
		},
		Remote:        s3Be,
		RemoteLatency: simRTT,
	})
	if err != nil {
		return coldStartResult{}, err
	}

	beforeReqs := s3Srv.Requests()
	totalReads := agents * nHot
	latencies := make([]time.Duration, totalReads)
	var verified atomic.Uint64
	var firstErr atomic.Value

	// Synchronized start barrier so all 16 agents burst simultaneously onto cold NVMe -> S3.
	startGate := make(chan struct{})
	var wg sync.WaitGroup
	for a := 0; a < agents; a++ {
		wg.Add(1)
		go func(agentIdx int) {
			defer wg.Done()
			<-startGate
			agentID := fmt.Sprintf("agent-%d", agentIdx)
			base := agentIdx * nHot
			for j := 0; j < nHot; j++ {
				blk := blocks[j]
				t0 := time.Now()
				got, err := eng.ReadShared(ctx, agentID, blk.ID)
				latencies[base+j] = time.Since(t0)
				if err != nil {
					firstErr.CompareAndSwap(nil, err)
					return
				}
				if blake3.Sum256(got) != blk.ID.Digest {
					firstErr.CompareAndSwap(nil, fmt.Errorf("cold start digest mismatch"))
					return
				}
				verified.Add(1)
			}
		}(a)
	}
	close(startGate)
	wg.Wait()
	if v := firstErr.Load(); v != nil {
		return coldStartResult{}, v.(error)
	}

	m := eng.Metrics()
	return coldStartResult{
		AgentCount:            agents,
		UniqueHotBlocks:       nHot,
		TotalReads:            totalReads,
		SimulatedS3RTTMS:      float64(simRTT.Microseconds()) / 1000.0,
		LiveS3HTTPRequests:    s3Srv.Requests() - beforeReqs,
		SingleFlightCoalesced: m.SingleFlightCoalesced,
		P50MS:                 round4(durUS(percentile(latencies, 0.50)) / 1000.0),
		P99MS:                 round4(durUS(percentile(latencies, 0.99)) / 1000.0),
		BLAKE3VerifiedReads:   verified.Load(),
	}, nil
}

func executeWriteHeavyNegative(ctx context.Context, cfg benchConfig, s3Be backend.Backend) ([]runResult, error) {
	const (
		agents    = 8
		numBlocks = 64
		totalOps  = 1200
	)
	ramBudget := int64(numBlocks) * int64(cfg.BlockSize)
	sc := most.Scope{TenantID: "study-tenant", VolumeID: "study-vol"}

	ids := make([]most.BlockID, numBlocks)
	initData := make([][]byte, numBlocks)
	for i := 0; i < numBlocks; i++ {
		id, err := most.NewPrivateBlockID(sc, fmt.Sprintf("mut-%d", i))
		if err != nil {
			return nil, err
		}
		ids[i] = id
		rng := rand.New(rand.NewPCG(cfg.Seed+uint64(i), 0x12345))
		b := make([]byte, cfg.BlockSize)
		for k := range b {
			b[k] = byte(rng.Uint32())
		}
		initData[i] = b
	}

	opsPerAgent := totalOps / agents
	type mutOp struct {
		isWrite bool
		blk     int
	}
	traces := make([][]mutOp, agents)
	for a := 0; a < agents; a++ {
		rng := rand.New(rand.NewPCG(cfg.Seed+202+uint64(a), uint64(a+7)))
		traces[a] = make([]mutOp, opsPerAgent)
		for j := 0; j < opsPerAgent; j++ {
			traces[a][j] = mutOp{
				isWrite: j%2 == 0, // 50% writes / 50% reads
				blk:     int(rng.Uint64() % numBlocks),
			}
		}
	}

	var results []runResult
	for _, regime := range []most.Regime{most.RegimeExclusiveMigration, most.RegimeMOSTSelectiveMirror} {
		nvmeBe, cleanup, err := openTempFileBackend()
		if err != nil {
			return nil, err
		}
		eng, err := most.NewEngine(most.Config{
			Regime:            regime,
			RAMBudgetBytes:    ramBudget,
			AgentCount:        agents,
			AllowedNamespaces: []string{"go-1-27-stdlib"},
			NVMe: most.NVMeConfig{
				Store:        nvmeBe,
				Channels:     cfg.NVMeChannels,
				ReadLatency:  time.Duration(cfg.NVMeReadLatUS) * time.Microsecond,
				WriteLatency: time.Duration(cfg.NVMeWriteLatUS) * time.Microsecond,
			},
			Remote:                     s3Be,
			AllowMutableMirrorForStudy: true,
		})
		if err != nil {
			cleanup()
			return nil, err
		}
		for i := 0; i < numBlocks; i++ {
			if err := eng.SeedMutable(ctx, ids[i], initData[i]); err != nil {
				cleanup()
				return nil, err
			}
		}
		// Warm all blocks into RAM before measuring write-heavy churn.
		for rep := 0; rep < 2; rep++ {
			for i := 0; i < numBlocks; i++ {
				if _, err := eng.ReadMutable(ctx, sc, ids[i]); err != nil {
					cleanup()
					return nil, err
				}
			}
		}

		// Snapshot counters after warm-up so timed-phase metrics exclude the 128 warm-up reads.
		warm := eng.Metrics()

		latencies := make([]time.Duration, agents*opsPerAgent)
		var firstErr atomic.Value
		startAll := time.Now()
		var wg sync.WaitGroup
		for a := 0; a < agents; a++ {
			wg.Add(1)
			go func(agentIdx int) {
				defer wg.Done()
				base := agentIdx * opsPerAgent
				for j, op := range traces[agentIdx] {
					t0 := time.Now()
					var opErr error
					if op.isWrite {
						opErr = eng.WriteMutable(ctx, sc, ids[op.blk], initData[op.blk])
					} else {
						_, opErr = eng.ReadMutable(ctx, sc, ids[op.blk])
					}
					latencies[base+j] = time.Since(t0)
					if opErr != nil {
						firstErr.CompareAndSwap(nil, opErr)
						return
					}
				}
			}(a)
		}
		wg.Wait()
		elapsed := time.Since(startAll)
		cleanup()

		if v := firstErr.Load(); v != nil {
			return nil, v.(error)
		}

		m := eng.Metrics()
		timedReads := m.Reads - warm.Reads
		timedRAMHits := m.RAMHits - warm.RAMHits
		timedNVMeHits := m.NVMeHits - warm.NVMeHits
		timedS3Fetches := m.S3Fetches - warm.S3Fetches
		timedAdmissions := m.MirrorAdmissions - warm.MirrorAdmissions
		timedRejections := m.MirrorRejections - warm.MirrorRejections
		timedInstantDrops := m.MirrorInstantDrops - warm.MirrorInstantDrops
		timedInvalidations := m.MirrorInvalidations - warm.MirrorInvalidations
		timedNVMeReadIOPS := m.NVMeReadIOPS - warm.NVMeReadIOPS
		timedDemotions := m.NVMeDemotionWrites - warm.NVMeDemotionWrites
		timedWritebackIOPS := m.NVMeWritebackIOPS - warm.NVMeWritebackIOPS
		timedWritebackBytes := m.NVMeWritebackBytes - warm.NVMeWritebackBytes
		timedHitRatePct := 0.0
		if timedReads > 0 {
			timedHitRatePct = float64(timedRAMHits) / float64(timedReads) * 100.0
		}

		totalRAM := m.PeakRAMBytes + m.TrackerMetaBytes
		elapsedSec := elapsed.Seconds()
		if elapsedSec <= 0 {
			elapsedSec = 1e-9
		}
		results = append(results, runResult{
			Scenario:            "neg_write_heavy_invalidation_churn",
			Regime:              regime,
			AgentCount:          agents,
			TotalOps:            agents * opsPerAgent,
			ElapsedMS:           round4(float64(elapsed.Microseconds()) / 1000.0),
			P50US:               durUS(percentile(latencies, 0.50)),
			P95US:               durUS(percentile(latencies, 0.95)),
			P99US:               durUS(percentile(latencies, 0.99)),
			MeanUS:              durUS(meanDuration(latencies)),
			MaxUS:               durUS(percentile(latencies, 1.0)),
			TotalRAMBytes:       totalRAM,
			TotalRAMMiB:         round4(float64(totalRAM) / (1024 * 1024)),
			TrackerMetaBytes:    m.TrackerMetaBytes,
			TrackerMetaKiB:      round4(float64(m.TrackerMetaBytes) / 1024.0),
			RAMPerAgentBytes:    round4(m.RAMPerAgentBytes),
			RAMPerAgentMiB:      round4(m.RAMPerAgentBytes / (1024 * 1024)),
			RAMBudgetMiB:        round4(float64(ramBudget) / (1024 * 1024)),
			OverBudget:          m.OverBudget,
			RAMHitRatePct:       round4(timedHitRatePct),
			RAMHits:             timedRAMHits,
			NVMeHits:            timedNVMeHits,
			S3Fetches:           timedS3Fetches,
			MirrorAdmissions:    timedAdmissions,
			MirrorRejections:    timedRejections,
			MirrorInstantDrops:  timedInstantDrops,
			MirrorInvalidations: timedInvalidations,
			NVMeReadIOPS:        timedNVMeReadIOPS,
			NVMeDemotionWrites:  timedDemotions,
			NVMeWritebackIOPS:   timedWritebackIOPS,
			NVMeWritebackBytes:  timedWritebackBytes,
			AggregateReadMiBS:   round4((float64(timedReads) * float64(cfg.BlockSize) / (1024 * 1024)) / elapsedSec),
			OpsPerSec:           round4(float64(agents*opsPerAgent) / elapsedSec),
			BLAKE3VerifiedReads: timedReads,
			CorruptionRejects:   m.CorruptionRejects - warm.CorruptionRejects,
			IsolationRejects:    m.IsolationRejects - warm.IsolationRejects,
		})
	}
	return results, nil
}

func executePQSecurityCheck(ctx context.Context, cfg benchConfig, s3Be backend.Backend) (pqSecurityResult, error) {
	nvmeBe, cleanup, err := openTempFileBackend()
	if err != nil {
		return pqSecurityResult{}, err
	}
	defer cleanup()

	idA, err := age.GenerateHybridIdentity()
	if err != nil {
		return pqSecurityResult{}, err
	}
	idB, err := age.GenerateHybridIdentity()
	if err != nil {
		return pqSecurityResult{}, err
	}
	scopeA := most.Scope{TenantID: "bench-tenant-a", VolumeID: "vol-a"}
	scopeB := most.Scope{TenantID: "bench-tenant-b", VolumeID: "vol-b"}

	eng, err := most.NewEngine(most.Config{
		Regime:            most.RegimeMOSTSelectiveMirror,
		RAMBudgetBytes:    256 << 10,
		AllowedNamespaces: []string{"go-1-27-stdlib"},
		NVMe:              most.NVMeConfig{Store: nvmeBe, Channels: 2},
		Remote:            s3Be,
		VolumeCiphers: map[most.Scope]vfs.Cipher{
			scopeA: vfs.Hybrid(idA),
			scopeB: vfs.Hybrid(idB),
		},
	})
	if err != nil {
		return pqSecurityResult{}, err
	}

	const samples = 20
	sealLats := make([]time.Duration, samples)
	openLats := make([]time.Duration, samples)
	leaks := 0
	marker := []byte("PRIVATE_TENANT_CANARY_HIP1345")

	for i := 0; i < samples; i++ {
		pid, err := most.NewPrivateBlockID(scopeA, fmt.Sprintf("pq-chunk-%d", i))
		if err != nil {
			return pqSecurityResult{}, err
		}
		plain := make([]byte, cfg.BlockSize)
		copy(plain, marker)
		t0 := time.Now()
		if err := eng.PutPrivate(ctx, scopeA, pid, plain); err != nil {
			return pqSecurityResult{}, err
		}
		sealLats[i] = time.Since(t0)

		raw, _, err := nvmeBe.Get(ctx, pid.Key, backend.Unbounded)
		if err != nil {
			return pqSecurityResult{}, err
		}
		if bytes.Contains(raw, marker) {
			leaks++
		}

		t1 := time.Now()
		got, err := eng.ReadPrivate(ctx, scopeA, pid)
		openLats[i] = time.Since(t1)
		if err != nil || !bytes.Equal(got, plain) {
			return pqSecurityResult{}, fmt.Errorf("pq roundtrip failed: %v", err)
		}
	}

	pid0, _ := most.NewPrivateBlockID(scopeA, "pq-chunk-0")
	_, crossErr := eng.ReadPrivate(ctx, scopeB, pid0)

	return pqSecurityResult{
		CipherSuite:       "vfs.Hybrid (X25519 + ML-KEM-768)",
		Samples:           samples,
		BlockSizeBytes:    cfg.BlockSize,
		SealP50US:         durUS(percentile(sealLats, 0.50)),
		OpenP50US:         durUS(percentile(openLats, 0.50)),
		PlaintextLeaks:    leaks,
		CrossTenantDenied: crossErr != nil,
	}, nil
}

func writeResults(outDir string, rep *benchReport) error {
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	return os.WriteFile(filepath.Join(outDir, "results.json"), raw, 0o644)
}

func printSummary(rep *benchReport) {
	fmt.Printf("=== HIP-1345 MOST Benchmark Summary (seed=%d, corpus=%.2f MiB, RAM budget=%.2f MiB) ===\n",
		rep.Environment.Seed, rep.Corpus.TotalCorpusMiB, rep.Environment.RAMBudgetMiB)
	fmt.Printf("%-6s %-26s %10s %10s %10s %10s %10s %8s\n",
		"Agents", "Regime", "p50(us)", "p99(us)", "RAM/ag(MiB)", "Demotions", "WB IOPS", "Hit%")
	for _, r := range rep.ScalingSweep {
		fmt.Printf("%-6d %-26s %10.1f %10.1f %10.4f %10d %10d %7.2f%%\n",
			r.AgentCount, r.Regime, r.P50US, r.P99US, r.RAMPerAgentMiB,
			r.NVMeDemotionWrites, r.NVMeWritebackIOPS, r.RAMHitRatePct)
	}
	fmt.Println("\n=== Negative Regimes ===")
	keys := make([]string, 0, len(rep.NegativeRegimes))
	for k := range rep.NegativeRegimes {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	for _, k := range keys {
		for _, r := range rep.NegativeRegimes[k] {
			fmt.Printf("%-34s %-26s M=%-2d p99=%8.1fus RAM/ag=%.4fMiB WB_IOPS=%-5d Inv=%-4d Hit=%6.2f%%\n",
				k, r.Regime, r.AgentCount, r.P99US, r.RAMPerAgentMiB,
				r.NVMeWritebackIOPS, r.MirrorInvalidations, r.RAMHitRatePct)
		}
	}
}

func percentile(samples []time.Duration, p float64) time.Duration {
	if len(samples) == 0 {
		return 0
	}
	cp := make([]time.Duration, len(samples))
	copy(cp, samples)
	slices.Sort(cp)
	idx := int(math.Round(float64(len(cp)-1) * p))
	if idx < 0 {
		idx = 0
	}
	if idx >= len(cp) {
		idx = len(cp) - 1
	}
	return cp[idx]
}

func meanDuration(samples []time.Duration) time.Duration {
	if len(samples) == 0 {
		return 0
	}
	var sum int64
	for _, d := range samples {
		sum += int64(d)
	}
	return time.Duration(sum / int64(len(samples)))
}

func durUS(d time.Duration) float64 {
	return round4(float64(d.Nanoseconds()) / 1000.0)
}

func round4(v float64) float64 {
	return math.Round(v*10000) / 10000
}
