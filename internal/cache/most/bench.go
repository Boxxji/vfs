package most

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

// ScenarioSpec defines a reproducible multi-agent benchmark scenario.
type ScenarioSpec struct {
	Name                 string  `json:"name"`
	Description          string  `json:"description"`
	Kind                 string  `json:"kind"` // "primary" or "negative_boundary"
	NumAgents            int     `json:"num_agents"`
	BlockSize            int     `json:"block_size_bytes"`
	HostRAMBudget        int64   `json:"host_ram_budget_bytes"`
	PrivateShareFraction float64 `json:"private_share_fraction"`
	MirrorFraction       float64 `json:"mirror_fraction"`
	MinMirrorHits        uint8   `json:"min_mirror_hits"`
	SharedBlocks         int     `json:"shared_blocks"`
	PrivateBlocksPerAg   int     `json:"private_blocks_per_agent"`
	OpsPerAgent          int     `json:"ops_per_agent"`
	SharedHotFrac        float64 `json:"shared_hot_fraction"`  // Zipfian shared toolchain reads
	SharedScanFrac       float64 `json:"shared_scan_fraction"` // sequential one-hit cold toolchain scan reads
	PrivateFrac          float64 `json:"private_fraction"`     // per-tenant private workspace reads
	SharedZipfS          float64 `json:"shared_zipf_s"`        // <= 1.0 means uniform flat access
	PrivateZipfS         float64 `json:"private_zipf_s"`       // <= 1.0 means uniform private access
	ScanBurstLen         int     `json:"scan_burst_len"`       // > 0 emits contiguous cold scan bursts
	Seed                 uint64  `json:"seed"`
}

// IsolationAudit records the result of live cross-tenant and unattested probes.
type IsolationAudit struct {
	CrossTenantProbes int `json:"cross_tenant_probes"`
	CrossTenantDenied int `json:"cross_tenant_denied"`
	CrossTenantLeaks  int `json:"cross_tenant_leaks"`
	UnattestedProbes  int `json:"unattested_probes"`
	UnattestedDenied  int `json:"unattested_denied"`
	UnattestedLeaks   int `json:"unattested_leaks"`
	SharedRAMEntries  int `json:"shared_ram_entries"`
	SharedUnattested  int `json:"shared_unattested_entries"`
}

// Passed returns true iff every adversarial probe failed closed with zero leaks.
func (a IsolationAudit) Passed() bool {
	return a.CrossTenantLeaks == 0 &&
		a.UnattestedLeaks == 0 &&
		a.SharedUnattested == 0 &&
		a.CrossTenantDenied == a.CrossTenantProbes &&
		a.UnattestedDenied == a.UnattestedProbes
}

// PolicyResult holds complete measured latency, memory, hit-rate, NVMe
// writeback, throughput, and isolation metrics for a single policy run.
type PolicyResult struct {
	Scenario         string         `json:"scenario"`
	Policy           string         `json:"policy"`
	TotalOps         int            `json:"total_ops"`
	ElapsedMs        float64        `json:"elapsed_ms"`
	ThroughputOpsSec float64        `json:"throughput_ops_sec"`
	P50LatencyUs     float64        `json:"p50_latency_us"`
	P95LatencyUs     float64        `json:"p95_latency_us"`
	P99LatencyUs     float64        `json:"p99_latency_us"`
	MeanLatencyUs    float64        `json:"mean_latency_us"`
	Stats            Stats          `json:"stats"`
	Isolation        IsolationAudit `json:"isolation_audit"`
}

// ScenarioReport bundles all matched policy results for a scenario.
type ScenarioReport struct {
	Spec    ScenarioSpec   `json:"spec"`
	Results []PolicyResult `json:"results"`
}

// SuiteReport is the complete reproducible benchmark output written to JSON.
type SuiteReport struct {
	GeneratedAt string           `json:"generated_at"`
	GoVersion   string           `json:"go_version"`
	GOOS        string           `json:"goos"`
	GOARCH      string           `json:"goarch"`
	NumCPU      int              `json:"num_cpu"`
	Scenarios   []ScenarioReport `json:"scenarios"`
}

// AllPolicies returns the four matched comparison policies in canonical order.
func AllPolicies() []Policy {
	return []Policy{
		PolicyPrivatePerAgentLRU,
		PolicyExclusiveMigration,
		PolicyMOSTSelectiveMirror,
		PolicyMirrorAllNegative,
	}
}

// DefaultScenarios returns the primary multi-agent skewed toolchain burst plus
// the three documented honest negative and boundary regimes.
func DefaultScenarios() []ScenarioSpec {
	const block32K = 32 << 10
	return []ScenarioSpec{
		{
			Name:                 "skewed_multi_agent_toolchain_burst",
			Description:          "32 concurrent agents reading Zipfian-skewed immutable shared toolchains (70%), cold toolchain scan bursts (15%), and hot per-tenant private workspace blocks (15%) under a fixed 16 MiB host RAM budget (64 MiB NVMe corpus).",
			Kind:                 "primary",
			NumAgents:            32,
			BlockSize:            block32K,
			HostRAMBudget:        16 << 20,
			PrivateShareFraction: 0.50,
			MirrorFraction:       0.25,
			MinMirrorHits:        3,
			SharedBlocks:         1024,
			PrivateBlocksPerAg:   32,
			OpsPerAgent:          2000,
			SharedHotFrac:        0.70,
			SharedScanFrac:       0.15,
			PrivateFrac:          0.15,
			SharedZipfS:          1.28,
			PrivateZipfS:         1.35,
			ScanBurstLen:         24,
			Seed:                 134401,
		},
		{
			Name:                 "uniform_flat_access_no_hot_skew",
			Description:          "Negative/boundary regime: 32 agents issuing uniformly random shared toolchain reads (zero skew) across a 32 MiB corpus under an 8 MiB host RAM budget (4x working-set overflow). Selective mirroring cannot beat NVMe p99 latency, while keeping NVMe eviction writes at 0.00 MiB.",
			Kind:                 "negative_boundary",
			NumAgents:            32,
			BlockSize:            block32K,
			HostRAMBudget:        8 << 20,
			PrivateShareFraction: 0.25,
			MirrorFraction:       0.25,
			MinMirrorHits:        3,
			SharedBlocks:         1024,
			PrivateBlocksPerAg:   8,
			OpsPerAgent:          1200,
			SharedHotFrac:        0.95,
			SharedScanFrac:       0.00,
			PrivateFrac:          0.05,
			SharedZipfS:          0.0, // strictly uniform
			PrivateZipfS:         0.0,
			ScanBurstLen:         0,
			Seed:                 134402,
		},
		{
			Name:                 "mutable_private_workspace_heavy_workload",
			Description:          "Negative/boundary regime: 90% of reads hit per-tenant private mutable workspace blocks (strictly barred from host-shared mirroring by tenant isolation) and 10% hit shared toolchains. Resident RAM/agent approaches private-LRU baseline, proving isolation bounds deduplication.",
			Kind:                 "negative_boundary",
			NumAgents:            32,
			BlockSize:            block32K,
			HostRAMBudget:        16 << 20,
			PrivateShareFraction: 0.75,
			MirrorFraction:       0.125,
			MinMirrorHits:        3,
			SharedBlocks:         512,
			PrivateBlocksPerAg:   32,
			OpsPerAgent:          1500,
			SharedHotFrac:        0.10,
			SharedScanFrac:       0.00,
			PrivateFrac:          0.90,
			SharedZipfS:          1.25,
			PrivateZipfS:         1.25,
			ScanBurstLen:         0,
			Seed:                 134403,
		},
		{
			Name:                 "mirror_all_ram_thrash_collapse",
			Description:          "Negative control regime: 32 agents alternating hot Zipfian toolchain reads (55%) with contiguous 48-block cold toolchain scan bursts (40%) and private reads (5%) under an 8 MiB host RAM cap. Unselective mirroring thrashes RAM and degrades p99 latency; MOST filters one-hit scans.",
			Kind:                 "negative_boundary",
			NumAgents:            32,
			BlockSize:            block32K,
			HostRAMBudget:        8 << 20,
			PrivateShareFraction: 0.25,
			MirrorFraction:       0.50,
			MinMirrorHits:        3,
			SharedBlocks:         1024,
			PrivateBlocksPerAg:   8,
			OpsPerAgent:          1500,
			SharedHotFrac:        0.55,
			SharedScanFrac:       0.40,
			PrivateFrac:          0.05,
			SharedZipfS:          1.32,
			PrivateZipfS:         1.30,
			ScanBurstLen:         48,
			Seed:                 134404,
		},
	}
}

type workloadOp struct {
	isPrivate bool
	blockIdx  int
}

func generateTrace(spec ScenarioSpec) [][]workloadOp {
	traces := make([][]workloadOp, spec.NumAgents)
	for a := 0; a < spec.NumAgents; a++ {
		rng := rand.New(rand.NewPCG(spec.Seed+uint64(a)*7919, spec.Seed^uint64(a+1)*104729))
		var sharedZipf *rand.Zipf
		if spec.SharedZipfS > 1.0 && spec.SharedBlocks > 1 {
			sharedZipf = rand.NewZipf(rng, spec.SharedZipfS, 1.0, uint64(spec.SharedBlocks-1))
		}
		var privZipf *rand.Zipf
		if spec.PrivateZipfS > 1.0 && spec.PrivateBlocksPerAg > 1 {
			privZipf = rand.NewZipf(rng, spec.PrivateZipfS, 1.0, uint64(spec.PrivateBlocksPerAg-1))
		}

		ops := make([]workloadOp, spec.OpsPerAgent)
		scanStart := spec.SharedBlocks / 4
		if scanStart < 1 {
			scanStart = 0
		}
		scanSpan := spec.SharedBlocks - scanStart
		if scanSpan < 1 {
			scanSpan = spec.SharedBlocks
		}
		scanCursor := (a * 37) % scanSpan
		scanBurstRemaining := 0
		var burstTriggerProb float64
		if spec.ScanBurstLen > 0 && spec.SharedScanFrac > 0 {
			burstTriggerProb = spec.SharedScanFrac / float64(spec.ScanBurstLen)
		}

		for i := 0; i < spec.OpsPerAgent; i++ {
			if scanBurstRemaining > 0 {
				idx := scanStart + scanCursor
				scanCursor = (scanCursor + 1) % scanSpan
				ops[i] = workloadOp{isPrivate: false, blockIdx: idx}
				scanBurstRemaining--
				continue
			}

			if spec.ScanBurstLen > 0 && spec.SharedScanFrac > 0 && rng.Float64() < burstTriggerProb {
				scanBurstRemaining = spec.ScanBurstLen - 1
				idx := scanStart + scanCursor
				scanCursor = (scanCursor + 1) % scanSpan
				ops[i] = workloadOp{isPrivate: false, blockIdx: idx}
				continue
			}

			nonScanTot := spec.SharedHotFrac + spec.PrivateFrac
			u := rng.Float64()
			if spec.ScanBurstLen == 0 {
				switch {
				case u < spec.SharedHotFrac:
					var idx int
					if sharedZipf != nil {
						idx = int(sharedZipf.Uint64())
					} else {
						idx = rng.IntN(spec.SharedBlocks)
					}
					ops[i] = workloadOp{isPrivate: false, blockIdx: idx}
				case u < spec.SharedHotFrac+spec.SharedScanFrac:
					idx := scanStart + scanCursor
					scanCursor = (scanCursor + 1) % scanSpan
					ops[i] = workloadOp{isPrivate: false, blockIdx: idx}
				default:
					var idx int
					if privZipf != nil {
						idx = int(privZipf.Uint64())
					} else {
						idx = rng.IntN(spec.PrivateBlocksPerAg)
					}
					ops[i] = workloadOp{isPrivate: true, blockIdx: idx}
				}
			} else {
				if nonScanTot <= 0 || u*nonScanTot < spec.SharedHotFrac {
					var idx int
					if sharedZipf != nil {
						idx = int(sharedZipf.Uint64())
					} else {
						idx = rng.IntN(spec.SharedBlocks)
					}
					ops[i] = workloadOp{isPrivate: false, blockIdx: idx}
				} else {
					var idx int
					if privZipf != nil {
						idx = int(privZipf.Uint64())
					} else {
						idx = rng.IntN(spec.PrivateBlocksPerAg)
					}
					ops[i] = workloadOp{isPrivate: true, blockIdx: idx}
				}
			}
		}
		traces[a] = ops
	}
	return traces
}

func makeDeterministicBlock(size int, tag uint64, idx uint64) []byte {
	buf := make([]byte, size)
	x := tag*0x9e3779b97f4a7c15 ^ idx*0xbf58476d1ce4e5b9 ^ 0x243f6a8885a308d3
	for off := 0; off+8 <= size; off += 8 {
		x ^= x >> 12
		x ^= x << 25
		x ^= x >> 27
		binary.LittleEndian.PutUint64(buf[off:], x*2685821657736338717)
	}
	return buf
}

// RunScenarioPolicy executes spec under policy with real RAM pools, real
// os.File NVMe backing, concurrent agent goroutines, byte-exact integrity
// spot-checks, and a live tenant-isolation audit.
func RunScenarioPolicy(spec ScenarioSpec, policy Policy) (PolicyResult, error) {
	verify := true
	eng, err := NewEngine(Config{
		Policy:               policy,
		NumAgents:            spec.NumAgents,
		BlockSize:            spec.BlockSize,
		HostRAMBudget:        spec.HostRAMBudget,
		PrivateShareFraction: spec.PrivateShareFraction,
		MirrorFraction:       spec.MirrorFraction,
		MinMirrorHits:        spec.MinMirrorHits,
		Shards:               8,
		HashAlgo:             AlgoBLAKE3,
		VerifyNVMeReads:      &verify,
	})
	if err != nil {
		return PolicyResult{}, err
	}
	defer eng.Close()

	toolchains := []string{"python-3.12", "node-22", "rust-1.85", "go-1.27"}
	sharedKeys := make([]SharedToolchainKey, spec.SharedBlocks)
	sharedFirst8 := make([][8]byte, spec.SharedBlocks)
	for i := 0; i < spec.SharedBlocks; i++ {
		blk := makeDeterministicBlock(spec.BlockSize, 0x534841524544, uint64(i))
		copy(sharedFirst8[i][:], blk[:8])
		k, err := eng.RegisterSharedToolchain(toolchains[i%len(toolchains)], blk)
		if err != nil {
			return PolicyResult{}, fmt.Errorf("register shared %d: %w", i, err)
		}
		sharedKeys[i] = k
	}

	contexts := make([]AgentContext, spec.NumAgents)
	privKeys := make([][]PrivateBlockKey, spec.NumAgents)
	privFirst8 := make([][][8]byte, spec.NumAgents)
	for a := 0; a < spec.NumAgents; a++ {
		ctx := AgentContext{
			AgentID:  fmt.Sprintf("agent-%02d", a),
			TenantID: fmt.Sprintf("tenant-%02d", a),
			VolumeID: fmt.Sprintf("vol-%02d", a),
		}
		contexts[a] = ctx
		privKeys[a] = make([]PrivateBlockKey, spec.PrivateBlocksPerAg)
		privFirst8[a] = make([][8]byte, spec.PrivateBlocksPerAg)
		for b := 0; b < spec.PrivateBlocksPerAg; b++ {
			pk := PrivateBlockKey{
				TenantID: ctx.TenantID,
				VolumeID: ctx.VolumeID,
				BlockID:  uint64(b),
			}
			blk := makeDeterministicBlock(spec.BlockSize, uint64(a+1)*0x50524956, uint64(b))
			copy(privFirst8[a][b][:], blk[:8])
			if err := eng.RegisterPrivateBlock(ctx, pk, blk); err != nil {
				return PolicyResult{}, fmt.Errorf("register private a=%d b=%d: %w", a, b, err)
			}
			privKeys[a][b] = pk
		}
	}

	traces := generateTrace(spec)
	perAgentLat := make([][]int64, spec.NumAgents)
	for a := range perAgentLat {
		perAgentLat[a] = make([]int64, spec.OpsPerAgent)
	}

	var firstErr atomic.Pointer[error]
	var wg sync.WaitGroup
	startGate := make(chan struct{})

	for a := 0; a < spec.NumAgents; a++ {
		wg.Add(1)
		go func(agentIdx int) {
			defer wg.Done()
			ctx := contexts[agentIdx]
			ops := traces[agentIdx]
			lats := perAgentLat[agentIdx]
			buf := make([]byte, spec.BlockSize)
			<-startGate

			for i, op := range ops {
				t0 := time.Now()
				var err error
				if op.isPrivate {
					_, err = eng.ReadPrivate(ctx, privKeys[agentIdx][op.blockIdx], buf)
					if err == nil && !bytes.Equal(buf[:8], privFirst8[agentIdx][op.blockIdx][:]) {
						err = fmt.Errorf("private data corruption agent=%d block=%d", agentIdx, op.blockIdx)
					}
				} else {
					_, err = eng.ReadShared(ctx, sharedKeys[op.blockIdx], buf)
					if err == nil && !bytes.Equal(buf[:8], sharedFirst8[op.blockIdx][:]) {
						err = fmt.Errorf("shared data corruption block=%d", op.blockIdx)
					}
				}
				lats[i] = time.Since(t0).Nanoseconds()
				if err != nil {
					eCopy := err
					firstErr.CompareAndSwap(nil, &eCopy)
					return
				}
			}
		}(a)
	}

	runtime.GC()
	wallStart := time.Now()
	close(startGate)
	wg.Wait()
	elapsed := time.Since(wallStart)

	if ep := firstErr.Load(); ep != nil {
		return PolicyResult{}, *ep
	}

	// Capture workload stats before running adversarial isolation probes so
	// hit-rate and latency counters reflect pure workload execution.
	stats := eng.Stats()

	// Run live tenant-isolation and unattested-block audit probes.
	audit := runIsolationAudit(eng, contexts, privKeys, spec.BlockSize)
	stats.TenantDeniedCount = eng.Stats().TenantDeniedCount
	stats.UnattestedDeniedCount = eng.Stats().UnattestedDeniedCount

	totalOps := spec.NumAgents * spec.OpsPerAgent
	allLats := make([]int64, 0, totalOps)
	var sumNs int64
	for a := 0; a < spec.NumAgents; a++ {
		for _, ns := range perAgentLat[a] {
			allLats = append(allLats, ns)
			sumNs += ns
		}
	}
	slices.Sort(allLats)

	p50 := percentileNs(allLats, 0.50) / 1000.0
	p95 := percentileNs(allLats, 0.95) / 1000.0
	p99 := percentileNs(allLats, 0.99) / 1000.0
	mean := (float64(sumNs) / float64(len(allLats))) / 1000.0
	throughput := float64(totalOps) / elapsed.Seconds()

	return PolicyResult{
		Scenario:         spec.Name,
		Policy:           policy.String(),
		TotalOps:         totalOps,
		ElapsedMs:        float64(elapsed.Microseconds()) / 1000.0,
		ThroughputOpsSec: throughput,
		P50LatencyUs:     p50,
		P95LatencyUs:     p95,
		P99LatencyUs:     p99,
		MeanLatencyUs:    mean,
		Stats:            stats,
		Isolation:        audit,
	}, nil
}

func runIsolationAudit(eng *Engine, contexts []AgentContext, privKeys [][]PrivateBlockKey, blockSize int) IsolationAudit {
	var audit IsolationAudit
	buf := make([]byte, blockSize)
	n := len(contexts)
	if n >= 2 {
		for a := 0; a < n; a++ {
			victim := (a + 1) % n
			audit.CrossTenantProbes++
			_, err := eng.ReadPrivate(contexts[a], privKeys[victim][0], buf)
			if err == ErrTenantAccessDenied {
				audit.CrossTenantDenied++
			} else {
				audit.CrossTenantLeaks++
			}
		}
	}
	// Probe unattested shared keys (e.g., forged private digest attempted via ReadShared).
	for i := 0; i < 8; i++ {
		audit.UnattestedProbes++
		forged := SharedToolchainKey{
			ToolchainID: "forged-tenant-data",
			ContentHash: ComputeDigest(AlgoBLAKE3, []byte(fmt.Sprintf("forged-%d", i))),
		}
		_, err := eng.ReadShared(contexts[0], forged, buf)
		if err == ErrUnattestedBlock {
			audit.UnattestedDenied++
		} else {
			audit.UnattestedLeaks++
		}
	}
	audit.SharedRAMEntries, audit.SharedUnattested = eng.AuditSharedTierIntegrity()
	return audit
}

func percentileNs(sorted []int64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)-1) * p)
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return float64(sorted[idx])
}

// RunSuite runs all scenarios across all four matched policies.
func RunSuite(scenarios []ScenarioSpec) (SuiteReport, error) {
	report := SuiteReport{
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
		GoVersion:   runtime.Version(),
		GOOS:        runtime.GOOS,
		GOARCH:      runtime.GOARCH,
		NumCPU:      runtime.NumCPU(),
		Scenarios:   make([]ScenarioReport, 0, len(scenarios)),
	}
	policies := AllPolicies()
	for _, spec := range scenarios {
		sr := ScenarioReport{
			Spec:    spec,
			Results: make([]PolicyResult, 0, len(policies)),
		}
		for _, pol := range policies {
			res, err := RunScenarioPolicy(spec, pol)
			if err != nil {
				return SuiteReport{}, fmt.Errorf("scenario %s policy %s: %w", spec.Name, pol, err)
			}
			sr.Results = append(sr.Results, res)
		}
		report.Scenarios = append(report.Scenarios, sr)
	}
	return report, nil
}
