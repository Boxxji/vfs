package most

import (
	"container/list"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/hanzoai/vfs/internal/bufpool"
)

// Policy identifies one of the four matched two-tier cache policies evaluated
// under HIP-1344.
type Policy uint8

const (
	// PolicyPrivatePerAgentLRU gives every agent an isolated per-agent RAM LRU
	// bounded to HostRAMBudget / NumAgents. Hot immutable toolchain blocks are
	// duplicated up to NumAgents times across private caches.
	PolicyPrivatePerAgentLRU Policy = iota

	// PolicyExclusiveMigration implements ordinary exclusive RAM <-> NVMe
	// promotion/demotion migration: each block lives in either RAM or NVMe,
	// never both. Promoting from NVMe frees the NVMe slot; evicting from RAM
	// must write the victim back to NVMe (nvme.WriteEviction).
	PolicyExclusiveMigration

	// PolicyMOSTSelectiveMirror implements Mirror-Optimized Storage Tiering
	// (MOST) selective hot-data mirroring: immutable attested shared toolchain
	// blocks remain permanently resident on NVMe and only hot blocks passing a
	// frequency+recency admission filter are mirrored in host-shared RAM, so
	// RAM eviction is a zero-writeback O(1) drop with instant NVMe fallback.
	PolicyMOSTSelectiveMirror

	// PolicyMirrorAllNegative is a documented negative control that mirrors
	// every read (including cold, one-hit, and scan-burst blocks) into RAM
	// without selective frequency admission filtering.
	PolicyMirrorAllNegative
)

// String returns the canonical snake_case benchmark policy identifier.
func (p Policy) String() string {
	switch p {
	case PolicyPrivatePerAgentLRU:
		return "private_per_agent_lru_baseline"
	case PolicyExclusiveMigration:
		return "ordinary_tier_migration_exclusive"
	case PolicyMOSTSelectiveMirror:
		return "most_selective_hot_mirroring"
	case PolicyMirrorAllNegative:
		return "mirror_all_unselective_negative"
	default:
		return fmt.Sprintf("unknown_policy(%d)", p)
	}
}

// Tier identifies which storage tier served a read request.
type Tier uint8

const (
	TierRAM Tier = iota + 1
	TierNVMe
)

// String returns the tier label.
func (t Tier) String() string {
	switch t {
	case TierRAM:
		return "RAM"
	case TierNVMe:
		return "NVMe"
	default:
		return "unknown"
	}
}

// Config configures a two-tier RAM/NVMe cache Engine.
type Config struct {
	Policy Policy

	// NumAgents is the number of concurrent sandbox agents sharing the host.
	NumAgents int

	// BlockSize is the fixed block payload size in bytes (e.g., 32 KiB).
	BlockSize int

	// HostRAMBudget is the fixed total host RAM budget in bytes across all
	// shared and per-tenant caches.
	HostRAMBudget int64

	// PrivateShareFraction is the fraction of HostRAMBudget reserved for
	// isolated per-tenant private caches in host-shared policies. Defaults to 0.50.
	PrivateShareFraction float64

	// MirrorFraction is the maximum fraction of HostRAMBudget that
	// PolicyMOSTSelectiveMirror may occupy for the host-shared hot mirror pool.
	// Defaults to 0.25.
	MirrorFraction float64

	// MinMirrorHits is the minimum Count-Min Sketch frequency required before
	// PolicyMOSTSelectiveMirror mirrors a block into RAM. Defaults to 3.
	MinMirrorHits uint8

	// Shards is the number of lock shards for shared RAM tiers. Defaults to 8.
	Shards int

	// HashAlgo selects BLAKE3 (default) or SHA-256 for immutable toolchain
	// digest verification.
	HashAlgo HashAlgo

	// NVMeDir is the directory for real os.File NVMe backing files. If empty,
	// a dedicated temporary directory is created and removed on Close.
	NVMeDir string

	// SyncOnEvictWrite controls whether PolicyExclusiveMigration calls
	// os.File.Sync after eviction writebacks.
	SyncOnEvictWrite bool

	// VerifyNVMeReads verifies cryptographic content digests on every shared
	// toolchain read from NVMe before serving or mirroring. Defaults to true.
	VerifyNVMeReads *bool
}

func (c Config) normalized() (Config, error) {
	if c.NumAgents <= 0 {
		return c, fmt.Errorf("%w: NumAgents must be > 0", ErrInvalidConfig)
	}
	if c.BlockSize <= 0 {
		return c, fmt.Errorf("%w: BlockSize must be > 0", ErrInvalidConfig)
	}
	if c.HostRAMBudget < int64(c.BlockSize) {
		return c, fmt.Errorf("%w: HostRAMBudget must be >= BlockSize", ErrInvalidConfig)
	}
	if c.PrivateShareFraction <= 0 || c.PrivateShareFraction >= 1 {
		c.PrivateShareFraction = 0.50
	}
	if c.MirrorFraction <= 0 || c.MirrorFraction >= 1 {
		c.MirrorFraction = 0.25
	}
	if c.MinMirrorHits == 0 {
		c.MinMirrorHits = 3
	}
	if c.Shards <= 0 {
		c.Shards = 8
	}
	if c.VerifyNVMeReads == nil {
		v := true
		c.VerifyNVMeReads = &v
	}
	return c, nil
}

// ReadResult describes how a read was satisfied.
type ReadResult struct {
	Tier     Tier
	Admitted bool
}

// Stats captures point-in-time memory, hit-rate, eviction, NVMe I/O, and
// tenant-isolation counters for an Engine.
type Stats struct {
	Policy                 string  `json:"policy"`
	NumAgents              int     `json:"num_agents"`
	BlockSize              int     `json:"block_size_bytes"`
	HostRAMBudgetBytes     int64   `json:"host_ram_budget_bytes"`
	ResidentRAMBytes       int64   `json:"resident_ram_bytes"`
	PeakResidentRAMBytes   int64   `json:"peak_resident_ram_bytes"`
	SharedRAMBytes         int64   `json:"shared_ram_bytes"`
	PrivateRAMBytes        int64   `json:"private_ram_bytes"`
	ResidentRAMPerAgentMiB float64 `json:"resident_ram_per_agent_mib"`
	PeakRAMPerAgentMiB     float64 `json:"peak_ram_per_agent_mib"`
	TotalHostRAMMiB        float64 `json:"total_host_ram_mib"`

	TotalReads     uint64  `json:"total_reads"`
	RAMHits        uint64  `json:"ram_hits"`
	NVMeHits       uint64  `json:"nvme_hits"`
	RAMHitRatePct  float64 `json:"ram_hit_rate_pct"`
	SharedReads    uint64  `json:"shared_reads"`
	SharedRAMHits  uint64  `json:"shared_ram_hits"`
	SharedNVMeHits uint64  `json:"shared_nvme_hits"`
	PrivateReads   uint64  `json:"private_reads"`
	PrivateRAMHits uint64  `json:"private_ram_hits"`
	PrivateNVMeHit uint64  `json:"private_nvme_hits"`

	AdmissionsAccepted uint64 `json:"admissions_accepted"`
	AdmissionsRejected uint64 `json:"admissions_rejected"`
	RAMEvictions       uint64 `json:"ram_evictions"`

	NVMeReadOps           uint64  `json:"nvme_read_ops"`
	NVMeReadBytes         uint64  `json:"nvme_read_bytes"`
	NVMeWriteOps          uint64  `json:"nvme_write_ops"`
	NVMeWriteBytes        uint64  `json:"nvme_write_bytes"`
	NVMeEvictionWriteOps  uint64  `json:"nvme_eviction_write_ops"`
	NVMeEvictionWriteByte uint64  `json:"nvme_eviction_write_bytes"`
	NVMeEvictionWriteMiB  float64 `json:"nvme_eviction_write_mib"`

	TenantDeniedCount     uint64 `json:"tenant_denied_count"`
	UnattestedDeniedCount uint64 `json:"unattested_denied_count"`
	DigestVerifyOps       uint64 `json:"digest_verify_ops"`
	DigestMismatchCount   uint64 `json:"digest_mismatch_count"`
}

// Engine is a concurrent two-tier RAM/NVMe cache engine supporting four
// matched tiering policies with strict multi-tenant isolation.
type Engine struct {
	cfg      Config
	manifest *Manifest

	budget      *bufpool.Budget
	sharedPool  *bufpool.Pool // immutable shared toolchain blocks
	privatePool *bufpool.Pool // zeroing pool for isolated tenant-private blocks

	sharedNVMe  *NVMeStore
	privateNVMe *NVMeStore

	// Authoritative immutable NVMe slot map for non-exclusive policies
	// (PrivatePerAgentLRU, MOSTSelectiveMirror, MirrorAllNegative).
	sharedSlotMu sync.RWMutex
	sharedSlots  map[SharedToolchainKey]int64

	// Per-policy RAM tiers.
	agentLRUs       sync.Map // agentID string -> *agentLRU (PolicyPrivatePerAgentLRU)
	exclusiveShards []*exclusiveShard
	mirrorShards    []*mirrorShard

	// Strictly isolated per-tenant private caches (keyed by TenantID).
	// Private blocks NEVER enter exclusiveShards or mirrorShards.
	tenantCaches sync.Map // tenantID string -> *tenantPrivateCache

	// Per-tenant NVMe slot maps (strictly separated by TenantID).
	privateSlotMu sync.RWMutex
	privateSlots  map[PrivateBlockKey]int64

	// Live resident RAM tracking.
	sharedRAMBytes  atomic.Int64
	privateRAMBytes atomic.Int64
	peakRAMBytes    atomic.Int64

	// Operation counters.
	sharedRAMHits      atomic.Uint64
	sharedNVMeHits     atomic.Uint64
	privateRAMHits     atomic.Uint64
	privateNVMeHits    atomic.Uint64
	admissionsAccepted atomic.Uint64
	admissionsRejected atomic.Uint64
	ramEvictions       atomic.Uint64
	tenantDenied       atomic.Uint64
	unattestedDenied   atomic.Uint64
	digestVerifyOps    atomic.Uint64
	digestMismatches   atomic.Uint64

	closed atomic.Bool
}

// NewEngine creates a two-tier RAM/NVMe Engine using cfg.
func NewEngine(cfg Config) (*Engine, error) {
	cfg, err := cfg.normalized()
	if err != nil {
		return nil, err
	}
	sharedNVMe, err := NewNVMeStore(cfg.NVMeDir, "shared_toolchains.nvme", cfg.BlockSize, cfg.SyncOnEvictWrite)
	if err != nil {
		return nil, err
	}
	privateNVMe, err := NewNVMeStore(cfg.NVMeDir, "private_workspaces.nvme", cfg.BlockSize, cfg.SyncOnEvictWrite)
	if err != nil {
		_ = sharedNVMe.Close()
		return nil, err
	}

	budget := bufpool.NewBudget(cfg.HostRAMBudget, 0)
	e := &Engine{
		cfg:          cfg,
		manifest:     NewManifest(),
		budget:       budget,
		sharedPool:   budget.Pool(false),
		privatePool:  budget.Pool(true),
		sharedNVMe:   sharedNVMe,
		privateNVMe:  privateNVMe,
		sharedSlots:  make(map[SharedToolchainKey]int64),
		privateSlots: make(map[PrivateBlockKey]int64),
	}

	privateTotalCap := int64(float64(cfg.HostRAMBudget) * cfg.PrivateShareFraction)
	sharedCeiling := cfg.HostRAMBudget - privateTotalCap
	if sharedCeiling < int64(cfg.BlockSize) {
		sharedCeiling = int64(cfg.BlockSize)
	}

	switch cfg.Policy {
	case PolicyExclusiveMigration:
		e.exclusiveShards = make([]*exclusiveShard, cfg.Shards)
		perShard := shardBudget(sharedCeiling, cfg.Shards, cfg.BlockSize)
		for i := range e.exclusiveShards {
			e.exclusiveShards[i] = newExclusiveShard(perShard, cfg.BlockSize)
		}
	case PolicyMOSTSelectiveMirror:
		mirrorTotal := int64(float64(cfg.HostRAMBudget) * cfg.MirrorFraction)
		if mirrorTotal > sharedCeiling {
			mirrorTotal = sharedCeiling
		}
		if mirrorTotal < int64(cfg.BlockSize) {
			mirrorTotal = int64(cfg.BlockSize)
		}
		e.mirrorShards = make([]*mirrorShard, cfg.Shards)
		perShard := shardBudget(mirrorTotal, cfg.Shards, cfg.BlockSize)
		for i := range e.mirrorShards {
			e.mirrorShards[i] = newMirrorShard(perShard, cfg.BlockSize, true, cfg.MinMirrorHits)
		}
	case PolicyMirrorAllNegative:
		e.mirrorShards = make([]*mirrorShard, cfg.Shards)
		perShard := shardBudget(sharedCeiling, cfg.Shards, cfg.BlockSize)
		for i := range e.mirrorShards {
			e.mirrorShards[i] = newMirrorShard(perShard, cfg.BlockSize, false, 1)
		}
	}

	return e, nil
}

func shardBudget(total int64, shards int, blockSize int) int64 {
	bs := int64(blockSize)
	per := (total / int64(shards)) / bs * bs
	if per < bs {
		per = bs
	}
	return per
}

func (e *Engine) addSharedRAM(delta int64) {
	sh := e.sharedRAMBytes.Add(delta)
	tot := sh + e.privateRAMBytes.Load()
	e.updatePeak(tot)
}

func (e *Engine) addPrivateRAM(delta int64) {
	pr := e.privateRAMBytes.Add(delta)
	tot := e.sharedRAMBytes.Load() + pr
	e.updatePeak(tot)
}

func (e *Engine) updatePeak(tot int64) {
	for {
		cur := e.peakRAMBytes.Load()
		if tot <= cur {
			return
		}
		if e.peakRAMBytes.CompareAndSwap(cur, tot) {
			return
		}
	}
}

// RegisterSharedToolchain attests an immutable shared toolchain block against
// its cryptographic ContentHash and stores it on the shared NVMe tier.
// Blocks are initially cold on NVMe (0 RAM occupied until accessed).
func (e *Engine) RegisterSharedToolchain(toolchainID string, data []byte) (SharedToolchainKey, error) {
	if e.closed.Load() {
		return SharedToolchainKey{}, ErrClosed
	}
	if len(data) != e.cfg.BlockSize {
		return SharedToolchainKey{}, ErrBlockSizeMismatch
	}
	key := SharedToolchainKey{
		ToolchainID: toolchainID,
		ContentHash: ComputeDigest(e.cfg.HashAlgo, data),
	}
	if err := e.manifest.Attest(key, data, e.cfg.HashAlgo); err != nil {
		return SharedToolchainKey{}, err
	}
	slot, err := e.sharedNVMe.WriteNew(data)
	if err != nil {
		return SharedToolchainKey{}, err
	}

	if e.cfg.Policy == PolicyExclusiveMigration {
		sh := e.exclusiveShards[key.shardHash()%uint64(len(e.exclusiveShards))]
		sh.mu.Lock()
		sh.nvmeSlots[key] = slot
		sh.mu.Unlock()
	} else {
		e.sharedSlotMu.Lock()
		e.sharedSlots[key] = slot
		e.sharedSlotMu.Unlock()
	}
	return key, nil
}

// RegisterPrivateBlock writes a tenant-private mutable workspace block into
// the isolated private NVMe partition. Private blocks NEVER enter the shared
// attestation manifest or host-shared mirror tier.
func (e *Engine) RegisterPrivateBlock(ctx AgentContext, key PrivateBlockKey, data []byte) error {
	if e.closed.Load() {
		return ErrClosed
	}
	if err := authorizePrivate(ctx, key); err != nil {
		e.tenantDenied.Add(1)
		return err
	}
	if len(data) != e.cfg.BlockSize {
		return ErrBlockSizeMismatch
	}
	slot, err := e.privateNVMe.WriteNew(data)
	if err != nil {
		return err
	}
	if e.cfg.Policy == PolicyExclusiveMigration {
		tc := e.getOrCreateTenantCache(key.TenantID)
		tc.mu.Lock()
		tc.nvmeSlots[key] = slot
		tc.mu.Unlock()
	} else {
		e.privateSlotMu.Lock()
		e.privateSlots[key] = slot
		e.privateSlotMu.Unlock()
	}
	return nil
}

// ReadShared reads an immutable attested shared toolchain block into dst.
// Unattested keys are rejected immediately with ErrUnattestedBlock.
func (e *Engine) ReadShared(ctx AgentContext, key SharedToolchainKey, dst []byte) (ReadResult, error) {
	if e.closed.Load() {
		return ReadResult{}, ErrClosed
	}
	if len(dst) != e.cfg.BlockSize {
		return ReadResult{}, ErrBlockSizeMismatch
	}
	meta, ok := e.manifest.Lookup(key)
	if !ok {
		e.unattestedDenied.Add(1)
		return ReadResult{}, ErrUnattestedBlock
	}

	switch e.cfg.Policy {
	case PolicyPrivatePerAgentLRU:
		return e.readSharedPrivateLRU(ctx, key, meta, dst)
	case PolicyExclusiveMigration:
		return e.readSharedExclusive(key, meta, dst)
	case PolicyMOSTSelectiveMirror, PolicyMirrorAllNegative:
		return e.readSharedMirror(key, meta, dst)
	default:
		return ReadResult{}, ErrInvalidConfig
	}
}

// ReadPrivate reads a tenant-private workspace block into dst.
// The hard tenant-isolation gate executes FIRST before touching any cache map
// or NVMe slot, failing closed with ErrTenantAccessDenied on mismatch.
func (e *Engine) ReadPrivate(ctx AgentContext, key PrivateBlockKey, dst []byte) (ReadResult, error) {
	if e.closed.Load() {
		return ReadResult{}, ErrClosed
	}
	if err := authorizePrivate(ctx, key); err != nil {
		e.tenantDenied.Add(1)
		return ReadResult{}, err
	}
	if len(dst) != e.cfg.BlockSize {
		return ReadResult{}, ErrBlockSizeMismatch
	}

	switch e.cfg.Policy {
	case PolicyPrivatePerAgentLRU:
		return e.readPrivateAgentLRU(ctx, key, dst)
	case PolicyExclusiveMigration:
		return e.readPrivateExclusive(key, dst)
	case PolicyMOSTSelectiveMirror:
		return e.readPrivateTenantMirror(key, dst, true)
	case PolicyMirrorAllNegative:
		return e.readPrivateTenantMirror(key, dst, false)
	default:
		return ReadResult{}, ErrInvalidConfig
	}
}

func (e *Engine) verifySharedNVMeBytes(key SharedToolchainKey, meta AttestedMeta, data []byte) error {
	if !*e.cfg.VerifyNVMeReads {
		return nil
	}
	e.digestVerifyOps.Add(1)
	if !VerifyDigest(meta.Algo, data, key.ContentHash) {
		e.digestMismatches.Add(1)
		return ErrDigestMismatch
	}
	return nil
}

// Stats returns a complete point-in-time metrics snapshot.
func (e *Engine) Stats() Stats {
	sRAM := e.sharedRAMBytes.Load()
	pRAM := e.privateRAMBytes.Load()
	resRAM := sRAM + pRAM
	peakRAM := e.peakRAMBytes.Load()
	if resRAM > peakRAM {
		peakRAM = resRAM
	}

	sRH := e.sharedRAMHits.Load()
	sNH := e.sharedNVMeHits.Load()
	pRH := e.privateRAMHits.Load()
	pNH := e.privateNVMeHits.Load()
	ramHits := sRH + pRH
	nvmeHits := sNH + pNH
	totReads := ramHits + nvmeHits

	var hitPct float64
	if totReads > 0 {
		hitPct = 100.0 * float64(ramHits) / float64(totReads)
	}

	shNV := e.sharedNVMe.Stats()
	prNV := e.privateNVMe.Stats()
	evictWriteOps := shNV.EvictionWriteOps + prNV.EvictionWriteOps
	evictWriteBytes := shNV.EvictionWriteBytes + prNV.EvictionWriteBytes

	const mib = 1024.0 * 1024.0
	agents := float64(e.cfg.NumAgents)

	return Stats{
		Policy:                 e.cfg.Policy.String(),
		NumAgents:              e.cfg.NumAgents,
		BlockSize:              e.cfg.BlockSize,
		HostRAMBudgetBytes:     e.cfg.HostRAMBudget,
		ResidentRAMBytes:       resRAM,
		PeakResidentRAMBytes:   peakRAM,
		SharedRAMBytes:         sRAM,
		PrivateRAMBytes:        pRAM,
		ResidentRAMPerAgentMiB: float64(resRAM) / agents / mib,
		PeakRAMPerAgentMiB:     float64(peakRAM) / agents / mib,
		TotalHostRAMMiB:        float64(resRAM) / mib,

		TotalReads:     totReads,
		RAMHits:        ramHits,
		NVMeHits:       nvmeHits,
		RAMHitRatePct:  hitPct,
		SharedReads:    sRH + sNH,
		SharedRAMHits:  sRH,
		SharedNVMeHits: sNH,
		PrivateReads:   pRH + pNH,
		PrivateRAMHits: pRH,
		PrivateNVMeHit: pNH,

		AdmissionsAccepted: e.admissionsAccepted.Load(),
		AdmissionsRejected: e.admissionsRejected.Load(),
		RAMEvictions:       e.ramEvictions.Load(),

		NVMeReadOps:           shNV.ReadOps + prNV.ReadOps,
		NVMeReadBytes:         shNV.ReadBytes + prNV.ReadBytes,
		NVMeWriteOps:          shNV.WriteOps + prNV.WriteOps,
		NVMeWriteBytes:        shNV.WriteBytes + prNV.WriteBytes,
		NVMeEvictionWriteOps:  evictWriteOps,
		NVMeEvictionWriteByte: evictWriteBytes,
		NVMeEvictionWriteMiB:  float64(evictWriteBytes) / mib,

		TenantDeniedCount:     e.tenantDenied.Load(),
		UnattestedDeniedCount: e.unattestedDenied.Load(),
		DigestVerifyOps:       e.digestVerifyOps.Load(),
		DigestMismatchCount:   e.digestMismatches.Load(),
	}
}

// SharedRAMHasPrivateBlocks is an audit invariant check returning false by
// construction because shared tiers only accept SharedToolchainKey. It also
// verifies that every key resident in any shared RAM shard exists in the
// immutable Manifest.
func (e *Engine) AuditSharedTierIntegrity() (sharedEntries int, unattested int) {
	for _, sh := range e.mirrorShards {
		sh.mu.Lock()
		for k := range sh.ram {
			sharedEntries++
			if _, ok := e.manifest.Lookup(k); !ok {
				unattested++
			}
		}
		sh.mu.Unlock()
	}
	for _, sh := range e.exclusiveShards {
		sh.mu.Lock()
		for k := range sh.ram {
			sharedEntries++
			if _, ok := e.manifest.Lookup(k); !ok {
				unattested++
			}
		}
		sh.mu.Unlock()
	}
	return sharedEntries, unattested
}

// Close releases all pooled RAM buffers and closes/removes NVMe backing files.
func (e *Engine) Close() error {
	if !e.closed.CompareAndSwap(false, true) {
		return nil
	}
	for _, sh := range e.mirrorShards {
		sh.releaseAll()
	}
	for _, sh := range e.exclusiveShards {
		sh.releaseAll()
	}
	e.agentLRUs.Range(func(_, v any) bool {
		v.(*agentLRU).releaseAll()
		return true
	})
	e.tenantCaches.Range(func(_, v any) bool {
		v.(*tenantPrivateCache).releaseAll()
		return true
	})
	e.sharedPool.Close()
	e.privatePool.Close()
	err1 := e.sharedNVMe.Close()
	err2 := e.privateNVMe.Close()
	if err1 != nil {
		return err1
	}
	return err2
}

// ---------------------------------------------------------------------------
// Policy 1: private_per_agent_lru_baseline
// ---------------------------------------------------------------------------

type agentCacheKey struct {
	isPrivate bool
	shared    SharedToolchainKey
	priv      PrivateBlockKey
}

type agentLRUEntry struct {
	key agentCacheKey
	buf *bufpool.Shared
}

type agentLRU struct {
	mu        sync.Mutex
	maxBytes  int64
	usedBytes int64
	ll        *list.List
	m         map[agentCacheKey]*list.Element
}

func newAgentLRU(maxBytes int64) *agentLRU {
	return &agentLRU{
		maxBytes: maxBytes,
		ll:       list.New(),
		m:        make(map[agentCacheKey]*list.Element),
	}
}

func (a *agentLRU) releaseAll() {
	a.mu.Lock()
	defer a.mu.Unlock()
	for el := a.ll.Front(); el != nil; el = el.Next() {
		el.Value.(*agentLRUEntry).buf.Release()
	}
	a.ll.Init()
	clear(a.m)
	a.usedBytes = 0
}

func (e *Engine) getOrCreateAgentLRU(agentID string) *agentLRU {
	if v, ok := e.agentLRUs.Load(agentID); ok {
		return v.(*agentLRU)
	}
	bs := int64(e.cfg.BlockSize)
	perAgent := (e.cfg.HostRAMBudget / int64(e.cfg.NumAgents)) / bs * bs
	if perAgent < bs {
		perAgent = bs
	}
	created := newAgentLRU(perAgent)
	actual, _ := e.agentLRUs.LoadOrStore(agentID, created)
	return actual.(*agentLRU)
}

func (e *Engine) readSharedPrivateLRU(ctx AgentContext, key SharedToolchainKey, meta AttestedMeta, dst []byte) (ReadResult, error) {
	agentID := ctx.AgentID
	if agentID == "" {
		agentID = ctx.TenantID
	}
	lru := e.getOrCreateAgentLRU(agentID)
	ck := agentCacheKey{isPrivate: false, shared: key}

	lru.mu.Lock()
	if el, ok := lru.m[ck]; ok {
		lru.ll.MoveToFront(el)
		sh := el.Value.(*agentLRUEntry).buf
		data, pinned := sh.Pin()
		lru.mu.Unlock()
		if pinned {
			copy(dst, data)
			sh.Unpin()
			e.sharedRAMHits.Add(1)
			return ReadResult{Tier: TierRAM}, nil
		}
	} else {
		lru.mu.Unlock()
	}

	e.sharedSlotMu.RLock()
	slot, ok := e.sharedSlots[key]
	e.sharedSlotMu.RUnlock()
	if !ok {
		return ReadResult{}, ErrBlockNotFound
	}
	if err := e.sharedNVMe.ReadSlot(slot, dst); err != nil {
		return ReadResult{}, err
	}
	if err := e.verifySharedNVMeBytes(key, meta, dst); err != nil {
		return ReadResult{}, err
	}
	e.sharedNVMeHits.Add(1)

	bs := int64(e.cfg.BlockSize)
	buf := e.sharedPool.Get(e.cfg.BlockSize)
	copy(buf, dst)
	shBuf := e.sharedPool.Share(buf)

	lru.mu.Lock()
	if el, exists := lru.m[ck]; exists {
		lru.ll.MoveToFront(el)
		lru.mu.Unlock()
		shBuf.Release()
		return ReadResult{Tier: TierNVMe, Admitted: false}, nil
	}
	var evictedBufs []*bufpool.Shared
	var evictedDelta int64
	for lru.usedBytes+bs > lru.maxBytes && lru.ll.Len() > 0 {
		back := lru.ll.Back()
		ent := back.Value.(*agentLRUEntry)
		lru.ll.Remove(back)
		delete(lru.m, ent.key)
		lru.usedBytes -= bs
		evictedDelta -= bs
		evictedBufs = append(evictedBufs, ent.buf)
	}
	lru.m[ck] = lru.ll.PushFront(&agentLRUEntry{key: ck, buf: shBuf})
	lru.usedBytes += bs
	lru.mu.Unlock()

	for _, eb := range evictedBufs {
		eb.Release()
	}
	e.addPrivateRAM(evictedDelta + bs)
	if len(evictedBufs) > 0 {
		e.ramEvictions.Add(uint64(len(evictedBufs)))
	}
	e.admissionsAccepted.Add(1)
	return ReadResult{Tier: TierNVMe, Admitted: true}, nil
}

func (e *Engine) readPrivateAgentLRU(ctx AgentContext, key PrivateBlockKey, dst []byte) (ReadResult, error) {
	agentID := ctx.AgentID
	if agentID == "" {
		agentID = ctx.TenantID
	}
	lru := e.getOrCreateAgentLRU(agentID)
	ck := agentCacheKey{isPrivate: true, priv: key}

	lru.mu.Lock()
	if el, ok := lru.m[ck]; ok {
		lru.ll.MoveToFront(el)
		sh := el.Value.(*agentLRUEntry).buf
		data, pinned := sh.Pin()
		lru.mu.Unlock()
		if pinned {
			copy(dst, data)
			sh.Unpin()
			e.privateRAMHits.Add(1)
			return ReadResult{Tier: TierRAM}, nil
		}
	} else {
		lru.mu.Unlock()
	}

	e.privateSlotMu.RLock()
	slot, ok := e.privateSlots[key]
	e.privateSlotMu.RUnlock()
	if !ok {
		return ReadResult{}, ErrBlockNotFound
	}
	if err := e.privateNVMe.ReadSlot(slot, dst); err != nil {
		return ReadResult{}, err
	}
	e.privateNVMeHits.Add(1)

	bs := int64(e.cfg.BlockSize)
	buf := e.privatePool.Get(e.cfg.BlockSize)
	copy(buf, dst)
	shBuf := e.privatePool.Share(buf)

	lru.mu.Lock()
	if el, exists := lru.m[ck]; exists {
		lru.ll.MoveToFront(el)
		lru.mu.Unlock()
		shBuf.Release()
		return ReadResult{Tier: TierNVMe, Admitted: false}, nil
	}
	var evictedBufs []*bufpool.Shared
	var evictedDelta int64
	for lru.usedBytes+bs > lru.maxBytes && lru.ll.Len() > 0 {
		back := lru.ll.Back()
		ent := back.Value.(*agentLRUEntry)
		lru.ll.Remove(back)
		delete(lru.m, ent.key)
		lru.usedBytes -= bs
		evictedDelta -= bs
		evictedBufs = append(evictedBufs, ent.buf)
	}
	lru.m[ck] = lru.ll.PushFront(&agentLRUEntry{key: ck, buf: shBuf})
	lru.usedBytes += bs
	lru.mu.Unlock()

	for _, eb := range evictedBufs {
		eb.Release()
	}
	e.addPrivateRAM(evictedDelta + bs)
	if len(evictedBufs) > 0 {
		e.ramEvictions.Add(uint64(len(evictedBufs)))
	}
	e.admissionsAccepted.Add(1)
	return ReadResult{Tier: TierNVMe, Admitted: true}, nil
}

// ---------------------------------------------------------------------------
// Policy 2: ordinary_tier_migration_exclusive
// ---------------------------------------------------------------------------

type exclusiveEntry struct {
	key SharedToolchainKey
	buf *bufpool.Shared
}

type exclusiveShard struct {
	mu        sync.Mutex
	maxBytes  int64
	usedBytes int64
	blockSize int64
	ll        *list.List
	ram       map[SharedToolchainKey]*list.Element
	nvmeSlots map[SharedToolchainKey]int64
	demoting  map[SharedToolchainKey]*bufpool.Shared
	inFlight  map[SharedToolchainKey]chan struct{}
}

func newExclusiveShard(maxBytes int64, blockSize int) *exclusiveShard {
	return &exclusiveShard{
		maxBytes:  maxBytes,
		blockSize: int64(blockSize),
		ll:        list.New(),
		ram:       make(map[SharedToolchainKey]*list.Element),
		nvmeSlots: make(map[SharedToolchainKey]int64),
		demoting:  make(map[SharedToolchainKey]*bufpool.Shared),
		inFlight:  make(map[SharedToolchainKey]chan struct{}),
	}
}

func (sh *exclusiveShard) releaseAll() {
	sh.mu.Lock()
	defer sh.mu.Unlock()
	for el := sh.ll.Front(); el != nil; el = el.Next() {
		el.Value.(*exclusiveEntry).buf.Release()
	}
	sh.ll.Init()
	clear(sh.ram)
	sh.usedBytes = 0
}

func (e *Engine) readSharedExclusive(key SharedToolchainKey, meta AttestedMeta, dst []byte) (ReadResult, error) {
	sh := e.exclusiveShards[key.shardHash()%uint64(len(e.exclusiveShards))]
	bs := int64(e.cfg.BlockSize)

	for {
		sh.mu.Lock()
		if el, ok := sh.ram[key]; ok {
			sh.ll.MoveToFront(el)
			sb := el.Value.(*exclusiveEntry).buf
			data, pinned := sb.Pin()
			sh.mu.Unlock()
			if pinned {
				copy(dst, data)
				sb.Unpin()
				e.sharedRAMHits.Add(1)
				return ReadResult{Tier: TierRAM}, nil
			}
			continue
		}
		if sb, ok := sh.demoting[key]; ok {
			data, pinned := sb.Pin()
			sh.mu.Unlock()
			if pinned {
				copy(dst, data)
				sb.Unpin()
				e.sharedRAMHits.Add(1)
				return ReadResult{Tier: TierRAM}, nil
			}
			continue
		}
		if ch, busy := sh.inFlight[key]; busy {
			sh.mu.Unlock()
			<-ch
			continue
		}
		slot, ok := sh.nvmeSlots[key]
		if !ok || slot < 0 {
			sh.mu.Unlock()
			return ReadResult{}, ErrBlockNotFound
		}
		done := make(chan struct{})
		sh.inFlight[key] = done
		sh.mu.Unlock()

		// Read block from NVMe outside lock.
		buf := e.sharedPool.Get(e.cfg.BlockSize)
		readErr := e.sharedNVMe.ReadSlot(slot, buf)
		if readErr == nil {
			readErr = e.verifySharedNVMeBytes(key, meta, buf)
		}
		if readErr != nil {
			e.sharedPool.Put(buf)
			sh.mu.Lock()
			delete(sh.inFlight, key)
			close(done)
			sh.mu.Unlock()
			return ReadResult{}, readErr
		}
		copy(dst, buf)
		e.sharedNVMeHits.Add(1)

		// Exclusive promotion: if RAM shard is full, evict LRU victim and
		// demote it to NVMe via real WriteEviction before admitting key.
		shBuf := e.sharedPool.Share(buf)
		sh.mu.Lock()
		for sh.usedBytes+bs > sh.maxBytes && sh.ll.Len() > 0 {
			back := sh.ll.Back()
			victim := back.Value.(*exclusiveEntry)
			sh.ll.Remove(back)
			delete(sh.ram, victim.key)
			sh.usedBytes -= bs
			e.addSharedRAM(-bs)
			e.ramEvictions.Add(1)

			vData, pinned := victim.buf.Pin()
			sh.demoting[victim.key] = victim.buf
			sh.mu.Unlock()

			var vSlot int64 = -1
			var wErr error
			if pinned {
				vSlot, wErr = e.sharedNVMe.WriteEviction(vData)
				victim.buf.Unpin()
			}

			sh.mu.Lock()
			delete(sh.demoting, victim.key)
			if wErr == nil && vSlot >= 0 {
				sh.nvmeSlots[victim.key] = vSlot
			}
			victim.buf.Release()
		}

		// Exclusive tier invariant: block now lives ONLY in RAM; free NVMe slot.
		delete(sh.nvmeSlots, key)
		sh.ram[key] = sh.ll.PushFront(&exclusiveEntry{key: key, buf: shBuf})
		sh.usedBytes += bs
		delete(sh.inFlight, key)
		close(done)
		sh.mu.Unlock()

		e.sharedNVMe.FreeSlot(slot)
		e.addSharedRAM(bs)
		e.admissionsAccepted.Add(1)
		return ReadResult{Tier: TierNVMe, Admitted: true}, nil
	}
}

// ---------------------------------------------------------------------------
// Policy 3 & 4: most_selective_hot_mirroring & mirror_all_unselective_negative
// ---------------------------------------------------------------------------

type mirrorEntry struct {
	key  SharedToolchainKey
	hash uint64
	buf  *bufpool.Shared
}

type mirrorShard struct {
	mu            sync.Mutex
	maxBytes      int64
	usedBytes     int64
	blockSize     int64
	selective     bool
	minMirrorHits uint8
	sketch        *freqSketch
	ll            *list.List
	ram           map[SharedToolchainKey]*list.Element
}

func newMirrorShard(maxBytes int64, blockSize int, selective bool, minHits uint8) *mirrorShard {
	slots := int(maxBytes / int64(blockSize))
	if slots < 16 {
		slots = 16
	}
	var sk *freqSketch
	if selective {
		sk = newFreqSketch(slots * 8)
	}
	return &mirrorShard{
		maxBytes:      maxBytes,
		blockSize:     int64(blockSize),
		selective:     selective,
		minMirrorHits: minHits,
		sketch:        sk,
		ll:            list.New(),
		ram:           make(map[SharedToolchainKey]*list.Element),
	}
}

func (sh *mirrorShard) releaseAll() {
	sh.mu.Lock()
	defer sh.mu.Unlock()
	for el := sh.ll.Front(); el != nil; el = el.Next() {
		el.Value.(*mirrorEntry).buf.Release()
	}
	sh.ll.Init()
	clear(sh.ram)
	sh.usedBytes = 0
}

func (e *Engine) readSharedMirror(key SharedToolchainKey, meta AttestedMeta, dst []byte) (ReadResult, error) {
	h := key.shardHash()
	sh := e.mirrorShards[h%uint64(len(e.mirrorShards))]
	bs := int64(e.cfg.BlockSize)

	sh.mu.Lock()
	if el, ok := sh.ram[key]; ok {
		sh.ll.MoveToFront(el)
		if sh.selective {
			sh.sketch.Increment(h)
		}
		sb := el.Value.(*mirrorEntry).buf
		data, pinned := sb.Pin()
		sh.mu.Unlock()
		if pinned {
			copy(dst, data)
			sb.Unpin()
			e.sharedRAMHits.Add(1)
			return ReadResult{Tier: TierRAM}, nil
		}
	} else {
		sh.mu.Unlock()
	}

	// Authoritative NVMe copy is always resident; read directly outside lock.
	e.sharedSlotMu.RLock()
	slot, ok := e.sharedSlots[key]
	e.sharedSlotMu.RUnlock()
	if !ok {
		return ReadResult{}, ErrBlockNotFound
	}
	if err := e.sharedNVMe.ReadSlot(slot, dst); err != nil {
		return ReadResult{}, err
	}
	if err := e.verifySharedNVMeBytes(key, meta, dst); err != nil {
		return ReadResult{}, err
	}
	e.sharedNVMeHits.Add(1)

	// Selective hotness admission check (MOST) vs blind mirror-all (negative).
	sh.mu.Lock()
	if el, exists := sh.ram[key]; exists {
		sh.ll.MoveToFront(el)
		if sh.selective {
			sh.sketch.Increment(h)
		}
		sh.mu.Unlock()
		return ReadResult{Tier: TierNVMe, Admitted: false}, nil
	}

	if sh.selective {
		freq := sh.sketch.Increment(h)
		if freq < sh.minMirrorHits {
			sh.mu.Unlock()
			e.admissionsRejected.Add(1)
			return ReadResult{Tier: TierNVMe, Admitted: false}, nil
		}
		if sh.usedBytes+bs > sh.maxBytes && sh.ll.Len() > 0 {
			victim := sh.ll.Back().Value.(*mirrorEntry)
			vFreq := sh.sketch.Estimate(victim.hash)
			if freq <= vFreq {
				sh.mu.Unlock()
				e.admissionsRejected.Add(1)
				return ReadResult{Tier: TierNVMe, Admitted: false}, nil
			}
		}
	}

	// Admit into RAM mirror. Any eviction is a zero-writeback O(1) drop because
	// the authoritative immutable copy stays permanently resident on NVMe.
	var evictedBufs []*bufpool.Shared
	var deltaRAM int64
	for sh.usedBytes+bs > sh.maxBytes && sh.ll.Len() > 0 {
		back := sh.ll.Back()
		victim := back.Value.(*mirrorEntry)
		sh.ll.Remove(back)
		delete(sh.ram, victim.key)
		sh.usedBytes -= bs
		deltaRAM -= bs
		evictedBufs = append(evictedBufs, victim.buf)
	}

	buf := e.sharedPool.Get(e.cfg.BlockSize)
	copy(buf, dst)
	shBuf := e.sharedPool.Share(buf)
	sh.ram[key] = sh.ll.PushFront(&mirrorEntry{key: key, hash: h, buf: shBuf})
	sh.usedBytes += bs
	deltaRAM += bs
	sh.mu.Unlock()

	for _, eb := range evictedBufs {
		eb.Release()
	}
	if deltaRAM != 0 {
		e.addSharedRAM(deltaRAM)
	}
	if len(evictedBufs) > 0 {
		e.ramEvictions.Add(uint64(len(evictedBufs)))
	}
	e.admissionsAccepted.Add(1)
	return ReadResult{Tier: TierNVMe, Admitted: true}, nil
}

// ---------------------------------------------------------------------------
// Strictly Isolated Per-Tenant Private Workspace Caches
// ---------------------------------------------------------------------------

type privateRAMEntry struct {
	key  PrivateBlockKey
	hash uint64
	buf  *bufpool.Shared
}

type tenantPrivateCache struct {
	mu            sync.Mutex
	maxBytes      int64
	usedBytes     int64
	minMirrorHits uint8
	sketch        *freqSketch
	ll            *list.List
	ram           map[PrivateBlockKey]*list.Element

	// Exclusive-mode per-tenant state (used only by PolicyExclusiveMigration).
	nvmeSlots map[PrivateBlockKey]int64
	demoting  map[PrivateBlockKey]*bufpool.Shared
	inFlight  map[PrivateBlockKey]chan struct{}
}

func newTenantPrivateCache(maxBytes int64, blockSize int, minHits uint8) *tenantPrivateCache {
	slots := int(maxBytes / int64(blockSize))
	if slots < 8 {
		slots = 8
	}
	return &tenantPrivateCache{
		maxBytes:      maxBytes,
		minMirrorHits: minHits,
		sketch:        newFreqSketch(slots * 8),
		ll:            list.New(),
		ram:           make(map[PrivateBlockKey]*list.Element),
		nvmeSlots:     make(map[PrivateBlockKey]int64),
		demoting:      make(map[PrivateBlockKey]*bufpool.Shared),
		inFlight:      make(map[PrivateBlockKey]chan struct{}),
	}
}

func (tc *tenantPrivateCache) releaseAll() {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	for el := tc.ll.Front(); el != nil; el = el.Next() {
		el.Value.(*privateRAMEntry).buf.Release()
	}
	tc.ll.Init()
	clear(tc.ram)
	tc.usedBytes = 0
}

func (e *Engine) getOrCreateTenantCache(tenantID string) *tenantPrivateCache {
	if v, ok := e.tenantCaches.Load(tenantID); ok {
		return v.(*tenantPrivateCache)
	}
	bs := int64(e.cfg.BlockSize)
	privateTotal := int64(float64(e.cfg.HostRAMBudget) * e.cfg.PrivateShareFraction)
	perTenant := (privateTotal / int64(e.cfg.NumAgents)) / bs * bs
	if perTenant < bs {
		perTenant = bs
	}
	minHits := e.cfg.MinMirrorHits
	if minHits > 2 {
		minHits = 2
	}
	created := newTenantPrivateCache(perTenant, e.cfg.BlockSize, minHits)
	actual, _ := e.tenantCaches.LoadOrStore(tenantID, created)
	return actual.(*tenantPrivateCache)
}

func (e *Engine) readPrivateExclusive(key PrivateBlockKey, dst []byte) (ReadResult, error) {
	tc := e.getOrCreateTenantCache(key.TenantID)
	bs := int64(e.cfg.BlockSize)

	for {
		tc.mu.Lock()
		if el, ok := tc.ram[key]; ok {
			tc.ll.MoveToFront(el)
			sb := el.Value.(*privateRAMEntry).buf
			data, pinned := sb.Pin()
			tc.mu.Unlock()
			if pinned {
				copy(dst, data)
				sb.Unpin()
				e.privateRAMHits.Add(1)
				return ReadResult{Tier: TierRAM}, nil
			}
			continue
		}
		if sb, ok := tc.demoting[key]; ok {
			data, pinned := sb.Pin()
			tc.mu.Unlock()
			if pinned {
				copy(dst, data)
				sb.Unpin()
				e.privateRAMHits.Add(1)
				return ReadResult{Tier: TierRAM}, nil
			}
			continue
		}
		if ch, busy := tc.inFlight[key]; busy {
			tc.mu.Unlock()
			<-ch
			continue
		}
		slot, ok := tc.nvmeSlots[key]
		if !ok || slot < 0 {
			tc.mu.Unlock()
			return ReadResult{}, ErrBlockNotFound
		}
		done := make(chan struct{})
		tc.inFlight[key] = done
		tc.mu.Unlock()

		buf := e.privatePool.Get(e.cfg.BlockSize)
		if err := e.privateNVMe.ReadSlot(slot, buf); err != nil {
			e.privatePool.Put(buf)
			tc.mu.Lock()
			delete(tc.inFlight, key)
			close(done)
			tc.mu.Unlock()
			return ReadResult{}, err
		}
		copy(dst, buf)
		e.privateNVMeHits.Add(1)

		shBuf := e.privatePool.Share(buf)
		tc.mu.Lock()
		for tc.usedBytes+bs > tc.maxBytes && tc.ll.Len() > 0 {
			back := tc.ll.Back()
			victim := back.Value.(*privateRAMEntry)
			tc.ll.Remove(back)
			delete(tc.ram, victim.key)
			tc.usedBytes -= bs
			e.addPrivateRAM(-bs)
			e.ramEvictions.Add(1)

			vData, pinned := victim.buf.Pin()
			tc.demoting[victim.key] = victim.buf
			tc.mu.Unlock()

			var vSlot int64 = -1
			var wErr error
			if pinned {
				vSlot, wErr = e.privateNVMe.WriteEviction(vData)
				victim.buf.Unpin()
			}

			tc.mu.Lock()
			delete(tc.demoting, victim.key)
			if wErr == nil && vSlot >= 0 {
				tc.nvmeSlots[victim.key] = vSlot
			}
			victim.buf.Release()
		}

		delete(tc.nvmeSlots, key)
		tc.ram[key] = tc.ll.PushFront(&privateRAMEntry{key: key, hash: key.sketchHash(), buf: shBuf})
		tc.usedBytes += bs
		delete(tc.inFlight, key)
		close(done)
		tc.mu.Unlock()

		e.privateNVMe.FreeSlot(slot)
		e.addPrivateRAM(bs)
		e.admissionsAccepted.Add(1)
		return ReadResult{Tier: TierNVMe, Admitted: true}, nil
	}
}

func (e *Engine) readPrivateTenantMirror(key PrivateBlockKey, dst []byte, selective bool) (ReadResult, error) {
	tc := e.getOrCreateTenantCache(key.TenantID)
	bs := int64(e.cfg.BlockSize)
	h := key.sketchHash()

	tc.mu.Lock()
	if el, ok := tc.ram[key]; ok {
		tc.ll.MoveToFront(el)
		if selective {
			tc.sketch.Increment(h)
		}
		sb := el.Value.(*privateRAMEntry).buf
		data, pinned := sb.Pin()
		tc.mu.Unlock()
		if pinned {
			copy(dst, data)
			sb.Unpin()
			e.privateRAMHits.Add(1)
			return ReadResult{Tier: TierRAM}, nil
		}
	} else {
		tc.mu.Unlock()
	}

	e.privateSlotMu.RLock()
	slot, ok := e.privateSlots[key]
	e.privateSlotMu.RUnlock()
	if !ok {
		return ReadResult{}, ErrBlockNotFound
	}
	if err := e.privateNVMe.ReadSlot(slot, dst); err != nil {
		return ReadResult{}, err
	}
	e.privateNVMeHits.Add(1)

	tc.mu.Lock()
	if el, exists := tc.ram[key]; exists {
		tc.ll.MoveToFront(el)
		if selective {
			tc.sketch.Increment(h)
		}
		tc.mu.Unlock()
		return ReadResult{Tier: TierNVMe, Admitted: false}, nil
	}
	if selective {
		freq := tc.sketch.Increment(h)
		if freq < tc.minMirrorHits {
			tc.mu.Unlock()
			e.admissionsRejected.Add(1)
			return ReadResult{Tier: TierNVMe, Admitted: false}, nil
		}
		if tc.usedBytes+bs > tc.maxBytes && tc.ll.Len() > 0 {
			victim := tc.ll.Back().Value.(*privateRAMEntry)
			if freq <= tc.sketch.Estimate(victim.hash) {
				tc.mu.Unlock()
				e.admissionsRejected.Add(1)
				return ReadResult{Tier: TierNVMe, Admitted: false}, nil
			}
		}
	}

	var evictedBufs []*bufpool.Shared
	var deltaRAM int64
	for tc.usedBytes+bs > tc.maxBytes && tc.ll.Len() > 0 {
		back := tc.ll.Back()
		victim := back.Value.(*privateRAMEntry)
		tc.ll.Remove(back)
		delete(tc.ram, victim.key)
		tc.usedBytes -= bs
		deltaRAM -= bs
		evictedBufs = append(evictedBufs, victim.buf)
	}

	buf := e.privatePool.Get(e.cfg.BlockSize)
	copy(buf, dst)
	shBuf := e.privatePool.Share(buf)
	tc.ram[key] = tc.ll.PushFront(&privateRAMEntry{key: key, hash: h, buf: shBuf})
	tc.usedBytes += bs
	deltaRAM += bs
	tc.mu.Unlock()

	for _, eb := range evictedBufs {
		eb.Release()
	}
	if deltaRAM != 0 {
		e.addPrivateRAM(deltaRAM)
	}
	if len(evictedBufs) > 0 {
		e.ramEvictions.Add(uint64(len(evictedBufs)))
	}
	e.admissionsAccepted.Add(1)
	return ReadResult{Tier: TierNVMe, Admitted: true}, nil
}
