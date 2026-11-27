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

package most

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/zeebo/blake3"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/pkg/backend"
)

var envelopeMagic = [4]byte{'M', 'S', 'T', 'P'}

// Engine orchestrates the 3-tier RAM (L1) / NVMe (L2) / Hanzo S3 (L3) storage
// hierarchy for multi-agent shared immutable toolchains and tenant-isolated
// private workspaces.
type Engine struct {
	cfg        Config
	allowedNS  map[string]struct{}
	nvme       *NVMeTier
	remote     backend.Backend
	tracker    *HotnessTracker
	sharedRAM  *ramPool
	flights    *singleFlight
	ciphersMu  sync.RWMutex
	ciphers    map[Scope]vfs.Cipher
	agentRAMMu sync.RWMutex
	agentRAMs  map[string]*ramPool

	// Peak aggregate RAM across all per-agent caches (for per_agent_private_cache).
	peakTotalRAM atomic.Int64

	reads                 atomic.Uint64
	writes                atomic.Uint64
	ramHits               atomic.Uint64
	nvmeHits              atomic.Uint64
	s3Fetches             atomic.Uint64
	singleFlightCoalesced atomic.Uint64
	mirrorAdmissions      atomic.Uint64
	mirrorRejections      atomic.Uint64
	mirrorInstantDrops    atomic.Uint64
	mirrorInvalidations   atomic.Uint64
	corruptionRejects     atomic.Uint64
	isolationRejects      atomic.Uint64
}

// NewEngine validates cfg and initializes an Engine across L1 RAM, L2 NVMe,
// and L3 remote Hanzo S3.
func NewEngine(cfg Config) (*Engine, error) {
	if !cfg.Regime.Valid() {
		return nil, fmt.Errorf("%w: unknown regime %q", ErrInvalidConfig, cfg.Regime)
	}
	if cfg.NVMe.Store == nil {
		return nil, fmt.Errorf("%w: NVMe store is required", ErrInvalidConfig)
	}
	if cfg.Remote == nil {
		return nil, fmt.Errorf("%w: remote L3 backend is required", ErrInvalidConfig)
	}
	if cfg.RAMBudgetBytes < 0 {
		return nil, fmt.Errorf("%w: negative RAMBudgetBytes", ErrInvalidConfig)
	}
	if cfg.AgentCount <= 0 {
		cfg.AgentCount = 1
	}

	nvme, err := NewNVMeTier(cfg.NVMe)
	if err != nil {
		return nil, err
	}

	allowed := make(map[string]struct{}, len(cfg.AllowedNamespaces))
	for _, ns := range cfg.AllowedNamespaces {
		if !validSegment(ns) {
			return nil, fmt.Errorf("%w: invalid allowed namespace %q", ErrInvalidConfig, ns)
		}
		allowed[ns] = struct{}{}
	}

	ciphers := make(map[Scope]vfs.Cipher, len(cfg.VolumeCiphers))
	for sc, c := range cfg.VolumeCiphers {
		if !sc.Valid() || c == nil {
			return nil, fmt.Errorf("%w: invalid volume cipher entry for scope %+v", ErrInvalidConfig, sc)
		}
		ciphers[sc] = c
	}

	var tracker *HotnessTracker
	if cfg.Regime == RegimeMOSTSelectiveMirror {
		tracker = NewHotnessTracker(cfg.Tracker)
	}

	return &Engine{
		cfg:       cfg,
		allowedNS: allowed,
		nvme:      nvme,
		remote:    cfg.Remote,
		tracker:   tracker,
		sharedRAM: newRAMPool(cfg.RAMBudgetBytes),
		flights:   newSingleFlight(cfg.FetchTimeout),
		ciphers:   ciphers,
		agentRAMs: make(map[string]*ramPool),
	}, nil
}

// RegisterVolumeCipher binds a per-volume vfs.Cipher (e.g. vfs.Hybrid) to a
// private tenant/volume Scope.
func (e *Engine) RegisterVolumeCipher(scope Scope, c vfs.Cipher) error {
	if !scope.Valid() || c == nil {
		return fmt.Errorf("%w: valid scope and non-nil cipher required", ErrInvalidConfig)
	}
	e.ciphersMu.Lock()
	e.ciphers[scope] = c
	e.ciphersMu.Unlock()
	return nil
}

// PutShared publishes an immutable, content-addressed shared toolchain block to
// L3 (Hanzo S3) and optionally pre-populates L2 (NVMe) when warmNVMe is true.
// It verifies the namespace allowlist and BLAKE3-256 digest before storing.
func (e *Engine) PutShared(ctx context.Context, id BlockID, data []byte, warmNVMe bool) error {
	if id.Class != ClassSharedToolchain {
		e.isolationRejects.Add(1)
		return fmt.Errorf("%w: PutShared only accepts ClassSharedToolchain blocks", ErrTenantIsolation)
	}
	if err := id.Validate(); err != nil {
		return err
	}
	if _, ok := e.allowedNS[id.Namespace]; !ok {
		e.isolationRejects.Add(1)
		return fmt.Errorf("%w: %q", ErrNamespaceDenied, id.Namespace)
	}
	if blake3.Sum256(data) != id.Digest {
		e.corruptionRejects.Add(1)
		return fmt.Errorf("%w: PutShared digest mismatch for %s", ErrCorruptBlock, id.Key)
	}

	cp := make([]byte, len(data))
	copy(cp, data)

	// Immutable shared content uses PutIfAbsent on L3; conflict means identical
	// content-addressed object already exists (idempotent).
	if _, err := e.remote.PutIfAbsent(ctx, id.Key, cp); err != nil && !errors.Is(err, backend.ErrConflict) {
		return err
	}
	if warmNVMe {
		if err := e.nvme.SeedDirectly(ctx, id.Key, cp); err != nil {
			return err
		}
	}
	return nil
}

// ReadShared reads an immutable shared toolchain block on behalf of agentID
// according to the Engine's configured tiering regime.
//
// Security invariants enforced on every call:
//   - Rejects ClassPrivateWorkspace blocks with ErrTenantIsolation.
//   - Rejects non-allowlisted namespaces with ErrNamespaceDenied.
//   - Verifies BLAKE3-256 digest on every L2/L3 fetch before serving or caching.
func (e *Engine) ReadShared(ctx context.Context, agentID string, id BlockID) ([]byte, error) {
	if id.Class != ClassSharedToolchain {
		e.isolationRejects.Add(1)
		return nil, fmt.Errorf("%w: private block cannot be read via shared toolchain path", ErrTenantIsolation)
	}
	if err := id.Validate(); err != nil {
		return nil, err
	}
	if _, ok := e.allowedNS[id.Namespace]; !ok {
		e.isolationRejects.Add(1)
		return nil, fmt.Errorf("%w: %q", ErrNamespaceDenied, id.Namespace)
	}
	if agentID == "" {
		agentID = "agent-0"
	}

	e.reads.Add(1)
	keyHash := HashKey(id.Key)

	switch e.cfg.Regime {
	case RegimeMOSTSelectiveMirror:
		return e.readMOST(ctx, id, keyHash)
	case RegimeExclusiveMigration:
		return e.readExclusive(ctx, id, keyHash)
	case RegimeNaiveFullRAMPin:
		return e.readNaiveFullPin(ctx, id, keyHash)
	case RegimePerAgentPrivateCache:
		return e.readPerAgentPrivate(ctx, agentID, id, keyHash)
	default:
		return nil, ErrInvalidConfig
	}
}

func (e *Engine) readMOST(ctx context.Context, id BlockID, keyHash uint64) ([]byte, error) {
	estReads, readRatio := e.tracker.ObserveRead(keyHash)

	// Fast path: parallel RLock read from shared deduplicated RAM mirror.
	if data, ok := e.sharedRAM.Get(id.Key); ok {
		e.ramHits.Add(1)
		return data, nil
	}

	// Miss in RAM mirror: coalesced detached single-flight read from L2 NVMe
	// (fallback L3 Hanzo S3). Canonical copy remains on NVMe!
	res, coalesced, err := e.flights.Do(ctx, id.Key, func(fctx context.Context) (flightResult, error) {
		return e.fetchVerifiedShared(fctx, id, true)
	})
	if coalesced {
		e.singleFlightCoalesced.Add(1)
	}
	if err != nil {
		return nil, err
	}
	if !coalesced {
		if res.fromS3 {
			e.s3Fetches.Add(1)
		} else if res.fromNVMe {
			e.nvmeHits.Add(1)
		}
	} else {
		// Coalesced waiter still benefitted from the in-flight NVMe/S3 fetch.
		if res.fromS3 {
			e.s3Fetches.Add(1)
		} else {
			e.nvmeHits.Add(1)
		}
	}

	admitted, dropped := e.sharedRAM.TryAdmitMirror(id.Key, keyHash, res.data, estReads, readRatio, e.tracker)
	if admitted {
		e.mirrorAdmissions.Add(1)
		if dropped > 0 {
			// O(1) instant drop — zero NVMe writeback IOPS!
			e.mirrorInstantDrops.Add(uint64(dropped))
		}
	} else {
		e.mirrorRejections.Add(1)
	}

	out := make([]byte, len(res.data))
	copy(out, res.data)
	return out, nil
}

func (e *Engine) readExclusive(ctx context.Context, id BlockID, keyHash uint64) ([]byte, error) {
	// Check L1 RAM first.
	if data, ok := e.sharedRAM.Get(id.Key); ok {
		e.ramHits.Add(1)
		return data, nil
	}

	// Miss in RAM: fetch from NVMe (or S3 fallback) via detached single-flight,
	// promote to RAM, and demote any evicted RAM victims back to NVMe!
	res, coalesced, err := e.flights.Do(ctx, id.Key, func(fctx context.Context) (flightResult, error) {
		// Re-check RAM in case a prior flight just promoted it.
		if data, ok := e.sharedRAM.Get(id.Key); ok {
			return flightResult{data: data}, nil
		}

		// Exclusive migration: read from NVMe (or S3); reclaim NVMe slot before
		// installing in RAM (all concurrent misses for id.Key coalesce on this
		// singleFlight, so no concurrent eviction of id.Key can interleave).
		fr, err := e.fetchVerifiedShared(fctx, id, false)
		if err != nil {
			return flightResult{}, err
		}
		if fr.fromNVMe {
			_ = e.nvme.Delete(fctx, id.Key)
		}

		evicted := e.sharedRAM.PutExclusive(id.Key, keyHash, fr.data, false)
		// Demote every evicted victim back to NVMe — real promotion/demotion
		// writeback IOPS and channel contention!
		for _, v := range evicted {
			if demErr := e.nvme.WriteDemotion(fctx, v.key, v.data); demErr != nil {
				e.sharedRAM.FinishDemotion(v.key)
				return flightResult{}, demErr
			}
			e.sharedRAM.FinishDemotion(v.key)
		}
		return fr, nil
	})
	if coalesced {
		e.singleFlightCoalesced.Add(1)
	}
	if err != nil {
		return nil, err
	}

	if !res.fromNVMe && !res.fromS3 {
		e.ramHits.Add(1)
	} else if res.fromS3 {
		e.s3Fetches.Add(1)
	} else {
		e.nvmeHits.Add(1)
	}

	out := make([]byte, len(res.data))
	copy(out, res.data)
	return out, nil
}

func (e *Engine) readNaiveFullPin(ctx context.Context, id BlockID, keyHash uint64) ([]byte, error) {
	if data, ok := e.sharedRAM.Get(id.Key); ok {
		e.ramHits.Add(1)
		return data, nil
	}

	res, coalesced, err := e.flights.Do(ctx, id.Key, func(fctx context.Context) (flightResult, error) {
		return e.fetchVerifiedShared(fctx, id, true)
	})
	if coalesced {
		e.singleFlightCoalesced.Add(1)
	}
	if err != nil {
		return nil, err
	}
	if res.fromS3 {
		e.s3Fetches.Add(1)
	} else {
		e.nvmeHits.Add(1)
	}

	if err := e.sharedRAM.PinUnbounded(id.Key, keyHash, res.data, e.cfg.EnforceRAMCap); err != nil {
		return nil, err
	}

	out := make([]byte, len(res.data))
	copy(out, res.data)
	return out, nil
}

func (e *Engine) readPerAgentPrivate(ctx context.Context, agentID string, id BlockID, keyHash uint64) ([]byte, error) {
	pool := e.getOrCreateAgentPool(agentID)
	if data, ok := pool.Get(id.Key); ok {
		e.ramHits.Add(1)
		return data, nil
	}

	// Each agent manages its own isolated private RAM cache (no cross-agent sharing).
	flightKey := agentID + "|" + id.Key
	res, coalesced, err := e.flights.Do(ctx, flightKey, func(fctx context.Context) (flightResult, error) {
		return e.fetchVerifiedShared(fctx, id, true)
	})
	if coalesced {
		e.singleFlightCoalesced.Add(1)
	}
	if err != nil {
		return nil, err
	}
	if res.fromS3 {
		e.s3Fetches.Add(1)
	} else {
		e.nvmeHits.Add(1)
	}

	evicted := pool.PutExclusive(id.Key, keyHash, res.data, false)
	for _, v := range evicted {
		// Private per-agent read-only cache drops clean copies without NVMe demotion.
		pool.FinishDemotion(v.key)
	}
	e.updatePeakTotalAgentRAM()

	out := make([]byte, len(res.data))
	copy(out, res.data)
	return out, nil
}

func (e *Engine) getOrCreateAgentPool(agentID string) *ramPool {
	e.agentRAMMu.RLock()
	p, ok := e.agentRAMs[agentID]
	e.agentRAMMu.RUnlock()
	if ok {
		return p
	}

	e.agentRAMMu.Lock()
	defer e.agentRAMMu.Unlock()
	if p, ok = e.agentRAMs[agentID]; ok {
		return p
	}
	budget := e.cfg.RAMBudgetBytes
	if e.cfg.PerAgentTotalBudgetCap && e.cfg.AgentCount > 0 {
		budget = e.cfg.RAMBudgetBytes / int64(e.cfg.AgentCount)
		if budget < 1 {
			budget = 1
		}
	}
	p = newRAMPool(budget)
	e.agentRAMs[agentID] = p
	return p
}

func (e *Engine) updatePeakTotalAgentRAM() {
	e.agentRAMMu.RLock()
	var total int64
	for _, p := range e.agentRAMs {
		used, _ := p.Stats()
		total += used
	}
	e.agentRAMMu.RUnlock()

	for {
		cur := e.peakTotalRAM.Load()
		if total <= cur {
			return
		}
		if e.peakTotalRAM.CompareAndSwap(cur, total) {
			return
		}
	}
}

func (e *Engine) fetchVerifiedShared(ctx context.Context, id BlockID, populateNVMeOnS3 bool) (flightResult, error) {
	data, err := e.nvme.Read(ctx, id.Key)
	if err == nil {
		if blake3.Sum256(data) != id.Digest {
			e.corruptionRejects.Add(1)
			return flightResult{}, fmt.Errorf("%w: NVMe block %s corrupted", ErrCorruptBlock, id.Key)
		}
		return flightResult{data: data, fromNVMe: true}, nil
	}
	if !errors.Is(err, ErrNotFound) {
		return flightResult{}, err
	}

	// L2 miss -> fetch from L3 (Hanzo S3).
	if err := sleepWithContext(ctx, e.cfg.RemoteLatency); err != nil {
		return flightResult{}, err
	}
	raw, _, err := e.remote.Get(ctx, id.Key, backend.Unbounded)
	if err != nil {
		if errors.Is(err, backend.ErrNotFound) {
			return flightResult{}, ErrNotFound
		}
		return flightResult{}, err
	}
	if blake3.Sum256(raw) != id.Digest {
		e.corruptionRejects.Add(1)
		return flightResult{}, fmt.Errorf("%w: S3 block %s corrupted", ErrCorruptBlock, id.Key)
	}
	if populateNVMeOnS3 {
		if err := e.nvme.WritePromotion(ctx, id.Key, raw); err != nil {
			return flightResult{}, err
		}
	}
	return flightResult{data: raw, fromS3: true}, nil
}

// PutPrivate stores a mutable tenant/volume-scoped workspace block.
// Private blocks are encrypted per-volume using the Scope's registered
// vfs.Cipher (e.g. vfs.Hybrid X25519 + ML-KEM-768) and are strictly excluded
// from the shared toolchain RAM mirror.
func (e *Engine) PutPrivate(ctx context.Context, caller Scope, id BlockID, plaintext []byte) error {
	if id.Class != ClassPrivateWorkspace {
		e.isolationRejects.Add(1)
		return fmt.Errorf("%w: PutPrivate requires ClassPrivateWorkspace block", ErrTenantIsolation)
	}
	if err := id.Validate(); err != nil {
		return err
	}
	if !caller.Valid() || !caller.Equal(id.Scope) {
		e.isolationRejects.Add(1)
		return fmt.Errorf("%w: caller scope %+v cannot write block scope %+v", ErrTenantIsolation, caller, id.Scope)
	}

	payload := make([]byte, len(plaintext))
	copy(payload, plaintext)

	if c, ok := e.cipherForScope(caller); ok {
		var err error
		payload, err = sealEnvelope(c, payload)
		if err != nil {
			return err
		}
	}

	e.writes.Add(1)
	if err := e.nvme.WriteSync(ctx, id.Key, payload); err != nil {
		return err
	}
	if err := e.remote.Put(ctx, id.Key, payload); err != nil {
		return err
	}
	return nil
}

// ReadPrivate reads a tenant/volume-scoped workspace block, enforcing strict
// cross-tenant isolation and per-volume PQ decryption via vfs.Cipher.
// Private blocks never enter the shared RAM mirror.
func (e *Engine) ReadPrivate(ctx context.Context, caller Scope, id BlockID) ([]byte, error) {
	if id.Class != ClassPrivateWorkspace {
		e.isolationRejects.Add(1)
		return nil, fmt.Errorf("%w: ReadPrivate requires ClassPrivateWorkspace block", ErrTenantIsolation)
	}
	if err := id.Validate(); err != nil {
		return nil, err
	}
	if !caller.Valid() || !caller.Equal(id.Scope) {
		e.isolationRejects.Add(1)
		return nil, fmt.Errorf("%w: caller scope %+v cannot read block scope %+v", ErrTenantIsolation, caller, id.Scope)
	}

	e.reads.Add(1)
	raw, err := e.nvme.Read(ctx, id.Key)
	if err != nil {
		if !errors.Is(err, ErrNotFound) {
			return nil, err
		}
		if err := sleepWithContext(ctx, e.cfg.RemoteLatency); err != nil {
			return nil, err
		}
		raw, _, err = e.remote.Get(ctx, id.Key, backend.Unbounded)
		if err != nil {
			if errors.Is(err, backend.ErrNotFound) {
				return nil, ErrNotFound
			}
			return nil, err
		}
		e.s3Fetches.Add(1)
		_ = e.nvme.WritePromotion(ctx, id.Key, raw)
	} else {
		e.nvmeHits.Add(1)
	}

	if c, ok := e.cipherForScope(caller); ok {
		plain, err := openEnvelope(c, raw)
		if err != nil {
			return nil, err
		}
		return plain, nil
	}

	out := make([]byte, len(raw))
	copy(out, raw)
	return out, nil
}

// AttemptMirrorPrivate explicitly verifies that private workspace blocks can
// never be admitted to the shared read-only toolchain mirror.
func (e *Engine) AttemptMirrorPrivate(id BlockID, _ []byte) error {
	e.isolationRejects.Add(1)
	return fmt.Errorf("%w: private block %q is strictly forbidden from entering shared RAM mirror", ErrTenantIsolation, id.Key)
}

// WriteMutable executes a mutable block overwrite for the write-heavy negative
// regime study (neg_write_heavy_invalidation_churn). It refuses immutable
// shared toolchain blocks and requires AllowMutableMirrorForStudy when invoked
// on RegimeMOSTSelectiveMirror so production policies remain strictly immutable.
func (e *Engine) WriteMutable(ctx context.Context, caller Scope, id BlockID, data []byte) error {
	if id.Class == ClassSharedToolchain {
		return fmt.Errorf("%w: %s", ErrImmutableMutation, id.Key)
	}
	if err := id.Validate(); err != nil {
		return err
	}
	if !caller.Valid() || !caller.Equal(id.Scope) {
		e.isolationRejects.Add(1)
		return fmt.Errorf("%w: caller scope %+v cannot mutate %+v", ErrTenantIsolation, caller, id.Scope)
	}

	e.writes.Add(1)
	keyHash := HashKey(id.Key)

	switch e.cfg.Regime {
	case RegimeExclusiveMigration:
		// Ordinary write-back cache-tier migration: absorb writes in RAM without
		// synchronous NVMe write-through; flush dirty victims to NVMe only on eviction.
		evicted := e.sharedRAM.PutExclusive(id.Key, keyHash, data, true)
		for _, v := range evicted {
			if err := e.nvme.WriteDemotion(ctx, v.key, v.data); err != nil {
				e.sharedRAM.FinishDemotion(v.key)
				return err
			}
			e.sharedRAM.FinishDemotion(v.key)
		}
		return nil

	case RegimeMOSTSelectiveMirror:
		if !e.cfg.AllowMutableMirrorForStudy {
			e.isolationRejects.Add(1)
			return fmt.Errorf("%w: MOST forbids mirroring mutable blocks in production mode", ErrTenantIsolation)
		}
		e.tracker.ObserveWrite(keyHash)
		// Dual RAM+NVMe mirror ablation: every write MUST synchronously sync
		// NVMe AND invalidate/refresh the mirrored RAM copy!
		if e.sharedRAM.Delete(id.Key) {
			e.mirrorInvalidations.Add(1)
		}
		if err := e.nvme.WriteSync(ctx, id.Key, data); err != nil {
			return err
		}
		return nil

	default:
		return e.nvme.WriteSync(ctx, id.Key, data)
	}
}

// ReadMutable reads a mutable block in the write-heavy negative regime study.
func (e *Engine) ReadMutable(ctx context.Context, caller Scope, id BlockID) ([]byte, error) {
	if id.Class == ClassSharedToolchain {
		return nil, fmt.Errorf("%w: use ReadShared for shared toolchain blocks", ErrInvalidBlock)
	}
	if err := id.Validate(); err != nil {
		return nil, err
	}
	if !caller.Valid() || !caller.Equal(id.Scope) {
		e.isolationRejects.Add(1)
		return nil, fmt.Errorf("%w: caller scope %+v cannot read %+v", ErrTenantIsolation, caller, id.Scope)
	}

	e.reads.Add(1)
	keyHash := HashKey(id.Key)

	switch e.cfg.Regime {
	case RegimeExclusiveMigration:
		if data, ok := e.sharedRAM.Get(id.Key); ok {
			e.ramHits.Add(1)
			return data, nil
		}
		data, err := e.nvme.Read(ctx, id.Key)
		if err != nil {
			return nil, err
		}
		e.nvmeHits.Add(1)
		evicted := e.sharedRAM.PutExclusive(id.Key, keyHash, data, false)
		for _, v := range evicted {
			if demErr := e.nvme.WriteDemotion(ctx, v.key, v.data); demErr != nil {
				e.sharedRAM.FinishDemotion(v.key)
				return nil, demErr
			}
			e.sharedRAM.FinishDemotion(v.key)
		}
		out := make([]byte, len(data))
		copy(out, data)
		return out, nil

	case RegimeMOSTSelectiveMirror:
		if !e.cfg.AllowMutableMirrorForStudy {
			e.isolationRejects.Add(1)
			return nil, fmt.Errorf("%w: MOST forbids mirroring mutable blocks in production mode", ErrTenantIsolation)
		}
		estReads, _ := e.tracker.ObserveRead(keyHash)
		if data, ok := e.sharedRAM.Get(id.Key); ok {
			e.ramHits.Add(1)
			return data, nil
		}
		data, err := e.nvme.Read(ctx, id.Key)
		if err != nil {
			return nil, err
		}
		e.nvmeHits.Add(1)
		// Force mirror admission in ablation mode to measure dual-copy invalidation churn.
		admitted, dropped := e.sharedRAM.TryAdmitMirror(id.Key, keyHash, data, max(estReads, 2), 1.0, e.tracker)
		if admitted {
			e.mirrorAdmissions.Add(1)
			e.mirrorInstantDrops.Add(uint64(dropped))
		} else {
			e.mirrorRejections.Add(1)
		}
		out := make([]byte, len(data))
		copy(out, data)
		return out, nil

	default:
		data, err := e.nvme.Read(ctx, id.Key)
		if err != nil {
			return nil, err
		}
		e.nvmeHits.Add(1)
		return data, nil
	}
}

// SeedMutable seeds a mutable block directly on NVMe (and S3) without charging
// steady-state counters (used by negative regime #2 setup).
func (e *Engine) SeedMutable(ctx context.Context, id BlockID, data []byte) error {
	if err := id.Validate(); err != nil {
		return err
	}
	cp := make([]byte, len(data))
	copy(cp, data)
	return e.nvme.SeedDirectly(ctx, id.Key, cp)
}

// SharedRAMContains reports whether key is currently cached in the shared RAM tier.
func (e *Engine) SharedRAMContains(key string) bool {
	return e.sharedRAM.Contains(key)
}

// Metrics returns a consistent telemetry snapshot of Engine state.
func (e *Engine) Metrics() Metrics {
	var curRAM, peakRAM int64
	if e.cfg.Regime == RegimePerAgentPrivateCache {
		e.agentRAMMu.RLock()
		for _, p := range e.agentRAMs {
			u, _ := p.Stats()
			curRAM += u
		}
		e.agentRAMMu.RUnlock()
		peakRAM = e.peakTotalRAM.Load()
		if curRAM > peakRAM {
			peakRAM = curRAM
		}
	} else {
		curRAM, peakRAM = e.sharedRAM.Stats()
	}

	var trackerBytes int64
	if e.tracker != nil {
		trackerBytes = e.tracker.MemoryBytes()
	}

	totalPeakWithMeta := peakRAM + trackerBytes
	agents := e.cfg.AgentCount
	if agents <= 0 {
		agents = 1
	}

	reads := e.reads.Load()
	ramHits := e.ramHits.Load()
	hitRate := 0.0
	if reads > 0 {
		hitRate = float64(ramHits) / float64(reads)
	}

	budget := e.cfg.RAMBudgetBytes
	overBudget := budget > 0 && peakRAM > budget

	return Metrics{
		Regime:                e.cfg.Regime,
		AgentCount:            agents,
		Reads:                 reads,
		Writes:                e.writes.Load(),
		RAMHits:               ramHits,
		NVMeHits:              e.nvmeHits.Load(),
		S3Fetches:             e.s3Fetches.Load(),
		SingleFlightCoalesced: e.singleFlightCoalesced.Load(),
		MirrorAdmissions:      e.mirrorAdmissions.Load(),
		MirrorRejections:      e.mirrorRejections.Load(),
		MirrorInstantDrops:    e.mirrorInstantDrops.Load(),
		MirrorInvalidations:   e.mirrorInvalidations.Load(),
		NVMeReadIOPS:          e.nvme.readIOPS.Load(),
		NVMeReadBytes:         e.nvme.readBytes.Load(),
		NVMePromotionWrites:   e.nvme.promoWrites.Load(),
		NVMeDemotionWrites:    e.nvme.demotionWrites.Load(),
		NVMeDemotionBytes:     e.nvme.demotionBytes.Load(),
		NVMeWritebackIOPS:     e.nvme.writeIOPS.Load(),
		NVMeWritebackBytes:    e.nvme.writeBytes.Load(),
		CurrentRAMBytes:       curRAM,
		PeakRAMBytes:          peakRAM,
		RAMBudgetBytes:        budget,
		TrackerMetaBytes:      trackerBytes,
		RAMPerAgentBytes:      float64(totalPeakWithMeta) / float64(agents),
		RAMHitRate:            hitRate,
		OverBudget:            overBudget,
		CorruptionRejects:     e.corruptionRejects.Load(),
		IsolationRejects:      e.isolationRejects.Load(),
	}
}

func (e *Engine) cipherForScope(sc Scope) (vfs.Cipher, bool) {
	e.ciphersMu.RLock()
	c, ok := e.ciphers[sc]
	e.ciphersMu.RUnlock()
	return c, ok
}

func sealEnvelope(c vfs.Cipher, plaintext []byte) ([]byte, error) {
	sealed, keyID, err := c.Seal(plaintext)
	if err != nil {
		return nil, err
	}
	if len(keyID) > 0xffff {
		return nil, fmt.Errorf("most: keyID too long (%d bytes)", len(keyID))
	}
	out := make([]byte, 4+2+len(keyID)+len(sealed))
	copy(out[:4], envelopeMagic[:])
	binary.BigEndian.PutUint16(out[4:6], uint16(len(keyID)))
	copy(out[6:6+len(keyID)], keyID)
	copy(out[6+len(keyID):], sealed)
	return out, nil
}

func openEnvelope(c vfs.Cipher, raw []byte) ([]byte, error) {
	if len(raw) < 6 || !bytes.Equal(raw[:4], envelopeMagic[:]) {
		return nil, fmt.Errorf("%w: invalid private cipher envelope", ErrCorruptBlock)
	}
	idLen := int(binary.BigEndian.Uint16(raw[4:6]))
	if 6+idLen > len(raw) {
		return nil, fmt.Errorf("%w: truncated private cipher envelope", ErrCorruptBlock)
	}
	keyID := string(raw[6 : 6+idLen])
	sealed := raw[6+idLen:]
	return c.Open(sealed, keyID)
}
