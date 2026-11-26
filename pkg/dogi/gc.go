// Copyright 2026 Hanzo AI Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package dogi

import (
	"context"
	"fmt"
	"sort"
)

// Collect runs a deterministic collection-time sweep over all sealed immutable
// packs stored in the backend:
//
//  1. 100%-dead packs (liveBytes == 0) are deleted directly via backend.Delete
//     with ZERO live-data copying (0 B rewritten, 0 GetRange ops).
//  2. Partially-dead packs whose dead-byte ratio meets or exceeds
//     Config.GCDeadRatioThreshold are compacted: surviving live chunks are read
//     via backend.GetRange, verified (and unsealed if encrypted), re-classified
//     for relocation, packed into fresh destination stream buffers, and the old
//     victim pack is deleted from the backend.
//  3. Healthy packs below the dead-byte threshold remain untouched.
//
// Collect returns exact per-sweep statistics and updates cumulative metrics.
func (p *Packer) Collect(ctx context.Context, currentEpoch uint64) (GCStats, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.gcCycles++
	stats := GCStats{
		Epoch:        currentEpoch,
		PacksScanned: len(p.packs),
	}

	// Deterministic pack ordering by key so GC behavior is reproducible across runs.
	victims := make([]*sealedPack, 0, len(p.packs))
	for _, sp := range p.packs {
		if sp.totalPlainBytes == 0 {
			victims = append(victims, sp)
			continue
		}
		if sp.liveBytes == 0 {
			victims = append(victims, sp)
			continue
		}
		deadRatio := float64(sp.deadBytes) / float64(sp.totalPlainBytes)
		if deadRatio >= p.cfg.GCDeadRatioThreshold {
			victims = append(victims, sp)
		}
	}
	sort.Slice(victims, func(i, j int) bool {
		return victims[i].key < victims[j].key
	})

	var compactedVictims []*sealedPack
	for _, sp := range victims {
		if sp.liveBytes == 0 {
			// Zero-copy reclamation: entire pack is dead, delete directly from store.
			if err := p.be.Delete(ctx, sp.key); err != nil {
				return stats, fmt.Errorf("dogi: delete dead pack %s: %w", sp.key, err)
			}
			delete(p.packs, sp.id)
			p.sealedStoredBytes -= sp.storedBytes
			p.packsDeleted++
			p.zeroCopyReclaimedPacks++

			stats.ZeroCopyReclaimedPacks++
			stats.ReclaimedBytes += sp.storedBytes
			continue
		}

		// Partially-dead victim pack: fetch and relocate ONLY surviving live chunks.
		for _, oldEntry := range sp.entries {
			if !oldEntry.live {
				continue
			}
			frame, err := p.be.GetRange(ctx, sp.key, oldEntry.off, oldEntry.frameLen)
			if err != nil {
				return stats, fmt.Errorf("dogi: gc read %s [%d:+%d]: %w", sp.key, oldEntry.off, oldEntry.frameLen, err)
			}
			stats.GetRangeOps++
			p.gcGetRangeOps++

			plain, err := decodeChunkFrame(p.cfg.Namespace, oldEntry.chunkID, frame, p.cfg.Sealer)
			if err != nil {
				return stats, fmt.Errorf("dogi: gc verify %s: %w", oldEntry.path, err)
			}

			newFrame, err := encodeChunkFrame(p.cfg.Namespace, oldEntry.chunkID, plain, p.cfg.Sealer)
			if err != nil {
				return stats, fmt.Errorf("dogi: gc re-encode %s: %w", oldEntry.path, err)
			}

			nextGen := oldEntry.gcGen + 1
			destClass := p.classifier.ClassifyRelocation(
				oldEntry.path,
				oldEntry.createdEpoch,
				currentEpoch,
				nextGen,
				oldEntry.oracleDeathEpoch,
			)

			relocated := &packEntry{
				path:             oldEntry.path,
				dir:              oldEntry.dir,
				chunkID:          oldEntry.chunkID,
				plainLen:         oldEntry.plainLen,
				createdEpoch:     oldEntry.createdEpoch,
				oracleDeathEpoch: oldEntry.oracleDeathEpoch,
				gcGen:            nextGen,
				live:             true,
			}

			// Mark old entry dead before installing relocated entry so live index
			// transitions cleanly without double-counting liveBytes.
			oldEntry.live = false
			if err := p.appendToStreamLocked(ctx, destClass, relocated, newFrame, currentEpoch); err != nil {
				return stats, fmt.Errorf("dogi: gc relocate %s: %w", oldEntry.path, err)
			}

			stats.LiveCopyBytes += oldEntry.plainLen
			stats.LiveCopyChunks++
			p.liveCopyBytes += oldEntry.plainLen
			p.liveCopyChunks++
		}
		compactedVictims = append(compactedVictims, sp)
	}

	// Flush all destination stream buffers holding relocated chunks ONCE so every
	// surviving chunk is durably persisted in the backend BEFORE any compacted
	// victim pack is deleted (crash-safe GC ordering without per-victim tail packs).
	if len(compactedVictims) > 0 {
		if err := p.flushAllLocked(ctx, currentEpoch); err != nil {
			return stats, fmt.Errorf("dogi: gc flush relocated packs: %w", err)
		}
		for _, sp := range compactedVictims {
			if err := p.be.Delete(ctx, sp.key); err != nil {
				return stats, fmt.Errorf("dogi: delete compacted pack %s: %w", sp.key, err)
			}
			delete(p.packs, sp.id)
			p.sealedStoredBytes -= sp.storedBytes
			p.packsDeleted++
			p.compactedPacks++

			stats.CompactedPacks++
			stats.ReclaimedBytes += sp.storedBytes
		}
	}

	p.updatePeakLocked()
	return stats, nil
}
