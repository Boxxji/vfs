// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package aiturbo

import (
	"context"
	"encoding/hex"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/zeebo/blake3"

	"github.com/hanzoai/vfs/pkg/backend"
)

const (
	// StrategyAITurboGrouped identifies checkpoints written by the AITURBO grouped IO planner.
	StrategyAITurboGrouped = "aiturbo_grouped_io"

	emptyDigestHex = "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262"
)

// Engine executes planned bulk checkpoint uploads and downloads over any
// canonical hanzoai/vfs backend.Backend.
type Engine struct {
	be backend.Backend
}

// NewEngine binds a grouped IO transfer Engine to backend be.
func NewEngine(be backend.Backend) *Engine {
	return &Engine{be: be}
}

// UploadResult summarizes an executed bulk checkpoint upload.
type UploadResult struct {
	Manifest           *Manifest
	HeadVersion        backend.Version
	Plan               *UploadPlan
	Duration           time.Duration
	LogicalBytesPerSec float64
	HeadPublished      bool
	Aborted            bool
	CleanedUpObjects   int
}

// DownloadResult summarizes an executed full or selective checkpoint download.
type DownloadResult struct {
	Manifest           *Manifest
	Plan               *DownloadPlan
	Files              map[string][]byte
	Duration           time.Duration
	LogicalBytesPerSec float64
	FetchedBytesPerSec float64
}

// UploadCheckpoint plans and executes an atomic bulk checkpoint upload for ns.
//
// S3 consistency guarantee: all grouped packs and direct objects are uploaded
// before the checkpoint manifest ("head") is written. If any data upload fails,
// head is never published (zero torn checkpoints), sibling workers cancel
// promptly, and uploaded uncommitted objects are cleaned up when CleanupOnAbort
// is enabled.
func (e *Engine) UploadCheckpoint(ctx context.Context, ns Namespace, checkpointID string, entries []Entry, cfg Config) (*UploadResult, error) {
	return e.uploadWithVersion(ctx, ns, checkpointID, entries, "", cfg)
}

// UpdateCheckpointHead atomically replaces an existing checkpoint head only if
// its stored backend.Version still equals expectedVersion (S3 If-Match CAS).
func (e *Engine) UpdateCheckpointHead(ctx context.Context, ns Namespace, checkpointID string, entries []Entry, expectedVersion backend.Version, cfg Config) (*UploadResult, error) {
	if expectedVersion == "" {
		return nil, fmt.Errorf("%w: expectedVersion is required for UpdateCheckpointHead", ErrInvalidConfig)
	}
	return e.uploadWithVersion(ctx, ns, checkpointID, entries, expectedVersion, cfg)
}

func (e *Engine) uploadWithVersion(ctx context.Context, ns Namespace, checkpointID string, entries []Entry, expectedVersion backend.Version, cfg Config) (*UploadResult, error) {
	start := time.Now()
	if err := ns.Validate(); err != nil {
		return nil, err
	}
	if err := ValidateCheckpointID(checkpointID); err != nil {
		return nil, err
	}
	normCfg, err := cfg.Normalized()
	if err != nil {
		return nil, err
	}

	// Enforce single-namespace isolation before planning or touching storage.
	for i, ent := range entries {
		entNS := Namespace{TenantID: ent.TenantID, VolumeID: ent.VolumeID}
		if entNS != ns {
			return nil, fmt.Errorf("%w: entry[%d] (%s,%s) does not match target namespace (%s,%s)",
				ErrTenantIsolation, i, entNS.TenantID, entNS.VolumeID, ns.TenantID, ns.VolumeID)
		}
	}

	plan, err := PlanUpload(entries, normCfg)
	if err != nil {
		return nil, err
	}

	// Upload all grouped packs and direct objects in parallel.
	uploadedKeys, uploadErr := e.uploadGroups(ctx, plan.Groups, normCfg.Concurrency)
	if uploadErr != nil {
		cleaned := 0
		if normCfg.CleanupOnAbort && len(uploadedKeys) > 0 {
			cleanCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			for _, k := range uploadedKeys {
				if err := e.be.Delete(cleanCtx, k); err == nil {
					cleaned++
				}
			}
			cancel()
		}
		return &UploadResult{
			Plan:             plan,
			Duration:         time.Since(start),
			HeadPublished:    false,
			Aborted:          true,
			CleanedUpObjects: cleaned,
		}, uploadErr
	}

	// All data objects are now durably present in S3. Build deterministic manifest.
	manifest := buildManifest(ns, checkpointID, StrategyAITurboGrouped, plan)
	rawManifest, err := manifest.MarshalSigned()
	if err != nil {
		return &UploadResult{
			Plan:          plan,
			Duration:      time.Since(start),
			HeadPublished: false,
			Aborted:       true,
		}, err
	}

	headKey := ns.HeadKey(checkpointID)
	var headVer backend.Version
	switch {
	case expectedVersion != "":
		headVer, err = e.be.PutIfVersion(ctx, headKey, rawManifest, expectedVersion)
	case normCfg.RequireConditionalHead:
		headVer, err = e.be.PutIfAbsent(ctx, headKey, rawManifest)
	default:
		err = e.be.Put(ctx, headKey, rawManifest)
	}
	if err != nil {
		return &UploadResult{
			Plan:          plan,
			Duration:      time.Since(start),
			HeadPublished: false,
			Aborted:       true,
		}, fmt.Errorf("aiturbo: publish checkpoint head %s: %w", checkpointID, err)
	}

	dur := time.Since(start)
	var bps float64
	if dur > 0 {
		bps = float64(plan.TotalLogicalBytes) / dur.Seconds()
	}
	return &UploadResult{
		Manifest:           manifest,
		HeadVersion:        headVer,
		Plan:               plan,
		Duration:           dur,
		LogicalBytesPerSec: bps,
		HeadPublished:      true,
		Aborted:            false,
	}, nil
}

func (e *Engine) uploadGroups(ctx context.Context, groups []UploadGroup, concurrency int) ([]string, error) {
	if len(groups) == 0 {
		return nil, nil
	}
	workers := min(concurrency, len(groups))
	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	jobs := make(chan int, len(groups))
	for i := range groups {
		jobs <- i
	}
	close(jobs)

	var (
		wg           sync.WaitGroup
		mu           sync.Mutex
		uploadedKeys []string
		firstErr     error
	)

	setErr := func(err error) {
		mu.Lock()
		if firstErr == nil {
			firstErr = err
			cancel()
		}
		mu.Unlock()
	}

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range jobs {
				if workCtx.Err() != nil {
					return
				}
				g := groups[idx]
				var payload []byte
				if g.DirectBypass {
					// Zero-copy direct bypass for large objects.
					payload = g.Items[0].Data
				} else {
					payload = make([]byte, g.TotalBytes)
					for _, it := range g.Items {
						copy(payload[it.Offset:it.Offset+it.Size], it.Data)
					}
				}
				if err := e.be.Put(workCtx, g.ObjectKey, payload); err != nil {
					setErr(fmt.Errorf("aiturbo: put group %s (%d bytes): %w", g.ObjectKey, len(payload), err))
					return
				}
				mu.Lock()
				uploadedKeys = append(uploadedKeys, g.ObjectKey)
				mu.Unlock()
			}
		}()
	}

	wg.Wait()
	if firstErr != nil {
		return uploadedKeys, firstErr
	}
	return uploadedKeys, nil
}

func buildManifest(ns Namespace, checkpointID, strategy string, plan *UploadPlan) *Manifest {
	groupMeta := make([]GroupObjectMeta, 0, len(plan.Groups))
	entries := make([]ManifestEntry, 0, plan.TotalEntries)

	for _, g := range plan.Groups {
		groupMeta = append(groupMeta, GroupObjectMeta{
			ObjectKey:    g.ObjectKey,
			Size:         g.TotalBytes,
			BLAKE3:       g.DigestHex,
			ItemCount:    len(g.Items),
			DirectBypass: g.DirectBypass,
		})
		for _, it := range g.Items {
			entries = append(entries, ManifestEntry{
				Path:         it.Path,
				Size:         it.Size,
				BLAKE3:       it.DigestHex,
				ObjectKey:    g.ObjectKey,
				Offset:       it.Offset,
				Length:       it.Size,
				DirectBypass: g.DirectBypass,
			})
		}
	}
	for _, empty := range plan.EmptyEntries {
		entries = append(entries, ManifestEntry{
			Path:   empty.Path,
			Size:   0,
			BLAKE3: empty.DigestHex,
		})
	}

	sort.Slice(groupMeta, func(i, j int) bool { return groupMeta[i].ObjectKey < groupMeta[j].ObjectKey })
	sort.Slice(entries, func(i, j int) bool { return entries[i].Path < entries[j].Path })

	return &Manifest{
		SchemaVersion:     SchemaVersion,
		CheckpointID:      checkpointID,
		TenantID:          ns.TenantID,
		VolumeID:          ns.VolumeID,
		Strategy:          strategy,
		CreatedUnixNano:   time.Now().UTC().UnixNano(),
		TotalLogicalBytes: plan.TotalLogicalBytes,
		TotalStoredBytes:  plan.TotalStoredBytes,
		GroupObjects:      groupMeta,
		Entries:           entries,
	}
}

// LoadManifest reads and verifies the published checkpoint manifest for (ns, checkpointID).
func (e *Engine) LoadManifest(ctx context.Context, ns Namespace, checkpointID string, cfg Config) (*Manifest, backend.Version, error) {
	if err := ns.Validate(); err != nil {
		return nil, "", err
	}
	if err := ValidateCheckpointID(checkpointID); err != nil {
		return nil, "", err
	}
	normCfg, err := cfg.Normalized()
	if err != nil {
		return nil, "", err
	}
	raw, ver, err := e.be.Get(ctx, ns.HeadKey(checkpointID), normCfg.MaxManifestBytes)
	if err != nil {
		return nil, "", err
	}
	manifest, err := UnmarshalManifest(raw, ns)
	if err != nil {
		return nil, "", err
	}
	if manifest.CheckpointID != checkpointID {
		return nil, "", fmt.Errorf("%w: checkpoint_id mismatch got %q want %q",
			ErrCorruptManifest, manifest.CheckpointID, checkpointID)
	}
	return manifest, ver, nil
}

// DownloadCheckpoint performs a verified full (paths == nil) or selective
// checkpoint restore using gap-coalesced Range-GETs and parallel workers.
func (e *Engine) DownloadCheckpoint(ctx context.Context, ns Namespace, checkpointID string, paths []string, cfg Config) (*DownloadResult, error) {
	start := time.Now()
	normCfg, err := cfg.Normalized()
	if err != nil {
		return nil, err
	}
	manifest, _, err := e.LoadManifest(ctx, ns, checkpointID, normCfg)
	if err != nil {
		return nil, err
	}
	plan, err := PlanDownload(manifest, paths, normCfg)
	if err != nil {
		return nil, err
	}

	files := make(map[string][]byte, plan.EntryCount)
	for _, empty := range plan.EmptyEntries {
		if empty.BLAKE3 != "" && empty.BLAKE3 != emptyDigestHex {
			return nil, fmt.Errorf("%w: empty entry %q invalid blake3 %s", ErrCorruptData, empty.Path, empty.BLAKE3)
		}
		files[empty.Path] = []byte{}
	}

	if len(plan.Ranges) > 0 {
		workers := min(normCfg.Concurrency, len(plan.Ranges))
		workCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		jobs := make(chan int, len(plan.Ranges))
		for i := range plan.Ranges {
			jobs <- i
		}
		close(jobs)

		var (
			wg       sync.WaitGroup
			mu       sync.Mutex
			firstErr error
		)

		setErr := func(err error) {
			mu.Lock()
			if firstErr == nil {
				firstErr = err
				cancel()
			}
			mu.Unlock()
		}

		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for idx := range jobs {
					if workCtx.Err() != nil {
						return
					}
					r := plan.Ranges[idx]
					var buf []byte
					var getErr error
					if r.FullObject {
						buf, _, getErr = e.be.Get(workCtx, r.ObjectKey, r.Length)
					} else {
						buf, getErr = e.be.GetRange(workCtx, r.ObjectKey, r.Offset, r.Length)
					}
					if getErr != nil {
						setErr(fmt.Errorf("aiturbo: fetch %s [%d+%d]: %w", r.ObjectKey, r.Offset, r.Length, getErr))
						return
					}
					if int64(len(buf)) != r.Length {
						setErr(fmt.Errorf("%w: short read for %s got %d want %d", ErrCorruptData, r.ObjectKey, len(buf), r.Length))
						return
					}

					local := make(map[string][]byte, len(r.Entries))
					for _, ent := range r.Entries {
						rel := ent.Offset - r.Offset
						if rel < 0 || rel+ent.Length > int64(len(buf)) {
							setErr(fmt.Errorf("%w: slice bounds out of range for %s", ErrCorruptData, ent.Path))
							return
						}
						slice := buf[rel : rel+ent.Length]
						sum := blake3.Sum256(slice)
						gotHex := hex.EncodeToString(sum[:])
						if gotHex != ent.BLAKE3 {
							setErr(fmt.Errorf("%w: entry %q blake3 mismatch got %s want %s",
								ErrCorruptData, ent.Path, gotHex, ent.BLAKE3))
							return
						}
						if len(r.Entries) == 1 && rel == 0 && ent.Length == int64(len(buf)) {
							local[ent.Path] = buf
						} else {
							owned := make([]byte, ent.Length)
							copy(owned, slice)
							local[ent.Path] = owned
						}
					}

					mu.Lock()
					for k, v := range local {
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

	dur := time.Since(start)
	var logicalBPS, fetchedBPS float64
	if dur > 0 {
		logicalBPS = float64(plan.TotalRequestedBytes) / dur.Seconds()
		fetchedBPS = float64(plan.TotalFetchedBytes) / dur.Seconds()
	}

	return &DownloadResult{
		Manifest:           manifest,
		Plan:               plan,
		Files:              files,
		Duration:           dur,
		LogicalBytesPerSec: logicalBPS,
		FetchedBytesPerSec: fetchedBPS,
	}, nil
}
