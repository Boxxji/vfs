// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package aiturbo

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/zeebo/blake3"

	"github.com/hanzoai/vfs/pkg/backend"
)

// Regime names the four matched checkpoint transfer strategies evaluated side by side.
type Regime string

const (
	// RegimeNaivePerObjectSerial uploads/downloads 1 S3 object per checkpoint file sequentially.
	RegimeNaivePerObjectSerial Regime = "naive_per_object_serial"

	// RegimeNaivePerObjectConcurrent uploads/downloads 1 S3 object per checkpoint file
	// using P parallel workers (same concurrency as AITURBO, without grouping).
	RegimeNaivePerObjectConcurrent Regime = "naive_per_object_concurrent"

	// RegimeNaiveSingleTarBlob packs the entire checkpoint into one monolithic archive
	// object (1 data PUT + 1 head PUT), eliminating request overhead at the expense of
	// upload/download parallelism and full-blob reads on selective restores.
	RegimeNaiveSingleTarBlob Regime = "naive_single_tar_blob"

	// RegimeAITurboGroupedIO is the AITURBO grouped IO planner (size-bounded packs +
	// large-object direct bypass + gap-coalesced Range-GETs + P parallel workers).
	RegimeAITurboGroupedIO Regime = "aiturbo_grouped_io_planner"
)

// RegimeResult records apples-to-apples performance, request efficiency, and
// consistency metrics for one regime over a workload.
type RegimeResult struct {
	Regime                Regime         `json:"regime"`
	LogicalBytes          int64          `json:"logical_bytes"`
	EntryCount            int            `json:"entry_count"`
	UploadDuration        time.Duration  `json:"upload_duration_ns"`
	UploadBytesPerSec     float64        `json:"upload_bytes_per_sec"`
	UploadMiBPerSec       float64        `json:"upload_mib_per_sec"`
	UploadMetrics         BackendMetrics `json:"upload_metrics"`
	UploadPUTsPerGiB      float64        `json:"upload_puts_per_gib"`
	DownloadDuration      time.Duration  `json:"download_duration_ns"`
	DownloadBytesPerSec   float64        `json:"download_bytes_per_sec"`
	DownloadMiBPerSec     float64        `json:"download_mib_per_sec"`
	DownloadMetrics       BackendMetrics `json:"download_metrics"`
	DownloadGETsPerGiB    float64        `json:"download_gets_per_gib"`
	SelectiveDuration     time.Duration  `json:"selective_duration_ns,omitempty"`
	SelectiveReqBytes     int64          `json:"selective_requested_bytes,omitempty"`
	SelectiveFetchedBytes int64          `json:"selective_fetched_bytes,omitempty"`
	SelectiveReadAmp      float64        `json:"selective_read_amplification,omitempty"`
	SelectiveGETs         int64          `json:"selective_gets,omitempty"`
	SelectiveMiBPerSec    float64        `json:"selective_mib_per_sec,omitempty"`
	BitExactVerified      bool           `json:"bit_exact_verified"`
}

const mibFloat = 1024.0 * 1024.0

// RunMatchedRegime executes a full upload, verified full download, and optional
// verified selective download under the specified Regime over be.
func RunMatchedRegime(
	ctx context.Context,
	be *InstrumentedBackend,
	regime Regime,
	ns Namespace,
	checkpointID string,
	entries []Entry,
	selectivePaths []string,
	cfg Config,
) (*RegimeResult, error) {
	normCfg, err := cfg.Normalized()
	if err != nil {
		return nil, err
	}
	if err := ns.Validate(); err != nil {
		return nil, err
	}
	if err := ValidateCheckpointID(checkpointID); err != nil {
		return nil, err
	}

	var logicalBytes int64
	expectedByPath := make(map[string][]byte, len(entries))
	for _, e := range entries {
		sz := e.Size
		if sz == 0 && len(e.Data) > 0 {
			sz = int64(len(e.Data))
		}
		logicalBytes += sz
		expectedByPath[e.Path] = e.Data
	}

	res := &RegimeResult{
		Regime:       regime,
		LogicalBytes: logicalBytes,
		EntryCount:   len(entries),
	}

	switch regime {
	case RegimeAITurboGroupedIO:
		eng := NewEngine(be)
		be.ResetMetrics()
		upRes, err := eng.UploadCheckpoint(ctx, ns, checkpointID, entries, normCfg)
		if err != nil {
			return nil, err
		}
		res.UploadDuration = upRes.Duration
		res.UploadMetrics = be.Metrics()

		be.ResetMetrics()
		downRes, err := eng.DownloadCheckpoint(ctx, ns, checkpointID, nil, normCfg)
		if err != nil {
			return nil, err
		}
		res.DownloadDuration = downRes.Duration
		res.DownloadMetrics = be.Metrics()
		if err := verifyExactFiles(expectedByPath, downRes.Files, nil); err != nil {
			return nil, err
		}

		if len(selectivePaths) > 0 {
			be.ResetMetrics()
			selRes, err := eng.DownloadCheckpoint(ctx, ns, checkpointID, selectivePaths, normCfg)
			if err != nil {
				return nil, err
			}
			selMetrics := be.Metrics()
			if err := verifyExactFiles(expectedByPath, selRes.Files, selectivePaths); err != nil {
				return nil, err
			}
			res.SelectiveDuration = selRes.Duration
			res.SelectiveReqBytes = selRes.Plan.TotalRequestedBytes
			res.SelectiveFetchedBytes = selRes.Plan.TotalFetchedBytes
			res.SelectiveReadAmp = selRes.Plan.ReadAmplification
			res.SelectiveGETs = selMetrics.TotalReadRequests()
			if selRes.Duration > 0 {
				res.SelectiveMiBPerSec = float64(res.SelectiveReqBytes) / selRes.Duration.Seconds() / mibFloat
			}
		}

	case RegimeNaivePerObjectSerial, RegimeNaivePerObjectConcurrent:
		workers := 1
		if regime == RegimeNaivePerObjectConcurrent {
			workers = normCfg.Concurrency
		}
		be.ResetMetrics()
		upDur, err := naivePerObjectUpload(ctx, be, ns, checkpointID, entries, workers, normCfg)
		if err != nil {
			return nil, err
		}
		res.UploadDuration = upDur
		res.UploadMetrics = be.Metrics()

		be.ResetMetrics()
		downFiles, downDur, err := naivePerObjectDownload(ctx, be, ns, checkpointID, nil, workers, normCfg)
		if err != nil {
			return nil, err
		}
		res.DownloadDuration = downDur
		res.DownloadMetrics = be.Metrics()
		if err := verifyExactFiles(expectedByPath, downFiles, nil); err != nil {
			return nil, err
		}

		if len(selectivePaths) > 0 {
			be.ResetMetrics()
			selFiles, selDur, err := naivePerObjectDownload(ctx, be, ns, checkpointID, selectivePaths, workers, normCfg)
			if err != nil {
				return nil, err
			}
			selMetrics := be.Metrics()
			if err := verifyExactFiles(expectedByPath, selFiles, selectivePaths); err != nil {
				return nil, err
			}
			var reqBytes int64
			for _, p := range selectivePaths {
				reqBytes += int64(len(expectedByPath[p]))
			}
			res.SelectiveDuration = selDur
			res.SelectiveReqBytes = reqBytes
			res.SelectiveFetchedBytes = reqBytes
			res.SelectiveReadAmp = 1.0
			res.SelectiveGETs = selMetrics.TotalReadRequests()
			if selDur > 0 {
				res.SelectiveMiBPerSec = float64(reqBytes) / selDur.Seconds() / mibFloat
			}
		}

	case RegimeNaiveSingleTarBlob:
		be.ResetMetrics()
		upDur, err := naiveSingleBlobUpload(ctx, be, ns, checkpointID, entries, normCfg)
		if err != nil {
			return nil, err
		}
		res.UploadDuration = upDur
		res.UploadMetrics = be.Metrics()

		be.ResetMetrics()
		downFiles, _, downDur, err := naiveSingleBlobDownload(ctx, be, ns, checkpointID, nil, normCfg)
		if err != nil {
			return nil, err
		}
		res.DownloadDuration = downDur
		res.DownloadMetrics = be.Metrics()
		if err := verifyExactFiles(expectedByPath, downFiles, nil); err != nil {
			return nil, err
		}

		if len(selectivePaths) > 0 {
			be.ResetMetrics()
			selFiles, blobBytes, selDur, err := naiveSingleBlobDownload(ctx, be, ns, checkpointID, selectivePaths, normCfg)
			if err != nil {
				return nil, err
			}
			selMetrics := be.Metrics()
			if err := verifyExactFiles(expectedByPath, selFiles, selectivePaths); err != nil {
				return nil, err
			}
			var reqBytes int64
			for _, p := range selectivePaths {
				reqBytes += int64(len(expectedByPath[p]))
			}
			res.SelectiveDuration = selDur
			res.SelectiveReqBytes = reqBytes
			res.SelectiveFetchedBytes = blobBytes
			if reqBytes > 0 {
				res.SelectiveReadAmp = float64(blobBytes) / float64(reqBytes)
			} else {
				res.SelectiveReadAmp = 1.0
			}
			res.SelectiveGETs = selMetrics.TotalReadRequests()
			if selDur > 0 {
				res.SelectiveMiBPerSec = float64(reqBytes) / selDur.Seconds() / mibFloat
			}
		}

	default:
		return nil, fmt.Errorf("%w: unknown regime %q", ErrInvalidConfig, regime)
	}

	if res.UploadDuration > 0 {
		res.UploadBytesPerSec = float64(logicalBytes) / res.UploadDuration.Seconds()
		res.UploadMiBPerSec = res.UploadBytesPerSec / mibFloat
	}
	if res.DownloadDuration > 0 {
		res.DownloadBytesPerSec = float64(logicalBytes) / res.DownloadDuration.Seconds()
		res.DownloadMiBPerSec = res.DownloadBytesPerSec / mibFloat
	}
	res.UploadPUTsPerGiB = res.UploadMetrics.WriteRequestsPerGiB(logicalBytes)
	res.DownloadGETsPerGiB = res.DownloadMetrics.ReadRequestsPerGiB(logicalBytes)
	res.BitExactVerified = true
	return res, nil
}

func verifyExactFiles(expected map[string][]byte, got map[string][]byte, subset []string) error {
	keys := subset
	if keys == nil {
		if len(got) != len(expected) {
			return fmt.Errorf("file count mismatch: got %d want %d", len(got), len(expected))
		}
		keys = make([]string, 0, len(expected))
		for k := range expected {
			keys = append(keys, k)
		}
	} else if len(got) != len(subset) {
		return fmt.Errorf("selective file count mismatch: got %d want %d", len(got), len(subset))
	}
	for _, k := range keys {
		wantBytes, ok := expected[k]
		if !ok {
			return fmt.Errorf("unexpected key %q", k)
		}
		gotBytes, ok := got[k]
		if !ok {
			return fmt.Errorf("missing key %q", k)
		}
		if len(gotBytes) != len(wantBytes) {
			return fmt.Errorf("length mismatch for %q: got %d want %d", k, len(gotBytes), len(wantBytes))
		}
		if blake3.Sum256(gotBytes) != blake3.Sum256(wantBytes) {
			return fmt.Errorf("content mismatch for %q", k)
		}
	}
	return nil
}

func naivePerObjectUpload(ctx context.Context, be backend.Backend, ns Namespace, checkpointID string, entries []Entry, workers int, cfg Config) (time.Duration, error) {
	start := time.Now()
	type item struct {
		path      string
		key       string
		digestHex string
		data      []byte
	}
	items := make([]item, 0, len(entries))
	var totalBytes int64
	for _, e := range entries {
		sum := blake3.Sum256(e.Data)
		h := hex.EncodeToString(sum[:])
		items = append(items, item{
			path:      e.Path,
			key:       ns.DirectKey(sum),
			digestHex: h,
			data:      e.Data,
		})
		totalBytes += int64(len(e.Data))
	}
	sort.Slice(items, func(i, j int) bool { return items[i].path < items[j].path })

	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan int, len(items))
	for i := range items {
		jobs <- i
	}
	close(jobs)

	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		firstErr error
	)
	for w := 0; w < min(workers, max(1, len(items))); w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range jobs {
				if workCtx.Err() != nil {
					return
				}
				it := items[idx]
				if len(it.data) == 0 {
					continue
				}
				if err := be.Put(workCtx, it.key, it.data); err != nil {
					mu.Lock()
					if firstErr == nil {
						firstErr = err
						cancel()
					}
					mu.Unlock()
					return
				}
			}
		}()
	}
	wg.Wait()
	if firstErr != nil {
		return time.Since(start), firstErr
	}

	groupSeen := make(map[string]struct{}, len(items))
	groups := make([]GroupObjectMeta, 0, len(items))
	mEntries := make([]ManifestEntry, 0, len(items))
	for _, it := range items {
		if len(it.data) == 0 {
			mEntries = append(mEntries, ManifestEntry{Path: it.path, Size: 0, BLAKE3: it.digestHex})
			continue
		}
		if _, dup := groupSeen[it.key]; !dup {
			groupSeen[it.key] = struct{}{}
			groups = append(groups, GroupObjectMeta{
				ObjectKey:    it.key,
				Size:         int64(len(it.data)),
				BLAKE3:       it.digestHex,
				ItemCount:    1,
				DirectBypass: true,
			})
		}
		mEntries = append(mEntries, ManifestEntry{
			Path:         it.path,
			Size:         int64(len(it.data)),
			BLAKE3:       it.digestHex,
			ObjectKey:    it.key,
			Offset:       0,
			Length:       int64(len(it.data)),
			DirectBypass: true,
		})
	}

	m := &Manifest{
		SchemaVersion:     SchemaVersion,
		CheckpointID:      checkpointID,
		TenantID:          ns.TenantID,
		VolumeID:          ns.VolumeID,
		Strategy:          string(RegimeNaivePerObjectConcurrent),
		CreatedUnixNano:   time.Now().UTC().UnixNano(),
		TotalLogicalBytes: totalBytes,
		TotalStoredBytes:  totalBytes,
		GroupObjects:      groups,
		Entries:           mEntries,
	}
	raw, err := m.MarshalSigned()
	if err != nil {
		return time.Since(start), err
	}
	if cfg.RequireConditionalHead {
		if _, err := be.PutIfAbsent(ctx, ns.HeadKey(checkpointID), raw); err != nil {
			return time.Since(start), err
		}
	} else {
		if err := be.Put(ctx, ns.HeadKey(checkpointID), raw); err != nil {
			return time.Since(start), err
		}
	}
	return time.Since(start), nil
}

func naivePerObjectDownload(ctx context.Context, be backend.Backend, ns Namespace, checkpointID string, paths []string, workers int, cfg Config) (map[string][]byte, time.Duration, error) {
	start := time.Now()
	raw, _, err := be.Get(ctx, ns.HeadKey(checkpointID), cfg.MaxManifestBytes)
	if err != nil {
		return nil, time.Since(start), err
	}
	m, err := UnmarshalManifest(raw, ns)
	if err != nil {
		return nil, time.Since(start), err
	}

	var targets []ManifestEntry
	if paths == nil {
		targets = m.Entries
	} else {
		byPath := make(map[string]ManifestEntry, len(m.Entries))
		for _, e := range m.Entries {
			byPath[e.Path] = e
		}
		for _, p := range paths {
			e, ok := byPath[p]
			if !ok {
				return nil, time.Since(start), fmt.Errorf("%w: %s", ErrEntryNotFound, p)
			}
			targets = append(targets, e)
		}
	}

	files := make(map[string][]byte, len(targets))
	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan int, len(targets))
	for i := range targets {
		jobs <- i
	}
	close(jobs)

	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		firstErr error
	)
	for w := 0; w < min(workers, max(1, len(targets))); w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range jobs {
				if workCtx.Err() != nil {
					return
				}
				ent := targets[idx]
				if ent.Size == 0 {
					mu.Lock()
					files[ent.Path] = []byte{}
					mu.Unlock()
					continue
				}
				data, _, err := be.Get(workCtx, ent.ObjectKey, ent.Size)
				if err != nil {
					mu.Lock()
					if firstErr == nil {
						firstErr = err
						cancel()
					}
					mu.Unlock()
					return
				}
				sum := blake3.Sum256(data)
				if hex.EncodeToString(sum[:]) != ent.BLAKE3 {
					mu.Lock()
					if firstErr == nil {
						firstErr = fmt.Errorf("%w: %s", ErrCorruptData, ent.Path)
						cancel()
					}
					mu.Unlock()
					return
				}
				mu.Lock()
				files[ent.Path] = data
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if firstErr != nil {
		return nil, time.Since(start), firstErr
	}
	return files, time.Since(start), nil
}

// naiveSingleBlobUpload encodes all checkpoint files into one monolithic
// length-prefixed archive object (modeling a single tar/zip checkpoint blob).
func naiveSingleBlobUpload(ctx context.Context, be backend.Backend, ns Namespace, checkpointID string, entries []Entry, cfg Config) (time.Duration, error) {
	start := time.Now()
	sorted := make([]Entry, len(entries))
	copy(sorted, entries)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Path < sorted[j].Path })

	var totalLogical int64
	var blobLen int64 = 4 // entry count uint32
	for _, e := range sorted {
		totalLogical += int64(len(e.Data))
		blobLen += 4 + int64(len(e.Path)) + 8 + int64(len(e.Data)) + 32
	}

	buf := make([]byte, 0, blobLen)
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(sorted)))
	for _, e := range sorted {
		sum := blake3.Sum256(e.Data)
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(e.Path)))
		buf = append(buf, e.Path...)
		buf = binary.LittleEndian.AppendUint64(buf, uint64(len(e.Data)))
		buf = append(buf, e.Data...)
		buf = append(buf, sum[:]...)
	}

	blobSum := blake3.Sum256(buf)
	blobHex := hex.EncodeToString(blobSum[:])
	blobKey := ns.PackKey(blobSum)

	if err := be.Put(ctx, blobKey, buf); err != nil {
		return time.Since(start), err
	}

	m := &Manifest{
		SchemaVersion:     SchemaVersion,
		CheckpointID:      checkpointID,
		TenantID:          ns.TenantID,
		VolumeID:          ns.VolumeID,
		Strategy:          string(RegimeNaiveSingleTarBlob),
		CreatedUnixNano:   time.Now().UTC().UnixNano(),
		TotalLogicalBytes: totalLogical,
		TotalStoredBytes:  int64(len(buf)),
		GroupObjects: []GroupObjectMeta{
			{
				ObjectKey:    blobKey,
				Size:         int64(len(buf)),
				BLAKE3:       blobHex,
				ItemCount:    len(sorted),
				DirectBypass: false,
			},
		},
		Entries: nil,
	}
	raw, err := m.MarshalSigned()
	if err != nil {
		return time.Since(start), err
	}
	if cfg.RequireConditionalHead {
		if _, err := be.PutIfAbsent(ctx, ns.HeadKey(checkpointID), raw); err != nil {
			return time.Since(start), err
		}
	} else {
		if err := be.Put(ctx, ns.HeadKey(checkpointID), raw); err != nil {
			return time.Since(start), err
		}
	}
	return time.Since(start), nil
}

func naiveSingleBlobDownload(ctx context.Context, be backend.Backend, ns Namespace, checkpointID string, paths []string, cfg Config) (map[string][]byte, int64, time.Duration, error) {
	start := time.Now()
	raw, _, err := be.Get(ctx, ns.HeadKey(checkpointID), cfg.MaxManifestBytes)
	if err != nil {
		return nil, 0, time.Since(start), err
	}
	m, err := UnmarshalManifest(raw, ns)
	if err != nil {
		return nil, 0, time.Since(start), err
	}
	if len(m.GroupObjects) != 1 {
		return nil, 0, time.Since(start), fmt.Errorf("%w: expected 1 tar blob, got %d", ErrCorruptManifest, len(m.GroupObjects))
	}
	blobMeta := m.GroupObjects[0]
	// A monolithic tar/archive blob must be fetched in full even for selective restores.
	buf, _, err := be.Get(ctx, blobMeta.ObjectKey, blobMeta.Size)
	if err != nil {
		return nil, 0, time.Since(start), err
	}
	sum := blake3.Sum256(buf)
	if hex.EncodeToString(sum[:]) != blobMeta.BLAKE3 {
		return nil, int64(len(buf)), time.Since(start), fmt.Errorf("%w: tar blob checksum mismatch", ErrCorruptData)
	}

	var wantSet map[string]struct{}
	if paths != nil {
		wantSet = make(map[string]struct{}, len(paths))
		for _, p := range paths {
			wantSet[p] = struct{}{}
		}
	}

	if len(buf) < 4 {
		return nil, int64(len(buf)), time.Since(start), ErrCorruptData
	}
	count := int(binary.LittleEndian.Uint32(buf[:4]))
	pos := 4
	files := make(map[string][]byte, count)
	for i := 0; i < count; i++ {
		if pos+4 > len(buf) {
			return nil, int64(len(buf)), time.Since(start), ErrCorruptData
		}
		pLen := int(binary.LittleEndian.Uint32(buf[pos : pos+4]))
		pos += 4
		if pos+pLen+8 > len(buf) {
			return nil, int64(len(buf)), time.Since(start), ErrCorruptData
		}
		p := string(buf[pos : pos+pLen])
		pos += pLen
		dLen := int(binary.LittleEndian.Uint64(buf[pos : pos+8]))
		pos += 8
		if pos+dLen+32 > len(buf) {
			return nil, int64(len(buf)), time.Since(start), ErrCorruptData
		}
		data := buf[pos : pos+dLen]
		pos += dLen
		wantSum := buf[pos : pos+32]
		pos += 32

		if wantSet != nil {
			if _, ok := wantSet[p]; !ok {
				continue
			}
		}
		gotSum := blake3.Sum256(data)
		if string(gotSum[:]) != string(wantSum) {
			return nil, int64(len(buf)), time.Since(start), fmt.Errorf("%w: tar entry %q mismatch", ErrCorruptData, p)
		}
		owned := make([]byte, dLen)
		copy(owned, data)
		files[p] = owned
	}
	return files, int64(len(buf)), time.Since(start), nil
}
