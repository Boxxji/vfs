// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package aiturbo

import (
	"encoding/hex"
	"fmt"
	"path"
	"sort"

	"github.com/zeebo/blake3"

	"github.com/hanzoai/vfs/pkg/backend"
)

// PlannedItem is a validated entry assigned to an offset within an UploadGroup.
type PlannedItem struct {
	Path      string
	Size      int64
	Digest    [32]byte
	DigestHex string
	Offset    int64
	Data      []byte
}

// UploadGroup is one immutable S3 object to be uploaded (either a packed group
// of small/medium entries or a zero-copy direct-bypass large entry).
type UploadGroup struct {
	Namespace    Namespace
	ObjectKey    string
	Digest       [32]byte
	DigestHex    string
	TotalBytes   int64
	DirectBypass bool
	Items        []PlannedItem
}

// UploadPlan is the deterministic, multi-tenant-isolated bulk upload plan
// produced by PlanUpload.
type UploadPlan struct {
	Config               Config
	Groups               []UploadGroup
	EmptyEntries         []PlannedItem // 0-byte files recorded in manifest without backend PUTs
	EmptyNamespaces      []Namespace   // namespaces corresponding 1:1 with EmptyEntries
	TotalLogicalBytes    int64
	TotalStoredBytes     int64
	DirectObjectCount    int
	PackedGroupCount     int
	TotalEntries         int
	TenantNamespaceCount int
}

// PlanUpload constructs a deterministic AITURBO grouped upload plan from entries.
//
// Invariants enforced:
//   - Strict multi-tenant isolation: entries are partitioned by (TenantID, VolumeID)
//     before any grouping; no UploadGroup ever mixes entries from different namespaces.
//   - Large-object direct bypass: entries with Size >= cfg.LargeObjectThreshold are
//     routed directly without copying into a pack buffer.
//   - Locality-preserving size-bounded packing: small/medium entries are ordered by
//     (directory, path) so related checkpoint layer files land at contiguous offsets
//     within bounded packs (<= cfg.TargetGroupBytes), enabling single Range-GET
//     selective restores with minimal gap amplification.
func PlanUpload(entries []Entry, cfg Config) (*UploadPlan, error) {
	normCfg, err := cfg.Normalized()
	if err != nil {
		return nil, err
	}

	type nsBucket struct {
		ns      Namespace
		entries []Entry
	}

	byNS := make(map[Namespace]*nsBucket)
	nsOrder := make([]Namespace, 0)

	for i, e := range entries {
		ns := Namespace{TenantID: e.TenantID, VolumeID: e.VolumeID}
		if err := ns.Validate(); err != nil {
			return nil, fmt.Errorf("entry[%d]: %w", i, err)
		}
		cleanPath, err := ValidateEntryPath(e.Path)
		if err != nil {
			return nil, fmt.Errorf("entry[%d]: %w", i, err)
		}
		e.Path = cleanPath

		if e.Size == 0 && len(e.Data) > 0 {
			e.Size = int64(len(e.Data))
		}
		if e.Size < 0 || int64(len(e.Data)) != e.Size {
			return nil, fmt.Errorf("%w: entry %q declared size=%d, len(data)=%d",
				ErrInvalidEntry, e.Path, e.Size, len(e.Data))
		}

		b, ok := byNS[ns]
		if !ok {
			b = &nsBucket{ns: ns}
			byNS[ns] = b
			nsOrder = append(nsOrder, ns)
		}
		b.entries = append(b.entries, e)
	}

	// Deterministic namespace ordering.
	sort.Slice(nsOrder, func(i, j int) bool {
		if nsOrder[i].TenantID != nsOrder[j].TenantID {
			return nsOrder[i].TenantID < nsOrder[j].TenantID
		}
		return nsOrder[i].VolumeID < nsOrder[j].VolumeID
	})

	plan := &UploadPlan{
		Config:               normCfg,
		TenantNamespaceCount: len(nsOrder),
		TotalEntries:         len(entries),
	}

	for _, ns := range nsOrder {
		bucket := byNS[ns]
		seenPaths := make(map[string]struct{}, len(bucket.entries))

		small := make([]PlannedItem, 0, len(bucket.entries))
		large := make([]PlannedItem, 0)

		for _, e := range bucket.entries {
			if _, dup := seenPaths[e.Path]; dup {
				return nil, fmt.Errorf("%w: (%s,%s) path %q", ErrDuplicatePath, ns.TenantID, ns.VolumeID, e.Path)
			}
			seenPaths[e.Path] = struct{}{}

			sum := blake3.Sum256(e.Data)
			sumHex := hex.EncodeToString(sum[:])
			if e.BLAKE3 != "" && e.BLAKE3 != sumHex {
				return nil, fmt.Errorf("%w: entry %q blake3 mismatch got %s want %s",
					ErrInvalidEntry, e.Path, e.BLAKE3, sumHex)
			}

			item := PlannedItem{
				Path:      e.Path,
				Size:      e.Size,
				Digest:    sum,
				DigestHex: sumHex,
				Data:      e.Data,
			}
			plan.TotalLogicalBytes += item.Size

			if item.Size == 0 {
				plan.EmptyEntries = append(plan.EmptyEntries, item)
				plan.EmptyNamespaces = append(plan.EmptyNamespaces, ns)
				continue
			}

			if item.Size >= normCfg.LargeObjectThreshold {
				large = append(large, item)
			} else {
				small = append(small, item)
			}
		}

		// Sort large entries deterministically by path and route each via direct bypass.
		// Identical large blobs within the same namespace deduplicate cleanly by key.
		sort.Slice(large, func(i, j int) bool { return large[i].Path < large[j].Path })
		directIndex := make(map[string]int, len(large))
		for _, item := range large {
			item.Offset = 0
			key := ns.DirectKey(item.Digest)
			if idx, exists := directIndex[key]; exists {
				plan.Groups[idx].Items = append(plan.Groups[idx].Items, item)
				continue
			}
			directIndex[key] = len(plan.Groups)
			plan.Groups = append(plan.Groups, UploadGroup{
				Namespace:    ns,
				ObjectKey:    key,
				Digest:       item.Digest,
				DigestHex:    item.DigestHex,
				TotalBytes:   item.Size,
				DirectBypass: true,
				Items:        []PlannedItem{item},
			})
			plan.TotalStoredBytes += item.Size
			plan.DirectObjectCount++
		}

		// Sort small/medium entries by (dir, path) to preserve spatial locality
		// across checkpoint modules/layers, then pack into size-bounded bins.
		sort.Slice(small, func(i, j int) bool {
			di, dj := path.Dir(small[i].Path), path.Dir(small[j].Path)
			if di != dj {
				return di < dj
			}
			return small[i].Path < small[j].Path
		})

		type packBin struct {
			items      []PlannedItem
			totalBytes int64
		}
		var bins []*packBin

		for _, item := range small {
			var chosen *packBin
			// Locality-first: prefer the most recent open bin so directory-adjacent
			// files remain contiguous at consecutive offsets.
			if n := len(bins); n > 0 && bins[n-1].totalBytes+item.Size <= normCfg.TargetGroupBytes {
				chosen = bins[n-1]
			} else {
				// Fallback to tightest-fitting earlier open bin (best-fit) before
				// opening a new bin, minimizing total PUTs/GiB.
				bestSlack := int64(1<<62 - 1)
				for _, b := range bins {
					if b.totalBytes+item.Size <= normCfg.TargetGroupBytes {
						slack := normCfg.TargetGroupBytes - (b.totalBytes + item.Size)
						if slack < bestSlack {
							bestSlack = slack
							chosen = b
						}
					}
				}
			}
			if chosen == nil {
				chosen = &packBin{}
				bins = append(bins, chosen)
			}
			item.Offset = chosen.totalBytes
			chosen.items = append(chosen.items, item)
			chosen.totalBytes += item.Size
		}

		packIndex := make(map[string]int, len(bins))
		for _, b := range bins {
			h := blake3.New()
			for _, it := range b.items {
				_, _ = h.Write(it.Data)
			}
			var digest [32]byte
			copy(digest[:], h.Sum(nil))
			digestHex := hex.EncodeToString(digest[:])
			key := ns.PackKey(digest)

			if idx, exists := packIndex[key]; exists {
				// Identical pack bytes within the same namespace share one immutable object.
				plan.Groups[idx].Items = append(plan.Groups[idx].Items, b.items...)
				continue
			}
			packIndex[key] = len(plan.Groups)
			plan.Groups = append(plan.Groups, UploadGroup{
				Namespace:    ns,
				ObjectKey:    key,
				Digest:       digest,
				DigestHex:    digestHex,
				TotalBytes:   b.totalBytes,
				DirectBypass: false,
				Items:        b.items,
			})
			plan.TotalStoredBytes += b.totalBytes
			plan.PackedGroupCount++
		}
	}

	if err := VerifyTenantIsolation(plan); err != nil {
		return nil, err
	}
	return plan, nil
}

// VerifyTenantIsolation independently audits an UploadPlan to guarantee zero
// cross-tenant co-residency, valid namespace-scoped keys, and non-overlapping
// object ownership across distinct (TenantID, VolumeID) namespaces.
func VerifyTenantIsolation(plan *UploadPlan) error {
	if plan == nil {
		return fmt.Errorf("%w: nil plan", ErrTenantIsolation)
	}
	keyOwner := make(map[string]Namespace, len(plan.Groups))
	for i, g := range plan.Groups {
		if err := g.Namespace.Validate(); err != nil {
			return fmt.Errorf("%w: group[%d] invalid namespace: %v", ErrTenantIsolation, i, err)
		}
		if err := backend.ValidKey(g.ObjectKey); err != nil {
			return fmt.Errorf("%w: group[%d] invalid key %q: %v", ErrTenantIsolation, i, g.ObjectKey, err)
		}
		if !g.Namespace.OwnsKey(g.ObjectKey) {
			return fmt.Errorf("%w: group[%d] key %q outside namespace (%s,%s)",
				ErrTenantIsolation, i, g.ObjectKey, g.Namespace.TenantID, g.Namespace.VolumeID)
		}
		if prev, exists := keyOwner[g.ObjectKey]; exists && prev != g.Namespace {
			return fmt.Errorf("%w: key %q shared between (%s,%s) and (%s,%s)",
				ErrTenantIsolation, g.ObjectKey,
				prev.TenantID, prev.VolumeID, g.Namespace.TenantID, g.Namespace.VolumeID)
		}
		keyOwner[g.ObjectKey] = g.Namespace
	}
	return nil
}

// PlannedRange describes a single backend Get or GetRange request that fetches
// one or more checkpoint entries from an immutable group or direct object.
type PlannedRange struct {
	ObjectKey      string
	Offset         int64
	Length         int64
	FullObject     bool
	Entries        []ManifestEntry
	RequestedBytes int64
	GapBytes       int64
}

// DownloadPlan is the deterministic, gap-coalesced Range-GET plan produced by
// PlanDownload.
type DownloadPlan struct {
	Namespace           Namespace
	CheckpointID        string
	Ranges              []PlannedRange
	EmptyEntries        []ManifestEntry
	TotalRequestedBytes int64
	TotalFetchedBytes   int64
	TotalGapBytes       int64
	ReadAmplification   float64
	EntryCount          int
}

// PlanDownload plans a full (requestedPaths == nil) or selective checkpoint
// download from manifest, coalescing nearby slices within the same group object
// separated by at most cfg.MaxGapBytes (up to cfg.MaxCoalescedRangeBytes).
func PlanDownload(manifest *Manifest, requestedPaths []string, cfg Config) (*DownloadPlan, error) {
	if manifest == nil {
		return nil, fmt.Errorf("%w: nil manifest", ErrCorruptManifest)
	}
	normCfg, err := cfg.Normalized()
	if err != nil {
		return nil, err
	}
	ns := Namespace{TenantID: manifest.TenantID, VolumeID: manifest.VolumeID}
	if err := ns.Validate(); err != nil {
		return nil, err
	}

	objSize := make(map[string]int64, len(manifest.GroupObjects))
	for _, g := range manifest.GroupObjects {
		if !ns.OwnsKey(g.ObjectKey) {
			return nil, fmt.Errorf("%w: manifest group %q outside namespace (%s,%s)",
				ErrTenantIsolation, g.ObjectKey, ns.TenantID, ns.VolumeID)
		}
		objSize[g.ObjectKey] = g.Size
	}

	selected := make([]ManifestEntry, 0, len(manifest.Entries))
	if requestedPaths == nil {
		selected = append(selected, manifest.Entries...)
	} else {
		byPath := make(map[string]ManifestEntry, len(manifest.Entries))
		for _, e := range manifest.Entries {
			byPath[e.Path] = e
		}
		seen := make(map[string]struct{}, len(requestedPaths))
		for _, p := range requestedPaths {
			cleanPath, err := ValidateEntryPath(p)
			if err != nil {
				return nil, err
			}
			if _, dup := seen[cleanPath]; dup {
				continue
			}
			seen[cleanPath] = struct{}{}
			e, ok := byPath[cleanPath]
			if !ok {
				return nil, fmt.Errorf("%w: %s", ErrEntryNotFound, cleanPath)
			}
			selected = append(selected, e)
		}
	}

	plan := &DownloadPlan{
		Namespace:    ns,
		CheckpointID: manifest.CheckpointID,
		EntryCount:   len(selected),
	}

	byObj := make(map[string][]ManifestEntry)
	objOrder := make([]string, 0)

	for _, e := range selected {
		if e.Size == 0 && e.Length == 0 {
			plan.EmptyEntries = append(plan.EmptyEntries, e)
			continue
		}
		if !ns.OwnsKey(e.ObjectKey) {
			return nil, fmt.Errorf("%w: entry %q key %q outside namespace (%s,%s)",
				ErrTenantIsolation, e.Path, e.ObjectKey, ns.TenantID, ns.VolumeID)
		}
		if _, ok := byObj[e.ObjectKey]; !ok {
			objOrder = append(objOrder, e.ObjectKey)
		}
		byObj[e.ObjectKey] = append(byObj[e.ObjectKey], e)
	}

	sort.Strings(objOrder)

	for _, key := range objOrder {
		items := byObj[key]
		sort.Slice(items, func(i, j int) bool {
			if items[i].Offset != items[j].Offset {
				return items[i].Offset < items[j].Offset
			}
			if items[i].Length != items[j].Length {
				return items[i].Length < items[j].Length
			}
			return items[i].Path < items[j].Path
		})

		totalObjSize := objSize[key]

		curStart := items[0].Offset
		curEnd := items[0].Offset + items[0].Length
		curEntries := []ManifestEntry{items[0]}

		flushRange := func() {
			rangeLen := curEnd - curStart
			reqCovered := coveredBytes(curEntries)
			gapBytes := rangeLen - reqCovered
			if gapBytes < 0 {
				gapBytes = 0
			}
			fullObj := curStart == 0 && totalObjSize > 0 && rangeLen == totalObjSize
			plan.Ranges = append(plan.Ranges, PlannedRange{
				ObjectKey:      key,
				Offset:         curStart,
				Length:         rangeLen,
				FullObject:     fullObj,
				Entries:        curEntries,
				RequestedBytes: reqCovered,
				GapBytes:       gapBytes,
			})
			plan.TotalRequestedBytes += reqCovered
			plan.TotalFetchedBytes += rangeLen
			plan.TotalGapBytes += gapBytes
		}

		for i := 1; i < len(items); i++ {
			next := items[i]
			nextStart := next.Offset
			nextEnd := next.Offset + next.Length

			gap := nextStart - curEnd
			if gap < 0 {
				gap = 0
			}
			mergedEnd := curEnd
			if nextEnd > mergedEnd {
				mergedEnd = nextEnd
			}
			mergedLen := mergedEnd - curStart

			// Coalesce if gap <= MaxGapBytes and merged span stays within MaxCoalescedRangeBytes.
			// Single items larger than MaxCoalescedRangeBytes are always emitted on their own.
			if gap <= normCfg.MaxGapBytes && mergedLen <= normCfg.MaxCoalescedRangeBytes {
				curEnd = mergedEnd
				curEntries = append(curEntries, next)
			} else {
				flushRange()
				curStart = nextStart
				curEnd = nextEnd
				curEntries = []ManifestEntry{next}
			}
		}
		flushRange()
	}

	if plan.TotalRequestedBytes > 0 {
		plan.ReadAmplification = float64(plan.TotalFetchedBytes) / float64(plan.TotalRequestedBytes)
	} else {
		plan.ReadAmplification = 1.0
	}
	return plan, nil
}

// coveredBytes computes the exact union length of sorted [Offset, Offset+Length)
// intervals so overlapping deduplicated entries never produce negative gap counts.
func coveredBytes(sorted []ManifestEntry) int64 {
	if len(sorted) == 0 {
		return 0
	}
	var total int64
	curStart := sorted[0].Offset
	curEnd := sorted[0].Offset + sorted[0].Length
	for i := 1; i < len(sorted); i++ {
		s := sorted[i].Offset
		e := sorted[i].Offset + sorted[i].Length
		if s <= curEnd {
			if e > curEnd {
				curEnd = e
			}
		} else {
			total += curEnd - curStart
			curStart = s
			curEnd = e
		}
	}
	total += curEnd - curStart
	return total
}
