// Copyright 2026 Hanzo AI Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package dogi

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"

	"github.com/hanzoai/vfs/pkg/backend"
)

type packID [16]byte

func newPackID() (packID, error) {
	var id packID
	if _, err := rand.Read(id[:]); err != nil {
		return packID{}, err
	}
	return id, nil
}

func (id packID) hex() string {
	return hex.EncodeToString(id[:])
}

type packEntry struct {
	path             string
	dir              string
	chunkID          [32]byte
	off              int64
	frameLen         int64
	plainLen         int64
	createdEpoch     uint64
	oracleDeathEpoch uint64
	gcGen            int
	live             bool
}

type openStream struct {
	class     LifetimeClass
	id        packID
	buf       []byte
	entries   []*packEntry
	plainSize int64
}

type sealedPack struct {
	id              packID
	seq             uint64
	key             string
	class           LifetimeClass
	entries         []*packEntry
	totalPlainBytes int64
	liveBytes       int64
	deadBytes       int64
	storedBytes     int64
	createdEpoch    uint64
}

type liveRecord struct {
	entry *packEntry
	// Exactly one of open or sealed is non-nil.
	open   *openStream
	sealed *sealedPack
}

// Packer packs incoming workspace chunks into bounded, immutable,
// namespace-isolated packs stored in a backend.Backend, and tracks exact live
// vs. dead bytes per pack for collection-time compaction evaluation.
//
// Packer is safe for concurrent use by multiple goroutines.
type Packer struct {
	be         backend.Backend
	cfg        Config
	nsPrefix   string
	classifier *Classifier

	mu          sync.Mutex
	streams     [NumClasses]*openStream
	packs       map[packID]*sealedPack
	live        map[string]*liveRecord
	nextPackSeq uint64

	// Cumulative telemetry.
	totalIngestBytes  int64
	totalIngestChunks int64
	liveBytes         int64
	liveChunks        int64
	sealedStoredBytes int64
	peakStoredBytes   int64
	peakStorageAmp    float64
	storageAmpSum     float64
	storageAmpSamples int64

	liveCopyBytes          int64
	liveCopyChunks         int64
	packsUploaded          int64
	packsDeleted           int64
	zeroCopyReclaimedPacks int64
	compactedPacks         int64
	gcGetRangeOps          int64
	gcCycles               int64
	classCounts            [NumClasses]int64
	epochSeries            []EpochMetric
	lastEpochLiveCopy      int64
}

// NewPacker creates a new namespace-isolated Packer backed by be.
func NewPacker(be backend.Backend, cfg Config) (*Packer, error) {
	if be == nil {
		return nil, fmt.Errorf("%w: backend is required", ErrInvalidConfig)
	}
	if err := cfg.normalize(); err != nil {
		return nil, err
	}
	p := &Packer{
		be:         be,
		cfg:        cfg,
		nsPrefix:   cfg.Namespace.KeyPrefix(),
		classifier: NewClassifier(cfg.Policy),
		packs:      make(map[packID]*sealedPack),
		live:       make(map[string]*liveRecord),
	}
	for i := 0; i < NumClasses; i++ {
		p.streams[i] = &openStream{class: LifetimeClass(i)}
	}
	return p, nil
}

// Classifier returns the Packer's lifetime classifier for inspection in tests
// and diagnostics.
func (p *Packer) Classifier() *Classifier {
	return p.classifier
}

// Namespace returns the strictly isolated (TenantID, VolumeID) namespace bound
// to this Packer.
func (p *Packer) Namespace() Namespace {
	return p.cfg.Namespace
}

func (p *Packer) packKey(id packID) string {
	h := id.hex()
	return p.nsPrefix + "/" + h[:2] + "/" + h
}

// PutChunk writes a chunk for meta.Path into the lifetime stream selected by
// the configured Policy. If meta.Path already exists, its previous chunk is
// invalidated (overwrite) and feeds the online lifetime tracker.
//
// Returns ErrTenantIsolation if meta.Namespace does not match the Packer's namespace.
func (p *Packer) PutChunk(ctx context.Context, meta ChunkMeta, data []byte) ([32]byte, LifetimeClass, error) {
	if meta.Namespace != p.cfg.Namespace {
		return [32]byte{}, 0, fmt.Errorf("%w: chunk namespace (%s/%s) != packer namespace (%s/%s)",
			ErrTenantIsolation, meta.Namespace.TenantID, meta.Namespace.VolumeID,
			p.cfg.Namespace.TenantID, p.cfg.Namespace.VolumeID)
	}
	if meta.Path == "" {
		return [32]byte{}, 0, fmt.Errorf("%w: empty path", ErrInvalidChunk)
	}
	if len(data) == 0 {
		return [32]byte{}, 0, fmt.Errorf("%w: empty payload", ErrInvalidChunk)
	}

	cleanPath, dir, _ := normalizePath(meta.Path)
	id := sumChunk(data)

	// Encode and optionally seal outside the packer mutex so encryption scales concurrently.
	frame, err := encodeChunkFrame(p.cfg.Namespace, id, data, p.cfg.Sealer)
	if err != nil {
		return [32]byte{}, 0, err
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	prevRec, isOverwrite := p.live[cleanPath]
	if isOverwrite {
		p.invalidatePathLocked(cleanPath, meta.Epoch, true)
	}

	cls := p.classifier.ClassifyWrite(meta, len(data))
	p.classifier.Tracker().RecordWrite(dir, len(data), meta.Epoch, isOverwrite)

	entry := &packEntry{
		path:             cleanPath,
		dir:              dir,
		chunkID:          id,
		plainLen:         int64(len(data)),
		createdEpoch:     meta.Epoch,
		oracleDeathEpoch: meta.OracleDeathEpoch,
		gcGen:            0,
		live:             true,
	}

	if err := p.appendToStreamLocked(ctx, cls, entry, frame, meta.Epoch); err != nil {
		// Restore previously live record and counters so failed auto-flushes leave
		// packer state and accounting invariants strictly consistent.
		if isOverwrite {
			prevRec.entry.live = true
			p.live[cleanPath] = prevRec
			p.liveBytes += prevRec.entry.plainLen
			p.liveChunks++
			if prevRec.sealed != nil {
				prevRec.sealed.liveBytes += prevRec.entry.plainLen
				prevRec.sealed.deadBytes -= prevRec.entry.plainLen
			}
		}
		return [32]byte{}, 0, err
	}

	p.totalIngestBytes += int64(len(data))
	p.totalIngestChunks++
	p.liveBytes += int64(len(data))
	p.liveChunks++
	p.classCounts[cls]++
	p.updatePeakLocked()

	return id, cls, nil
}

// DeletePath invalidates the currently live chunk at rawPath and reports the
// observed lifetime to the online directory tracker. Deleting a non-existent
// path returns false, nil.
func (p *Packer) DeletePath(rawPath string, epoch uint64) (bool, error) {
	if rawPath == "" {
		return false, fmt.Errorf("%w: empty path", ErrInvalidChunk)
	}
	cleanPath, _, _ := normalizePath(rawPath)

	p.mu.Lock()
	defer p.mu.Unlock()
	ok := p.invalidatePathLocked(cleanPath, epoch, false)
	if ok {
		p.updatePeakLocked()
	}
	return ok, nil
}

func (p *Packer) invalidatePathLocked(cleanPath string, epoch uint64, isOverwrite bool) bool {
	rec, ok := p.live[cleanPath]
	if !ok {
		return false
	}
	delete(p.live, cleanPath)
	e := rec.entry
	if e.live {
		e.live = false
		p.liveBytes -= e.plainLen
		p.liveChunks--
		if rec.sealed != nil {
			rec.sealed.liveBytes -= e.plainLen
			rec.sealed.deadBytes += e.plainLen
		}
		p.classifier.Tracker().RecordDelete(e.dir, e.createdEpoch, epoch, isOverwrite)
	}
	return true
}

func (p *Packer) appendToStreamLocked(ctx context.Context, cls LifetimeClass, entry *packEntry, frame []byte, epoch uint64) error {
	streamIdx := int(cls)
	if p.cfg.Policy == PolicyFIFOMixed {
		streamIdx = 0
	}
	st := p.streams[streamIdx]
	prevBufLen := len(st.buf)
	prevEntriesLen := len(st.entries)
	prevPlainSize := st.plainSize
	prevID := st.id
	prevLiveRec, hadPrevLive := p.live[entry.path]

	if len(st.buf) == 0 {
		id, err := newPackID()
		if err != nil {
			return err
		}
		st.id = id
		hdr := encodePackHeader(LifetimeClass(streamIdx), p.cfg.Sealer != nil)
		st.buf = append(st.buf[:0], hdr[:]...)
		st.entries = st.entries[:0]
		st.plainSize = 0
	}

	entry.off = int64(len(st.buf))
	entry.frameLen = int64(len(frame))
	st.buf = append(st.buf, frame...)
	st.entries = append(st.entries, entry)
	st.plainSize += entry.plainLen
	p.live[entry.path] = &liveRecord{entry: entry, open: st}

	if len(st.buf) >= p.cfg.PackTargetBytes {
		if err := p.flushStreamLocked(ctx, streamIdx, epoch); err != nil {
			// Roll back stream buffer and live index atomically so a failed upload
			// never leaves an uncounted frame or dangling open entry.
			st.buf = st.buf[:prevBufLen]
			st.entries = st.entries[:prevEntriesLen]
			st.plainSize = prevPlainSize
			if prevBufLen == 0 {
				st.id = prevID
			}
			if hadPrevLive {
				p.live[entry.path] = prevLiveRec
			} else {
				delete(p.live, entry.path)
			}
			return err
		}
	}
	return nil
}

func (p *Packer) flushStreamLocked(ctx context.Context, streamIdx int, epoch uint64) error {
	st := p.streams[streamIdx]
	if len(st.entries) == 0 {
		return nil
	}

	key := p.packKey(st.id)
	payload := append([]byte(nil), st.buf...)
	if _, err := p.be.PutIfAbsent(ctx, key, payload); err != nil {
		if errors.Is(err, backend.ErrConflict) {
			// Reconcile ambiguous prior uploads whose acknowledgement was lost:
			// if the existing immutable object matches byte-for-byte, seal it directly;
			// otherwise allocate a fresh pack ID and retry without wedging the stream.
			existing, _, getErr := p.be.Get(ctx, key, int64(len(payload))+1)
			if getErr == nil && bytes.Equal(existing, payload) {
				err = nil
			} else {
				freshID, idErr := newPackID()
				if idErr != nil {
					return idErr
				}
				st.id = freshID
				key = p.packKey(freshID)
				_, err = p.be.PutIfAbsent(ctx, key, payload)
			}
		}
		if err != nil {
			return fmt.Errorf("dogi: put pack %s: %w", key, err)
		}
	}

	p.nextPackSeq++
	sp := &sealedPack{
		id:           st.id,
		seq:          p.nextPackSeq,
		key:          key,
		class:        st.class,
		entries:      append([]*packEntry(nil), st.entries...),
		storedBytes:  int64(len(payload)),
		createdEpoch: epoch,
	}
	for _, e := range sp.entries {
		sp.totalPlainBytes += e.plainLen
		if e.live {
			sp.liveBytes += e.plainLen
			if rec, ok := p.live[e.path]; ok && rec.entry == e {
				rec.open = nil
				rec.sealed = sp
			}
		} else {
			sp.deadBytes += e.plainLen
		}
	}

	p.packs[sp.id] = sp
	p.sealedStoredBytes += sp.storedBytes
	p.packsUploaded++

	// Reset open stream buffer.
	st.buf = st.buf[:0]
	st.entries = st.entries[:0]
	st.plainSize = 0
	p.updatePeakLocked()
	return nil
}

// FlushAll seals and uploads all non-empty open lifetime stream buffers to the
// backend (matching generation/checkpoint publication boundaries in VFS).
func (p *Packer) FlushAll(ctx context.Context, epoch uint64) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.flushAllLocked(ctx, epoch)
}

func (p *Packer) flushAllLocked(ctx context.Context, epoch uint64) error {
	for i := 0; i < NumClasses; i++ {
		if err := p.flushStreamLocked(ctx, i, epoch); err != nil {
			return err
		}
	}
	return nil
}

// ReadPath reads and verifies the currently live chunk for rawPath, using a
// ranged backend read (GetRange) when the chunk resides in a sealed pack, or
// directly from the open buffer if not yet flushed.
//
// If concurrent GC relocates a live chunk and deletes its old source pack
// between metadata lookup and GetRange, ReadPath detects the relocation and
// retries transparently against the current live record.
func (p *Packer) ReadPath(ctx context.Context, rawPath string) ([]byte, [32]byte, error) {
	if rawPath == "" {
		return nil, [32]byte{}, fmt.Errorf("%w: empty path", ErrInvalidChunk)
	}
	cleanPath, _, _ := normalizePath(rawPath)

	const maxRelocationRetries = 8
	for attempt := 0; attempt < maxRelocationRetries; attempt++ {
		p.mu.Lock()
		rec, ok := p.live[cleanPath]
		if !ok {
			p.mu.Unlock()
			return nil, [32]byte{}, fmt.Errorf("%w: %s", ErrChunkNotFound, cleanPath)
		}
		chunkID := rec.entry.chunkID
		off := rec.entry.off
		frameLen := rec.entry.frameLen
		var frame []byte
		var packKey string
		if rec.open != nil {
			frame = append([]byte(nil), rec.open.buf[off:off+frameLen]...)
		} else {
			packKey = rec.sealed.key
		}
		p.mu.Unlock()

		if frame == nil {
			var err error
			frame, err = p.be.GetRange(ctx, packKey, off, frameLen)
			if err != nil {
				if errors.Is(err, backend.ErrNotFound) {
					// Distinguish concurrent GC relocation / deletion from genuine pack corruption.
					p.mu.Lock()
					cur, stillLive := p.live[cleanPath]
					relocated := stillLive && (cur.open != nil ||
						cur.sealed.key != packKey ||
						cur.entry.off != off ||
						cur.entry.chunkID != chunkID)
					p.mu.Unlock()
					if !stillLive {
						return nil, [32]byte{}, fmt.Errorf("%w: %s", ErrChunkNotFound, cleanPath)
					}
					if relocated {
						continue
					}
					return nil, [32]byte{}, fmt.Errorf("%w: pack %s missing", ErrCorruptPack, packKey)
				}
				return nil, [32]byte{}, fmt.Errorf("dogi: read range %s: %w", packKey, err)
			}
		}

		plain, err := decodeChunkFrame(p.cfg.Namespace, chunkID, frame, p.cfg.Sealer)
		if err != nil {
			return nil, [32]byte{}, err
		}
		return plain, chunkID, nil
	}
	return nil, [32]byte{}, fmt.Errorf("%w: %s moved concurrently too many times", ErrCorruptPack, cleanPath)
}

// VerifyAllLive reads back EVERY currently live file from the backend (via
// ranged reads) and verifies bit-exact BLAKE3 integrity against expectedHashes.
func (p *Packer) VerifyAllLive(ctx context.Context, expectedHashes map[string][32]byte) error {
	p.mu.Lock()
	livePaths := make([]string, 0, len(p.live))
	for k := range p.live {
		livePaths = append(livePaths, k)
	}
	p.mu.Unlock()

	if expectedHashes != nil && len(livePaths) != len(expectedHashes) {
		return fmt.Errorf("%w: live path count %d != expected %d", ErrCorruptPack, len(livePaths), len(expectedHashes))
	}
	for _, lp := range livePaths {
		plain, id, err := p.ReadPath(ctx, lp)
		if err != nil {
			return fmt.Errorf("verify %s: %w", lp, err)
		}
		if sumChunk(plain) != id {
			return fmt.Errorf("%w: verify %s digest mismatch", ErrCorruptPack, lp)
		}
		if expectedHashes != nil {
			want, ok := expectedHashes[lp]
			if !ok {
				return fmt.Errorf("%w: unexpected live path %s", ErrCorruptPack, lp)
			}
			if id != want {
				return fmt.Errorf("%w: verify %s hash %x != expected %x", ErrCorruptPack, lp, id[:6], want[:6])
			}
		}
	}
	return nil
}

// AdvanceEpoch records epoch-end survival evidence for live chunks (so long-lived
// directories under opaque or inverted names converge in OnlineDirLifetimeTracker)
// and appends an EpochMetric snapshot for time-series analysis.
func (p *Packer) AdvanceEpoch(epoch uint64, gcStats GCStats) EpochMetric {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Feed survival observations for chunks that have lived >= 2 epochs.
	dirCounts := make(map[string]int)
	dirMaxAge := make(map[string]uint64)
	for _, rec := range p.live {
		e := rec.entry
		if epoch >= e.createdEpoch+2 {
			age := epoch - e.createdEpoch
			dirCounts[e.dir]++
			if age > dirMaxAge[e.dir] {
				dirMaxAge[e.dir] = age
			}
		}
	}
	for dir, cnt := range dirCounts {
		p.classifier.Tracker().RecordEpochSurvival(dir, cnt, dirMaxAge[dir])
	}

	stored := p.currentStoredBytesLocked()
	amp := 1.0
	if p.liveBytes > 0 {
		amp = float64(stored) / float64(p.liveBytes)
	}
	p.storageAmpSum += amp
	p.storageAmpSamples++
	if amp > p.peakStorageAmp {
		p.peakStorageAmp = amp
	}
	if stored > p.peakStoredBytes {
		p.peakStoredBytes = stored
	}

	waf := 1.0
	if p.totalIngestBytes > 0 {
		waf = float64(p.totalIngestBytes+p.liveCopyBytes) / float64(p.totalIngestBytes)
	}
	epochCopy := p.liveCopyBytes - p.lastEpochLiveCopy
	p.lastEpochLiveCopy = p.liveCopyBytes

	em := EpochMetric{
		Epoch:                  epoch,
		LiveBytes:              p.liveBytes,
		StoredBytes:            stored,
		StorageAmp:             amp,
		EpochLiveCopyBytes:     epochCopy,
		CumulativeLiveCopy:     p.liveCopyBytes,
		CumulativeIngest:       p.totalIngestBytes,
		CumulativeWAF:          waf,
		ZeroCopyReclaimedPacks: gcStats.ZeroCopyReclaimedPacks,
		CompactedPacks:         gcStats.CompactedPacks,
		ActivePacks:            len(p.packs),
	}
	p.epochSeries = append(p.epochSeries, em)
	return em
}

func (p *Packer) currentStoredBytesLocked() int64 {
	total := p.sealedStoredBytes
	for i := 0; i < NumClasses; i++ {
		total += int64(len(p.streams[i].buf))
	}
	return total
}

func (p *Packer) updatePeakLocked() {
	stored := p.currentStoredBytesLocked()
	if stored > p.peakStoredBytes {
		p.peakStoredBytes = stored
	}
}

// SnapshotMetrics returns an exact point-in-time snapshot of all packing, GC,
// write-amplification, and storage-amplification metrics.
func (p *Packer) SnapshotMetrics() Metrics {
	p.mu.Lock()
	defer p.mu.Unlock()

	stored := p.currentStoredBytesLocked()
	finalAmp := 1.0
	if p.liveBytes > 0 {
		finalAmp = float64(stored) / float64(p.liveBytes)
	}
	meanAmp := finalAmp
	peakAmp := p.peakStorageAmp
	if p.storageAmpSamples > 0 {
		meanAmp = p.storageAmpSum / float64(p.storageAmpSamples)
	} else {
		peakAmp = finalAmp
	}
	waf := 1.0
	if p.totalIngestBytes > 0 {
		waf = float64(p.totalIngestBytes+p.liveCopyBytes) / float64(p.totalIngestBytes)
	}

	series := make([]EpochMetric, len(p.epochSeries))
	copy(series, p.epochSeries)

	return Metrics{
		Policy:                 p.cfg.Policy,
		PackTargetBytes:        p.cfg.PackTargetBytes,
		GCDeadRatioThreshold:   p.cfg.GCDeadRatioThreshold,
		TotalIngestBytes:       p.totalIngestBytes,
		TotalIngestChunks:      p.totalIngestChunks,
		LiveBytes:              p.liveBytes,
		LiveChunks:             p.liveChunks,
		StoredBytes:            stored,
		ActivePacks:            len(p.packs),
		PeakStoredBytes:        p.peakStoredBytes,
		PeakStorageAmp:         peakAmp,
		MeanStorageAmp:         meanAmp,
		FinalStorageAmp:        finalAmp,
		LiveCopyBytes:          p.liveCopyBytes,
		LiveCopyChunks:         p.liveCopyChunks,
		WriteAmplification:     waf,
		PacksUploaded:          p.packsUploaded,
		PacksDeleted:           p.packsDeleted,
		ZeroCopyReclaimedPacks: p.zeroCopyReclaimedPacks,
		CompactedPacks:         p.compactedPacks,
		GCGetRangeOps:          p.gcGetRangeOps,
		GCCycles:               p.gcCycles,
		ClassCounts:            p.classCounts,
		EpochSeries:            series,
	}
}

// CheckAccountingInvariant verifies that internal live-byte accounting matches
// the sum of live bytes across all sealed packs and open buffers. Exposed for
// rigorous correctness testing.
func (p *Packer) CheckAccountingInvariant() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	var sumLive int64
	var sumChunks int64
	for _, sp := range p.packs {
		var packLive, packDead int64
		for _, e := range sp.entries {
			if e.live {
				packLive += e.plainLen
				sumChunks++
			} else {
				packDead += e.plainLen
			}
		}
		if packLive != sp.liveBytes || packDead != sp.deadBytes || packLive+packDead != sp.totalPlainBytes {
			return fmt.Errorf("pack %s accounting mismatch: live=%d/%d dead=%d/%d total=%d",
				sp.key, packLive, sp.liveBytes, packDead, sp.deadBytes, sp.totalPlainBytes)
		}
		sumLive += packLive
	}
	for i := 0; i < NumClasses; i++ {
		for _, e := range p.streams[i].entries {
			if e.live {
				sumLive += e.plainLen
				sumChunks++
			}
		}
	}
	if sumLive != p.liveBytes || sumChunks != p.liveChunks || int64(len(p.live)) != p.liveChunks {
		return fmt.Errorf("global accounting mismatch: sumLive=%d liveBytes=%d sumChunks=%d liveChunks=%d mapLen=%d",
			sumLive, p.liveBytes, sumChunks, p.liveChunks, len(p.live))
	}
	return nil
}
