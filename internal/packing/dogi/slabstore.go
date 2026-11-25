package dogi

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

const (
	// DefaultSlabTargetBytes is the default packed slab seal size used by
	// tests and benchmarks (256 KiB; production hanzoai/vfs packTarget is 16 MiB).
	DefaultSlabTargetBytes = 256 << 10
	// DefaultMaxOpenSlabsPerTenant bounds the number of simultaneously open
	// packing buffers per tenant so open-slab RAM stays strictly capped.
	DefaultMaxOpenSlabsPerTenant = 4

	recordMagic      uint32 = 0x444F4749         // "DOGI"
	recordHeaderSize        = 4 + 8 + 8 + 4 + 32 // magic(4) + tenantHash(8) + objID(8) + payloadLen(4) + sha256(32) = 56 bytes
)

var (
	ErrNotFound        = errors.New("dogi: object not found")
	ErrTenantMismatch  = errors.New("dogi: cross-tenant access denied")
	ErrCorruptChecksum = errors.New("dogi: sha256 integrity mismatch")
	ErrCorruptHeader   = errors.New("dogi: corrupt record frame header")
	ErrInvalidTenant   = errors.New("dogi: empty tenant id")
)

// StoreConfig configures the real on-disk packed slab store.
type StoreConfig struct {
	// RootDir is the directory where tenant-isolated .pack slab files are stored.
	RootDir string
	// SlabTargetBytes is the byte threshold at which an open slab is sealed
	// and written to disk as an immutable .pack file.
	SlabTargetBytes int
	// MaxOpenSlabsPerTenant caps the number of simultaneously open slab buffers
	// per tenant. Exceeding this cap flushes the least-recently-used open slab.
	// If <= 0, DefaultMaxOpenSlabsPerTenant is used.
	MaxOpenSlabsPerTenant int
	// SyncOnSeal calls file.Sync() after writing each sealed slab file.
	SyncOnSeal bool
}

// StoreMetrics captures exact, non-simulated storage and I/O measurements.
type StoreMetrics struct {
	Policy                PolicyKind `json:"policy"`
	SlabTargetBytes       int        `json:"slab_target_bytes"`
	MaxOpenSlabsPerTenant int        `json:"max_open_slabs_per_tenant"`

	ObjectsWritten        int64   `json:"objects_written"`
	ObjectsDeleted        int64   `json:"objects_deleted"`
	ObjectsLive           int64   `json:"objects_live"`
	LogicalBytesWritten   int64   `json:"logical_bytes_written"`
	LiveLogicalBytes      int64   `json:"live_logical_bytes"`
	LiveBytesCopied       int64   `json:"live_bytes_copied"`
	PhysicalBytesWritten  int64   `json:"physical_bytes_written"`
	DiskBytesOnStore      int64   `json:"disk_bytes_on_store"`
	PeakDiskBytesOnStore  int64   `json:"peak_disk_bytes_on_store"`
	WAF                   float64 `json:"waf"`
	EffectiveWAF          float64 `json:"effective_waf"`
	FinalSpaceAmp         float64 `json:"final_space_amp"`
	PeakSpaceAmp          float64 `json:"peak_space_amp"`
	SlabsSealedTotal      int64   `json:"slabs_sealed_total"`
	SlabsActiveOnDisk     int64   `json:"slabs_active_on_disk"`
	SlabsReclaimedTotal   int64   `json:"slabs_reclaimed_total"`
	CleanSlabsZeroCopy    int64   `json:"clean_slabs_zero_copy"`
	ZeroCopyReclaimRate   float64 `json:"zero_copy_reclaim_rate"`
	AvgSlabFillRatio      float64 `json:"avg_slab_fill_ratio"`
	OpenSlabEvictions     int64   `json:"open_slab_evictions"`
	PeakOpenSlabs         int     `json:"peak_open_slabs"`
	PeakOpenSlabBytes     int64   `json:"peak_open_slab_bytes"`
	IntegrityChecksPassed int64   `json:"integrity_checks_passed"`
	IntegrityErrors       int64   `json:"integrity_errors"`
}

type openSlabKey struct {
	tenantID string
	bucket   int
}

type openSlab struct {
	id         uint64
	tenantID   string
	bucket     int
	buf        []byte
	objIDs     []uint64
	lastTouch  uint64
	payloadSum int64
}

type sealedSlab struct {
	ID         uint64   `json:"id"`
	TenantID   string   `json:"tenant_id"`
	Bucket     int      `json:"bucket"`
	Path       string   `json:"path"`
	TotalBytes int64    `json:"total_bytes"`
	LiveBytes  int64    `json:"live_bytes"`
	LiveCount  int      `json:"live_count"`
	TotalCount int      `json:"total_count"`
	ObjIDs     []uint64 `json:"obj_ids"`
}

type objLoc struct {
	ObjID      uint64   `json:"obj_id"`
	TenantID   string   `json:"tenant_id"`
	Path       string   `json:"path"`
	Bucket     int      `json:"bucket"`
	SlabID     uint64   `json:"slab_id"`
	Sealed     bool     `json:"sealed"`
	Offset     int64    `json:"offset"`
	PayloadLen int64    `json:"payload_len"`
	FrameLen   int64    `json:"frame_len"`
	SHA256     [32]byte `json:"sha256"`
	Live       bool     `json:"live"`
}

type persistedIndex struct {
	NextSlabID uint64                 `json:"next_slab_id"`
	NextObjID  uint64                 `json:"next_obj_id"`
	Sealed     map[uint64]*sealedSlab `json:"sealed"`
	Objects    map[uint64]*objLoc     `json:"objects"`
}

// SlabStore is a real on-disk packed slab storage engine with SHA-256 object
// integrity, strict per-tenant slab isolation, bounded open-slab memory, and
// a shared downstream reclaim evaluator.
type SlabStore struct {
	mu     sync.Mutex
	cfg    StoreConfig
	policy PackingPolicy

	nextSlabID uint64
	nextObjID  uint64
	touchSeq   uint64

	openByKey  map[openSlabKey]*openSlab
	openByID   map[uint64]*openSlab
	sealedByID map[uint64]*sealedSlab
	objects    map[uint64]*objLoc

	// Exact telemetry counters.
	objectsWritten        int64
	objectsDeleted        int64
	logicalBytesWritten   int64
	liveLogicalBytes      int64
	liveBytesCopied       int64
	physicalBytesWritten  int64
	diskBytesOnStore      int64
	peakDiskBytesOnStore  int64
	peakSpaceAmp          float64
	slabsSealedTotal      int64
	sealedFillSum         float64
	slabsReclaimedTotal   int64
	cleanSlabsZeroCopy    int64
	openSlabEvictions     int64
	peakOpenSlabs         int
	peakOpenSlabBytes     int64
	integrityChecksPassed int64
	integrityErrors       int64
}

// NewSlabStore creates a new on-disk SlabStore rooted at cfg.RootDir.
func NewSlabStore(cfg StoreConfig, policy PackingPolicy) (*SlabStore, error) {
	if cfg.RootDir == "" {
		return nil, errors.New("dogi: RootDir is required")
	}
	if cfg.SlabTargetBytes <= 0 {
		cfg.SlabTargetBytes = DefaultSlabTargetBytes
	}
	if cfg.MaxOpenSlabsPerTenant <= 0 {
		cfg.MaxOpenSlabsPerTenant = DefaultMaxOpenSlabsPerTenant
	}
	if policy == nil {
		policy = NewPolicy(PolicyMixedFIFO)
	}
	if err := os.MkdirAll(filepath.Join(cfg.RootDir, "tenants"), 0o755); err != nil {
		return nil, fmt.Errorf("dogi: mkdir root: %w", err)
	}
	return &SlabStore{
		cfg:        cfg,
		policy:     policy,
		nextSlabID: 1,
		nextObjID:  1,
		openByKey:  make(map[openSlabKey]*openSlab),
		openByID:   make(map[uint64]*openSlab),
		sealedByID: make(map[uint64]*sealedSlab),
		objects:    make(map[uint64]*objLoc),
	}, nil
}

// Policy returns the active PackingPolicy.
func (s *SlabStore) Policy() PackingPolicy {
	return s.policy
}

func tenantHash64(tenantID string) uint64 {
	sum := sha256.Sum256([]byte("dogi-tenant:" + tenantID))
	return binary.LittleEndian.Uint64(sum[:8])
}

func tenantDirSlug(tenantID string) string {
	sum := sha256.Sum256([]byte("dogi-tenant-dir:" + tenantID))
	return hex.EncodeToString(sum[:8])
}

func encodeFrame(tenantID string, objID uint64, payload []byte) ([]byte, [32]byte) {
	digest := sha256.Sum256(payload)
	frame := make([]byte, recordHeaderSize+len(payload))
	binary.LittleEndian.PutUint32(frame[0:4], recordMagic)
	binary.LittleEndian.PutUint64(frame[4:12], tenantHash64(tenantID))
	binary.LittleEndian.PutUint64(frame[12:20], objID)
	binary.LittleEndian.PutUint32(frame[20:24], uint32(len(payload)))
	copy(frame[24:56], digest[:])
	copy(frame[56:], payload)
	return frame, digest
}

func decodeAndVerifyFrame(tenantID string, expectedObjID uint64, frame []byte) ([]byte, [32]byte, error) {
	if len(frame) < recordHeaderSize {
		return nil, [32]byte{}, ErrCorruptHeader
	}
	magic := binary.LittleEndian.Uint32(frame[0:4])
	if magic != recordMagic {
		return nil, [32]byte{}, ErrCorruptHeader
	}
	tHash := binary.LittleEndian.Uint64(frame[4:12])
	if tHash != tenantHash64(tenantID) {
		return nil, [32]byte{}, ErrTenantMismatch
	}
	gotObjID := binary.LittleEndian.Uint64(frame[12:20])
	if gotObjID != expectedObjID {
		return nil, [32]byte{}, ErrCorruptHeader
	}
	pLen := binary.LittleEndian.Uint32(frame[20:24])
	if int(pLen) != len(frame)-recordHeaderSize {
		return nil, [32]byte{}, ErrCorruptHeader
	}
	var want [32]byte
	copy(want[:], frame[24:56])
	payload := make([]byte, pLen)
	copy(payload, frame[56:])
	got := sha256.Sum256(payload)
	if got != want {
		return nil, [32]byte{}, ErrCorruptChecksum
	}
	return payload, got, nil
}

// Put writes a new object into the tenant's assigned lifetime-segregated slab
// and returns its unique object ID and SHA-256 digest.
func (s *SlabStore) Put(h ObjectHint, payload []byte) (uint64, [32]byte, error) {
	if h.TenantID == "" {
		return 0, [32]byte{}, ErrInvalidTenant
	}
	h.Size = int64(len(payload))
	bucket := s.policy.AssignBucket(h)

	s.mu.Lock()
	defer s.mu.Unlock()

	objID := s.nextObjID
	s.nextObjID++

	if err := s.policy.ObserveWrite(h.TenantID, objID, h.Path, h.Epoch); err != nil {
		return 0, [32]byte{}, err
	}

	frame, digest := encodeFrame(h.TenantID, objID, payload)
	sl, err := s.getOrCreateOpenSlabLocked(h.TenantID, bucket)
	if err != nil {
		return 0, [32]byte{}, err
	}

	off := int64(len(sl.buf))
	sl.buf = append(sl.buf, frame...)
	sl.objIDs = append(sl.objIDs, objID)
	sl.payloadSum += int64(len(payload))
	s.touchSeq++
	sl.lastTouch = s.touchSeq

	s.objects[objID] = &objLoc{
		ObjID:      objID,
		TenantID:   h.TenantID,
		Path:       h.Path,
		Bucket:     bucket,
		SlabID:     sl.id,
		Sealed:     false,
		Offset:     off,
		PayloadLen: int64(len(payload)),
		FrameLen:   int64(len(frame)),
		SHA256:     digest,
		Live:       true,
	}

	s.objectsWritten++
	s.logicalBytesWritten += int64(len(payload))
	s.liveLogicalBytes += int64(len(payload))

	if len(sl.buf) >= s.cfg.SlabTargetBytes {
		if err := s.sealOpenSlabLocked(sl); err != nil {
			return 0, [32]byte{}, err
		}
	}
	s.updateSpacePeaksLocked()
	return objID, digest, nil
}

// Get reads an object back for tenantID (from its open buffer or from its
// immutable on-disk .pack file via ReadAt) and verifies bit-exact SHA-256
// integrity before returning the payload.
func (s *SlabStore) Get(tenantID string, objID uint64) ([]byte, error) {
	if tenantID == "" {
		return nil, ErrInvalidTenant
	}
	s.mu.Lock()
	loc, ok := s.objects[objID]
	if !ok || !loc.Live {
		s.mu.Unlock()
		return nil, ErrNotFound
	}
	if loc.TenantID != tenantID {
		s.mu.Unlock()
		return nil, ErrTenantMismatch
	}

	var frame []byte
	if !loc.Sealed {
		sl, ok := s.openByID[loc.SlabID]
		if !ok {
			s.mu.Unlock()
			return nil, ErrNotFound
		}
		end := loc.Offset + loc.FrameLen
		if end > int64(len(sl.buf)) {
			s.integrityErrors++
			s.mu.Unlock()
			return nil, ErrCorruptHeader
		}
		frame = make([]byte, loc.FrameLen)
		copy(frame, sl.buf[loc.Offset:end])
		s.mu.Unlock()
	} else {
		sl, ok := s.sealedByID[loc.SlabID]
		if !ok {
			s.mu.Unlock()
			return nil, ErrNotFound
		}
		slabPath := sl.Path
		off := loc.Offset
		flen := loc.FrameLen
		s.mu.Unlock()

		f, err := os.Open(slabPath)
		if err != nil {
			return nil, fmt.Errorf("dogi: open slab %d: %w", loc.SlabID, err)
		}
		frame = make([]byte, flen)
		_, rerr := f.ReadAt(frame, off)
		_ = f.Close()
		if rerr != nil {
			return nil, fmt.Errorf("dogi: readAt slab %d: %w", loc.SlabID, rerr)
		}
	}

	payload, got, err := decodeAndVerifyFrame(tenantID, objID, frame)
	s.mu.Lock()
	if err != nil {
		s.integrityErrors++
		s.mu.Unlock()
		return nil, err
	}
	if got != loc.SHA256 {
		s.integrityErrors++
		s.mu.Unlock()
		return nil, ErrCorruptChecksum
	}
	s.integrityChecksPassed++
	s.mu.Unlock()
	return payload, nil
}

// Delete marks objID dead for tenantID at epoch and notifies the adaptive
// classifier. Physical slab space is reclaimed later by ReclaimDeadSlabs.
func (s *SlabStore) Delete(tenantID string, objID uint64, epoch int64) error {
	if tenantID == "" {
		return ErrInvalidTenant
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	loc, ok := s.objects[objID]
	if !ok || !loc.Live {
		return ErrNotFound
	}
	if loc.TenantID != tenantID {
		return ErrTenantMismatch
	}
	if err := s.policy.ObserveDelete(tenantID, objID, epoch); err != nil {
		return err
	}

	loc.Live = false
	s.objectsDeleted++
	s.liveLogicalBytes -= loc.PayloadLen
	if loc.Sealed {
		if sl, ok := s.sealedByID[loc.SlabID]; ok {
			sl.LiveBytes -= loc.FrameLen
			sl.LiveCount--
		}
	}
	s.updateSpacePeaksLocked()
	return nil
}

// AdvanceEpoch advances the causal clock for tenantID.
func (s *SlabStore) AdvanceEpoch(tenantID string, epoch int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.policy.AdvanceEpoch(tenantID, epoch)
}

// FlushAll seals all currently open slabs to disk (analogous to hanzoai/vfs
// packer.flush at generation boundaries).
func (s *SlabStore) FlushAll() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	open := make([]*openSlab, 0, len(s.openByID))
	for _, sl := range s.openByID {
		open = append(open, sl)
	}
	sort.Slice(open, func(i, j int) bool { return open[i].id < open[j].id })
	for _, sl := range open {
		if err := s.sealOpenSlabLocked(sl); err != nil {
			return err
		}
	}
	s.updateSpacePeaksLocked()
	return nil
}

// ReclaimDeadSlabs runs a fixed, policy-agnostic slab reclamation pass over
// sealed on-disk slabs whose dead-byte ratio meets or exceeds deadThreshold
// (in (0, 1]).
//
//   - If a slab has 0 live bytes (100% dead), it is deleted in place with
//     ZERO live-data copies (CleanSlabsZeroCopy++).
//   - Otherwise, surviving live objects are read from disk via ReadAt, verified
//     with SHA-256, copied into new tenant-isolated compacted slabs, and the
//     old slab file is removed.
func (s *SlabStore) ReclaimDeadSlabs(deadThreshold float64) (int, error) {
	if deadThreshold <= 0 || deadThreshold > 1 {
		deadThreshold = 0.50
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	var victims []*sealedSlab
	for _, sl := range s.sealedByID {
		if sl.TotalBytes <= 0 {
			continue
		}
		deadBytes := sl.TotalBytes - sl.LiveBytes
		deadRatio := float64(deadBytes) / float64(sl.TotalBytes)
		if sl.LiveCount == 0 || deadRatio >= deadThreshold {
			victims = append(victims, sl)
		}
	}
	sort.Slice(victims, func(i, j int) bool { return victims[i].ID < victims[j].ID })

	// Group partial-live victims by (tenantID, bucket) so compacted slabs
	// preserve both tenant isolation and lifetime segregation.
	type compactKey struct {
		tenantID string
		bucket   int
	}
	compactors := make(map[compactKey]*openSlab)

	reclaimed := 0
	for _, victim := range victims {
		s.slabsReclaimedTotal++
		reclaimed++

		if victim.LiveCount == 0 || victim.LiveBytes <= 0 {
			s.cleanSlabsZeroCopy++
			if err := os.Remove(victim.Path); err != nil && !errors.Is(err, os.ErrNotExist) {
				return reclaimed, fmt.Errorf("dogi: remove clean slab %d: %w", victim.ID, err)
			}
			s.diskBytesOnStore -= victim.TotalBytes
			delete(s.sealedByID, victim.ID)
			continue
		}

		// Partial-live slab: must read and copy every surviving live object.
		f, err := os.Open(victim.Path)
		if err != nil {
			return reclaimed, fmt.Errorf("dogi: open victim slab %d: %w", victim.ID, err)
		}

		ck := compactKey{tenantID: victim.TenantID, bucket: victim.Bucket}
		dst := compactors[ck]
		for _, oid := range victim.ObjIDs {
			loc, ok := s.objects[oid]
			if !ok || !loc.Live || loc.SlabID != victim.ID {
				continue
			}
			frame := make([]byte, loc.FrameLen)
			if _, rerr := f.ReadAt(frame, loc.Offset); rerr != nil {
				_ = f.Close()
				return reclaimed, fmt.Errorf("dogi: readAt victim slab %d obj %d: %w", victim.ID, oid, rerr)
			}
			_, got, verr := decodeAndVerifyFrame(victim.TenantID, oid, frame)
			if verr != nil || got != loc.SHA256 {
				s.integrityErrors++
				_ = f.Close()
				return reclaimed, fmt.Errorf("dogi: corrupt object %d during reclaim: %w", oid, verr)
			}
			s.integrityChecksPassed++

			if dst == nil {
				dst = &openSlab{
					id:       s.nextSlabID,
					tenantID: victim.TenantID,
					bucket:   victim.Bucket,
					buf:      make([]byte, 0, s.cfg.SlabTargetBytes),
				}
				s.nextSlabID++
				compactors[ck] = dst
			}

			off := int64(len(dst.buf))
			dst.buf = append(dst.buf, frame...)
			dst.objIDs = append(dst.objIDs, oid)
			dst.payloadSum += loc.PayloadLen

			loc.SlabID = dst.id
			loc.Offset = off
			loc.Sealed = false

			s.liveBytesCopied += loc.PayloadLen

			if len(dst.buf) >= s.cfg.SlabTargetBytes {
				if serr := s.sealDetachedSlabLocked(dst); serr != nil {
					_ = f.Close()
					return reclaimed, serr
				}
				dst = nil
				compactors[ck] = nil
			}
		}
		_ = f.Close()

		if err := os.Remove(victim.Path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return reclaimed, fmt.Errorf("dogi: remove compacted slab %d: %w", victim.ID, err)
		}
		s.diskBytesOnStore -= victim.TotalBytes
		delete(s.sealedByID, victim.ID)
	}

	// Seal any remaining non-empty compaction buffers so every migrated object
	// is durably persisted on disk.
	var remaining []*openSlab
	for _, dst := range compactors {
		if dst != nil && len(dst.buf) > 0 {
			remaining = append(remaining, dst)
		}
	}
	sort.Slice(remaining, func(i, j int) bool { return remaining[i].id < remaining[j].id })
	for _, dst := range remaining {
		if err := s.sealDetachedSlabLocked(dst); err != nil {
			return reclaimed, err
		}
	}

	s.updateSpacePeaksLocked()
	return reclaimed, nil
}

func (s *SlabStore) getOrCreateOpenSlabLocked(tenantID string, bucket int) (*openSlab, error) {
	key := openSlabKey{tenantID: tenantID, bucket: bucket}
	if sl, ok := s.openByKey[key]; ok {
		return sl, nil
	}

	// Enforce per-tenant bounded open-slab RAM cap.
	var tenantOpen []*openSlab
	for _, sl := range s.openByID {
		if sl.tenantID == tenantID {
			tenantOpen = append(tenantOpen, sl)
		}
	}
	if len(tenantOpen) >= s.cfg.MaxOpenSlabsPerTenant {
		// Evict least-recently-written open slab for this tenant.
		victim := tenantOpen[0]
		for _, cand := range tenantOpen[1:] {
			if cand.lastTouch < victim.lastTouch {
				victim = cand
			}
		}
		s.openSlabEvictions++
		if err := s.sealOpenSlabLocked(victim); err != nil {
			return nil, err
		}
	}

	sl := &openSlab{
		id:       s.nextSlabID,
		tenantID: tenantID,
		bucket:   bucket,
		buf:      make([]byte, 0, s.cfg.SlabTargetBytes),
	}
	s.nextSlabID++
	s.openByKey[key] = sl
	s.openByID[sl.id] = sl

	openCount := len(s.openByID)
	if openCount > s.peakOpenSlabs {
		s.peakOpenSlabs = openCount
	}
	// Each open slab reserves a packing buffer of SlabTargetBytes (matching
	// hanzoai/vfs packSlabs.Get(packTarget + packRoom)).
	openBytes := int64(openCount) * int64(s.cfg.SlabTargetBytes)
	if openBytes > s.peakOpenSlabBytes {
		s.peakOpenSlabBytes = openBytes
	}
	return sl, nil
}

func (s *SlabStore) sealOpenSlabLocked(sl *openSlab) error {
	delete(s.openByKey, openSlabKey{tenantID: sl.tenantID, bucket: sl.bucket})
	delete(s.openByID, sl.id)
	if len(sl.buf) == 0 {
		return nil
	}
	return s.sealDetachedSlabLocked(sl)
}

func (s *SlabStore) sealDetachedSlabLocked(sl *openSlab) error {
	tDir := filepath.Join(s.cfg.RootDir, "tenants", tenantDirSlug(sl.tenantID))
	if err := os.MkdirAll(tDir, 0o755); err != nil {
		return fmt.Errorf("dogi: mkdir tenant dir: %w", err)
	}
	slabPath := filepath.Join(tDir, fmt.Sprintf("slab-%08d.pack", sl.id))
	f, err := os.OpenFile(slabPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("dogi: create slab file: %w", err)
	}
	n, werr := f.WriteAt(sl.buf, 0)
	if werr != nil {
		_ = f.Close()
		return fmt.Errorf("dogi: writeAt slab %d: %w", sl.id, werr)
	}
	if n != len(sl.buf) {
		_ = f.Close()
		return fmt.Errorf("dogi: short writeAt slab %d: %d != %d", sl.id, n, len(sl.buf))
	}
	if s.cfg.SyncOnSeal {
		if serr := f.Sync(); serr != nil {
			_ = f.Close()
			return fmt.Errorf("dogi: sync slab %d: %w", sl.id, serr)
		}
	}
	if cerr := f.Close(); cerr != nil {
		return fmt.Errorf("dogi: close slab %d: %w", sl.id, cerr)
	}

	var liveBytes int64
	liveCount := 0
	for _, oid := range sl.objIDs {
		if loc, ok := s.objects[oid]; ok {
			loc.Sealed = true
			if loc.Live {
				liveBytes += loc.FrameLen
				liveCount++
			}
		}
	}

	totalBytes := int64(len(sl.buf))
	s.sealedByID[sl.id] = &sealedSlab{
		ID:         sl.id,
		TenantID:   sl.tenantID,
		Bucket:     sl.bucket,
		Path:       slabPath,
		TotalBytes: totalBytes,
		LiveBytes:  liveBytes,
		LiveCount:  liveCount,
		TotalCount: len(sl.objIDs),
		ObjIDs:     append([]uint64(nil), sl.objIDs...),
	}

	s.physicalBytesWritten += totalBytes
	s.diskBytesOnStore += totalBytes
	s.slabsSealedTotal++
	fill := float64(totalBytes) / float64(s.cfg.SlabTargetBytes)
	if fill > 1.0 {
		fill = 1.0
	}
	s.sealedFillSum += fill
	return nil
}

func (s *SlabStore) updateSpacePeaksLocked() {
	if s.diskBytesOnStore > s.peakDiskBytesOnStore {
		s.peakDiskBytesOnStore = s.diskBytesOnStore
	}
	if s.liveLogicalBytes > 0 && s.diskBytesOnStore > 0 {
		sa := float64(s.diskBytesOnStore) / float64(s.liveLogicalBytes)
		if sa > s.peakSpaceAmp {
			s.peakSpaceAmp = sa
		}
	}
}

// VerifyTenantIsolation audits every open and sealed slab to verify that zero
// slabs contain objects from more than one tenant.
func (s *SlabStore) VerifyTenantIsolation() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, sl := range s.openByID {
		for _, oid := range sl.objIDs {
			if loc, ok := s.objects[oid]; ok && loc.TenantID != sl.tenantID {
				return fmt.Errorf("%w: open slab %d tenant %q contains obj %d from %q",
					ErrTenantMismatch, sl.id, sl.tenantID, oid, loc.TenantID)
			}
		}
	}
	for _, sl := range s.sealedByID {
		for _, oid := range sl.ObjIDs {
			if loc, ok := s.objects[oid]; ok && loc.TenantID != sl.TenantID {
				return fmt.Errorf("%w: sealed slab %d tenant %q contains obj %d from %q",
					ErrTenantMismatch, sl.ID, sl.TenantID, oid, loc.TenantID)
			}
		}
	}
	return nil
}

// SaveIndex flushes all open slabs and writes the slab/object index to disk so
// the store can be reopened cleanly.
func (s *SlabStore) SaveIndex() error {
	if err := s.FlushAll(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	idx := persistedIndex{
		NextSlabID: s.nextSlabID,
		NextObjID:  s.nextObjID,
		Sealed:     s.sealedByID,
		Objects:    s.objects,
	}
	raw, err := json.Marshal(idx)
	if err != nil {
		return fmt.Errorf("dogi: marshal index: %w", err)
	}
	idxPath := filepath.Join(s.cfg.RootDir, "index.json")
	if err := os.WriteFile(idxPath, raw, 0o644); err != nil {
		return fmt.Errorf("dogi: write index: %w", err)
	}
	return nil
}

// ReopenSlabStore loads a previously persisted SlabStore from cfg.RootDir.
func ReopenSlabStore(cfg StoreConfig, policy PackingPolicy) (*SlabStore, error) {
	s, err := NewSlabStore(cfg, policy)
	if err != nil {
		return nil, err
	}
	raw, err := os.ReadFile(filepath.Join(cfg.RootDir, "index.json"))
	if err != nil {
		return nil, fmt.Errorf("dogi: read index: %w", err)
	}
	var idx persistedIndex
	if err := json.Unmarshal(raw, &idx); err != nil {
		return nil, fmt.Errorf("dogi: unmarshal index: %w", err)
	}
	s.nextSlabID = idx.NextSlabID
	s.nextObjID = idx.NextObjID
	if idx.Sealed != nil {
		s.sealedByID = idx.Sealed
	}
	if idx.Objects != nil {
		s.objects = idx.Objects
	}
	for _, sl := range s.sealedByID {
		s.diskBytesOnStore += sl.TotalBytes
	}
	for _, loc := range s.objects {
		if loc.Live {
			s.liveLogicalBytes += loc.PayloadLen
		}
	}
	return s, nil
}

// Metrics returns a consistent snapshot of all measured store counters.
func (s *SlabStore) Metrics() StoreMetrics {
	s.mu.Lock()
	defer s.mu.Unlock()

	var waf, effWAF, finalSA, zcRate, avgFill float64
	if s.logicalBytesWritten > 0 {
		waf = float64(s.physicalBytesWritten) / float64(s.logicalBytesWritten)
		effWAF = float64(s.logicalBytesWritten+s.liveBytesCopied) / float64(s.logicalBytesWritten)
	}
	if s.liveLogicalBytes > 0 {
		finalSA = float64(s.diskBytesOnStore) / float64(s.liveLogicalBytes)
	}
	if s.slabsReclaimedTotal > 0 {
		zcRate = float64(s.cleanSlabsZeroCopy) / float64(s.slabsReclaimedTotal)
	}
	if s.slabsSealedTotal > 0 {
		avgFill = s.sealedFillSum / float64(s.slabsSealedTotal)
	}

	return StoreMetrics{
		Policy:                s.policy.Kind(),
		SlabTargetBytes:       s.cfg.SlabTargetBytes,
		MaxOpenSlabsPerTenant: s.cfg.MaxOpenSlabsPerTenant,
		ObjectsWritten:        s.objectsWritten,
		ObjectsDeleted:        s.objectsDeleted,
		ObjectsLive:           s.objectsWritten - s.objectsDeleted,
		LogicalBytesWritten:   s.logicalBytesWritten,
		LiveLogicalBytes:      s.liveLogicalBytes,
		LiveBytesCopied:       s.liveBytesCopied,
		PhysicalBytesWritten:  s.physicalBytesWritten,
		DiskBytesOnStore:      s.diskBytesOnStore,
		PeakDiskBytesOnStore:  s.peakDiskBytesOnStore,
		WAF:                   waf,
		EffectiveWAF:          effWAF,
		FinalSpaceAmp:         finalSA,
		PeakSpaceAmp:          s.peakSpaceAmp,
		SlabsSealedTotal:      s.slabsSealedTotal,
		SlabsActiveOnDisk:     int64(len(s.sealedByID)),
		SlabsReclaimedTotal:   s.slabsReclaimedTotal,
		CleanSlabsZeroCopy:    s.cleanSlabsZeroCopy,
		ZeroCopyReclaimRate:   zcRate,
		AvgSlabFillRatio:      avgFill,
		OpenSlabEvictions:     s.openSlabEvictions,
		PeakOpenSlabs:         s.peakOpenSlabs,
		PeakOpenSlabBytes:     s.peakOpenSlabBytes,
		IntegrityChecksPassed: s.integrityChecksPassed,
		IntegrityErrors:       s.integrityErrors,
	}
}
