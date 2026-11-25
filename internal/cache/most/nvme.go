package most

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
)

// NVMeStats snapshots real OS file I/O counters for the NVMe backing tier.
type NVMeStats struct {
	ReadOps            uint64
	ReadBytes          uint64
	WriteOps           uint64
	WriteBytes         uint64
	EvictionWriteOps   uint64
	EvictionWriteBytes uint64
	SlotsAllocated     int64
	SlotsInUse         int64
}

// NVMeStore is a real on-disk backing tier backed by an os.File accessed via
// positional ReadAt (pread64) and WriteAt (pwrite64) on fixed-size block slots.
//
// Each slot occupies blockSize bytes at byte offset slot * blockSize. Concurrent
// ReadSlot and WriteSlot calls perform OS I/O outside the slot-allocator mutex.
type NVMeStore struct {
	file             *os.File
	path             string
	ownDir           string
	blockSize        int64
	syncOnEvictWrite bool

	mu        sync.Mutex
	closed    bool
	nextSlot  int64
	freeSlots []int64
	inUse     int64

	readOps            atomic.Uint64
	readBytes          atomic.Uint64
	writeOps           atomic.Uint64
	writeBytes         atomic.Uint64
	evictionWriteOps   atomic.Uint64
	evictionWriteBytes atomic.Uint64
}

// NewNVMeStore opens or creates a backing file inside dir (or a temporary
// directory if dir is empty). Each block slot has fixed size blockSize bytes.
func NewNVMeStore(dir string, name string, blockSize int, syncOnEvictWrite bool) (*NVMeStore, error) {
	if blockSize <= 0 {
		return nil, fmt.Errorf("%w: blockSize must be > 0", ErrInvalidConfig)
	}
	var ownDir string
	if dir == "" {
		tmp, err := os.MkdirTemp("", "vfs-most-nvme-*")
		if err != nil {
			return nil, fmt.Errorf("most: create nvme temp dir: %w", err)
		}
		dir = tmp
		ownDir = tmp
	} else {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return nil, fmt.Errorf("most: mkdir nvme dir: %w", err)
		}
	}
	if name == "" {
		name = "nvme_tier.bin"
	}
	path := filepath.Join(dir, name)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o600)
	if err != nil {
		if ownDir != "" {
			_ = os.RemoveAll(ownDir)
		}
		return nil, fmt.Errorf("most: open nvme backing file: %w", err)
	}
	return &NVMeStore{
		file:             f,
		path:             path,
		ownDir:           ownDir,
		blockSize:        int64(blockSize),
		syncOnEvictWrite: syncOnEvictWrite,
	}, nil
}

// Path returns the backing file path on disk.
func (s *NVMeStore) Path() string { return s.path }

func (s *NVMeStore) allocSlot() (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return -1, ErrClosed
	}
	var slot int64
	if n := len(s.freeSlots); n > 0 {
		slot = s.freeSlots[n-1]
		s.freeSlots = s.freeSlots[:n-1]
	} else {
		slot = s.nextSlot
		s.nextSlot++
	}
	s.inUse++
	return slot, nil
}

// FreeSlot returns slot to the free list (used by exclusive tier migration when
// a block is promoted from NVMe into RAM and evicted from NVMe).
func (s *NVMeStore) FreeSlot(slot int64) {
	if slot < 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.freeSlots = append(s.freeSlots, slot)
	if s.inUse > 0 {
		s.inUse--
	}
}

// WriteNew allocates a slot and writes an initial block to NVMe. Initial
// population writes are tracked in WriteOps/WriteBytes but not counted as
// eviction writebacks.
func (s *NVMeStore) WriteNew(data []byte) (int64, error) {
	if int64(len(data)) != s.blockSize {
		return -1, ErrBlockSizeMismatch
	}
	slot, err := s.allocSlot()
	if err != nil {
		return -1, err
	}
	off := slot * s.blockSize
	n, err := s.file.WriteAt(data, off)
	if err != nil {
		s.FreeSlot(slot)
		return -1, fmt.Errorf("most: nvme WriteAt slot %d: %w", slot, err)
	}
	if int64(n) != s.blockSize {
		s.FreeSlot(slot)
		return -1, fmt.Errorf("most: short nvme write %d != %d", n, s.blockSize)
	}
	s.writeOps.Add(1)
	s.writeBytes.Add(uint64(n))
	return slot, nil
}

// WriteEviction writes a block demoted from RAM back to NVMe and records it as
// eviction writeback traffic. Used by ordinary exclusive RAM<->NVMe migration.
func (s *NVMeStore) WriteEviction(data []byte) (int64, error) {
	if int64(len(data)) != s.blockSize {
		return -1, ErrBlockSizeMismatch
	}
	slot, err := s.allocSlot()
	if err != nil {
		return -1, err
	}
	off := slot * s.blockSize
	n, err := s.file.WriteAt(data, off)
	if err != nil {
		s.FreeSlot(slot)
		return -1, fmt.Errorf("most: nvme eviction WriteAt slot %d: %w", slot, err)
	}
	if int64(n) != s.blockSize {
		s.FreeSlot(slot)
		return -1, fmt.Errorf("most: short nvme eviction write %d != %d", n, s.blockSize)
	}
	if s.syncOnEvictWrite {
		if err := s.file.Sync(); err != nil {
			s.FreeSlot(slot)
			return -1, fmt.Errorf("most: nvme eviction Sync: %w", err)
		}
	}
	s.writeOps.Add(1)
	s.writeBytes.Add(uint64(n))
	s.evictionWriteOps.Add(1)
	s.evictionWriteBytes.Add(uint64(n))
	return slot, nil
}

// ReadSlot reads exactly blockSize bytes from slot into dst using os.File.ReadAt.
func (s *NVMeStore) ReadSlot(slot int64, dst []byte) error {
	if int64(len(dst)) != s.blockSize {
		return ErrBlockSizeMismatch
	}
	if slot < 0 {
		return ErrBlockNotFound
	}
	off := slot * s.blockSize
	n, err := s.file.ReadAt(dst, off)
	if err != nil {
		return fmt.Errorf("most: nvme ReadAt slot %d: %w", slot, err)
	}
	if int64(n) != s.blockSize {
		return fmt.Errorf("most: short nvme read %d != %d", n, s.blockSize)
	}
	s.readOps.Add(1)
	s.readBytes.Add(uint64(n))
	return nil
}

// Stats returns a point-in-time snapshot of NVMe tier I/O counters.
func (s *NVMeStore) Stats() NVMeStats {
	s.mu.Lock()
	alloc := s.nextSlot
	inUse := s.inUse
	s.mu.Unlock()
	return NVMeStats{
		ReadOps:            s.readOps.Load(),
		ReadBytes:          s.readBytes.Load(),
		WriteOps:           s.writeOps.Load(),
		WriteBytes:         s.writeBytes.Load(),
		EvictionWriteOps:   s.evictionWriteOps.Load(),
		EvictionWriteBytes: s.evictionWriteBytes.Load(),
		SlotsAllocated:     alloc,
		SlotsInUse:         inUse,
	}
}

// Close closes and removes the backing file (and temp directory if owned).
func (s *NVMeStore) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	f := s.file
	path := s.path
	ownDir := s.ownDir
	s.mu.Unlock()

	err := f.Close()
	_ = os.Remove(path)
	if ownDir != "" {
		_ = os.RemoveAll(ownDir)
	}
	return err
}
