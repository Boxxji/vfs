// Copyright 2026 Hanzo AI Inc.
// SPDX-License-Identifier: Apache-2.0

package parasync

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"math/bits"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"

	"github.com/hanzoai/vfs/pkg/backend"
	"github.com/zeebo/blake3"
)

// DefaultPackMaxBytes is the maximum payload size per immutable pack object
// written by PublishCheckpoint (4 MiB).
const DefaultPackMaxBytes = 4 << 20

// ChunkSpan represents one cut chunk inside a byte slice or file.
type ChunkSpan struct {
	Offset int64
	Length int64
	Digest Digest
}

// Chunker splits byte streams into chunks according to a ChunkConfig
// (either normalized FastCDC or fixed-size blocks). Safe for concurrent use.
type Chunker struct {
	cfg   ChunkConfig
	gear  [256]uint64
	maskS uint64
	maskL uint64
}

// NewChunker validates cfg and returns a ready-to-use Chunker.
func NewChunker(cfg ChunkConfig) (*Chunker, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	ch := &Chunker{cfg: cfg}
	if cfg.Mode == ChunkingFastCDC {
		ch.gear = deriveGearTable(cfg.GearSeed)
		avgBits := bits.TrailingZeros(uint(cfg.AvgSize))
		sBits := avgBits + 2
		if sBits > 60 {
			sBits = 60
		}
		lBits := avgBits - 2
		if lBits < 2 {
			lBits = 2
		}
		ch.maskS = ((uint64(1) << sBits) - 1) << (64 - sBits)
		ch.maskL = ((uint64(1) << lBits) - 1) << (64 - lBits)
	}
	return ch, nil
}

func deriveGearTable(seed [32]byte) [256]uint64 {
	h := blake3.New()
	_, _ = h.WriteString("hanzoai/vfs/parasync/fastcdc-gear-v1")
	_, _ = h.Write(seed[:])
	var raw [256 * 8]byte
	d := h.Digest()
	_, _ = d.Read(raw[:])
	var table [256]uint64
	for i := range table {
		table[i] = binary.LittleEndian.Uint64(raw[i*8:])
	}
	return table
}

// Cut returns the byte length of the next chunk starting at data[0].
// For FastCDC, the cut depends only on data[:result] so after an insertion or
// deletion the chunk boundaries re-synchronize within 1-2 chunks.
func (c *Chunker) Cut(data []byte) int {
	n := len(data)
	if n == 0 {
		return 0
	}
	if c.cfg.Mode == ChunkingFixed {
		if n <= c.cfg.FixedSize {
			return n
		}
		return c.cfg.FixedSize
	}

	limit := n
	if limit > c.cfg.MaxSize {
		limit = c.cfg.MaxSize
	}
	if limit <= c.cfg.MinSize {
		return limit
	}
	mid := c.cfg.AvgSize
	if mid > limit {
		mid = limit
	}

	var fp uint64
	i := c.cfg.MinSize
	for ; i < mid; i++ {
		fp = (fp << 1) + c.gear[data[i]]
		if fp&c.maskS == 0 {
			return i + 1
		}
	}
	for ; i < limit; i++ {
		fp = (fp << 1) + c.gear[data[i]]
		if fp&c.maskL == 0 {
			return i + 1
		}
	}
	return limit
}

// ChunkBytes partitions data into content-defined or fixed chunks and computes
// each chunk's BLAKE3-256 digest.
func (c *Chunker) ChunkBytes(data []byte) []ChunkSpan {
	if len(data) == 0 {
		return nil
	}
	est := len(data)/c.estimatedAvgSize() + 1
	spans := make([]ChunkSpan, 0, est)
	var off int
	for off < len(data) {
		n := c.Cut(data[off:])
		chunk := data[off : off+n]
		spans = append(spans, ChunkSpan{
			Offset: int64(off),
			Length: int64(n),
			Digest: SumDigest(chunk),
		})
		off += n
	}
	return spans
}

func (c *Chunker) estimatedAvgSize() int {
	if c.cfg.Mode == ChunkingFixed {
		return c.cfg.FixedSize
	}
	return c.cfg.AvgSize
}

// PublishOptions configures checkpoint creation and remote pack publication.
type PublishOptions struct {
	Workers      int
	PackMaxBytes int64
	Cipher       Cipher
}

func (o PublishOptions) normalized() PublishOptions {
	if o.Workers <= 0 {
		o.Workers = runtime.NumCPU()
		if o.Workers < 1 {
			o.Workers = 1
		}
	}
	if o.PackMaxBytes <= 0 {
		o.PackMaxBytes = DefaultPackMaxBytes
	}
	return o
}

// TenantScopePrefix returns the backend-valid key prefix scoped strictly to
// (tenantID, volumeID). Every pack object written for this namespace stays
// beneath this prefix.
func TenantScopePrefix(tenantID, volumeID string) string {
	th := blake3.Sum256([]byte("parasync-tenant:" + tenantID))
	vh := blake3.Sum256([]byte("parasync-volume:" + volumeID))
	return "t-" + hex.EncodeToString(th[:8]) + "/v-" + hex.EncodeToString(vh[:8])
}

// sealChunkFrame encodes a chunk for storage in a remote pack. When cipher is
// nil, the raw plaintext bytes are stored directly. When cipher is non-nil,
// the chunk is sealed and framed as [keyIDLen:uint16][keyID][sealedBytes] so
// ranged reads can open individual chunks independently.
func sealChunkFrame(cipher Cipher, plain []byte) ([]byte, error) {
	if cipher == nil {
		return plain, nil
	}
	sealed, keyID, err := cipher.Seal(plain)
	if err != nil {
		return nil, fmt.Errorf("parasync: seal chunk: %w", err)
	}
	if len(keyID) > 0xffff {
		return nil, fmt.Errorf("parasync: keyID too long (%d bytes)", len(keyID))
	}
	frame := make([]byte, 2+len(keyID)+len(sealed))
	binary.LittleEndian.PutUint16(frame[:2], uint16(len(keyID)))
	copy(frame[2:2+len(keyID)], keyID)
	copy(frame[2+len(keyID):], sealed)
	return frame, nil
}

// openChunkFrame decodes a chunk previously produced by sealChunkFrame.
func openChunkFrame(cipher Cipher, frame []byte) ([]byte, error) {
	if cipher == nil {
		return frame, nil
	}
	if len(frame) < 2 {
		return nil, fmt.Errorf("%w: truncated encrypted chunk frame", ErrCorruptChunk)
	}
	keyLen := int(binary.LittleEndian.Uint16(frame[:2]))
	if 2+keyLen > len(frame) {
		return nil, fmt.Errorf("%w: invalid encrypted chunk keyID length", ErrCorruptChunk)
	}
	keyID := string(frame[2 : 2+keyLen])
	plain, err := cipher.Open(frame[2+keyLen:], keyID)
	if err != nil {
		return nil, fmt.Errorf("%w: decrypt chunk: %v", ErrCorruptChunk, err)
	}
	return plain, nil
}

type scannedFile struct {
	relPath string
	absPath string
	size    int64
	mode    uint32
}

func collectRegularFiles(rootDir string) ([]scannedFile, error) {
	var files []scannedFile
	err := filepath.WalkDir(rootDir, func(p string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			if p != rootDir && strings.HasPrefix(d.Name(), ".parasync-stage-") {
				return filepath.SkipDir
			}
			return nil
		}
		if !d.Type().IsRegular() {
			return nil
		}
		rel, err := filepath.Rel(rootDir, p)
		if err != nil {
			return err
		}
		relSlash := filepath.ToSlash(rel)
		if err := ValidatePath(relSlash); err != nil {
			return err
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		files = append(files, scannedFile{
			relPath: relSlash,
			absPath: p,
			size:    info.Size(),
			mode:    uint32(info.Mode().Perm()),
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(files, func(i, j int) bool {
		return files[i].relPath < files[j].relPath
	})
	return files, nil
}

// BuildLocalCheckpoint chunks and hashes all regular files in dir in parallel
// without writing to a remote store. Used both for local checkpoint snapshots
// and for on-the-fly local directory indexing before delta planning.
func BuildLocalCheckpoint(ctx context.Context, dir string, meta CheckpointMeta, cfg ChunkConfig, workers int) (*Checkpoint, error) {
	if meta.TenantID == "" || meta.VolumeID == "" || meta.CipherSuiteID == "" {
		return nil, fmt.Errorf("%w: tenant_id, volume_id, and cipher_suite_id are required", ErrIncompatibleCheckpoint)
	}
	chunker, err := NewChunker(cfg)
	if err != nil {
		return nil, err
	}
	if workers <= 0 {
		workers = runtime.NumCPU()
		if workers < 1 {
			workers = 1
		}
	}
	scanned, err := collectRegularFiles(dir)
	if err != nil {
		return nil, err
	}

	entries := make([]FileEntry, len(scanned))
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	jobs := make(chan int, len(scanned))
	for i := range scanned {
		jobs <- i
	}
	close(jobs)

	var (
		wg       sync.WaitGroup
		firstErr error
		errOnce  sync.Once
	)
	setErr := func(e error) {
		errOnce.Do(func() {
			firstErr = e
			cancel()
		})
	}

	nWorkers := workers
	if nWorkers > len(scanned) && len(scanned) > 0 {
		nWorkers = len(scanned)
	}
	for w := 0; w < nWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range jobs {
				if runCtx.Err() != nil {
					setErr(runCtx.Err())
					return
				}
				sf := scanned[idx]
				data, err := os.ReadFile(sf.absPath)
				if err != nil {
					setErr(fmt.Errorf("parasync: read %s: %w", sf.relPath, err))
					return
				}
				spans := chunker.ChunkBytes(data)
				refs := make([]ChunkRef, len(spans))
				for j, sp := range spans {
					refs[j] = ChunkRef{
						Offset: sp.Offset,
						Length: sp.Length,
						Digest: sp.Digest,
					}
				}
				entries[idx] = FileEntry{
					Path:       sf.relPath,
					Size:       int64(len(data)),
					Mode:       sf.mode,
					FileDigest: SumDigest(data),
					Chunks:     refs,
				}
			}
		}()
	}
	wg.Wait()
	if firstErr != nil {
		return nil, firstErr
	}

	ckpt := &Checkpoint{
		ID:            meta.ID,
		TenantID:      meta.TenantID,
		VolumeID:      meta.VolumeID,
		CipherSuiteID: meta.CipherSuiteID,
		Chunking:      cfg,
		Files:         entries,
	}
	ckpt.RootDigest = ComputeRootDigest(ckpt)
	return ckpt, nil
}

// PublishCheckpoint chunks all regular files in dir, packs unique chunks into
// immutable tenant/volume-scoped pack objects in store, and returns the
// verified Checkpoint manifest.
func PublishCheckpoint(ctx context.Context, dir string, store backend.Backend, meta CheckpointMeta, cfg ChunkConfig, opts PublishOptions) (*Checkpoint, error) {
	if store == nil {
		return nil, fmt.Errorf("parasync: nil backend store")
	}
	if meta.TenantID == "" || meta.VolumeID == "" || meta.CipherSuiteID == "" {
		return nil, fmt.Errorf("%w: tenant_id, volume_id, and cipher_suite_id are required", ErrIncompatibleCheckpoint)
	}
	opts = opts.normalized()
	chunker, err := NewChunker(cfg)
	if err != nil {
		return nil, err
	}
	scanned, err := collectRegularFiles(dir)
	if err != nil {
		return nil, err
	}

	type fileResult struct {
		entry FileEntry
		data  []byte
	}
	type packLoc struct {
		key string
		off int64
		len int64
	}
	type pendingRef struct {
		fileIdx  int
		chunkIdx int
	}

	nWorkers := opts.Workers
	if nWorkers < 1 {
		nWorkers = 1
	}

	entries := make([]FileEntry, len(scanned))
	locByDigest := make(map[Digest]*packLoc)

	var (
		packBuf     bytes.Buffer
		curPackLocs []*packLoc
		curPackRefs []pendingRef
	)
	flushPack := func() error {
		if packBuf.Len() == 0 {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		payload := append([]byte(nil), packBuf.Bytes()...)
		packDigest := SumDigest(payload)
		packHex := packDigest.Hex()
		key := fmt.Sprintf("%s/pack/%s/%s", TenantScopePrefix(meta.TenantID, meta.VolumeID), packHex[:2], packHex)
		if err := backend.ValidKey(key); err != nil {
			return err
		}
		if _, err := store.PutIfAbsent(ctx, key, payload); err != nil && !errors.Is(err, backend.ErrConflict) {
			return fmt.Errorf("parasync: put immutable pack %s: %w", key, err)
		}
		for _, loc := range curPackLocs {
			loc.key = key
		}
		for _, ref := range curPackRefs {
			entries[ref.fileIdx].Chunks[ref.chunkIdx].PackKey = key
		}
		packBuf.Reset()
		curPackLocs = curPackLocs[:0]
		curPackRefs = curPackRefs[:0]
		return nil
	}

	// Stream files in bounded parallel batches of size nWorkers so retained
	// payload memory scales with O(workers * file_size + PackMaxBytes) rather
	// than total checkpoint size.
	for batchStart := 0; batchStart < len(scanned); batchStart += nWorkers {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		batchEnd := batchStart + nWorkers
		if batchEnd > len(scanned) {
			batchEnd = len(scanned)
		}
		batchLen := batchEnd - batchStart
		batchResults := make([]fileResult, batchLen)

		runCtx, cancel := context.WithCancel(ctx)
		var (
			wg       sync.WaitGroup
			firstErr error
			errOnce  sync.Once
		)
		setErr := func(e error) {
			errOnce.Do(func() {
				firstErr = e
				cancel()
			})
		}

		for bi := 0; bi < batchLen; bi++ {
			wg.Add(1)
			go func(localIdx, globalIdx int) {
				defer wg.Done()
				if runCtx.Err() != nil {
					setErr(runCtx.Err())
					return
				}
				sf := scanned[globalIdx]
				data, err := os.ReadFile(sf.absPath)
				if err != nil {
					setErr(fmt.Errorf("parasync: read %s: %w", sf.relPath, err))
					return
				}
				spans := chunker.ChunkBytes(data)
				refs := make([]ChunkRef, len(spans))
				for j, sp := range spans {
					refs[j] = ChunkRef{
						Offset: sp.Offset,
						Length: sp.Length,
						Digest: sp.Digest,
					}
				}
				batchResults[localIdx] = fileResult{
					entry: FileEntry{
						Path:       sf.relPath,
						Size:       int64(len(data)),
						Mode:       sf.mode,
						FileDigest: SumDigest(data),
						Chunks:     refs,
					},
					data: data,
				}
			}(bi, batchStart+bi)
		}
		wg.Wait()
		cancel()
		if firstErr != nil {
			return nil, firstErr
		}

		for bi := 0; bi < batchLen; bi++ {
			fileIdx := batchStart + bi
			fe := batchResults[bi].entry
			data := batchResults[bi].data
			entries[fileIdx] = fe
			for j := range entries[fileIdx].Chunks {
				ch := &entries[fileIdx].Chunks[j]
				if loc, ok := locByDigest[ch.Digest]; ok {
					ch.PackOffset = loc.off
					ch.PackLength = loc.len
					if loc.key != "" {
						ch.PackKey = loc.key
					} else {
						curPackRefs = append(curPackRefs, pendingRef{fileIdx: fileIdx, chunkIdx: j})
					}
					continue
				}
				plain := data[ch.Offset : ch.Offset+ch.Length]
				frame, err := sealChunkFrame(opts.Cipher, plain)
				if err != nil {
					return nil, err
				}
				if int64(packBuf.Len()) > 0 && int64(packBuf.Len())+int64(len(frame)) > opts.PackMaxBytes {
					if err := flushPack(); err != nil {
						return nil, err
					}
				}
				off := int64(packBuf.Len())
				packBuf.Write(frame)
				loc := &packLoc{off: off, len: int64(len(frame))}
				locByDigest[ch.Digest] = loc
				curPackLocs = append(curPackLocs, loc)
				curPackRefs = append(curPackRefs, pendingRef{fileIdx: fileIdx, chunkIdx: j})
				ch.PackOffset = loc.off
				ch.PackLength = loc.len
			}
			// Release file buffer immediately after packing.
			batchResults[bi].data = nil
		}
	}
	if err := flushPack(); err != nil {
		return nil, err
	}

	ckpt := &Checkpoint{
		ID:            meta.ID,
		TenantID:      meta.TenantID,
		VolumeID:      meta.VolumeID,
		CipherSuiteID: meta.CipherSuiteID,
		Chunking:      cfg,
		Files:         entries,
	}
	ckpt.RootDigest = ComputeRootDigest(ckpt)
	if err := ckpt.ValidateStructure(); err != nil {
		return nil, err
	}
	return ckpt, nil
}
