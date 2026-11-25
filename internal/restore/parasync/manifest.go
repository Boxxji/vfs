package parasync

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

// ChunkRef references one content-addressed ciphertext chunk stored in S3.
// Plaintext ChunkIDs exist only inside the encrypted Checkpoint manifest and
// are never exposed to S3.
type ChunkRef struct {
	ChunkID      Digest `json:"chunk_id"`
	CipherDigest Digest `json:"cipher_digest"`
	ObjectKey    string `json:"object_key"`
	Offset       int64  `json:"offset"`
	PlainSize    int    `json:"plain_size"`
	CipherSize   int    `json:"cipher_size"`
}

// FileEntry records one file inside an encrypted volume checkpoint.
type FileEntry struct {
	Path   string     `json:"path"`
	Size   int64      `json:"size"`
	Mode   uint32     `json:"mode"`
	SHA256 string     `json:"sha256"`
	Chunks []ChunkRef `json:"chunks"`
}

// Checkpoint is the decrypted metadata manifest of an encrypted volume snapshot.
// On S3, this entire structure is stored strictly as an AEAD-sealed ciphertext
// object under tenants/<nsHash>/manifests/<hash>.
type Checkpoint struct {
	ID               string       `json:"id"`
	TenantID         string       `json:"tenant_id"`
	KeyEpoch         uint64       `json:"key_epoch"`
	ChunkingMode     ChunkingMode `json:"chunking_mode"`
	Files            []FileEntry  `json:"files"`
	TotalPlainBytes  int64        `json:"total_plain_bytes"`
	TotalCipherBytes int64        `json:"total_cipher_bytes"`
	TotalChunks      int          `json:"total_chunks"`
	UniqueChunks     int          `json:"unique_chunks"`
	VolumeSHA256     string       `json:"volume_sha256"`
	ManifestKey      string       `json:"manifest_key"`
	ManifestDigest   Digest       `json:"manifest_digest"`
}

// BuildAndUploadCheckpoint scans volumeDir, chunks every file according to mode
// (FastCDC with per-tenant gear table or fixed 64 KiB), seals chunks with
// AES-256-GCM, uploads only unseen ciphertext objects to store, and persists
// the encrypted manifest to store.
//
// If prev is non-nil and shares the exact same (TenantID, KeyEpoch, ChunkingMode),
// any chunk whose tenant-keyed ChunkID matches an already-uploaded chunk in prev
// reuses its existing immutable ciphertext object in S3 without re-encrypting
// or re-uploading. Cross-tenant or cross-epoch reuse is strictly forbidden.
func BuildAndUploadCheckpoint(
	ctx context.Context,
	store ObjectStore,
	key *TenantKey,
	mode ChunkingMode,
	cfg CDCConfig,
	checkpointID string,
	volumeDir string,
	prev *Checkpoint,
) (*Checkpoint, error) {
	cfg = cfg.normalized()
	gear := NewGearTable(key.GearSeed())

	// Index existing chunks strictly within the same (TenantID, KeyEpoch).
	existingByChunkID := make(map[Digest]ChunkRef)
	if prev != nil && prev.TenantID == key.TenantID && prev.KeyEpoch == key.Epoch {
		for _, f := range prev.Files {
			for _, c := range f.Chunks {
				if _, ok := existingByChunkID[c.ChunkID]; !ok {
					existingByChunkID[c.ChunkID] = c
				}
			}
		}
	}

	var relPaths []string
	err := filepath.WalkDir(volumeDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(volumeDir, path)
		if err != nil {
			return err
		}
		relPaths = append(relPaths, filepath.ToSlash(rel))
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("parasync: walk volume %s: %w", volumeDir, err)
	}
	sort.Strings(relPaths)

	cp := &Checkpoint{
		ID:           checkpointID,
		TenantID:     key.TenantID,
		KeyEpoch:     key.Epoch,
		ChunkingMode: mode,
		Files:        make([]FileEntry, len(relPaths)),
	}

	// Track chunks already uploaded within this checkpoint or reused from prev.
	var mu sync.Mutex
	uploaded := make(map[Digest]ChunkRef, len(existingByChunkID))
	for id, ref := range existingByChunkID {
		uploaded[id] = ref
	}

	volHash := sha256.New()
	uniqueCipher := make(map[Digest]struct{})

	for idx, rel := range relPaths {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		fullPath := filepath.Join(volumeDir, filepath.FromSlash(rel))
		info, err := os.Stat(fullPath)
		if err != nil {
			return nil, fmt.Errorf("parasync: stat %s: %w", rel, err)
		}
		data, err := os.ReadFile(fullPath)
		if err != nil {
			return nil, fmt.Errorf("parasync: read %s: %w", rel, err)
		}
		fSum := sha256.Sum256(data)
		fHex := hex.EncodeToString(fSum[:])
		writeVolumeHashEntry(volHash, rel, int64(len(data)), fSum[:])

		spans := ChunkBytes(mode, gear, data, cfg)
		refs := make([]ChunkRef, len(spans))

		for j, sp := range spans {
			plain := data[sp.Offset : sp.Offset+int64(sp.Length)]
			cid := key.ChunkID(plain)

			mu.Lock()
			known, ok := uploaded[cid]
			mu.Unlock()

			if ok && known.PlainSize == len(plain) {
				ref := ChunkRef{
					ChunkID:      known.ChunkID,
					CipherDigest: known.CipherDigest,
					ObjectKey:    known.ObjectKey,
					Offset:       sp.Offset,
					PlainSize:    known.PlainSize,
					CipherSize:   known.CipherSize,
				}
				refs[j] = ref
				cp.TotalCipherBytes += int64(ref.CipherSize)
				uniqueCipher[ref.CipherDigest] = struct{}{}
				continue
			}

			chunkID, cipherDigest, ciphertext, err := key.SealChunk(plain)
			if err != nil {
				return nil, fmt.Errorf("parasync: seal chunk %s@%d: %w", rel, sp.Offset, err)
			}
			objKey := key.ChunkObjectKey(cipherDigest)
			if err := store.Put(ctx, objKey, ciphertext); err != nil {
				return nil, fmt.Errorf("parasync: put chunk %s: %w", objKey, err)
			}
			ref := ChunkRef{
				ChunkID:      chunkID,
				CipherDigest: cipherDigest,
				ObjectKey:    objKey,
				Offset:       sp.Offset,
				PlainSize:    len(plain),
				CipherSize:   len(ciphertext),
			}
			mu.Lock()
			uploaded[chunkID] = ref
			mu.Unlock()

			refs[j] = ref
			cp.TotalCipherBytes += int64(ref.CipherSize)
			uniqueCipher[ref.CipherDigest] = struct{}{}
		}

		cp.Files[idx] = FileEntry{
			Path:   rel,
			Size:   int64(len(data)),
			Mode:   uint32(info.Mode().Perm()),
			SHA256: fHex,
			Chunks: refs,
		}
		cp.TotalPlainBytes += int64(len(data))
		cp.TotalChunks += len(refs)
	}

	cp.UniqueChunks = len(uniqueCipher)
	cp.VolumeSHA256 = hex.EncodeToString(volHash.Sum(nil))
	cp.ManifestKey = key.ManifestObjectKey(checkpointID)

	rawManifest, err := json.Marshal(cp)
	if err != nil {
		return nil, fmt.Errorf("parasync: marshal manifest: %w", err)
	}
	sealedManifest, manDigest, err := key.SealManifest(checkpointID, rawManifest)
	if err != nil {
		return nil, fmt.Errorf("parasync: seal manifest: %w", err)
	}
	if err := store.Put(ctx, cp.ManifestKey, sealedManifest); err != nil {
		return nil, fmt.Errorf("parasync: upload manifest: %w", err)
	}
	cp.ManifestDigest = manDigest
	return cp, nil
}

// LoadCheckpointManifest downloads and decrypts a checkpoint manifest from S3.
func LoadCheckpointManifest(ctx context.Context, store ObjectStore, key *TenantKey, checkpointID string) (*Checkpoint, error) {
	manKey := key.ManifestObjectKey(checkpointID)
	sealed, err := store.Get(ctx, manKey)
	if err != nil {
		return nil, fmt.Errorf("parasync: fetch manifest %s: %w", checkpointID, err)
	}
	plain, err := key.OpenManifest(checkpointID, sealed)
	if err != nil {
		return nil, err
	}
	var cp Checkpoint
	if err := json.Unmarshal(plain, &cp); err != nil {
		return nil, fmt.Errorf("parasync: unmarshal manifest: %w", err)
	}
	return &cp, nil
}

// ComputeVolumeSHA256 computes a deterministic whole-volume SHA-256 digest over
// all files (sorted relative paths, sizes, and exact file bytes) under volumeDir.
func ComputeVolumeSHA256(volumeDir string) (string, int64, error) {
	var relPaths []string
	err := filepath.WalkDir(volumeDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(volumeDir, path)
		if err != nil {
			return err
		}
		relPaths = append(relPaths, filepath.ToSlash(rel))
		return nil
	})
	if err != nil {
		return "", 0, err
	}
	sort.Strings(relPaths)

	volHash := sha256.New()
	var totalBytes int64
	for _, rel := range relPaths {
		data, err := os.ReadFile(filepath.Join(volumeDir, filepath.FromSlash(rel)))
		if err != nil {
			return "", 0, err
		}
		fSum := sha256.Sum256(data)
		writeVolumeHashEntry(volHash, rel, int64(len(data)), fSum[:])
		totalBytes += int64(len(data))
	}
	return hex.EncodeToString(volHash.Sum(nil)), totalBytes, nil
}

func writeVolumeHashEntry(h interface{ Write([]byte) (int, error) }, relPath string, size int64, fileSHA256 []byte) {
	var lenBuf [8]byte
	binary.LittleEndian.PutUint64(lenBuf[:], uint64(len(relPath)))
	_, _ = h.Write(lenBuf[:])
	_, _ = h.Write([]byte(relPath))
	binary.LittleEndian.PutUint64(lenBuf[:], uint64(size))
	_, _ = h.Write(lenBuf[:])
	_, _ = h.Write(fileSHA256)
}
