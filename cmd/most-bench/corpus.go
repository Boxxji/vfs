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

package main

import (
	"encoding/hex"
	"fmt"
	"io/fs"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/zeebo/blake3"

	"github.com/hanzoai/vfs/pkg/most"
)

// corpusBlock represents one fixed-size shared immutable toolchain block.
type corpusBlock struct {
	ID        most.BlockID
	Data      []byte
	Namespace string
	Origin    string // "repo" or "synthetic_elf"
}

// corpusStats summarizes the benchmark corpus without leaking host paths.
type corpusStats struct {
	RepoFilesSampled        int      `json:"repo_files_sampled"`
	RepoBlocksCount         int      `json:"repo_blocks_count"`
	SyntheticELFBlocksCount int      `json:"synthetic_elf_blocks_count"`
	TotalBlocks             int      `json:"total_blocks"`
	BlockSizeBytes          int      `json:"block_size_bytes"`
	TotalCorpusBytes        int64    `json:"total_corpus_bytes"`
	TotalCorpusMiB          float64  `json:"total_corpus_mib"`
	Namespaces              []string `json:"namespaces"`
	CorpusBLAKE3Root        string   `json:"corpus_blake3_root"`
}

// buildToolchainCorpus constructs a deterministic shared toolchain corpus
// grounded in real repository files under repoRoot plus deterministic
// compiler/runtime ELF & header blocks to reach targetBlocks.
func buildToolchainCorpus(repoRoot string, seed uint64, blockSize, targetBlocks int) ([]corpusBlock, corpusStats, error) {
	if blockSize <= 0 || targetBlocks <= 0 {
		return nil, corpusStats{}, fmt.Errorf("invalid blockSize=%d targetBlocks=%d", blockSize, targetBlocks)
	}

	var relPaths []string
	err := filepath.WalkDir(repoRoot, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return nil
		}
		rel, err := filepath.Rel(repoRoot, path)
		if err != nil {
			return nil
		}
		rel = filepath.ToSlash(rel)
		if d.IsDir() {
			base := d.Name()
			if base == ".git" || base == "node_modules" || strings.HasPrefix(rel, "docs/research") {
				return filepath.SkipDir
			}
			return nil
		}
		ext := strings.ToLower(filepath.Ext(d.Name()))
		switch ext {
		case ".go", ".md", ".mod", ".sum", ".s", ".json", ".yml", ".sh":
			info, err := d.Info()
			if err == nil && info.Mode().IsRegular() && info.Size() > 0 && info.Size() <= 2<<20 {
				relPaths = append(relPaths, rel)
			}
		}
		return nil
	})
	if err != nil {
		return nil, corpusStats{}, err
	}
	sort.Strings(relPaths)

	namespaces := []string{"go-1-27-stdlib", "rust-1-85-core", "node-22-runtime"}
	seenDigests := make(map[[32]byte]struct{}, targetBlocks)
	blocks := make([]corpusBlock, 0, targetBlocks)
	repoFilesSampled := 0
	repoBlocks := 0

	// Cap real-repo contribution at ~70% of corpus so compiler/runtime ELF blocks
	// always represent the remaining ~30% of a realistic multi-language toolchain.
	maxRepoBlocks := (targetBlocks * 7) / 10
	if maxRepoBlocks < 1 {
		maxRepoBlocks = targetBlocks
	}

	rng := rand.New(rand.NewPCG(seed, seed^0x517cc1b727220a95))

	for _, rel := range relPaths {
		if repoBlocks >= maxRepoBlocks {
			break
		}
		raw, err := os.ReadFile(filepath.Join(repoRoot, filepath.FromSlash(rel)))
		if err != nil || len(raw) == 0 {
			continue
		}
		repoFilesSampled++
		ns := namespaces[repoFilesSampled%len(namespaces)]
		for off := 0; off < len(raw) && repoBlocks < maxRepoBlocks; off += blockSize {
			end := off + blockSize
			if end > len(raw) {
				end = len(raw)
			}
			chunk := make([]byte, blockSize)
			n := copy(chunk, raw[off:end])
			if n < blockSize {
				// Deterministic tail padding seeded by relative path & offset (no host path).
				tailHash := blake3.Sum256([]byte(fmt.Sprintf("%s:%d:%d", rel, off, seed)))
				for i := n; i < blockSize; i++ {
					chunk[i] = tailHash[i%len(tailHash)] ^ byte(i)
				}
			}
			sum := blake3.Sum256(chunk)
			if _, dup := seenDigests[sum]; dup {
				continue
			}
			seenDigests[sum] = struct{}{}
			id, err := most.NewSharedBlockIDWithDigest(ns, sum)
			if err != nil {
				return nil, corpusStats{}, err
			}
			blocks = append(blocks, corpusBlock{
				ID:        id,
				Data:      chunk,
				Namespace: ns,
				Origin:    "repo",
			})
			repoBlocks++
		}
	}

	// Fill remaining blocks with deterministic ELF/runtime header & symbol table blocks.
	elfMagic := []byte{0x7f, 'E', 'L', 'F', 2, 1, 1, 0}
	synthCount := 0
	for len(blocks) < targetBlocks {
		ns := namespaces[synthCount%len(namespaces)]
		chunk := make([]byte, blockSize)
		copy(chunk, elfMagic)
		// Embed deterministic section table & instruction-like words.
		for i := len(elfMagic); i+8 <= blockSize; i += 8 {
			v := rng.Uint64()
			chunk[i] = byte(v)
			chunk[i+1] = byte(v >> 8)
			chunk[i+2] = byte(v >> 16)
			chunk[i+3] = byte(v >> 24)
			chunk[i+4] = byte(v >> 32)
			chunk[i+5] = byte(v >> 40)
			chunk[i+6] = byte(v >> 48)
			chunk[i+7] = byte(v >> 56)
		}
		sum := blake3.Sum256(chunk)
		if _, dup := seenDigests[sum]; dup {
			continue
		}
		seenDigests[sum] = struct{}{}
		id, err := most.NewSharedBlockIDWithDigest(ns, sum)
		if err != nil {
			return nil, corpusStats{}, err
		}
		blocks = append(blocks, corpusBlock{
			ID:        id,
			Data:      chunk,
			Namespace: ns,
			Origin:    "synthetic_elf",
		})
		synthCount++
	}

	// Compute root BLAKE3 digest over all corpus block digests for reproducibility.
	rootHasher := blake3.New()
	for _, b := range blocks {
		_, _ = rootHasher.Write(b.ID.Digest[:])
	}
	var rootSum [32]byte
	rootHasher.Sum(rootSum[:0])

	totalBytes := int64(len(blocks)) * int64(blockSize)
	stats := corpusStats{
		RepoFilesSampled:        repoFilesSampled,
		RepoBlocksCount:         repoBlocks,
		SyntheticELFBlocksCount: synthCount,
		TotalBlocks:             len(blocks),
		BlockSizeBytes:          blockSize,
		TotalCorpusBytes:        totalBytes,
		TotalCorpusMiB:          float64(totalBytes) / (1024 * 1024),
		Namespaces:              namespaces,
		CorpusBLAKE3Root:        hex.EncodeToString(rootSum[:]),
	}
	return blocks, stats, nil
}
