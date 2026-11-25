package parasync

import (
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
)

// WorkloadKind identifies one of the 4 canonical checkpoint-evolution workloads.
type WorkloadKind string

const (
	WorkloadSmallEdit2Pct    WorkloadKind = "small_edit_2pct_churn"
	WorkloadMediumBuild15Pct WorkloadKind = "medium_build_15pct_churn"
	WorkloadByteShift5Pct    WorkloadKind = "byte_shift_insertion_5pct"
	WorkloadFullRewrite95Pct WorkloadKind = "full_rewrite_95pct_churn"
)

// AllWorkloads returns the 4 matched checkpoint-evolution workloads in order.
func AllWorkloads() []WorkloadKind {
	return []WorkloadKind{
		WorkloadSmallEdit2Pct,
		WorkloadMediumBuild15Pct,
		WorkloadByteShift5Pct,
		WorkloadFullRewrite95Pct,
	}
}

// WorkloadFixture holds the generated base and target volume directories along
// with plaintext canary markers for zero-knowledge S3 auditing.
type WorkloadFixture struct {
	Kind          WorkloadKind
	BaseDir       string
	TargetDir     string
	BaseBytes     int64
	TargetBytes   int64
	CanaryMarkers [][]byte
}

func fillDeterministicFile(seed uint64, size int, canary string) []byte {
	rng := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
	buf := make([]byte, size)
	for i := 0; i < len(buf); i += 8 {
		v := rng.Uint64()
		rem := len(buf) - i
		if rem >= 8 {
			buf[i+0] = byte(v)
			buf[i+1] = byte(v >> 8)
			buf[i+2] = byte(v >> 16)
			buf[i+3] = byte(v >> 24)
			buf[i+4] = byte(v >> 32)
			buf[i+5] = byte(v >> 40)
			buf[i+6] = byte(v >> 48)
			buf[i+7] = byte(v >> 56)
		} else {
			for j := 0; j < rem; j++ {
				buf[i+j] = byte(v >> (8 * uint(j)))
			}
		}
	}
	// Stamp plaintext canary markers at predictable offsets so ciphertext-only S3
	// audits can verify zero plaintext ever reaches object storage.
	cb := []byte(canary)
	if len(cb) > 0 && len(buf) >= len(cb)+64 {
		copy(buf[32:], cb)
		if mid := len(buf) / 2; mid+len(cb) < len(buf) {
			copy(buf[mid:], cb)
		}
	}
	return buf
}

func writeFile(root, rel string, data []byte) error {
	full := filepath.Join(root, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		return err
	}
	return os.WriteFile(full, data, 0o644)
}

// GenerateWorkload creates deterministic baseDir and targetDir volumes under
// workDir for the requested WorkloadKind with approximately totalSizeMiB of
// volume data across 16 realistic project/artifact files.
func GenerateWorkload(workDir string, kind WorkloadKind, totalSizeMiB int) (*WorkloadFixture, error) {
	if totalSizeMiB <= 0 {
		totalSizeMiB = 16
	}
	const numFiles = 16
	fileSize := (totalSizeMiB << 20) / numFiles
	if fileSize < 128<<10 {
		fileSize = 128 << 10
	}

	baseDir := filepath.Join(workDir, string(kind)+"_base")
	targetDir := filepath.Join(workDir, string(kind)+"_target")
	if err := os.MkdirAll(baseDir, 0o755); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(targetDir, 0o755); err != nil {
		return nil, err
	}

	fix := &WorkloadFixture{
		Kind:      kind,
		BaseDir:   baseDir,
		TargetDir: targetDir,
	}

	relPaths := make([]string, numFiles)
	baseContents := make([][]byte, numFiles)
	for i := 0; i < numFiles; i++ {
		sub := "src"
		ext := "bin"
		if i%4 == 0 {
			sub = "artifacts"
			ext = "o"
		} else if i%4 == 1 {
			sub = "data"
			ext = "pack"
		} else if i%4 == 2 {
			sub = "pkg"
			ext = "dat"
		}
		rel := fmt.Sprintf("%s/module_%02d.%s", sub, i, ext)
		relPaths[i] = rel
		canary := fmt.Sprintf("VFS_PARASYNC_CANARY_%s_FILE_%02d", string(kind), i)
		fix.CanaryMarkers = append(fix.CanaryMarkers, []byte(canary), []byte(rel))

		raw := fillDeterministicFile(uint64(1000+i)*0x10001, fileSize, canary)
		baseContents[i] = raw
		if err := writeFile(baseDir, rel, raw); err != nil {
			return nil, err
		}
		fix.BaseBytes += int64(len(raw))
	}

	// Build target volume according to the workload evolution pattern.
	for i := 0; i < numFiles; i++ {
		rel := relPaths[i]
		targetData := append([]byte(nil), baseContents[i]...)

		switch kind {
		case WorkloadSmallEdit2Pct:
			// ~2% total volume churn: unaligned in-place edits in 4 of 16 files
			// (8% of each of those 4 files = 2.0% of total volume).
			if i < 4 {
				editLen := fileSize / 12
				if editLen < 4096 {
					editLen = 4096
				}
				patch := fillDeterministicFile(uint64(20000+i), editLen, fmt.Sprintf("EDIT_2PCT_%02d", i))
				off := (fileSize / 4) + 13579
				if off+editLen > len(targetData) {
					off = 13579
				}
				copy(targetData[off:off+editLen], patch)
			}

		case WorkloadMediumBuild15Pct:
			// ~15% total volume churn: 2 files completely rebuilt (12.5%) +
			// 4 files partially edited (~2.5%).
			if i < 2 {
				targetData = fillDeterministicFile(uint64(30000+i), fileSize, fmt.Sprintf("REBUILD_15PCT_%02d", i))
			} else if i < 6 {
				editLen := fileSize / 10
				patch := fillDeterministicFile(uint64(31000+i), editLen, fmt.Sprintf("PATCH_15PCT_%02d", i))
				off := (fileSize / 3) + 7919
				if off+editLen > len(targetData) {
					off = 7919
				}
				copy(targetData[off:off+editLen], patch)
			}

		case WorkloadByteShift5Pct:
			// ~5% total volume insertion near the start of 12 of 16 files:
			// shifts every subsequent byte forward by ~6.67% of fileSize
			// (total added bytes = 12/16 * 6.67% = 5.0% of volume).
			// Fixed 64 KiB chunking loses alignment across all 12 shifted files (~75% churn),
			// while FastCDC re-synchronizes after the inserted prefix!
			if i < 12 {
				insertLen := fileSize / 15 // ~68 KiB on a 1 MiB file; NOT a multiple of 64 KiB
				// Ensure insertLen is intentionally misaligned to 64 KiB boundaries.
				if insertLen%4096 == 0 {
					insertLen += 1373
				}
				insertBlock := fillDeterministicFile(uint64(40000+i), insertLen, fmt.Sprintf("INSERT_5PCT_%02d", i))
				insertAt := 2048
				shifted := make([]byte, 0, len(targetData)+len(insertBlock))
				shifted = append(shifted, targetData[:insertAt]...)
				shifted = append(shifted, insertBlock...)
				shifted = append(shifted, targetData[insertAt:]...)
				targetData = shifted
			}

		case WorkloadFullRewrite95Pct:
			// >=95% churn (honest negative regime): 15 of 16 files completely
			// rewritten (93.75%) + 40% of the last file rewritten = ~96.25% churn!
			if i < 15 {
				targetData = fillDeterministicFile(uint64(50000+i), fileSize, fmt.Sprintf("REWRITE_95PCT_%02d", i))
			} else {
				editLen := (fileSize * 4) / 10
				patch := fillDeterministicFile(uint64(51000+i), editLen, "REWRITE_95PCT_TAIL")
				copy(targetData[:editLen], patch)
			}

		default:
			return nil, fmt.Errorf("parasync: unknown workload kind %q", kind)
		}

		if err := writeFile(targetDir, rel, targetData); err != nil {
			return nil, err
		}
		fix.TargetBytes += int64(len(targetData))
	}

	return fix, nil
}
