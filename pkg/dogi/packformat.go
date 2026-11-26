// Copyright 2026 Hanzo AI Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package dogi

import (
	"crypto/subtle"
	"encoding/binary"
	"fmt"

	"github.com/zeebo/blake3"
)

var packMagic = [4]byte{'D', 'O', 'G', 'I'}

const (
	packFormatVersion uint8 = 1
	packHeaderSize          = 8 // [4 magic][1 ver][1 class][1 flags][1 reserved]
	packFlagEncrypted uint8 = 1 << 0

	// frameFixedHeaderSize = 32 (chunkID) + 4 (plainLen) + 4 (payloadLen) + 2 (keyIDLen)
	frameFixedHeaderSize = 32 + 4 + 4 + 2

	aadLen = 32
)

// sumChunk computes the canonical BLAKE3-256 content digest of chunk plaintext.
func sumChunk(plain []byte) [32]byte {
	return blake3.Sum256(plain)
}

// chunkAAD derives a 32-byte domain-separated binding tag tying a sealed chunk
// to its exact (TenantID, VolumeID, ChunkID), preventing cross-tenant or
// cross-volume frame transplant even if a cipher were accidentally reused.
func chunkAAD(ns Namespace, id [32]byte) [aadLen]byte {
	h := blake3.New()
	_, _ = h.Write([]byte("hanzoai/vfs/dogi/chunk/v1\x00"))
	_, _ = h.Write([]byte(ns.TenantID))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(ns.VolumeID))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write(id[:])
	var out [aadLen]byte
	copy(out[:], h.Sum(nil))
	return out
}

func encodePackHeader(class LifetimeClass, encrypted bool) [packHeaderSize]byte {
	var hdr [packHeaderSize]byte
	copy(hdr[0:4], packMagic[:])
	hdr[4] = packFormatVersion
	hdr[5] = uint8(class)
	if encrypted {
		hdr[6] = packFlagEncrypted
	}
	return hdr
}

func validatePackHeader(hdr []byte) (LifetimeClass, bool, error) {
	if len(hdr) < packHeaderSize {
		return 0, false, fmt.Errorf("%w: truncated pack header (%d bytes)", ErrCorruptPack, len(hdr))
	}
	if hdr[0] != packMagic[0] || hdr[1] != packMagic[1] || hdr[2] != packMagic[2] || hdr[3] != packMagic[3] {
		return 0, false, fmt.Errorf("%w: invalid pack magic", ErrCorruptPack)
	}
	if hdr[4] != packFormatVersion {
		return 0, false, fmt.Errorf("%w: unsupported pack format version %d", ErrCorruptPack, hdr[4])
	}
	if hdr[5] >= NumClasses {
		return 0, false, fmt.Errorf("%w: invalid lifetime class %d", ErrCorruptPack, hdr[5])
	}
	return LifetimeClass(hdr[5]), (hdr[6] & packFlagEncrypted) != 0, nil
}

// encodeChunkFrame builds a self-describing chunk frame suitable for ranged reads.
func encodeChunkFrame(ns Namespace, id [32]byte, plain []byte, sealer Sealer) ([]byte, error) {
	var keyID string
	payload := plain

	if sealer != nil {
		aad := chunkAAD(ns, id)
		buf := make([]byte, 0, aadLen+len(plain))
		buf = append(buf, aad[:]...)
		buf = append(buf, plain...)
		sealed, kid, err := sealer.Seal(buf)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrCipherFailed, err)
		}
		if len(kid) == 0 || len(kid) > 65535 {
			return nil, fmt.Errorf("%w: invalid keyID length %d", ErrCipherFailed, len(kid))
		}
		keyID = kid
		payload = sealed
	}

	frame := make([]byte, frameFixedHeaderSize+len(keyID)+len(payload))
	copy(frame[0:32], id[:])
	binary.LittleEndian.PutUint32(frame[32:36], uint32(len(plain)))
	binary.LittleEndian.PutUint32(frame[36:40], uint32(len(payload)))
	binary.LittleEndian.PutUint16(frame[40:42], uint16(len(keyID)))
	off := frameFixedHeaderSize
	copy(frame[off:off+len(keyID)], keyID)
	off += len(keyID)
	copy(frame[off:], payload)
	return frame, nil
}

// decodeChunkFrame validates framing, decrypts (if sealed), checks AAD binding,
// and verifies the BLAKE3-256 content digest against expectedID.
func decodeChunkFrame(ns Namespace, expectedID [32]byte, frame []byte, sealer Sealer) ([]byte, error) {
	if len(frame) < frameFixedHeaderSize {
		return nil, fmt.Errorf("%w: truncated chunk frame (%d bytes)", ErrCorruptPack, len(frame))
	}
	var storedID [32]byte
	copy(storedID[:], frame[0:32])
	if storedID != expectedID {
		return nil, fmt.Errorf("%w: frame chunk ID mismatch", ErrCorruptPack)
	}
	plainLen := int(binary.LittleEndian.Uint32(frame[32:36]))
	payloadLen := int(binary.LittleEndian.Uint32(frame[36:40]))
	keyIDLen := int(binary.LittleEndian.Uint16(frame[40:42]))
	wantLen := frameFixedHeaderSize + keyIDLen + payloadLen
	if len(frame) != wantLen {
		return nil, fmt.Errorf("%w: frame length %d != expected %d", ErrCorruptPack, len(frame), wantLen)
	}

	off := frameFixedHeaderSize
	keyID := string(frame[off : off+keyIDLen])
	off += keyIDLen
	payload := frame[off : off+payloadLen]

	var plain []byte
	if sealer != nil {
		if keyIDLen == 0 {
			return nil, fmt.Errorf("%w: expected encrypted frame but keyID is empty", ErrCorruptPack)
		}
		opened, err := sealer.Open(append([]byte(nil), payload...), keyID)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrCipherFailed, err)
		}
		if len(opened) < aadLen {
			return nil, fmt.Errorf("%w: decrypted payload shorter than AAD", ErrCorruptPack)
		}
		wantAAD := chunkAAD(ns, expectedID)
		if subtle.ConstantTimeCompare(opened[:aadLen], wantAAD[:]) != 1 {
			return nil, fmt.Errorf("%w: namespace/chunk AAD mismatch", ErrCorruptPack)
		}
		plain = opened[aadLen:]
	} else {
		if keyIDLen != 0 {
			return nil, fmt.Errorf("%w: frame is encrypted but no Sealer configured", ErrCipherFailed)
		}
		plain = append([]byte(nil), payload...)
	}

	if len(plain) != plainLen {
		return nil, fmt.Errorf("%w: decoded plaintext length %d != header %d", ErrCorruptPack, len(plain), plainLen)
	}
	if sumChunk(plain) != expectedID {
		return nil, fmt.Errorf("%w: BLAKE3 content verification failed", ErrCorruptPack)
	}
	return plain, nil
}
