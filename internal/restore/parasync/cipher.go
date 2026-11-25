// Package parasync implements a clean-room, ParaSync-inspired parallel delta
// restore planner and executor for encrypted hanzoai/vfs checkpoints over
// ciphertext-only S3 object storage (HIP-0302, HIP-1344).
//
// Reference: Zhang et al., "ParaSync", USENIX FAST '26
// (https://www.usenix.org/conference/fast26/presentation/zhang-zhihao-parasync).
package parasync

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/subtle"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/hanzoai/vfs/internal/blake3"
)

const (
	// DigestSize is the byte length of BLAKE3 digests used for chunk and
	// ciphertext addressing.
	DigestSize = blake3.Size

	// gcmNonceSize is the standard 96-bit AES-256-GCM nonce length.
	gcmNonceSize = 12

	// cipherFrameHeader is the domain tag prefix inside AEAD associated data.
	cipherFrameDomain   = "hanzoai/vfs/parasync/chunk-aad/v1"
	manifestFrameDomain = "hanzoai/vfs/parasync/manifest-aad/v1"
)

var (
	// ErrCiphertextCorrupt is returned when a downloaded object fails its
	// content-addressed ciphertext BLAKE3 digest check before decryption.
	ErrCiphertextCorrupt = errors.New("parasync: ciphertext digest mismatch")

	// ErrAuthFailed is returned when AES-256-GCM AEAD tag or associated-data
	// verification fails (tampering, wrong tenant key, or wrong key epoch).
	ErrAuthFailed = errors.New("parasync: AEAD authentication failed")

	// ErrPlaintextMismatch is returned when decrypted bytes fail their
	// tenant-keyed ChunkID check.
	ErrPlaintextMismatch = errors.New("parasync: decrypted chunk ID mismatch")

	// ErrCipherLeak is a defensive guard matching vfs/cipher.go: sealing must
	// never emit raw plaintext.
	ErrCipherLeak = errors.New("parasync: sealed ciphertext contains plaintext")
)

// Digest is a 32-byte BLAKE3 digest with compact lowercase hex JSON encoding.
type Digest [DigestSize]byte

// String returns the lowercase hex representation of d.
func (d Digest) String() string {
	return hex.EncodeToString(d[:])
}

// IsZero reports whether d is all zeros.
func (d Digest) IsZero() bool {
	var z Digest
	return d == z
}

// MarshalJSON encodes d as a lowercase hex JSON string.
func (d Digest) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.String())
}

// UnmarshalJSON decodes a lowercase hex JSON string into d.
func (d *Digest) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	raw, err := hex.DecodeString(s)
	if err != nil {
		return fmt.Errorf("parasync: decode digest hex: %w", err)
	}
	if len(raw) != DigestSize {
		return fmt.Errorf("parasync: digest length %d, want %d", len(raw), DigestSize)
	}
	copy(d[:], raw)
	return nil
}

// TenantKey holds per-tenant, per-key-epoch cryptographic material derived from
// a 256-bit tenant master secret.
//
// Strict multi-tenant isolation invariant:
//   - AES-256-GCM encryption keys, keyed BLAKE3 chunk IDs, FastCDC gear table
//     seeds, and opaque S3 namespace hashes are all cryptographically bound to
//     (TenantID, Epoch, MasterSecret).
//   - Two tenants storing identical plaintext produce completely distinct
//     FastCDC cut points, distinct ChunkIDs, distinct CipherDigests, and
//     disjoint S3 object prefixes (0% cross-tenant dedup by construction).
//   - Rotating Epoch for the same tenant derives fresh encryption, chunkID, and
//     gear subkeys so old and new epochs never silently splice.
type TenantKey struct {
	TenantID        string
	Epoch           uint64
	NamespacePrefix string // opaque S3 prefix: tenants/<nsHash>

	aead       cipher.AEAD
	chunkIDKey [32]byte
	gearSeed   [32]byte
}

// NewTenantKey constructs a TenantKey from an explicit 32-byte master secret.
func NewTenantKey(tenantID string, epoch uint64, masterSecret [32]byte) (*TenantKey, error) {
	if tenantID == "" {
		return nil, errors.New("parasync: tenantID must be non-empty")
	}
	if epoch == 0 {
		return nil, errors.New("parasync: key epoch must be >= 1")
	}

	material := make([]byte, 0, len(masterSecret)+len(tenantID)+8)
	material = append(material, masterSecret[:]...)
	material = append(material, []byte(tenantID)...)
	material = binary.LittleEndian.AppendUint64(material, epoch)

	var encKey [32]byte
	blake3.DeriveKey("hanzoai/vfs/parasync/aes256gcm/v1", material, encKey[:])

	block, err := aes.NewCipher(encKey[:])
	if err != nil {
		return nil, fmt.Errorf("parasync: init AES-256: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("parasync: init AES-GCM: %w", err)
	}

	var chunkIDKey [32]byte
	blake3.DeriveKey("hanzoai/vfs/parasync/chunk-id-key/v1", material, chunkIDKey[:])

	var gearSeed [32]byte
	blake3.DeriveKey("hanzoai/vfs/parasync/fastcdc-gear/v1", material, gearSeed[:])

	// Namespace prefix depends on tenantID + masterSecret (not epoch) so S3
	// never observes raw tenant names, while isolating tenants into disjoint prefixes.
	nsInput := make([]byte, 0, len(masterSecret)+len(tenantID))
	nsInput = append(nsInput, masterSecret[:]...)
	nsInput = append(nsInput, []byte(tenantID)...)
	var nsRaw [16]byte
	blake3.DeriveKey("hanzoai/vfs/parasync/s3-namespace/v1", nsInput, nsRaw[:])

	return &TenantKey{
		TenantID:        tenantID,
		Epoch:           epoch,
		NamespacePrefix: "tenants/" + hex.EncodeToString(nsRaw[:]),
		aead:            aead,
		chunkIDKey:      chunkIDKey,
		gearSeed:        gearSeed,
	}, nil
}

// DeriveTenantKey deterministically derives a TenantKey from a caller-provided
// seed string (intended for reproducible benchmarks and tests).
func DeriveTenantKey(tenantID string, epoch uint64, seed string) (*TenantKey, error) {
	var master [32]byte
	blake3.DeriveKey("hanzoai/vfs/parasync/test-master-seed/v1", []byte(tenantID+"\x00"+seed), master[:])
	return NewTenantKey(tenantID, epoch, master)
}

// GearSeed returns the per-tenant, per-epoch FastCDC gear table seed.
func (k *TenantKey) GearSeed() [32]byte {
	return k.gearSeed
}

// ChunkID computes the tenant-and-epoch-keyed BLAKE3 content identity of plain.
// This digest appears only inside encrypted checkpoint manifests and never on S3.
func (k *TenantKey) ChunkID(plain []byte) Digest {
	h := blake3.Keyed(&k.chunkIDKey)
	_, _ = h.Write(plain)
	var out Digest
	copy(out[:], h.Sum(nil))
	return out
}

// ChunkObjectKey returns the ciphertext-only S3 object key for a sealed chunk.
func (k *TenantKey) ChunkObjectKey(cipherDigest Digest) string {
	h := cipherDigest.String()
	return k.NamespacePrefix + "/chunks/" + h[:2] + "/" + h
}

// ManifestObjectKey returns the ciphertext-only S3 object key for a checkpoint manifest.
func (k *TenantKey) ManifestObjectKey(checkpointID string) string {
	sum := blake3.Sum([]byte(k.NamespacePrefix + "/manifest/" + checkpointID))
	return k.NamespacePrefix + "/manifests/" + hex.EncodeToString(sum[:16])
}

func (k *TenantKey) chunkAAD(chunkID Digest) []byte {
	aad := make([]byte, 0, len(cipherFrameDomain)+1+len(k.TenantID)+8+DigestSize)
	aad = append(aad, cipherFrameDomain...)
	aad = append(aad, '|')
	aad = append(aad, k.TenantID...)
	aad = binary.LittleEndian.AppendUint64(aad, k.Epoch)
	aad = append(aad, chunkID[:]...)
	return aad
}

func (k *TenantKey) manifestAAD(checkpointID string) []byte {
	aad := make([]byte, 0, len(manifestFrameDomain)+1+len(k.TenantID)+8+len(checkpointID))
	aad = append(aad, manifestFrameDomain...)
	aad = append(aad, '|')
	aad = append(aad, k.TenantID...)
	aad = binary.LittleEndian.AppendUint64(aad, k.Epoch)
	aad = append(aad, checkpointID...)
	return aad
}

// SealChunk encrypts plain using AES-256-GCM with a fresh cryptographic 96-bit
// random nonce and AAD bound to (TenantID, Epoch, ChunkID).
//
// Wire format stored in S3: [12-byte random nonce][AES-256-GCM ciphertext + 16-byte tag].
// S3 sees only this opaque blob indexed by its BLAKE3 ciphertext digest.
func (k *TenantKey) SealChunk(plain []byte) (chunkID Digest, cipherDigest Digest, ciphertext []byte, err error) {
	chunkID = k.ChunkID(plain)
	nonce := make([]byte, gcmNonceSize)
	if _, err := rand.Read(nonce); err != nil {
		return Digest{}, Digest{}, nil, fmt.Errorf("parasync: generate nonce: %w", err)
	}
	aad := k.chunkAAD(chunkID)
	out := make([]byte, gcmNonceSize, gcmNonceSize+len(plain)+k.aead.Overhead())
	copy(out, nonce)
	out = k.aead.Seal(out, nonce, plain, aad)

	// Defensive leak check for non-trivial chunks.
	if len(plain) >= 32 && bytes.Contains(out, plain[:32]) {
		return Digest{}, Digest{}, nil, ErrCipherLeak
	}
	cipherDigest = blake3.Sum(out)
	return chunkID, cipherDigest, out, nil
}

// OpenChunk verifies the content-addressed ciphertext BLAKE3 digest, decrypts
// and authenticates the AES-256-GCM payload against (TenantID, Epoch, ChunkID),
// and confirms the decrypted plaintext matches expectedChunkID.
func (k *TenantKey) OpenChunk(expectedChunkID, expectedCipherDigest Digest, ciphertext []byte) ([]byte, error) {
	if len(ciphertext) < gcmNonceSize+k.aead.Overhead() {
		return nil, fmt.Errorf("%w: truncated ciphertext (%d bytes)", ErrCiphertextCorrupt, len(ciphertext))
	}
	gotCipherDigest := Digest(blake3.Sum(ciphertext))
	if subtle.ConstantTimeCompare(gotCipherDigest[:], expectedCipherDigest[:]) != 1 {
		return nil, fmt.Errorf("%w: got %s want %s", ErrCiphertextCorrupt, gotCipherDigest.String()[:12], expectedCipherDigest.String()[:12])
	}
	nonce := ciphertext[:gcmNonceSize]
	sealed := ciphertext[gcmNonceSize:]
	aad := k.chunkAAD(expectedChunkID)
	plain, err := k.aead.Open(nil, nonce, sealed, aad)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrAuthFailed, err)
	}
	gotChunkID := k.ChunkID(plain)
	if subtle.ConstantTimeCompare(gotChunkID[:], expectedChunkID[:]) != 1 {
		return nil, ErrPlaintextMismatch
	}
	return plain, nil
}

// SealManifest encrypts a serialized checkpoint manifest for ciphertext-only S3 storage.
func (k *TenantKey) SealManifest(checkpointID string, plainJSON []byte) ([]byte, Digest, error) {
	nonce := make([]byte, gcmNonceSize)
	if _, err := rand.Read(nonce); err != nil {
		return nil, Digest{}, fmt.Errorf("parasync: manifest nonce: %w", err)
	}
	aad := k.manifestAAD(checkpointID)
	out := make([]byte, gcmNonceSize, gcmNonceSize+len(plainJSON)+k.aead.Overhead())
	copy(out, nonce)
	out = k.aead.Seal(out, nonce, plainJSON, aad)
	return out, blake3.Sum(out), nil
}

// OpenManifest decrypts and authenticates an encrypted checkpoint manifest.
func (k *TenantKey) OpenManifest(checkpointID string, ciphertext []byte) ([]byte, error) {
	if len(ciphertext) < gcmNonceSize+k.aead.Overhead() {
		return nil, fmt.Errorf("%w: truncated manifest", ErrCiphertextCorrupt)
	}
	nonce := ciphertext[:gcmNonceSize]
	sealed := ciphertext[gcmNonceSize:]
	plain, err := k.aead.Open(nil, nonce, sealed, k.manifestAAD(checkpointID))
	if err != nil {
		return nil, fmt.Errorf("%w: manifest open: %v", ErrAuthFailed, err)
	}
	return plain, nil
}
