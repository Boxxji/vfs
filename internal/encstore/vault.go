// Copyright 2026 Hanzo AI Inc.
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

package encstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hanzoai/vfs/internal/blake3"
	"github.com/hanzoai/vfs/pkg/backend"
)

const (
	// DefaultChunkSize is the default plaintext chunk size (256 KiB).
	DefaultChunkSize = 256 << 10

	// MaxChunkSize caps per-chunk memory (4 MiB).
	MaxChunkSize = 4 << 20
)

// Vault manages an owner's cryptographic identity, monotonic key epoch,
// authorized peers, revocation state, and local DEK cache (which is explicitly
// zeroed on CryptoShred).
type Vault struct {
	owner *Identity

	mu              sync.Mutex
	currentEpoch    uint64
	revSeq          uint64
	authorizedPeers map[string]PublicIdentity
	revokedPeers    map[string]RevocationEntry
	revokedConsents map[string]struct{}
	localDEKCache   map[string][][]byte // artifactID -> cached DEKs (zeroed on CryptoShred)
	artifactHistory map[string][]string // artifactID -> manifestCIDs written by this vault
}

// NewVault creates a new Vault at epoch 1 for owner.
func NewVault(owner *Identity) (*Vault, error) {
	if owner == nil {
		return nil, errors.New("encstore: nil owner identity")
	}
	return &Vault{
		owner:           owner,
		currentEpoch:    1,
		authorizedPeers: make(map[string]PublicIdentity),
		revokedPeers:    make(map[string]RevocationEntry),
		revokedConsents: make(map[string]struct{}),
		localDEKCache:   make(map[string][][]byte),
		artifactHistory: make(map[string][]string),
	}, nil
}

// Owner returns the vault's owner identity.
func (v *Vault) Owner() *Identity { return v.owner }

// CurrentEpoch returns the vault's current key epoch.
func (v *Vault) CurrentEpoch() uint64 {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.currentEpoch
}

// AuthorizePeer grants peer decryption access for future artifacts written by
// this vault. If peer was previously revoked, re-authorization requires a
// strictly newer epoch (caller must explicitly re-authorize).
func (v *Vault) AuthorizePeer(peer PublicIdentity) error {
	if _, _, err := peer.Validate(); err != nil {
		return err
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	v.authorizedPeers[peer.KeyID] = peer
	delete(v.revokedPeers, peer.KeyID)
	return nil
}

// LocalDEKCount returns how many DEK copies are currently cached in memory for
// artifactID (verified to drop to 0 after CryptoShred).
func (v *Vault) LocalDEKCount(artifactID string) int {
	v.mu.Lock()
	defer v.mu.Unlock()
	return len(v.localDEKCache[artifactID])
}

// PutRequest describes an artifact to encrypt, sign, and replicate across
// independent storage providers.
type PutRequest struct {
	ArtifactID          string
	ArtifactName        string
	Kind                ArtifactKind
	ParentManifestCID   string
	License             LicensePolicy
	Consent             ConsentRecord
	Labels              map[string]string
	ProvenanceNotes     string
	ExperienceLedgerRef string
	ChunkSize           int
	Concurrency         int
	Data                []byte
}

// PutResult captures the signed manifest and telemetry of an encrypted write.
type PutResult struct {
	Manifest            *SignedManifest `json:"manifest"`
	ManifestCID         string          `json:"manifest_cid"`
	PlaintextBytes      int64           `json:"plaintext_bytes"`
	CiphertextBytes     int64           `json:"ciphertext_bytes"`
	ChunkCount          int             `json:"chunk_count"`
	RecipientCount      int             `json:"recipient_count"`
	PlaintextRootBLAKE3 string          `json:"plaintext_root_blake3"`
	PlaintextRootSHA256 string          `json:"plaintext_root_sha256"`
	Elapsed             time.Duration   `json:"elapsed"`
	EncryptDuration     time.Duration   `json:"encrypt_duration"`
	ReplicateDuration   time.Duration   `json:"replicate_duration"`
}

// PutArtifact encrypts req.Data into per-chunk AES-256-GCM frames, stores all
// ciphertext chunks under `enc/chunk/<2hex>/<ciphertext_blake3>`, encrypts
// plaintext metadata inside PrivateManifestPayload, wraps the DEK for the owner
// and all currently authorized peers, signs the manifest with Ed25519, and
// replicates across store.
func (v *Vault) PutArtifact(ctx context.Context, store *ReplicatedStore, req PutRequest) (*PutResult, error) {
	start := time.Now()
	if store == nil {
		return nil, errors.New("encstore: nil replicated store")
	}
	if err := ValidateSegment(req.ArtifactID); err != nil {
		return nil, err
	}
	if !req.Kind.Valid() {
		return nil, fmt.Errorf("%w: %q", ErrInvalidKind, req.Kind)
	}
	if err := req.License.Validate(); err != nil {
		return nil, err
	}
	consent := req.Consent
	if consent.GrantorKeyID == "" {
		consent.GrantorKeyID = v.owner.KeyID
	}
	if consent.IssuedAt.IsZero() {
		consent.IssuedAt = time.Now().UTC()
	}
	if consent.ConsentHash == "" {
		consent.ConsentHash = consent.ComputeConsentHash()
	}
	if err := consent.Validate(); err != nil {
		return nil, err
	}
	if len(req.Data) == 0 {
		return nil, fmt.Errorf("%w: artifact payload must be non-empty", ErrInvalidManifest)
	}
	if req.ParentManifestCID != "" {
		if err := ValidateHexDigest(req.ParentManifestCID); err != nil {
			return nil, fmt.Errorf("%w: parent_manifest_cid: %v", ErrInvalidManifest, err)
		}
	}

	chunkSize := req.ChunkSize
	if chunkSize <= 0 {
		chunkSize = DefaultChunkSize
	}
	if chunkSize > MaxChunkSize {
		chunkSize = MaxChunkSize
	}
	workers := req.Concurrency
	if workers <= 0 {
		workers = min(max(runtime.GOMAXPROCS(0), 2), 8)
	}

	// Snapshot epoch and authorized recipients deterministically.
	v.mu.Lock()
	epoch := v.currentEpoch
	recipients := make([]PublicIdentity, 0, len(v.authorizedPeers)+1)
	recipients = append(recipients, v.owner.Public("owner"))
	peerIDs := make([]string, 0, len(v.authorizedPeers))
	for pid := range v.authorizedPeers {
		if pid != v.owner.KeyID {
			peerIDs = append(peerIDs, pid)
		}
	}
	sort.Strings(peerIDs)
	for _, pid := range peerIDs {
		recipients = append(recipients, v.authorizedPeers[pid])
	}
	v.mu.Unlock()

	dek, err := randomBytes(KeySize)
	if err != nil {
		return nil, err
	}
	salt, err := randomBytes(SaltSize)
	if err != nil {
		zeroBytes(dek)
		return nil, err
	}

	// Compute plaintext root digests (stored ONLY inside encrypted PrivateManifestPayload).
	ptRootB3 := blake3.Sum(req.Data)
	ptRootS256 := sha256.Sum256(req.Data)
	ptRootB3Hex := hex.EncodeToString(ptRootB3[:])
	ptRootS256Hex := hex.EncodeToString(ptRootS256[:])

	numChunks := (len(req.Data) + chunkSize - 1) / chunkSize
	cipherRefs := make([]CipherChunkRef, numChunks)
	plainMetas := make([]PlainChunkMeta, numChunks)
	cipherFrames := make([][]byte, numChunks)

	encStart := time.Now()
	{
		var (
			wg      sync.WaitGroup
			nextIdx atomic.Int64
			errMu   sync.Mutex
			firstEr error
		)
		wCount := min(workers, numChunks)
		for w := 0; w < wCount; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					i := int(nextIdx.Add(1)) - 1
					if i >= numChunks {
						return
					}
					errMu.Lock()
					failed := firstEr != nil
					errMu.Unlock()
					if failed {
						return
					}
					lo := i * chunkSize
					hi := min(lo+chunkSize, len(req.Data))
					pt := req.Data[lo:hi]

					ptSum := blake3.Sum(pt)
					frame, encErr := encryptChunk(
						dek, salt, req.ArtifactID, epoch, uint64(i),
						req.Kind, req.License.SPDX, consent.ConsentHash, pt,
					)
					if encErr != nil {
						errMu.Lock()
						if firstEr == nil {
							firstEr = encErr
						}
						errMu.Unlock()
						return
					}
					ctSum := blake3.Sum(frame)
					cipherFrames[i] = frame
					cipherRefs[i] = CipherChunkRef{
						Index:          uint64(i),
						CiphertextCID:  hex.EncodeToString(ctSum[:]),
						CiphertextSize: int64(len(frame)),
					}
					plainMetas[i] = PlainChunkMeta{
						Index:           uint64(i),
						PlaintextBLAKE3: hex.EncodeToString(ptSum[:]),
						PlaintextSize:   int64(len(pt)),
					}
				}
			}()
		}
		wg.Wait()
		if firstEr != nil {
			zeroBytes(dek)
			return nil, firstEr
		}
	}
	encDur := time.Since(encStart)

	var totalCipherBytes int64
	for _, ref := range cipherRefs {
		totalCipherBytes += ref.CiphertextSize
	}

	// Build and encrypt PrivateManifestPayload (never visible in plaintext on nodes).
	artifactName := req.ArtifactName
	if artifactName == "" {
		artifactName = req.ArtifactID
	}
	privPayload := PrivateManifestPayload{
		ArtifactName:        artifactName,
		PlaintextTotalSize:  int64(len(req.Data)),
		PlaintextRootBLAKE3: ptRootB3Hex,
		PlaintextRootSHA256: ptRootS256Hex,
		Chunks:              plainMetas,
		Labels:              req.Labels,
		ProvenanceNotes:     req.ProvenanceNotes,
		ExperienceLedgerRef: req.ExperienceLedgerRef,
	}
	privJSON, err := json.Marshal(privPayload)
	if err != nil {
		zeroBytes(dek)
		return nil, err
	}
	privNonceHex, privCTHex, err := encryptPrivatePayload(
		dek, salt, req.ArtifactID, epoch, req.Kind, v.owner.KeyID, req.License.SPDX, consent.ConsentHash, privJSON,
	)
	if err != nil {
		zeroBytes(dek)
		return nil, err
	}

	// Wrap DEK in per-recipient X25519 ECDH + HKDF-SHA256 + AES-256-GCM envelopes.
	envelopes := make([]KeyEnvelope, 0, len(recipients))
	for _, rec := range recipients {
		env, err := wrapDEKForRecipient(dek, salt, req.ArtifactID, epoch, consent.ConsentHash, rec)
		if err != nil {
			zeroBytes(dek)
			return nil, err
		}
		envelopes = append(envelopes, env)
	}

	manifest := &SignedManifest{
		Version:                    FormatVersion,
		ArtifactID:                 req.ArtifactID,
		Kind:                       req.Kind,
		Epoch:                      epoch,
		ParentManifestCID:          req.ParentManifestCID,
		Owner:                      v.owner.Public("owner"),
		License:                    req.License,
		Consent:                    consent,
		EpochSaltHex:               hex.EncodeToString(salt),
		CipherChunks:               cipherRefs,
		PrivatePayloadNonceHex:     privNonceHex,
		EncryptedPrivatePayloadHex: privCTHex,
		KeyEnvelopes:               envelopes,
		CreatedAt:                  time.Now().UTC(),
	}
	if err := manifest.Sign(v.owner); err != nil {
		zeroBytes(dek)
		return nil, err
	}
	manifestBytes, err := json.Marshal(manifest)
	if err != nil {
		zeroBytes(dek)
		return nil, err
	}

	// Replicate ciphertext chunks concurrently across independent storage nodes.
	repStart := time.Now()
	{
		var (
			wg      sync.WaitGroup
			nextIdx atomic.Int64
			errMu   sync.Mutex
			firstEr error
		)
		wCount := min(workers, numChunks)
		for w := 0; w < wCount; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					i := int(nextIdx.Add(1)) - 1
					if i >= numChunks {
						return
					}
					errMu.Lock()
					failed := firstEr != nil
					errMu.Unlock()
					if failed {
						return
					}
					key, keyErr := ChunkKey(cipherRefs[i].CiphertextCID)
					if keyErr != nil {
						errMu.Lock()
						if firstEr == nil {
							firstEr = keyErr
						}
						errMu.Unlock()
						return
					}
					if _, putErr := store.PutQuorum(ctx, key, cipherFrames[i]); putErr != nil {
						errMu.Lock()
						if firstEr == nil {
							firstEr = putErr
						}
						errMu.Unlock()
						return
					}
				}
			}()
		}
		wg.Wait()
		if firstEr != nil {
			zeroBytes(dek)
			return nil, firstEr
		}
	}

	mKey, err := ManifestKey(manifest.ManifestCID)
	if err != nil {
		zeroBytes(dek)
		return nil, err
	}
	if _, err := store.PutQuorum(ctx, mKey, manifestBytes); err != nil {
		zeroBytes(dek)
		return nil, err
	}

	headKey, err := HeadKey(req.ArtifactID)
	if err != nil {
		zeroBytes(dek)
		return nil, err
	}
	headBytes, err := json.Marshal(HeadPointer{
		ArtifactID:  req.ArtifactID,
		Epoch:       epoch,
		ManifestCID: manifest.ManifestCID,
		UpdatedAt:   time.Now().UTC(),
	})
	if err != nil {
		zeroBytes(dek)
		return nil, err
	}
	if _, err := store.PutQuorum(ctx, headKey, headBytes); err != nil {
		zeroBytes(dek)
		return nil, err
	}
	repDur := time.Since(repStart)

	// Record local DEK copy (explicitly zeroed on CryptoShred) and manifest history.
	dekCopy := append([]byte(nil), dek...)
	zeroBytes(dek)
	v.mu.Lock()
	v.localDEKCache[req.ArtifactID] = append(v.localDEKCache[req.ArtifactID], dekCopy)
	v.artifactHistory[req.ArtifactID] = append(v.artifactHistory[req.ArtifactID], manifest.ManifestCID)
	v.mu.Unlock()

	return &PutResult{
		Manifest:            manifest,
		ManifestCID:         manifest.ManifestCID,
		PlaintextBytes:      int64(len(req.Data)),
		CiphertextBytes:     totalCipherBytes,
		ChunkCount:          numChunks,
		RecipientCount:      len(envelopes),
		PlaintextRootBLAKE3: ptRootB3Hex,
		PlaintextRootSHA256: ptRootS256Hex,
		Elapsed:             time.Since(start),
		EncryptDuration:     encDur,
		ReplicateDuration:   repDur,
	}, nil
}

// RevokePeer removes peerKeyID from future key distribution, increments the
// vault's key epoch, signs an updated SignedRevocationList, and replicates it
// to all independent nodes in store.
func (v *Vault) RevokePeer(ctx context.Context, store *ReplicatedStore, peerKeyID, reason string) (*SignedRevocationList, error) {
	if err := ValidateSegment(peerKeyID); err != nil {
		return nil, err
	}
	if peerKeyID == v.owner.KeyID {
		return nil, errors.New("encstore: owner cannot revoke self")
	}
	v.mu.Lock()
	delete(v.authorizedPeers, peerKeyID)
	v.currentEpoch++
	v.revSeq++
	v.revokedPeers[peerKeyID] = RevocationEntry{
		PeerKeyID:      peerKeyID,
		RevokedAtEpoch: v.currentEpoch,
		RevokedAt:      time.Now().UTC(),
		Reason:         reason,
	}
	revList := v.buildRevocationListLocked()
	v.mu.Unlock()

	if err := revList.Sign(v.owner); err != nil {
		return nil, err
	}
	if store != nil {
		if err := v.publishRevocationList(ctx, store, revList); err != nil {
			return nil, err
		}
	}
	return revList, nil
}

// RevokeConsent marks consentID as revoked in the owner's SignedRevocationList
// and replicates the updated revocation list across store.
func (v *Vault) RevokeConsent(ctx context.Context, store *ReplicatedStore, consentID string) (*SignedRevocationList, error) {
	if err := ValidateSegment(consentID); err != nil {
		return nil, err
	}
	v.mu.Lock()
	v.revokedConsents[consentID] = struct{}{}
	v.revSeq++
	revList := v.buildRevocationListLocked()
	v.mu.Unlock()

	if err := revList.Sign(v.owner); err != nil {
		return nil, err
	}
	if store != nil {
		if err := v.publishRevocationList(ctx, store, revList); err != nil {
			return nil, err
		}
	}
	return revList, nil
}

func (v *Vault) buildRevocationListLocked() *SignedRevocationList {
	peers := make([]RevocationEntry, 0, len(v.revokedPeers))
	for _, entry := range v.revokedPeers {
		peers = append(peers, entry)
	}
	sort.Slice(peers, func(i, j int) bool { return peers[i].PeerKeyID < peers[j].PeerKeyID })

	consents := make([]string, 0, len(v.revokedConsents))
	for cid := range v.revokedConsents {
		consents = append(consents, cid)
	}
	sort.Strings(consents)

	return &SignedRevocationList{
		Version:         FormatVersion,
		Owner:           v.owner.Public("owner"),
		CurrentEpoch:    v.currentEpoch,
		Seq:             v.revSeq,
		RevokedPeers:    peers,
		RevokedConsents: consents,
		UpdatedAt:       time.Now().UTC(),
	}
}

func (v *Vault) publishRevocationList(ctx context.Context, store *ReplicatedStore, revList *SignedRevocationList) error {
	key, err := RevocationKey(v.owner.KeyID)
	if err != nil {
		return err
	}
	raw, err := json.Marshal(revList)
	if err != nil {
		return err
	}
	_, err = store.PutQuorum(ctx, key, raw)
	return err
}

// ShredReceipt documents the verifiable actions taken during CryptoShred.
type ShredReceipt struct {
	Tombstone         *DeletionTombstone `json:"tombstone"`
	LocalDEKsZeroed   int                `json:"local_deks_zeroed"`
	ManifestsDeleted  int                `json:"manifests_deleted"`
	ChunksDeleteOps   int                `json:"chunks_delete_ops"`
	TombstoneReplicas int                `json:"tombstone_replicas"`
}

// CryptoShred performs cryptographic erasure for artifactID:
//  1. Zeroes and deletes all locally cached DEKs for artifactID.
//  2. Publishes an owner-signed DeletionTombstone across independent storage nodes.
//  3. Best-effort deletes ciphertext chunks, manifests, and head pointer from reachable nodes.
func (v *Vault) CryptoShred(ctx context.Context, store *ReplicatedStore, artifactID, reason string) (*ShredReceipt, error) {
	if err := ValidateSegment(artifactID); err != nil {
		return nil, err
	}
	v.mu.Lock()
	epoch := v.currentEpoch
	cachedDEKs := v.localDEKCache[artifactID]
	for _, dek := range cachedDEKs {
		zeroBytes(dek)
	}
	zeroed := len(cachedDEKs)
	delete(v.localDEKCache, artifactID)
	manifestCIDs := append([]string(nil), v.artifactHistory[artifactID]...)
	delete(v.artifactHistory, artifactID)
	v.mu.Unlock()

	tombstone := &DeletionTombstone{
		Version:        FormatVersion,
		ArtifactID:     artifactID,
		Owner:          v.owner.Public("owner"),
		DeletedAtEpoch: epoch,
		DeletedAt:      time.Now().UTC(),
		Reason:         reason,
	}
	if err := tombstone.Sign(v.owner); err != nil {
		return nil, err
	}

	receipt := &ShredReceipt{
		Tombstone:       tombstone,
		LocalDEKsZeroed: zeroed,
	}
	if store == nil {
		return receipt, nil
	}

	// Best-effort delete known manifests and their ciphertext chunks.
	for _, mCID := range manifestCIDs {
		if m, _, err := store.GetVerifiedManifest(ctx, mCID, nil); err == nil {
			for _, ref := range m.CipherChunks {
				if ck, err := ChunkKey(ref.CiphertextCID); err == nil {
					receipt.ChunksDeleteOps += store.DeleteBestEffort(ctx, ck)
				}
			}
		}
		if mk, err := ManifestKey(mCID); err == nil {
			if store.DeleteBestEffort(ctx, mk) > 0 {
				receipt.ManifestsDeleted++
			}
		}
	}
	if hk, err := HeadKey(artifactID); err == nil {
		store.DeleteBestEffort(ctx, hk)
	}

	tk, err := TombstoneKey(artifactID)
	if err != nil {
		return nil, err
	}
	tBytes, err := json.Marshal(tombstone)
	if err != nil {
		return nil, err
	}
	replicas, err := store.PutQuorum(ctx, tk, tBytes)
	if err != nil {
		return nil, err
	}
	receipt.TombstoneReplicas = replicas
	return receipt, nil
}

// ReadResult returns the decrypted artifact bytes, verified metadata, and
// real latency/verification measurements.
type ReadResult struct {
	Manifest        *SignedManifest         `json:"manifest"`
	PrivateMeta     *PrivateManifestPayload `json:"private_meta"`
	Data            []byte                  `json:"-"`
	CiphertextBytes int64                   `json:"ciphertext_bytes"`
	PlaintextBytes  int64                   `json:"plaintext_bytes"`
	ChunkCount      int                     `json:"chunk_count"`
	Elapsed         time.Duration           `json:"elapsed"`
	TTFT            time.Duration           `json:"ttft"`
	ChunkLatencies  []time.Duration         `json:"-"`
	ServedByNodes   map[string]int          `json:"served_by_nodes"`
}

// ReadArtifact fetches manifestCID from store, enforces cryptographic signature,
// tombstone, revocation, license, and consent policies fail-closed, unwraps the
// DEK for reader, fetches encrypted chunks with automatic replica failover,
// decrypts every chunk with AES-256-GCM + AAD verification, and verifies
// per-chunk and full-payload plaintext BLAKE3 + SHA-256 digests.
func ReadArtifact(ctx context.Context, store *ReplicatedStore, manifestCID string, reader *Identity, policy ReadPolicy) (*ReadResult, error) {
	start := time.Now()
	if store == nil {
		return nil, errors.New("encstore: nil replicated store")
	}
	if reader == nil {
		return nil, fmt.Errorf("%w: nil reader identity", ErrAccessDenied)
	}

	manifest, _, err := store.GetVerifiedManifest(ctx, manifestCID, policy.TrustedOwnerEdPubs)
	if err != nil {
		return nil, err
	}
	ownerEdPub, _, err := manifest.Owner.Validate()
	if err != nil {
		return nil, err
	}

	// Check owner-signed DeletionTombstone fail-closed.
	tombstone, err := store.GetTombstone(ctx, manifest.ArtifactID, ownerEdPub)
	if err != nil {
		return nil, err
	}
	if tombstone != nil {
		return nil, fmt.Errorf("%w: artifact %s tombstoned at epoch %d (%s)", ErrTombstoned, manifest.ArtifactID, tombstone.DeletedAtEpoch, tombstone.Reason)
	}

	// Check owner-signed RevocationList (unless explicitly bypassed in prior-epoch negative boundary test).
	var revList *SignedRevocationList
	if !policy.IgnoreRevocationList {
		revList, err = store.GetLatestRevocationList(ctx, manifest.Owner.KeyID, ownerEdPub)
		if err != nil {
			return nil, err
		}
	}

	// Enforce license, consent, and revocation policies.
	if err := policy.Enforce(manifest, revList, reader.KeyID); err != nil {
		return nil, err
	}

	// Locate and unwrap reader's KeyEnvelope.
	env, ok := manifest.FindEnvelope(reader.KeyID)
	if !ok {
		return nil, fmt.Errorf("%w: no key envelope for reader %s in epoch %d", ErrAccessDenied, reader.KeyID, manifest.Epoch)
	}
	salt, err := hex.DecodeString(manifest.EpochSaltHex)
	if err != nil || len(salt) != SaltSize {
		return nil, fmt.Errorf("%w: invalid epoch_salt_hex", ErrInvalidManifest)
	}
	dek, err := unwrapDEK(env, salt, manifest.ArtifactID, manifest.Epoch, manifest.Consent.ConsentHash, reader)
	if err != nil {
		return nil, err
	}
	defer zeroBytes(dek)

	// Decrypt PrivateManifestPayload (contains plaintext hashes and sizes).
	privJSON, err := decryptPrivatePayload(
		dek, salt, manifest.ArtifactID, manifest.Epoch, manifest.Kind,
		manifest.Owner.KeyID, manifest.License.SPDX, manifest.Consent.ConsentHash,
		manifest.PrivatePayloadNonceHex, manifest.EncryptedPrivatePayloadHex,
	)
	if err != nil {
		return nil, err
	}
	var privMeta PrivateManifestPayload
	if err := json.Unmarshal(privJSON, &privMeta); err != nil {
		return nil, fmt.Errorf("%w: decode private manifest payload: %v", ErrInvalidManifest, err)
	}
	if len(privMeta.Chunks) != len(manifest.CipherChunks) {
		return nil, fmt.Errorf("%w: private chunk count %d != cipher chunk count %d", ErrInvalidManifest, len(privMeta.Chunks), len(manifest.CipherChunks))
	}

	// Fetch, verify, and decrypt all chunks concurrently.
	numChunks := len(manifest.CipherChunks)
	plainChunks := make([][]byte, numChunks)
	chunkLats := make([]time.Duration, numChunks)
	servedBy := make(map[string]int)
	var (
		ttftOnce sync.Once
		ttft     time.Duration
		servedMu sync.Mutex
		wg       sync.WaitGroup
		nextIdx  atomic.Int64
		errMu    sync.Mutex
		firstErr error
		totalCT  atomic.Int64
	)

	workers := min(max(runtime.GOMAXPROCS(0), 2), min(numChunks, 8))
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := int(nextIdx.Add(1)) - 1
				if i >= numChunks {
					return
				}
				errMu.Lock()
				failed := firstErr != nil
				errMu.Unlock()
				if failed {
					return
				}
				cStart := time.Now()
				cRef := manifest.CipherChunks[i]
				pMeta := privMeta.Chunks[i]

				frame, nodeName, fetchErr := store.GetVerifiedChunk(ctx, cRef.CiphertextCID, cRef.CiphertextSize)
				if fetchErr != nil {
					errMu.Lock()
					if firstErr == nil {
						firstErr = fetchErr
					}
					errMu.Unlock()
					return
				}
				pt, decErr := decryptChunk(
					dek, salt, manifest.ArtifactID, manifest.Epoch, cRef.Index,
					manifest.Kind, manifest.License.SPDX, manifest.Consent.ConsentHash, frame,
				)
				if decErr != nil {
					errMu.Lock()
					if firstErr == nil {
						firstErr = decErr
					}
					errMu.Unlock()
					return
				}
				if int64(len(pt)) != pMeta.PlaintextSize {
					errMu.Lock()
					if firstErr == nil {
						firstErr = fmt.Errorf("%w: chunk %d plaintext size %d != %d", ErrPlaintextMismatch, i, len(pt), pMeta.PlaintextSize)
					}
					errMu.Unlock()
					return
				}
				ptSum := blake3.Sum(pt)
				if hex.EncodeToString(ptSum[:]) != pMeta.PlaintextBLAKE3 {
					errMu.Lock()
					if firstErr == nil {
						firstErr = fmt.Errorf("%w: chunk %d plaintext BLAKE3 mismatch", ErrPlaintextMismatch, i)
					}
					errMu.Unlock()
					return
				}

				cLat := time.Since(cStart)
				if i == 0 {
					ttftOnce.Do(func() { ttft = time.Since(start) })
				}
				plainChunks[i] = pt
				chunkLats[i] = cLat
				totalCT.Add(int64(len(frame)))
				servedMu.Lock()
				servedBy[nodeName]++
				servedMu.Unlock()
			}
		}()
	}
	wg.Wait()
	if firstErr != nil {
		return nil, firstErr
	}

	// Reassemble plaintext and verify full-artifact dual root digests (BLAKE3 + SHA-256).
	out := make([]byte, 0, privMeta.PlaintextTotalSize)
	for _, pt := range plainChunks {
		out = append(out, pt...)
	}
	if int64(len(out)) != privMeta.PlaintextTotalSize {
		return nil, fmt.Errorf("%w: reassembled size %d != %d", ErrPlaintextMismatch, len(out), privMeta.PlaintextTotalSize)
	}
	gotB3 := blake3.Sum(out)
	if hex.EncodeToString(gotB3[:]) != privMeta.PlaintextRootBLAKE3 {
		return nil, fmt.Errorf("%w: full plaintext root BLAKE3 mismatch", ErrPlaintextMismatch)
	}
	gotS256 := sha256.Sum256(out)
	if hex.EncodeToString(gotS256[:]) != privMeta.PlaintextRootSHA256 {
		return nil, fmt.Errorf("%w: full plaintext root SHA-256 mismatch", ErrPlaintextMismatch)
	}
	if ttft == 0 {
		ttft = time.Since(start)
	}

	return &ReadResult{
		Manifest:        manifest,
		PrivateMeta:     &privMeta,
		Data:            out,
		CiphertextBytes: totalCT.Load(),
		PlaintextBytes:  int64(len(out)),
		ChunkCount:      numChunks,
		Elapsed:         time.Since(start),
		TTFT:            ttft,
		ChunkLatencies:  chunkLats,
		ServedByNodes:   servedBy,
	}, nil
}

// ExportResult reports the artifacts and objects written during a sovereign
// offline VFS export.
type ExportResult struct {
	ExportDir         string                 `json:"export_dir"`
	Index             *AuthorizedBundleIndex `json:"index"`
	ArtifactsExported int                    `json:"artifacts_exported"`
	ChunksExported    int                    `json:"chunks_exported"`
	TotalBytesWritten int64                  `json:"total_bytes_written"`
	ZeroSecondFormat  bool                   `json:"zero_second_format"`
}

// ExportAuthorizedBundle exports the specified artifacts (signed manifests,
// verified ciphertext chunks, latest revocation list, and an owner-signed
// AuthorizedBundleIndex) directly into a standalone pkg/backend/file VFS
// directory. Zero Hanzo accounts or non-VFS archive formats are used.
func (v *Vault) ExportAuthorizedBundle(ctx context.Context, src *ReplicatedStore, exportDir string, manifestCIDs []string) (*ExportResult, error) {
	if src == nil {
		return nil, errors.New("encstore: nil source store")
	}
	if len(manifestCIDs) == 0 {
		return nil, errors.New("encstore: at least one manifestCID is required for export")
	}
	dstBackend, err := backend.Open(ctx, "file://"+exportDir)
	if err != nil {
		return nil, fmt.Errorf("encstore: open export file backend: %w", err)
	}
	defer dstBackend.Close()

	entries := make([]BundleArtifactEntry, 0, len(manifestCIDs))
	var (
		chunksWritten int
		bytesWritten  int64
	)

	for _, mCID := range manifestCIDs {
		m, rawM, err := src.GetVerifiedManifest(ctx, mCID, nil)
		if err != nil {
			return nil, fmt.Errorf("encstore: export fetch manifest %s: %w", mCID, err)
		}
		mKey, err := ManifestKey(mCID)
		if err != nil {
			return nil, err
		}
		if err := dstBackend.Put(ctx, mKey, rawM); err != nil {
			return nil, err
		}
		bytesWritten += int64(len(rawM))

		for _, ref := range m.CipherChunks {
			frame, _, err := src.GetVerifiedChunk(ctx, ref.CiphertextCID, ref.CiphertextSize)
			if err != nil {
				return nil, fmt.Errorf("encstore: export chunk %s: %w", ref.CiphertextCID[:12], err)
			}
			cKey, err := ChunkKey(ref.CiphertextCID)
			if err != nil {
				return nil, err
			}
			if err := dstBackend.Put(ctx, cKey, frame); err != nil {
				return nil, err
			}
			chunksWritten++
			bytesWritten += int64(len(frame))
		}

		entries = append(entries, BundleArtifactEntry{
			ArtifactID:  m.ArtifactID,
			Kind:        m.Kind,
			Epoch:       m.Epoch,
			ManifestCID: mCID,
		})
	}

	hasRev := false
	if revList, err := src.GetLatestRevocationList(ctx, v.owner.KeyID, v.owner.Ed25519Pub); err == nil && revList != nil {
		if rKey, err := RevocationKey(v.owner.KeyID); err == nil {
			rawR, err := json.Marshal(revList)
			if err != nil {
				return nil, err
			}
			if err := dstBackend.Put(ctx, rKey, rawR); err != nil {
				return nil, err
			}
			bytesWritten += int64(len(rawR))
			hasRev = true
		}
	}

	index := &AuthorizedBundleIndex{
		Version:          FormatVersion,
		Owner:            v.owner.Public("owner"),
		ExportedAt:       time.Now().UTC(),
		Artifacts:        entries,
		HasRevocationLog: hasRev,
	}
	if err := index.Sign(v.owner); err != nil {
		return nil, err
	}
	idxBytes, err := json.Marshal(index)
	if err != nil {
		return nil, err
	}
	if err := dstBackend.Put(ctx, BundleIndexKey(), idxBytes); err != nil {
		return nil, err
	}
	bytesWritten += int64(len(idxBytes))

	return &ExportResult{
		ExportDir:         exportDir,
		Index:             index,
		ArtifactsExported: len(entries),
		ChunksExported:    chunksWritten,
		TotalBytesWritten: bytesWritten,
		ZeroSecondFormat:  true,
	}, nil
}

// RestoredArtifact summarizes one restored and cryptographically verified artifact.
type RestoredArtifact struct {
	ArtifactID          string       `json:"artifact_id"`
	Kind                ArtifactKind `json:"kind"`
	Epoch               uint64       `json:"epoch"`
	ManifestCID         string       `json:"manifest_cid"`
	PlaintextBytes      int64        `json:"plaintext_bytes"`
	PlaintextRootBLAKE3 string       `json:"plaintext_root_blake3"`
	Data                []byte       `json:"-"`
}

// RestoreResult captures the outcome of restoring an entire exported agent
// state bundle on an independent storage node with zero Hanzo dependencies.
type RestoreResult struct {
	Index            *AuthorizedBundleIndex `json:"index"`
	Restored         []RestoredArtifact     `json:"restored"`
	TotalPlainBytes  int64                  `json:"total_plain_bytes"`
	Elapsed          time.Duration          `json:"elapsed"`
	ZeroHanzoAccount bool                   `json:"zero_hanzo_account"`
}

// RestoreAndVerify restores and verifies an exported AuthorizedBundleIndex from
// any independent ReplicatedStore (e.g., backed by a fresh file:// directory or
// independent HTTP node with zero Hanzo account).
func RestoreAndVerify(ctx context.Context, independentStore *ReplicatedStore, reader *Identity, policy ReadPolicy) (*RestoreResult, error) {
	start := time.Now()
	if independentStore == nil {
		return nil, errors.New("encstore: nil independent store")
	}
	var (
		rawIdx  []byte
		lastErr error
	)
	for _, ep := range independentStore.Nodes() {
		data, _, err := ep.Backend.Get(ctx, BundleIndexKey(), 4<<20)
		if err == nil {
			rawIdx = data
			break
		}
		lastErr = err
	}
	if rawIdx == nil {
		return nil, fmt.Errorf("%w: bundle index not found: %v", ErrAllReplicasFailed, lastErr)
	}
	var idx AuthorizedBundleIndex
	if err := json.Unmarshal(rawIdx, &idx); err != nil {
		return nil, fmt.Errorf("%w: decode bundle index: %v", ErrInvalidManifest, err)
	}
	if err := idx.Verify(policy.TrustedOwnerEdPubs); err != nil {
		return nil, err
	}

	restored := make([]RestoredArtifact, 0, len(idx.Artifacts))
	var totalPlain int64
	for _, entry := range idx.Artifacts {
		res, err := ReadArtifact(ctx, independentStore, entry.ManifestCID, reader, policy)
		if err != nil {
			return nil, fmt.Errorf("encstore: restore artifact %s (%s): %w", entry.ArtifactID, entry.Kind, err)
		}
		restored = append(restored, RestoredArtifact{
			ArtifactID:          res.Manifest.ArtifactID,
			Kind:                res.Manifest.Kind,
			Epoch:               res.Manifest.Epoch,
			ManifestCID:         res.Manifest.ManifestCID,
			PlaintextBytes:      res.PlaintextBytes,
			PlaintextRootBLAKE3: res.PrivateMeta.PlaintextRootBLAKE3,
			Data:                res.Data,
		})
		totalPlain += res.PlaintextBytes
	}

	return &RestoreResult{
		Index:            &idx,
		Restored:         restored,
		TotalPlainBytes:  totalPlain,
		Elapsed:          time.Since(start),
		ZeroHanzoAccount: true,
	}, nil
}

// PrivacyScanReport summarizes what an honest-but-curious storage provider or
// passive network observer can learn by scanning raw stored objects on disk.
type PrivacyScanReport struct {
	ObjectsScanned        int      `json:"objects_scanned"`
	TotalBytesScanned     int64    `json:"total_bytes_scanned"`
	CanariesTotal         int      `json:"canaries_total"`
	CanariesLeaked        int      `json:"canaries_leaked"`
	PlaintextHashesTotal  int      `json:"plaintext_hashes_total"`
	PlaintextHashesLeaked int      `json:"plaintext_hashes_leaked"`
	LeakageRatePct        float64  `json:"leakage_rate_pct"`
	LeakedCanaryMatches   []string `json:"leaked_canary_matches,omitempty"`
}

// UnsafeUnencryptedContentAddressedPutForNegativeControl intentionally writes
// raw, unencrypted plaintext chunks addressed by plaintext BLAKE3 CID into
// `unenc/chunk/<2hex>/<plaintext_blake3>` on dst.
//
// THIS FUNCTION IS ONLY FOR NEGATIVE-CONTROL EXPERIMENTS demonstrating that
// content addressing is NOT privacy and leaks sensitive agent memory / PII
// verbatim to any passive storage provider or peer.
func UnsafeUnencryptedContentAddressedPutForNegativeControl(ctx context.Context, dst backend.Backend, data []byte, chunkSize int) ([]string, error) {
	if chunkSize <= 0 {
		chunkSize = DefaultChunkSize
	}
	var cids []string
	for off := 0; off < len(data); off += chunkSize {
		hi := min(off+chunkSize, len(data))
		chunk := data[off:hi]
		sum := blake3.Sum(chunk)
		cid := hex.EncodeToString(sum[:])
		key := "unenc/chunk/" + cid[:2] + "/" + cid
		if err := dst.Put(ctx, key, chunk); err != nil {
			return nil, err
		}
		cids = append(cids, cid)
	}
	return cids, nil
}

// ScanBackendForPlaintextCanaries inspects every raw object stored in b (both
// keys and raw byte payloads) as a passive storage operator and reports how
// many sensitive plaintext canary strings or plaintext BLAKE3 hashes are visible.
func ScanBackendForPlaintextCanaries(ctx context.Context, b backend.Backend, prefix string, canaries []string, plaintextHashes []string) (*PrivacyScanReport, error) {
	objCh, errCh := b.List(ctx, prefix)
	foundCanaries := make(map[string]struct{})
	foundHashes := make(map[string]struct{})
	report := &PrivacyScanReport{
		CanariesTotal:        len(canaries),
		PlaintextHashesTotal: len(plaintextHashes),
	}

	for obj := range objCh {
		data, _, err := b.Get(ctx, obj.Key, backend.Unbounded)
		if err != nil {
			continue
		}
		report.ObjectsScanned++
		report.TotalBytesScanned += int64(len(data))

		for _, c := range canaries {
			if strings.Contains(obj.Key, c) || bytes.Contains(data, []byte(c)) {
				foundCanaries[c] = struct{}{}
			}
		}
		for _, h := range plaintextHashes {
			if strings.Contains(obj.Key, h) || bytes.Contains(data, []byte(h)) {
				foundHashes[h] = struct{}{}
			}
		}
	}
	if err := <-errCh; err != nil {
		return nil, err
	}

	for c := range foundCanaries {
		report.LeakedCanaryMatches = append(report.LeakedCanaryMatches, c)
	}
	slices.Sort(report.LeakedCanaryMatches)
	report.CanariesLeaked = len(foundCanaries)
	report.PlaintextHashesLeaked = len(foundHashes)

	denom := len(canaries) + len(plaintextHashes)
	if denom > 0 {
		report.LeakageRatePct = 100.0 * float64(report.CanariesLeaked+report.PlaintextHashesLeaked) / float64(denom)
	}
	return report, nil
}
