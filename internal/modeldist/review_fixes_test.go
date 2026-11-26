package modeldist

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hanzoai/vfs/internal/storetest"
	"github.com/hanzoai/vfs/pkg/backend"
)

type stallingChunkProvider struct {
	ChunkProvider
	stallHash     string
	release       <-chan struct{}
	stalledNotify chan struct{}
	stallOnce     sync.Once
	getCount      atomic.Int64
}

func (p *stallingChunkProvider) GetChunk(ctx context.Context, blake3Hex string, maxSize int64) ([]byte, error) {
	p.getCount.Add(1)
	if blake3Hex == p.stallHash {
		p.stallOnce.Do(func() { close(p.stalledNotify) })
		select {
		case <-p.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return p.ChunkProvider.GetChunk(ctx, blake3Hex, maxSize)
}

// TestPullArtifactStreamBoundsDispatchWhenEarlyChunkStalls verifies that when
// chunk 0 stalls, PullArtifactStream caps total dispatched chunks at the
// in-order consumption window (2 * concurrency) rather than buffering the rest
// of the artifact in memory.
func TestPullArtifactStreamBoundsDispatchWhenEarlyChunkStalls(t *testing.T) {
	ctx := context.Background()
	pub, priv := mustKeyPair(t)
	store := NewVFSStoreProvider("stall-mirror", false, storetest.NewMem())

	const chunkSize int64 = 16 << 10
	const totalChunks = 32
	art, err := IngestArtifact(
		ctx,
		"weights/bounded-stream.gguf",
		AssetKindGGUF,
		"Q4_K_M",
		bytes.NewReader(deterministicPayload(777, int(chunkSize)*totalChunks)),
		chunkSize,
		store,
	)
	if err != nil {
		t.Fatalf("IngestArtifact: %v", err)
	}
	m := &ManifestV1{
		SchemaVersion: SchemaVersionV1,
		ModelID:       "bounded-stream-model",
		Version:       "1.0.0",
		License:       "Apache-2.0",
		Provenance:    Provenance{CreatedAt: "2026-11-26T00:00:00Z"},
		Artifacts:     []Artifact{art},
	}
	if err := m.Sign(priv, "test-pub"); err != nil {
		t.Fatalf("Sign: %v", err)
	}

	release := make(chan struct{})
	stalled := make(chan struct{})
	provider := &stallingChunkProvider{
		ChunkProvider: store,
		stallHash:     art.Chunks[0].BLAKE3,
		release:       release,
		stalledNotify: stalled,
	}

	const concurrency = 2
	const maxWindow = concurrency * 2
	cli := NewClient(ClientConfig{
		Policy:      VerificationPolicy{TrustedKeys: []ed25519.PublicKey{pub}},
		Concurrency: concurrency,
	})

	type result struct {
		rep *PullReport
		err error
	}
	done := make(chan result, 1)
	go func() {
		rep, err := cli.PullArtifactStream(ctx, m, 0, []ChunkProvider{provider}, io.Discard)
		done <- result{rep: rep, err: err}
	}()

	select {
	case <-stalled:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for chunk 0 to stall")
	}

	// Give other workers plenty of time to drain all available credits.
	deadline := time.Now().Add(2 * time.Second)
	for provider.getCount.Load() < maxWindow && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	// Hold slightly longer to prove no additional chunks are dispatched while chunk 0 is stalled.
	time.Sleep(80 * time.Millisecond)

	dispatchedWhileStalled := provider.getCount.Load()
	if dispatchedWhileStalled > maxWindow {
		close(release)
		t.Fatalf("dispatched %d chunks while chunk 0 stalled; must be bounded to <= %d (2 * concurrency)",
			dispatchedWhileStalled, maxWindow)
	}

	// Release chunk 0 and confirm the entire stream finishes cleanly.
	close(release)
	res := <-done
	if res.err != nil {
		t.Fatalf("PullArtifactStream failed after releasing chunk 0: %v", res.err)
	}
	if res.rep.DownloadedChunks != totalChunks || res.rep.VerifiedBytes != art.Size {
		t.Fatalf("unexpected report: downloaded=%d/%d bytes=%d/%d",
			res.rep.DownloadedChunks, totalChunks, res.rep.VerifiedBytes, art.Size)
	}
	if res.rep.ArtifactRootBLAKE3[art.Path] != art.RootBLAKE3 {
		t.Fatalf("root BLAKE3 mismatch after stalled stream")
	}
}

// TestPullToDirRejectsSymlinkEscape verifies PullToDir confines opens to outDir
// and refuses to traverse or overwrite symlinks pointing outside or inside outDir.
func TestPullToDirRejectsSymlinkEscape(t *testing.T) {
	ctx := context.Background()
	pub, priv := mustKeyPair(t)
	store := NewVFSStoreProvider("symlink-mirror", false, storetest.NewMem())
	art, err := IngestArtifact(
		ctx,
		"weights/model.gguf",
		AssetKindGGUF,
		"Q4_K_M",
		bytes.NewReader(deterministicPayload(123, 8192)),
		4096,
		store,
	)
	if err != nil {
		t.Fatalf("IngestArtifact: %v", err)
	}
	m := &ManifestV1{
		SchemaVersion: SchemaVersionV1,
		ModelID:       "symlink-test",
		Version:       "1.0.0",
		License:       "Apache-2.0",
		Provenance:    Provenance{CreatedAt: "2026-11-26T00:00:00Z"},
		Artifacts:     []Artifact{art},
	}
	if err := m.Sign(priv, "test-pub"); err != nil {
		t.Fatalf("Sign: %v", err)
	}
	cli := NewClient(ClientConfig{
		Policy:      VerificationPolicy{TrustedKeys: []ed25519.PublicKey{pub}},
		Concurrency: 2,
	})
	sentinel := []byte("DO-NOT-OVERWRITE-OUTSIDE")

	t.Run("dir_symlink_absolute_outside", func(t *testing.T) {
		outsideDir := t.TempDir()
		outsideFile := filepath.Join(outsideDir, "model.gguf")
		if err := os.WriteFile(outsideFile, sentinel, 0o644); err != nil {
			t.Fatalf("WriteFile sentinel: %v", err)
		}
		outDir := t.TempDir()
		if err := os.Symlink(outsideDir, filepath.Join(outDir, "weights")); err != nil {
			t.Fatalf("Symlink: %v", err)
		}
		if _, err := cli.PullToDir(ctx, m, []ChunkProvider{store}, outDir, PullOptions{}); err == nil {
			t.Fatal("expected PullToDir to reject symlinked parent directory escaping outDir")
		}
		got, err := os.ReadFile(outsideFile)
		if err != nil || !bytes.Equal(got, sentinel) {
			t.Fatalf("outside file was modified: err=%v content=%q", err, string(got))
		}
	})

	t.Run("dir_symlink_relative_outside", func(t *testing.T) {
		base := t.TempDir()
		outsideDir := filepath.Join(base, "outside")
		outDir := filepath.Join(base, "out")
		if err := os.MkdirAll(outsideDir, 0o755); err != nil {
			t.Fatalf("MkdirAll outside: %v", err)
		}
		if err := os.MkdirAll(outDir, 0o755); err != nil {
			t.Fatalf("MkdirAll out: %v", err)
		}
		outsideFile := filepath.Join(outsideDir, "model.gguf")
		if err := os.WriteFile(outsideFile, sentinel, 0o644); err != nil {
			t.Fatalf("WriteFile sentinel: %v", err)
		}
		if err := os.Symlink("../outside", filepath.Join(outDir, "weights")); err != nil {
			t.Fatalf("Symlink: %v", err)
		}
		if _, err := cli.PullToDir(ctx, m, []ChunkProvider{store}, outDir, PullOptions{}); err == nil {
			t.Fatal("expected PullToDir to reject relative symlink escaping outDir")
		}
		got, err := os.ReadFile(outsideFile)
		if err != nil || !bytes.Equal(got, sentinel) {
			t.Fatalf("outside file was modified: err=%v content=%q", err, string(got))
		}
	})

	t.Run("file_symlink_outside_existing_and_dangling", func(t *testing.T) {
		outsideDir := t.TempDir()
		outsideExisting := filepath.Join(outsideDir, "secret.bin")
		if err := os.WriteFile(outsideExisting, sentinel, 0o644); err != nil {
			t.Fatalf("WriteFile sentinel: %v", err)
		}
		outDir := t.TempDir()
		if err := os.MkdirAll(filepath.Join(outDir, "weights"), 0o755); err != nil {
			t.Fatalf("MkdirAll weights: %v", err)
		}
		if err := os.Symlink(outsideExisting, filepath.Join(outDir, "weights", "model.gguf")); err != nil {
			t.Fatalf("Symlink existing: %v", err)
		}
		if _, err := cli.PullToDir(ctx, m, []ChunkProvider{store}, outDir, PullOptions{}); err == nil {
			t.Fatal("expected PullToDir to reject symlinked artifact file pointing outside outDir")
		}
		got, err := os.ReadFile(outsideExisting)
		if err != nil || !bytes.Equal(got, sentinel) {
			t.Fatalf("outside existing file was modified: err=%v content=%q", err, string(got))
		}

		// Dangling symlink pointing to a non-existent file outside outDir.
		outDir2 := t.TempDir()
		if err := os.MkdirAll(filepath.Join(outDir2, "weights"), 0o755); err != nil {
			t.Fatalf("MkdirAll weights2: %v", err)
		}
		outsideNew := filepath.Join(outsideDir, "created-outside.bin")
		if err := os.Symlink(outsideNew, filepath.Join(outDir2, "weights", "model.gguf")); err != nil {
			t.Fatalf("Symlink dangling: %v", err)
		}
		if _, err := cli.PullToDir(ctx, m, []ChunkProvider{store}, outDir2, PullOptions{}); err == nil {
			t.Fatal("expected PullToDir to reject dangling symlink pointing outside outDir")
		}
		if _, err := os.Stat(outsideNew); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("dangling symlink target outside outDir must not be created, stat err=%v", err)
		}
	})

	t.Run("in_dir_symlink_rejected", func(t *testing.T) {
		outDir := t.TempDir()
		otherFile := filepath.Join(outDir, "other.txt")
		if err := os.WriteFile(otherFile, sentinel, 0o644); err != nil {
			t.Fatalf("WriteFile other: %v", err)
		}
		if err := os.MkdirAll(filepath.Join(outDir, "weights"), 0o755); err != nil {
			t.Fatalf("MkdirAll weights: %v", err)
		}
		if err := os.Symlink("../other.txt", filepath.Join(outDir, "weights", "model.gguf")); err != nil {
			t.Fatalf("Symlink in-dir: %v", err)
		}
		if _, err := cli.PullToDir(ctx, m, []ChunkProvider{store}, outDir, PullOptions{}); err == nil {
			t.Fatal("expected PullToDir to reject in-dir symlink")
		}
		got, err := os.ReadFile(otherFile)
		if err != nil || !bytes.Equal(got, sentinel) {
			t.Fatalf("in-dir symlink target was modified: err=%v content=%q", err, string(got))
		}
	})
}

type staticManifestProvider struct {
	id  string
	raw []byte
}

func (p *staticManifestProvider) ID() string     { return p.id }
func (p *staticManifestProvider) IsOrigin() bool { return false }
func (p *staticManifestProvider) HasChunk(context.Context, string) (bool, error) {
	return false, nil
}
func (p *staticManifestProvider) GetChunk(context.Context, string, int64) ([]byte, error) {
	return nil, ErrChunkNotFound
}
func (p *staticManifestProvider) GetManifest(context.Context, string) ([]byte, error) {
	return append([]byte(nil), p.raw...), nil
}
func (p *staticManifestProvider) Stats() ProviderStats { return ProviderStats{} }

// TestFetchVerifiedManifestFailoverAfterIntegrityFailure verifies that a
// corrupted or malicious mirror returning a matching ManifestCID JSON field
// with modified unsigned content or stripped/forged signatures does not abort
// failover before healthy mirrors are tried.
func TestFetchVerifiedManifestFailoverAfterIntegrityFailure(t *testing.T) {
	ctx := context.Background()
	m, honestRaw, pub, _, _, _ := buildTestBundle(t, ctx, 32<<10)

	cli := NewClient(ClientConfig{
		Policy: VerificationPolicy{
			TrustedKeys: []ed25519.PublicKey{pub},
			License:     LicensePolicy{RequireLicense: true, AllowedLicenses: []string{"Apache-2.0"}},
		},
	})

	// Case 1: Mirror 1 modifies an unsigned field while keeping ManifestCID string equal.
	tamperedBody := *m
	tamperedBody.Description = "evil mirror modified unsigned field"
	tamperedBodyRaw, err := tamperedBody.MarshalJSONCanonical()
	if err != nil {
		t.Fatalf("MarshalJSONCanonical tamperedBody: %v", err)
	}
	badBodyMirror := &staticManifestProvider{id: "evil-body-mirror", raw: tamperedBodyRaw}
	honestMirror := &staticManifestProvider{id: "honest-mirror", raw: honestRaw}

	fetched, via, err := cli.FetchVerifiedManifest(ctx, m.ManifestCID, []ChunkProvider{badBodyMirror, honestMirror})
	if err != nil {
		t.Fatalf("expected failover to honest mirror after unsigned-field tamper, got error: %v", err)
	}
	if via.ID() != "honest-mirror" || fetched.Description != m.Description {
		t.Fatalf("unexpected manifest/provider: via=%s desc=%q", via.ID(), fetched.Description)
	}

	// Case 2: Mirror 1 keeps exact unsigned body (matching computed CID) but strips signatures.
	strippedSigs := *m
	strippedSigs.Signatures = nil
	strippedSigsRaw, err := strippedSigs.MarshalJSONCanonical()
	if err != nil {
		t.Fatalf("MarshalJSONCanonical strippedSigs: %v", err)
	}
	badSigMirror := &staticManifestProvider{id: "evil-sig-mirror", raw: strippedSigsRaw}

	fetched, via, err = cli.FetchVerifiedManifest(ctx, m.ManifestCID, []ChunkProvider{badSigMirror, honestMirror})
	if err != nil {
		t.Fatalf("expected failover to honest mirror after stripped signatures, got error: %v", err)
	}
	if via.ID() != "honest-mirror" || len(fetched.Signatures) == 0 {
		t.Fatalf("unexpected manifest/provider after sig failover: via=%s sigs=%d", via.ID(), len(fetched.Signatures))
	}

	// Case 3: All mirrors return tampered manifests -> still fail closed.
	if _, _, err := cli.FetchVerifiedManifest(ctx, m.ManifestCID, []ChunkProvider{badBodyMirror}); !errors.Is(err, ErrAllProvidersFailed) || !errors.Is(err, ErrManifestCIDMismatch) {
		t.Fatalf("expected ErrAllProvidersFailed+ErrManifestCIDMismatch when all mirrors tampered, got %v", err)
	}

	// Case 4: Verified CID violating local license policy still fails closed with ErrLicenseRejected.
	strictCli := NewClient(ClientConfig{
		Policy: VerificationPolicy{
			TrustedKeys: []ed25519.PublicKey{pub},
			License:     LicensePolicy{RequireLicense: true, AllowedLicenses: []string{"MIT"}},
		},
	})
	if _, _, err := strictCli.FetchVerifiedManifest(ctx, m.ManifestCID, []ChunkProvider{honestMirror}); !errors.Is(err, ErrLicenseRejected) {
		t.Fatalf("expected ErrLicenseRejected for policy-violating verified CID, got %v", err)
	}
}

// TestVerifyChecksParentCIDBeforeRecursion ensures an untrusted ParentResolver
// returning the signed child (or a spoofed/nil manifest) is rejected before
// recursing instead of overflowing the stack.
func TestVerifyChecksParentCIDBeforeRecursion(t *testing.T) {
	ctx := context.Background()
	parent, _, pub, priv, _, _ := buildTestBundle(t, ctx, 32<<10)

	child := *parent
	child.ModelID = "zen-7b-child"
	child.Version = "1.3.0"
	child.Provenance.ParentManifestCID = parent.ManifestCID
	child.ManifestCID = ""
	child.Signatures = nil
	if err := child.Sign(priv, "child-pub"); err != nil {
		t.Fatalf("Sign child: %v", err)
	}

	t.Run("resolver_returns_signed_child", func(t *testing.T) {
		calls := 0
		policy := VerificationPolicy{
			TrustedKeys: []ed25519.PublicKey{pub},
			ParentResolver: func(cid string) (*ManifestV1, error) {
				calls++
				if calls > 2 {
					t.Fatalf("infinite recursion detected in parent verification (calls=%d)", calls)
				}
				return &child, nil
			},
		}
		err := child.Verify(policy)
		if !errors.Is(err, ErrProvenanceInvalid) {
			t.Fatalf("expected ErrProvenanceInvalid, got %v", err)
		}
		if calls != 1 {
			t.Fatalf("expected resolver to be called exactly once before rejecting mismatched parent CID, got %d", calls)
		}
	})

	t.Run("resolver_returns_spoofed_parent_cid", func(t *testing.T) {
		spoofed := child
		spoofed.ManifestCID = parent.ManifestCID
		calls := 0
		policy := VerificationPolicy{
			TrustedKeys: []ed25519.PublicKey{pub},
			ParentResolver: func(cid string) (*ManifestV1, error) {
				calls++
				return &spoofed, nil
			},
		}
		err := child.Verify(policy)
		if !errors.Is(err, ErrProvenanceInvalid) {
			t.Fatalf("expected ErrProvenanceInvalid for spoofed parent CID, got %v", err)
		}
		if calls != 1 {
			t.Fatalf("expected exactly 1 resolver call for spoofed parent, got %d", calls)
		}
	})

	t.Run("resolver_returns_nil", func(t *testing.T) {
		policy := VerificationPolicy{
			TrustedKeys: []ed25519.PublicKey{pub},
			ParentResolver: func(cid string) (*ManifestV1, error) {
				return nil, nil
			},
		}
		if err := child.Verify(policy); !errors.Is(err, ErrProvenanceInvalid) {
			t.Fatalf("expected ErrProvenanceInvalid for nil parent, got %v", err)
		}
	})
}

// TestRefKeyPreservesDistinctReleases verifies distinct (modelID, version)
// tuples such as 1.0.0-rc.1 and 1.0.0-rc-1 never collide on backend keys.
func TestRefKeyPreservesDistinctReleases(t *testing.T) {
	ctx := context.Background()
	pairs := [][2]string{
		{"zen-7b", "1.0.0-rc.1"},
		{"zen-7b", "1.0.0-rc-1"},
		{"zen/7b", "1.0.0-rc.1"},
		{"Zen-7B", "1.0.0-rc.1"},
		{"zen-7b", "1.0.0-rc.10"},
	}
	seen := make(map[string][2]string, len(pairs))
	for _, p := range pairs {
		k, err := RefKey(p[0], p[1])
		if err != nil {
			t.Fatalf("RefKey(%q, %q) unexpected error: %v", p[0], p[1], err)
		}
		if err := backend.ValidKey(k); err != nil {
			t.Fatalf("RefKey(%q, %q) = %q failed backend.ValidKey: %v", p[0], p[1], k, err)
		}
		if prev, dup := seen[k]; dup {
			t.Fatalf("RefKey collision: (%q, %q) and (%q, %q) both produced %q",
				prev[0], prev[1], p[0], p[1], k)
		}
		seen[k] = p
	}

	// End-to-end: publishing 1.0.0-rc.1 and 1.0.0-rc-1 keeps both refs intact.
	pub, priv := mustKeyPair(t)
	_ = pub
	mem := storetest.NewMem()
	store := NewVFSStoreProvider("ref-store", true, mem)
	art, err := IngestArtifact(ctx, "weights/w.gguf", AssetKindGGUF, "Q4_K_M", bytes.NewReader(deterministicPayload(55, 4096)), 4096, store)
	if err != nil {
		t.Fatalf("IngestArtifact: %v", err)
	}
	makeRelease := func(ver string) *ManifestV1 {
		m := &ManifestV1{
			SchemaVersion: SchemaVersionV1,
			ModelID:       "zen-7b",
			Version:       ver,
			License:       "Apache-2.0",
			Provenance:    Provenance{CreatedAt: "2026-11-26T00:00:00Z"},
			Artifacts:     []Artifact{art},
		}
		if err := m.Sign(priv, "zen-release"); err != nil {
			t.Fatalf("Sign %s: %v", ver, err)
		}
		if _, err := PublishManifest(ctx, m, store); err != nil {
			t.Fatalf("PublishManifest %s: %v", ver, err)
		}
		return m
	}

	mDot := makeRelease("1.0.0-rc.1")
	mDash := makeRelease("1.0.0-rc-1")

	kDot, _ := RefKey("zen-7b", "1.0.0-rc.1")
	kDash, _ := RefKey("zen-7b", "1.0.0-rc-1")

	gotDot, _, err := mem.Get(ctx, kDot, backend.Unbounded)
	if err != nil {
		t.Fatalf("Get rc.1 ref: %v", err)
	}
	gotDash, _, err := mem.Get(ctx, kDash, backend.Unbounded)
	if err != nil {
		t.Fatalf("Get rc-1 ref: %v", err)
	}
	if string(gotDot) != mDot.ManifestCID {
		t.Fatalf("rc.1 ref was overwritten: got %s, want %s", string(gotDot), mDot.ManifestCID)
	}
	if string(gotDash) != mDash.ManifestCID {
		t.Fatalf("rc-1 ref mismatch: got %s, want %s", string(gotDash), mDash.ManifestCID)
	}
}

// TestSemVerPrereleaseNumericPrecedence verifies SemVer 2.0.0 prerelease
// comparison (numeric, non-numeric, and prefix rules) and policy bounds.
func TestSemVerPrereleaseNumericPrecedence(t *testing.T) {
	ordered := []string{
		"1.0.0-1",
		"1.0.0-2",
		"1.0.0-10",
		"1.0.0-alpha",
		"1.0.0-alpha.1",
		"1.0.0-alpha.2",
		"1.0.0-alpha.10",
		"1.0.0-alpha.beta",
		"1.0.0-beta",
		"1.0.0-beta.2",
		"1.0.0-beta.11",
		"1.0.0-rc.1",
		"1.0.0-rc.2",
		"1.0.0-rc.10",
		"1.0.0-rc.10.0",
		"1.0.0-rc.99999999999999999999999",
		"1.0.0",
	}
	parsed := make([]SemVer, len(ordered))
	for i, s := range ordered {
		v, err := ParseSemVer(s)
		if err != nil {
			t.Fatalf("ParseSemVer(%q): %v", s, err)
		}
		parsed[i] = v
	}
	for i := 0; i < len(parsed); i++ {
		if c := parsed[i].Compare(parsed[i]); c != 0 {
			t.Fatalf("%s == %s gave %d", ordered[i], ordered[i], c)
		}
		for j := i + 1; j < len(parsed); j++ {
			if c := parsed[i].Compare(parsed[j]); c != -1 {
				t.Fatalf("expected %s < %s (-1), got %d", ordered[i], ordered[j], c)
			}
			if c := parsed[j].Compare(parsed[i]); c != 1 {
				t.Fatalf("expected %s > %s (+1), got %d", ordered[j], ordered[i], c)
			}
		}
	}

	for _, bad := range []string{"1.0.0-", "1.0.0-rc..1", "1.0.0-.rc", "1.0.0-rc."} {
		if _, err := ParseSemVer(bad); err == nil {
			t.Fatalf("expected ParseSemVer(%q) to fail", bad)
		}
	}

	// Policy enforcement: rc.10 must be rejected when MaxVersion is rc.2.
	ctx := context.Background()
	base, _, pub, priv, _, _ := buildTestBundle(t, ctx, 32<<10)
	rc10 := *base
	rc10.Version = "1.0.0-rc.10"
	rc10.ManifestCID = ""
	rc10.Signatures = nil
	if err := rc10.Sign(priv, "zen-release"); err != nil {
		t.Fatalf("Sign rc10: %v", err)
	}
	maxPolicy := VerificationPolicy{
		TrustedKeys: []ed25519.PublicKey{pub},
		MaxVersion:  "1.0.0-rc.2",
	}
	if err := rc10.Verify(maxPolicy); !errors.Is(err, ErrVersionRejected) {
		t.Fatalf("expected rc.10 to be rejected by MaxVersion=1.0.0-rc.2, got %v", err)
	}
	minPolicy := VerificationPolicy{
		TrustedKeys: []ed25519.PublicKey{pub},
		MinVersion:  "1.0.0-rc.2",
		MaxVersion:  "1.0.0-rc.10",
	}
	if err := rc10.Verify(minPolicy); err != nil {
		t.Fatalf("expected rc.10 to pass [1.0.0-rc.2, 1.0.0-rc.10], got %v", err)
	}
}
