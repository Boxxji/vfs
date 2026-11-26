// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package aiturbo

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/hanzoai/vfs/internal/storetest"
	"github.com/hanzoai/vfs/pkg/backend"
	_ "github.com/hanzoai/vfs/pkg/backend/file"
	s3be "github.com/hanzoai/vfs/pkg/backend/s3"
)

func buildMixedTestCheckpoint(ns Namespace, seed int64) ([]Entry, []string) {
	entries := make([]Entry, 0, 48)
	var selective []string
	for i := 0; i < 36; i++ {
		p := fmt.Sprintf("layers/l%02d/t%02d.bin", i/12, i%12)
		entries = append(entries, Entry{
			TenantID: ns.TenantID,
			VolumeID: ns.VolumeID,
			Path:     p,
			Data:     makeDeterministicBytes(seed+int64(i), 16<<10), // 16 KiB
		})
		if i < 6 {
			selective = append(selective, p)
		}
	}
	for i := 0; i < 8; i++ {
		entries = append(entries, Entry{
			TenantID: ns.TenantID,
			VolumeID: ns.VolumeID,
			Path:     fmt.Sprintf("medium/m%02d.bin", i),
			Data:     makeDeterministicBytes(seed+100+int64(i), 128<<10), // 128 KiB
		})
	}
	for i := 0; i < 2; i++ {
		entries = append(entries, Entry{
			TenantID: ns.TenantID,
			VolumeID: ns.VolumeID,
			Path:     fmt.Sprintf("weights/w%02d.bin", i),
			Data:     makeDeterministicBytes(seed+200+int64(i), 1<<20), // 1 MiB
		})
	}
	entries = append(entries, Entry{
		TenantID: ns.TenantID,
		VolumeID: ns.VolumeID,
		Path:     "meta/empty.txt",
		Data:     []byte{},
	})
	return entries, selective
}

func TestRoundTripBitExactAcrossAllFourRegimes(t *testing.T) {
	ctx := context.Background()
	ns := Namespace{TenantID: "org-hanzo", VolumeID: "vol-zen-01"}
	entries, selective := buildMixedTestCheckpoint(ns, 1345)

	cfg := DefaultConfig()
	cfg.TargetGroupBytes = 1 << 20
	cfg.LargeObjectThreshold = 1 << 20
	cfg.MaxGapBytes = 64 << 10
	cfg.Concurrency = 4

	regimes := []Regime{
		RegimeNaivePerObjectSerial,
		RegimeNaivePerObjectConcurrent,
		RegimeNaiveSingleTarBlob,
		RegimeAITurboGroupedIO,
	}

	results := make(map[Regime]*RegimeResult, len(regimes))
	for _, r := range regimes {
		mem := NewMemoryBackend()
		be := NewInstrumentedBackend(mem, S3ModelConfig{})
		res, err := RunMatchedRegime(ctx, be, r, ns, "ckpt-"+string(r), entries, selective, cfg)
		if err != nil {
			t.Fatalf("regime %s failed: %v", r, err)
		}
		if !res.BitExactVerified {
			t.Fatalf("regime %s was not bit-exact verified", r)
		}
		results[r] = res
	}

	// AITURBO must dramatically cut PUTs/GiB vs per-object naive regimes.
	naivePUTs := results[RegimeNaivePerObjectConcurrent].UploadMetrics.TotalWriteRequests()
	aiturboPUTs := results[RegimeAITurboGroupedIO].UploadMetrics.TotalWriteRequests()
	if aiturboPUTs >= naivePUTs/4 {
		t.Fatalf("expected AITURBO PUTs (%d) << naive PUTs (%d)", aiturboPUTs, naivePUTs)
	}

	// AITURBO selective restore must fetch far fewer bytes than single-tar-blob (100% read).
	tarAmp := results[RegimeNaiveSingleTarBlob].SelectiveReadAmp
	aiAmp := results[RegimeAITurboGroupedIO].SelectiveReadAmp
	if aiAmp >= tarAmp/5 {
		t.Fatalf("expected AITURBO selective read amp (%.2fx) << single-tar amp (%.2fx)", aiAmp, tarAmp)
	}
}

func TestS3ConsistencyMidUploadPartialFailureAtomicity(t *testing.T) {
	ctx := context.Background()
	ns := Namespace{TenantID: "org-atomic", VolumeID: "vol-1"}
	entries, _ := buildMixedTestCheckpoint(ns, 777)

	cfg := DefaultConfig()
	cfg.TargetGroupBytes = 512 << 10
	cfg.LargeObjectThreshold = 1 << 20
	cfg.Concurrency = 4
	cfg.CleanupOnAbort = true

	mem := NewMemoryBackend()
	// Fail on the 3rd Put call during a multi-group upload.
	be := NewInstrumentedBackend(mem, S3ModelConfig{
		FailOnPutCall: 3,
	})
	eng := NewEngine(be)

	res, err := eng.UploadCheckpoint(ctx, ns, "ckpt-atomic-fail", entries, cfg)
	if !errors.Is(err, ErrInjectedFailure) {
		t.Fatalf("expected ErrInjectedFailure, got %v", err)
	}
	if res == nil || res.HeadPublished || !res.Aborted {
		t.Fatalf("expected HeadPublished=false, Aborted=true; got %+v", res)
	}

	// S3 Consistency Invariant: NO torn checkpoint is visible to readers.
	be.SetModelConfig(S3ModelConfig{}) // clear fault injection
	if _, _, err := eng.LoadManifest(ctx, ns, "ckpt-atomic-fail", cfg); !errors.Is(err, backend.ErrNotFound) {
		t.Fatalf("torn manifest visible after aborted upload! got err=%v", err)
	}
	if _, err := eng.DownloadCheckpoint(ctx, ns, "ckpt-atomic-fail", nil, cfg); !errors.Is(err, backend.ErrNotFound) {
		t.Fatalf("DownloadCheckpoint succeeded on aborted upload! got err=%v", err)
	}
	// CleanupOnAbort should leave 0 orphaned objects in the store.
	if got := mem.ObjectCount(); got != 0 {
		t.Fatalf("expected 0 leaked objects after aborted upload cleanup, got %d", got)
	}

	// Subsequent clean retry of the same checkpoint ID succeeds atomically.
	okRes, err := eng.UploadCheckpoint(ctx, ns, "ckpt-atomic-fail", entries, cfg)
	if err != nil || !okRes.HeadPublished {
		t.Fatalf("clean retry failed: res=%+v err=%v", okRes, err)
	}
	down, err := eng.DownloadCheckpoint(ctx, ns, "ckpt-atomic-fail", nil, cfg)
	if err != nil || len(down.Files) != len(entries) {
		t.Fatalf("download after retry failed: err=%v files=%d", err, len(down.Files))
	}
}

func TestS3ConsistencyHeadConflictAndCASUpdate(t *testing.T) {
	ctx := context.Background()
	ns := Namespace{TenantID: "org-cas", VolumeID: "vol-1"}
	cfg := DefaultConfig()
	cfg.RequireConditionalHead = true

	be := NewInstrumentedBackend(NewMemoryBackend(), S3ModelConfig{})
	eng := NewEngine(be)

	v1Entries := []Entry{{TenantID: ns.TenantID, VolumeID: ns.VolumeID, Path: "step.txt", Data: []byte("step-1")}}
	v2Entries := []Entry{{TenantID: ns.TenantID, VolumeID: ns.VolumeID, Path: "step.txt", Data: []byte("step-2")}}

	res1, err := eng.UploadCheckpoint(ctx, ns, "latest", v1Entries, cfg)
	if err != nil || res1.HeadVersion == "" {
		t.Fatalf("initial PutIfAbsent upload failed: %v", err)
	}

	// Duplicate UploadCheckpoint with RequireConditionalHead=true must fail with backend.ErrConflict.
	if _, err := eng.UploadCheckpoint(ctx, ns, "latest", v2Entries, cfg); !errors.Is(err, backend.ErrConflict) {
		t.Fatalf("expected backend.ErrConflict on duplicate conditional head, got %v", err)
	}

	// UpdateCheckpointHead with stale version must fail with backend.ErrConflict.
	if _, err := eng.UpdateCheckpointHead(ctx, ns, "latest", v2Entries, "stale-etag", cfg); !errors.Is(err, backend.ErrConflict) {
		t.Fatalf("expected backend.ErrConflict on stale CAS version, got %v", err)
	}

	// UpdateCheckpointHead with exact current version succeeds atomically.
	res2, err := eng.UpdateCheckpointHead(ctx, ns, "latest", v2Entries, res1.HeadVersion, cfg)
	if err != nil || !res2.HeadPublished || res2.HeadVersion == res1.HeadVersion {
		t.Fatalf("CAS UpdateCheckpointHead failed: res=%+v err=%v", res2, err)
	}

	// Read-after-write sees step-2 immediately.
	down, err := eng.DownloadCheckpoint(ctx, ns, "latest", nil, cfg)
	if err != nil || string(down.Files["step.txt"]) != "step-2" {
		t.Fatalf("read-after-write mismatch: got %q err=%v", string(down.Files["step.txt"]), err)
	}
}

func TestBLAKE3CorruptionRejectionOnDownload(t *testing.T) {
	ctx := context.Background()
	ns := Namespace{TenantID: "org-verify", VolumeID: "vol-1"}
	entries, selective := buildMixedTestCheckpoint(ns, 9001)

	cfg := DefaultConfig()
	cfg.TargetGroupBytes = 1 << 20
	cfg.LargeObjectThreshold = 1 << 20

	be := NewInstrumentedBackend(NewMemoryBackend(), S3ModelConfig{})
	eng := NewEngine(be)

	upRes, err := eng.UploadCheckpoint(ctx, ns, "ckpt-corrupt", entries, cfg)
	if err != nil {
		t.Fatal(err)
	}

	var packKey, directKey string
	for _, ent := range upRes.Manifest.Entries {
		if ent.Path == selective[0] {
			packKey = ent.ObjectKey
		}
		if ent.DirectBypass && directKey == "" {
			directKey = ent.ObjectKey
		}
	}
	if packKey == "" || directKey == "" {
		t.Fatalf("expected both pack and direct objects, got pack=%q direct=%q", packKey, directKey)
	}

	// Flip 1 bit on packed object during selective Range-GET -> must reject with ErrCorruptData.
	be.SetModelConfig(S3ModelConfig{CorruptOnReadKey: packKey})
	if _, err := eng.DownloadCheckpoint(ctx, ns, "ckpt-corrupt", selective, cfg); !errors.Is(err, ErrCorruptData) {
		t.Fatalf("expected ErrCorruptData on corrupted packed Range-GET, got %v", err)
	}

	// Flip 1 bit on direct-bypass object during full GET -> must reject with ErrCorruptData.
	be.SetModelConfig(S3ModelConfig{CorruptOnReadKey: directKey})
	if _, err := eng.DownloadCheckpoint(ctx, ns, "ckpt-corrupt", nil, cfg); !errors.Is(err, ErrCorruptData) {
		t.Fatalf("expected ErrCorruptData on corrupted direct GET, got %v", err)
	}
}

func TestUploadRejectsCrossTenantMixing(t *testing.T) {
	ctx := context.Background()
	nsA := Namespace{TenantID: "tenant-a", VolumeID: "vol-1"}
	entries := []Entry{
		{TenantID: "tenant-a", VolumeID: "vol-1", Path: "a.bin", Data: []byte("ok")},
		{TenantID: "tenant-b", VolumeID: "vol-1", Path: "b.bin", Data: []byte("cross")},
	}
	eng := NewEngine(NewMemoryBackend())
	if _, err := eng.UploadCheckpoint(ctx, nsA, "ckpt-mix", entries, DefaultConfig()); !errors.Is(err, ErrTenantIsolation) {
		t.Fatalf("expected ErrTenantIsolation, got %v", err)
	}
}

func TestConcurrentPlannersAndTransfersRace(t *testing.T) {
	ctx := context.Background()
	be := NewInstrumentedBackend(NewMemoryBackend(), S3ModelConfig{
		PutRTT:               1 * time.Millisecond,
		GetRTT:               1 * time.Millisecond,
		PerStreamBytesPerSec: 200 << 20,
		AggregateBytesPerSec: 800 << 20,
	})
	eng := NewEngine(be)

	cfg := DefaultConfig()
	cfg.TargetGroupBytes = 256 << 10
	cfg.LargeObjectThreshold = 512 << 10
	cfg.Concurrency = 4

	var wg sync.WaitGroup
	errCh := make(chan error, 16)

	for tenant := 0; tenant < 4; tenant++ {
		for worker := 0; worker < 4; worker++ {
			wg.Add(1)
			go func(tid, wid int) {
				defer wg.Done()
				ns := Namespace{
					TenantID: fmt.Sprintf("tenant-%d", tid),
					VolumeID: fmt.Sprintf("vol-%d", wid%2),
				}
				ckptID := fmt.Sprintf("step-%d-%d", tid, wid)
				entries := make([]Entry, 12)
				for i := range entries {
					entries[i] = Entry{
						TenantID: ns.TenantID,
						VolumeID: ns.VolumeID,
						Path:     fmt.Sprintf("dir/f-%02d.bin", i),
						Data:     makeDeterministicBytes(int64(tid*1000+wid*100+i), 32<<10),
					}
				}
				upRes, err := eng.UploadCheckpoint(ctx, ns, ckptID, entries, cfg)
				if err != nil {
					errCh <- fmt.Errorf("upload %s: %w", ckptID, err)
					return
				}
				if !upRes.HeadPublished {
					errCh <- fmt.Errorf("head not published for %s", ckptID)
					return
				}
				// Selective restore of 3 contiguous files.
				sel := []string{"dir/f-02.bin", "dir/f-03.bin", "dir/f-04.bin"}
				downRes, err := eng.DownloadCheckpoint(ctx, ns, ckptID, sel, cfg)
				if err != nil {
					errCh <- fmt.Errorf("download %s: %w", ckptID, err)
					return
				}
				for _, idx := range []int{2, 3, 4} {
					p := fmt.Sprintf("dir/f-%02d.bin", idx)
					if !bytes.Equal(downRes.Files[p], entries[idx].Data) {
						errCh <- fmt.Errorf("data mismatch for %s/%s", ckptID, p)
						return
					}
				}
			}(tenant, worker)
		}
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
}

func TestContextCancellationDuringTransfer(t *testing.T) {
	ns := Namespace{TenantID: "org-cancel", VolumeID: "vol-1"}
	entries, _ := buildMixedTestCheckpoint(ns, 321)

	be := NewInstrumentedBackend(NewMemoryBackend(), S3ModelConfig{
		PutRTT: 50 * time.Millisecond,
	})
	eng := NewEngine(be)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Millisecond)
	defer cancel()

	cfg := DefaultConfig()
	cfg.TargetGroupBytes = 512 << 10
	res, err := eng.UploadCheckpoint(ctx, ns, "ckpt-cancel", entries, cfg)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded, got %v", err)
	}
	if res == nil || res.HeadPublished {
		t.Fatalf("canceled upload must never publish head: %+v", res)
	}
}

func TestRealFileBackendIntegration(t *testing.T) {
	ctx := context.Background()
	fBe, err := backend.Open(ctx, "file://"+t.TempDir())
	if err != nil {
		t.Fatalf("backend.Open file:// failed: %v", err)
	}
	defer fBe.Close()

	ns := Namespace{TenantID: "org-file", VolumeID: "vol-disk"}
	entries, selective := buildMixedTestCheckpoint(ns, 4040)

	cfg := DefaultConfig()
	cfg.TargetGroupBytes = 1 << 20
	cfg.LargeObjectThreshold = 1 << 20

	be := NewInstrumentedBackend(fBe, S3ModelConfig{})
	res, err := RunMatchedRegime(ctx, be, RegimeAITurboGroupedIO, ns, "ckpt-file-be", entries, selective, cfg)
	if err != nil {
		t.Fatalf("file backend integration failed: %v", err)
	}
	if !res.BitExactVerified {
		t.Fatalf("file backend round-trip was not verified")
	}
}

func TestRealHanzoS3HTTPIntegration(t *testing.T) {
	ctx := context.Background()
	srv := storetest.NewS3(t, "hanzo-checkpoints")
	rawS3, err := s3be.New(s3be.Config{
		Endpoint:  srv.Endpoint(),
		Insecure:  true,
		Region:    "us-east-1",
		Bucket:    "hanzo-checkpoints",
		Prefix:    "vfs/aiturbo",
		AccessKey: "test-key",
		SecretKey: "test-secret",
	})
	if err != nil {
		t.Fatalf("s3.New failed: %v", err)
	}
	defer rawS3.Close()

	ns := Namespace{TenantID: "org-hanzo-s3", VolumeID: "vol-gpu-cluster"}
	entries, selective := buildMixedTestCheckpoint(ns, 8080)

	cfg := DefaultConfig()
	cfg.TargetGroupBytes = 1 << 20
	cfg.LargeObjectThreshold = 1 << 20
	cfg.RequireConditionalHead = true

	be := NewInstrumentedBackend(rawS3, S3ModelConfig{})
	res, err := RunMatchedRegime(ctx, be, RegimeAITurboGroupedIO, ns, "ckpt-s3-wire", entries, selective, cfg)
	if err != nil {
		t.Fatalf("real Hanzo S3 HTTP integration failed: %v", err)
	}
	if !res.BitExactVerified {
		t.Fatalf("real Hanzo S3 HTTP round-trip was not verified")
	}
	if srv.Requests() == 0 {
		t.Fatalf("expected real HTTP S3 requests against storetest.S3, got 0")
	}

	// Verify S3 conditional write 412 PreconditionFailed maps to backend.ErrConflict.
	eng := NewEngine(be)
	if _, err := eng.UploadCheckpoint(ctx, ns, "ckpt-s3-wire", entries, cfg); !errors.Is(err, backend.ErrConflict) {
		t.Fatalf("expected HTTP 412 -> backend.ErrConflict on duplicate checkpoint head, got %v", err)
	}
}
