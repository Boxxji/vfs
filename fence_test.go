package vfs_test

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/internal/storetest"
	"github.com/hanzoai/vfs/pkg/backend"
	"github.com/hanzoai/vfs/pkg/backend/s3"
)

// store is one place a Volume can live; client opens a new connection to it, as
// a second process would.
type store struct {
	name   string
	client func(t *testing.T) backend.Backend
}

// stores are the backends the fence is proved against: memory with S3's
// conditional semantics, the file backend, the S3 backend talking over HTTP to a
// server that implements If-Match and If-None-Match, and, where an `s3` binary
// is on PATH, the same backend against a real Hanzo S3 server.
func stores(t *testing.T) []store {
	mem := storetest.NewMem()
	dir := t.TempDir()
	srv := storetest.NewS3(t, "disk")
	list := []store{
		{"mem", func(*testing.T) backend.Backend { return mem }},
		{"file", func(t *testing.T) backend.Backend { return reopenBackend(t, dir) }},
		{"s3", func(t *testing.T) backend.Backend {
			be, err := s3.New(s3.Config{Endpoint: srv.Endpoint(), Insecure: true, Region: "us-east-1", Bucket: "disk", Prefix: "vol", AccessKey: "k", SecretKey: "s"})
			if err != nil {
				t.Fatal(err)
			}
			return be
		}},
	}
	if storetest.HaveReal() {
		prefix := ""
		list = append(list, store{"hanzo-s3", func(t *testing.T) backend.Backend {
			real := storetest.RealS3(t)
			if prefix == "" {
				prefix = real.Prefix("vol")
			}
			be, err := s3.New(s3.Config{Endpoint: real.Endpoint, Insecure: true, Region: "us-east-1", Bucket: real.Bucket,
				Prefix: prefix, AccessKey: real.AccessKey, SecretKey: real.SecretKey})
			if err != nil {
				t.Fatal(err)
			}
			return be
		}})
	}
	return list
}

// TestFenceRefusesOlderRound: after a newer Round attaches, the older one cannot
// commit, and everything it had not committed stays uncommitted.
func TestFenceRefusesOlderRound(t *testing.T) {
	for _, st := range stores(t) {
		t.Run(st.name, func(t *testing.T) {
			c := vfsAge(t)
			if err := vfs.Create(ctx, cfg(st.client(t), c, 0)); err != nil {
				t.Fatal(err)
			}
			a, err := vfs.Attach(ctx, cfg(st.client(t), c, 5))
			if err != nil {
				t.Fatal(err)
			}
			write(t, a, vfs.RootInode, "from-a", []byte("a1"))
			flush(t, a)

			b, err := vfs.Attach(ctx, cfg(st.client(t), c, 6))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := b.Resolve(ctx, "/from-a"); err != nil {
				t.Fatalf("the new owner lost the old owner's committed file: %v", err)
			}

			// The deposed writer still has a Volume in hand and uncommitted work.
			write(t, a, vfs.RootInode, "late-from-a", []byte("never"))
			if err := a.Flush(ctx); !errors.Is(err, vfs.ErrStaleRound) {
				t.Fatalf("older round's Flush = %v, want ErrStaleRound", err)
			}
			if _, err := a.Create(ctx, vfs.RootInode, "more", 0o644); !errors.Is(err, vfs.ErrStaleRound) {
				t.Fatalf("a fenced Volume accepted a write: %v", err)
			}
			write(t, b, vfs.RootInode, "from-b", []byte("b1"))
			flush(t, b)

			got := snapshot(t, reopen(t, st.client(t), c))
			if _, ok := got["/late-from-a"]; ok {
				t.Fatal("a fenced writer's change reached the store")
			}
			for _, p := range []string{"/from-a", "/from-b"} {
				if _, ok := got[p]; !ok {
					t.Fatalf("%s is missing", p)
				}
			}
		})
	}
}

// TestAttachNeedsAHigherRound: a Round must exceed the recorded one; an equal or
// lower Round is refused, and so is zero.
func TestAttachNeedsAHigherRound(t *testing.T) {
	for _, st := range stores(t) {
		t.Run(st.name, func(t *testing.T) {
			c := vfsAge(t)
			if err := vfs.Create(ctx, cfg(st.client(t), c, 0)); err != nil {
				t.Fatal(err)
			}
			if _, err := vfs.Attach(ctx, cfg(st.client(t), c, 7)); err != nil {
				t.Fatal(err)
			}
			for _, r := range []uint64{7, 6, 1} {
				if _, err := vfs.Attach(ctx, cfg(st.client(t), c, r)); !errors.Is(err, vfs.ErrStaleRound) {
					t.Fatalf("Attach at round %d after 7 = %v, want ErrStaleRound", r, err)
				}
			}
			if _, err := vfs.Attach(ctx, cfg(st.client(t), c, 8)); err != nil {
				t.Fatalf("Attach at round 8: %v", err)
			}
		})
	}
}

// TestTakeoverLosesNoAcknowledgedCommit: one writer commits as fast as it can
// while another takes the Volume over. Every commit the first writer saw
// acknowledged is in the new owner's tree, and the first writer's next commit is
// refused.
func TestTakeoverLosesNoAcknowledgedCommit(t *testing.T) {
	for _, st := range stores(t) {
		t.Run(st.name, func(t *testing.T) {
			c := vfsAge(t)
			if err := vfs.Create(ctx, cfg(st.client(t), c, 0)); err != nil {
				t.Fatal(err)
			}
			a, err := vfs.Attach(ctx, cfg(st.client(t), c, 1))
			if err != nil {
				t.Fatal(err)
			}
			var acked []string
			var started atomic.Int32
			done := make(chan error, 1)
			go func() {
				for i := 0; ; i++ {
					name := fmt.Sprintf("c%04d", i)
					if _, err := a.Create(ctx, vfs.RootInode, name, 0o644); err != nil {
						done <- err
						return
					}
					started.Add(1)
					if err := a.Flush(ctx); err != nil {
						done <- err
						return
					}
					acked = append(acked, name)
				}
			}()
			for started.Load() < 20 {
				time.Sleep(time.Millisecond)
			}
			b, err := vfs.Attach(ctx, cfg(st.client(t), c, 2))
			if err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-done:
				if !errors.Is(err, vfs.ErrStaleRound) {
					t.Fatalf("the deposed writer stopped with %v, want ErrStaleRound", err)
				}
			case <-time.After(30 * time.Second):
				t.Fatal("the deposed writer is still committing 30s after the takeover: it was never fenced")
			}
			for _, name := range acked {
				if _, err := b.Resolve(ctx, "/"+name); err != nil {
					t.Fatalf("acknowledged commit %s lost across the takeover: %v", name, err)
				}
			}
			t.Logf("%d commits acknowledged before the takeover, all present", len(acked))
		})
	}
}

// TestRacingClaims: several processes try to attach at once at different rounds.
// Exactly the highest one ends as owner, and no lower one commits after it.
func TestRacingClaims(t *testing.T) {
	for _, st := range stores(t) {
		t.Run(st.name, func(t *testing.T) {
			c := vfsAge(t)
			if err := vfs.Create(ctx, cfg(st.client(t), c, 0)); err != nil {
				t.Fatal(err)
			}
			const n = 6
			vols := make([]*vfs.Volume, n)
			var wg sync.WaitGroup
			for i := range n {
				wg.Add(1)
				go func() {
					defer wg.Done()
					v, err := vfs.Attach(ctx, cfg(st.client(t), c, uint64(10+i)))
					if err == nil {
						vols[i] = v
					} else if !errors.Is(err, vfs.ErrStaleRound) {
						t.Errorf("claim %d: %v", i, err)
					}
				}()
			}
			wg.Wait()
			if vols[n-1] == nil {
				t.Fatal("the highest round did not win")
			}
			won := 0
			for i, v := range vols {
				if v == nil {
					continue
				}
				if _, err := v.Create(ctx, vfs.RootInode, fmt.Sprint("by", i), 0o644); err != nil {
					t.Fatal(err)
				}
				err := v.Flush(ctx)
				switch {
				case i == n-1 && err != nil:
					t.Fatalf("the owner cannot commit: %v", err)
				case i != n-1 && err == nil:
					t.Fatalf("round %d committed after round %d claimed", 10+i, 10+n-1)
				case err == nil:
					won++
				}
			}
			if won != 1 {
				t.Fatalf("%d writers committed, want 1", won)
			}
		})
	}
}

// TestVolumeOnEveryStore commits two states of a Volume with small files, a
// chunked file and edits, and reads them back from a fresh Open, on every store:
// ranged reads of packs, conditional writes of the Head, and shards all go through
// the real client.
func TestVolumeOnEveryStore(t *testing.T) {
	one, two := expected(t)
	for _, st := range stores(t) {
		t.Run(st.name, func(t *testing.T) {
			c := vfsAge(t)
			if err := vfs.Create(ctx, cfg(st.client(t), c, 0)); err != nil {
				t.Fatal(err)
			}
			v, err := vfs.Attach(ctx, cfg(st.client(t), c, 1))
			if err != nil {
				t.Fatal(err)
			}
			stateOne(t, v)
			flush(t, v)
			sameSnapshot(t, "state one", snapshot(t, reopen(t, st.client(t), c)), one)
			stateTwo(t, v)
			flush(t, v)
			sameSnapshot(t, "state two", snapshot(t, reopen(t, st.client(t), c)), two)
		})
	}
}
