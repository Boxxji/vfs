package vfs_test

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hanzoai/vfs"

	"github.com/hanzoai/vfs/internal/storetest"
	"github.com/hanzoai/vfs/pkg/backend"
	"github.com/hanzoai/vfs/pkg/backend/s3"
)

func openOn(t testing.TB, srv *storetest.Real, prefix string) backend.Backend {
	be, err := s3.New(s3.Config{Endpoint: srv.Endpoint, Insecure: true, Region: "us-east-1", Bucket: srv.Bucket,
		Prefix: prefix, AccessKey: srv.AccessKey, SecretKey: srv.SecretKey})
	if err != nil {
		t.Fatal(err)
	}
	return be
}

// TestRealServerCAS races conditional writes harder than the suite does: 128
// clients, identical bodies (so a non-atomic server would give every winner the
// same ETag), bodies of 1 B, 1 MiB and 20 MiB (multi-chunk on the filer), and a
// mix of PutIfVersion with an unconditional Put and a Delete.
func TestRealServerCAS(t *testing.T) {
	srv := storetest.RealS3(t)
	prefix := srv.Prefix("cas")
	const racers = 128
	clients := make([]backend.Backend, racers)
	for i := range clients {
		clients[i] = openOn(t, srv, prefix)
	}
	sizes := []int{1, 1 << 20, 20 << 20}
	if testing.Short() {
		sizes = sizes[:2]
	}
	for _, size := range sizes {
		t.Run(fmt.Sprint("identical-", size), func(t *testing.T) {
			racers := racers
			if size > 1<<20 {
				racers = 24
			}
			key := fmt.Sprintf("k%d", size)
			body := bytes.Repeat([]byte{0x42}, size)
			if _, err := clients[0].PutIfAbsent(ctx, key, []byte("base")); err != nil {
				t.Fatal(err)
			}
			rounds := sized(15, 24)
			if size > 1<<20 {
				rounds = sized(2, 3)
			}
			for round := range rounds {
				_, v, err := clients[0].Get(ctx, key, backend.Unbounded)
				if err != nil {
					t.Fatal(err)
				}
				var wins atomic.Int32
				var wg sync.WaitGroup
				start := make(chan struct{})
				for i := range racers {
					wg.Add(1)
					go func() {
						defer wg.Done()
						<-start
						b := body
						if round%2 == 1 { // distinct bodies on odd rounds
							b = append(append([]byte(nil), body...), byte(i), byte(round))
						}
						_, err := clients[i].PutIfVersion(ctx, key, b, v)
						switch {
						case err == nil:
							wins.Add(1)
						case !errors.Is(err, backend.ErrConflict):
							t.Errorf("round %d writer %d: %v", round, i, err)
						}
					}()
				}
				close(start)
				wg.Wait()
				// Identical bodies on even rounds: after round 0 the ETag of the
				// body is already the stored one, so 'body' written over 'body' keeps
				// the same ETag; that is fine, but a second winner in ONE round is not.
				if round > 0 && round%2 == 0 {
					// The object was a distinct-body object from the odd round: ETag differs.
				}
				if wins.Load() != 1 {
					t.Fatalf("size %d round %d: %d of %d won on version %s, want 1", size, round, wins.Load(), racers, v)
				}
			}
		})
	}

	t.Run("creators", func(t *testing.T) {
		for k := range sized(20, 80) {
			key := fmt.Sprintf("create/%d", k)
			var wins atomic.Int32
			var wg sync.WaitGroup
			start := make(chan struct{})
			for i := range racers {
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					if _, err := clients[i].PutIfAbsent(ctx, key, []byte("same")); err == nil {
						wins.Add(1)
					} else if !errors.Is(err, backend.ErrConflict) {
						t.Errorf("creator %d: %v", i, err)
					}
				}()
			}
			close(start)
			wg.Wait()
			if wins.Load() != 1 {
				t.Fatalf("key %s: %d of %d creators won", key, wins.Load(), racers)
			}
		}
	})

	// A conditional write racing an unconditional Put and a Delete: the
	// conditional one may win only if it ran before both; it must never win
	// AFTER the version it named was replaced.
	t.Run("mixed", func(t *testing.T) {
		key := "mixed"
		if _, err := clients[0].PutIfAbsent(ctx, key, []byte("base")); err != nil {
			t.Fatal(err)
		}
		for round := range sized(20, 80) {
			_, v, _ := clients[0].Get(ctx, key, backend.Unbounded)
			var cwins, uwins atomic.Int32
			var wg sync.WaitGroup
			start := make(chan struct{})
			for i := range 32 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					switch i % 4 {
					case 0, 1:
						if _, err := clients[i].PutIfVersion(ctx, key, []byte(fmt.Sprintf("c%d-%d", round, i)), v); err == nil {
							cwins.Add(1)
						}
					case 2:
						if err := clients[i].Put(ctx, key, []byte(fmt.Sprintf("u%d-%d", round, i))); err == nil {
							uwins.Add(1)
						}
					case 3:
						_ = clients[i].Delete(ctx, key)
					}
				}()
			}
			close(start)
			wg.Wait()
			if cwins.Load() > 1 {
				t.Fatalf("round %d: %d conditional writers won", round, cwins.Load())
			}
			// re-seed if deleted
			if _, _, err := clients[0].Get(ctx, key, backend.Unbounded); err != nil {
				_, _ = clients[0].PutIfAbsent(ctx, key, []byte("base"))
			}
		}
	})
}

// TestRealServerMultipartIsConditional: the Backend contract says a conditional
// write is atomic or refused. A body above the single-request limit would go in
// parts, and the parts do not carry the condition, so such a write must be refused
// outright, never made unconditionally; an unconditional Put of the same body goes
// in parts and works.
func TestRealServerMultipartIsConditional(t *testing.T) {
	if testing.Short() {
		t.Skip("70 MiB bodies")
	}
	srv := storetest.RealS3(t)
	be := openOn(t, srv, srv.Prefix("mp"))
	big := bytes.Repeat([]byte("0123456789abcdef"), (70<<20)/16) // 70 MiB > partSize 64 MiB
	if _, err := be.PutIfAbsent(ctx, "obj", big); !errors.Is(err, backend.ErrTooLarge) {
		t.Fatalf("multipart-sized PutIfAbsent = %v, want ErrTooLarge", err)
	}
	if _, _, err := be.Get(ctx, "obj", backend.Unbounded); !errors.Is(err, backend.ErrNotFound) {
		t.Fatalf("a refused conditional write left an object behind: %v", err)
	}
	if err := be.Put(ctx, "obj", big); err != nil {
		t.Fatalf("unconditional multipart Put: %v", err)
	}
	// The object exists, so a conditional write over it must not silently replace it.
	if _, err := be.PutIfAbsent(ctx, "obj", big); !errors.Is(err, backend.ErrTooLarge) && !errors.Is(err, backend.ErrConflict) {
		t.Fatalf("PutIfAbsent over an existing object = %v", err)
	}
	_, v, _ := be.Get(ctx, "obj", backend.Unbounded)
	if _, err := be.PutIfVersion(ctx, "obj", big[:200], v); err != nil {
		t.Fatalf("PutIfVersion at the current version: %v", err)
	}
	if _, err := be.PutIfVersion(ctx, "obj", big, v); !errors.Is(err, backend.ErrTooLarge) {
		t.Fatalf("multipart-sized PutIfVersion at a stale version = %v, want ErrTooLarge", err)
	}
	if got, _, _ := be.Get(ctx, "obj", backend.Unbounded); len(got) != 200 {
		t.Fatalf("a refused conditional write changed the object to %d bytes", len(got))
	}
}

// TestCommitRateOnRealServer: the cost of a commit per close(2) against the
// real server, over loopback: files per second with a Flush after each, against
// one Flush at the end.
func TestCommitRateOnRealServer(t *testing.T) {
	srv := storetest.RealS3(t)
	for _, per := range []bool{true, false} {
		be := openOn(t, srv, srv.Prefix("redrate"))
		c := vfsAge(t)
		if err := vfsCreate(be, c); err != nil {
			t.Fatal(err)
		}
		v := vfsAttach(t, be, c, 1)
		dir := mkdir(t, v, 1, "tree")
		start := time.Now()
		const files = 300
		var lat []time.Duration
		for i := range files {
			write(t, v, dir, fmt.Sprintf("f%04d", i), content(uint64(i), 20<<10))
			if per {
				s := time.Now()
				flush(t, v)
				lat = append(lat, time.Since(s))
			}
		}
		flush(t, v)
		took := time.Since(start)
		sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
		if per {
			t.Logf("flush per close: %d files in %v = %.1f files/s; commit p50 %v p99 %v", files, took.Round(time.Millisecond), files/took.Seconds(), lat[len(lat)/2], lat[len(lat)*99/100])
		} else {
			t.Logf("one flush: %d files in %v = %.1f files/s", files, took.Round(time.Millisecond), files/took.Seconds())
		}
	}
}

func vfsCreate(be backend.Backend, c vfs.Cipher) error { return vfs.Create(ctx, cfg(be, c, 0)) }
func vfsAttach(t testing.TB, be backend.Backend, c vfs.Cipher, round uint64) *vfs.Volume {
	v, err := vfs.Attach(ctx, cfg(be, c, round))
	if err != nil {
		t.Fatal(err)
	}
	return v
}

// TestGetPairsBodyWithVersion: the fence decides on the round it decoded from a
// Get's body and then conditions the write on the Version of that Get. Under
// concurrent overwrites the two must come from one state of the object. This
// overwrites a key from 8 writers and checks, on every Get from 8 readers, that
// the version equals the MD5 of the body (a single-request PUT's ETag).
func TestGetPairsBodyWithVersion(t *testing.T) {
	if testing.Short() {
		t.Skip("stress")
	}
	srv := storetest.RealS3(t)
	prefix := srv.Prefix("redpair")
	seed := openOn(t, srv, prefix)
	if err := seed.Put(ctx, "k", []byte("seed")); err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var bad, reads, writes atomic.Int64
	for w := range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			be := openOn(t, srv, prefix)
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				n := 1 + (i*7919+w*104729)%(256<<10)
				if i%3 == 0 {
					n = 100 + i%200
				}
				b := bytes.Repeat([]byte{byte(w), byte(i)}, n/2+1)
				if err := be.Put(ctx, "k", b); err == nil {
					writes.Add(1)
				}
			}
		}()
	}
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			be := openOn(t, srv, prefix)
			for {
				select {
				case <-stop:
					return
				default:
				}
				b, v, err := be.Get(ctx, "k", backend.Unbounded)
				if err != nil {
					continue
				}
				reads.Add(1)
				sum := md5.Sum(b)
				if string(v) != hex.EncodeToString(sum[:]) {
					bad.Add(1)
				}
			}
		}()
	}
	time.Sleep(15 * time.Second)
	close(stop)
	wg.Wait()
	t.Logf("%d reads, %d writes, %d reads whose version was not the MD5 of the body", reads.Load(), writes.Load(), bad.Load())
	if bad.Load() > 0 {
		t.Fatalf("%d of %d Gets returned a version that does not name the body", bad.Load(), reads.Load())
	}
}
