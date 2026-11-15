package s3

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/hanzoai/vfs/internal/storetest"
	"github.com/hanzoai/vfs/pkg/backend"
)

func TestMain(m *testing.M) {
	code := m.Run()
	storetest.StopReal()
	os.Exit(code)
}

// TestRealServerConforms runs the conformance suite, including the race of eight
// writers on one version and eight creators of one key, against a real Hanzo S3
// server. It is skipped where no `s3` binary is on PATH.
func TestRealServerConforms(t *testing.T) {
	srv := storetest.RealS3(t)
	storetest.Run(t, func(t *testing.T) backend.Backend {
		be, err := New(Config{Endpoint: srv.Endpoint, Insecure: true, Region: "us-east-1", Bucket: srv.Bucket,
			Prefix: srv.Prefix("conform"), AccessKey: srv.AccessKey, SecretKey: srv.SecretKey})
		if err != nil {
			t.Fatal(err)
		}
		return be
	})
}

// TestRealServerRaceStress hammers the one property the fence stands on: of N
// writers that hold the same version, exactly one write lands. It races sixteen
// writers on one version for 150 rounds, and sixteen creators of one key for 100
// keys, through separate clients, against the real server.
func TestRealServerRaceStress(t *testing.T) {
	if testing.Short() {
		t.Skip("stress")
	}
	srv := storetest.RealS3(t)
	ctx := context.Background()
	prefix := srv.Prefix("race")
	open := func() backend.Backend {
		be, err := New(Config{Endpoint: srv.Endpoint, Insecure: true, Region: "us-east-1", Bucket: srv.Bucket,
			Prefix: prefix, AccessKey: srv.AccessKey, SecretKey: srv.SecretKey})
		if err != nil {
			t.Fatal(err)
		}
		return be
	}
	const racers = 16
	clients := make([]backend.Backend, racers)
	for i := range clients {
		clients[i] = open()
	}

	if _, err := clients[0].PutIfAbsent(ctx, "head", []byte("base")); err != nil {
		t.Fatal(err)
	}
	for round := range 150 {
		_, v, err := clients[0].Get(ctx, "head")
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
				_, err := clients[i].PutIfVersion(ctx, "head", []byte(fmt.Sprintf("round %d writer %d", round, i)), v)
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
		if wins.Load() != 1 {
			t.Fatalf("round %d: %d of %d writers on version %s won, want exactly 1", round, wins.Load(), racers, v)
		}
		got, _, _ := clients[0].Get(ctx, "head")
		if !strings.HasPrefix(string(got), fmt.Sprintf("round %d writer ", round)) {
			t.Fatalf("round %d: the object holds %q, not a write of this round", round, got)
		}
	}

	for k := range 100 {
		key := fmt.Sprintf("create/%d", k)
		var wins atomic.Int32
		var wg sync.WaitGroup
		start := make(chan struct{})
		for i := range racers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				if _, err := clients[i].PutIfAbsent(ctx, key, []byte{byte(i)}); err == nil {
					wins.Add(1)
				} else if !errors.Is(err, backend.ErrConflict) {
					t.Errorf("key %s creator %d: %v", key, i, err)
				}
			}()
		}
		close(start)
		wg.Wait()
		if wins.Load() != 1 {
			t.Fatalf("key %s: %d of %d creators won, want exactly 1", key, wins.Load(), racers)
		}
		got, _, _ := clients[0].Get(ctx, key)
		if len(got) != 1 {
			t.Fatalf("key %s holds %d bytes", key, len(got))
		}
	}
	t.Logf("150 version races and 100 creation races, %d racers each: exactly one winner every time", racers)
}
