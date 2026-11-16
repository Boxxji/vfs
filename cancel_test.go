package vfs_test

// A request cancelled while the S3 client is mid-flight must come
// back as context.Canceled (EINTR on a mount), never as another error (EIO) and
// never as success with wrong data.

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/internal/storetest"
	s3be "github.com/hanzoai/vfs/pkg/backend/s3"
)

func TestCancelledS3RequestsAreNotFaults(t *testing.T) {
	real := storetest.RealS3(t)
	prefix := real.Prefix("cancel")
	mk := func() *s3be.Config {
		return &s3be.Config{Endpoint: real.Endpoint, Insecure: true, Region: "us-east-1", Bucket: real.Bucket, Prefix: prefix, AccessKey: real.AccessKey, SecretKey: real.SecretKey}
	}
	be, err := s3be.New(*mk())
	if err != nil {
		t.Fatal(err)
	}
	key := newKey(t)
	c := vfs.Hybrid(key)
	if err := vfs.Create(ctx, cfg(be, c, 0)); err != nil {
		t.Fatal(err)
	}
	v, err := vfs.Attach(ctx, cfg(be, c, 1))
	if err != nil {
		t.Fatal(err)
	}
	dir, _ := v.Mkdir(ctx, vfs.RootInode, "d", 0o755)
	for i := range 3000 {
		write(t, v, dir.ID, fmt.Sprintf("f%05d", i), content(uint64(i), 100+i%3000))
	}
	big := content(77, 6<<20)
	bigIn := write(t, v, vfs.RootInode, "big", big)
	flush(t, v)

	var mu sync.Mutex
	kinds := map[string]int{}
	note := func(op string, err error) {
		mu.Lock()
		defer mu.Unlock()
		switch {
		case err == nil:
			kinds[op+": ok"]++
		case errors.Is(err, context.Canceled):
			kinds[op+": canceled"]++
		case errors.Is(err, context.DeadlineExceeded):
			kinds[op+": deadline"]++
		default:
			kinds[fmt.Sprintf("%s: OTHER %.120v", op, err)]++
		}
	}
	rng := rand.New(rand.NewPCG(5, 5))
	var wg sync.WaitGroup
	for round := range 120 {
		be2, err := s3be.New(*mk())
		if err != nil {
			t.Fatal(err)
		}
		cold, err := vfs.Open(ctx, cfg(be2, c, 0))
		if err != nil {
			t.Fatal(err)
		}
		delay := time.Duration(rng.IntN(4000)) * time.Microsecond
		cctx, cancel := context.WithCancel(ctx)
		time.AfterFunc(delay, cancel)
		wg.Add(1)
		go func() {
			defer wg.Done()
			switch round % 4 {
			case 0:
				_, err := cold.Lookup(cctx, vfs.RootInode, "d")
				note("lookup", err)
			case 1:
				_, err := cold.ReadDir(cctx, dir.ID)
				note("readdir", err)
			case 2:
				in, err := cold.Lookup(cctx, dir.ID, "f01234")
				if err == nil {
					_, err = cold.Stat(cctx, in.ID)
				}
				note("lookup+stat", err)
			case 3:
				f, err := cold.Open(ctx, bigIn.ID)
				if err != nil {
					note("open", err)
					return
				}
				buf := make([]byte, 1<<20)
				_, err = f.ReadAt(cctx, buf, 2<<20)
				note("read", err)
				if err == nil {
					want := big[2<<20 : 3<<20]
					if string(buf) != string(want) {
						note("read", errors.New("WRONG DATA"))
					}
				}
			}
		}()
		wg.Wait()
		cancel()
	}
	var keys []string
	for k := range kinds {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	other := 0
	for _, k := range keys {
		t.Logf("%5d  %s", kinds[k], k)
		if len(k) > 0 && contains(k, "OTHER") {
			other += kinds[k]
		}
	}
	if other > 0 {
		t.Errorf("%d cancelled requests came back as something other than context.Canceled", other)
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
