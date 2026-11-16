package vfs_test

import (
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/hanzoai/vfs/pkg/backend"
	"github.com/hanzoai/vfs/pkg/backend/s3"
)

// TestBackendBodyIsBounded: the Volume reads head and shards with Get, which
// takes the largest body the caller will accept. A hostile or broken store that answers a read of
// "head" with a 256 MiB body makes the reader allocate it all. Object sizes are
// known (a head is under 2 KiB, a shard under 600 KiB, a chunk's range is exact),
// so the reader can bound the body.
func TestBackendBodyIsBounded(t *testing.T) {
	const size = 256 << 20
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Etag", `"d41d8cd98f00b204e9800998ecf8427e"`)
		w.Header().Set("Content-Length", "268435456")
		w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
		w.WriteHeader(200)
		buf := make([]byte, 1<<20)
		for range 256 {
			if _, err := w.Write(buf); err != nil {
				return
			}
		}
	}))
	srv.Listener, _ = net.Listen("tcp", "127.0.0.1:0")
	srv.Start()
	defer srv.Close()
	be, err := s3.New(s3.Config{Endpoint: strings.TrimPrefix(srv.URL, "http://"), Insecure: true, Region: "us-east-1", Bucket: "bucket", Prefix: "p", AccessKey: "k", SecretKey: "s"})
	if err != nil {
		t.Fatal(err)
	}
	var m0, m1 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m0)
	b, _, err := be.Get(ctx, "head", 64<<10)
	runtime.ReadMemStats(&m1)
	t.Logf("Get(head) returned %d bytes, err=%v, allocated %d MiB", len(b), err, (m1.TotalAlloc-m0.TotalAlloc)>>20)
	if !errors.Is(err, backend.ErrTooLarge) || len(b) != 0 {
		t.Fatalf("Get of a 256 MiB body with a 64 KiB bound = %d bytes, %v; want ErrTooLarge", len(b), err)
	}
	if got := (m1.TotalAlloc - m0.TotalAlloc) >> 20; got > 8 {
		t.Fatalf("a bounded Get allocated %d MiB for a 64 KiB bound", got)
	}
}
