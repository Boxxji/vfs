package vfs_test

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/luxfi/age"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/internal/storetest"
	"github.com/hanzoai/vfs/pkg/backend/s3"
)

type killArgs struct {
	Endpoint, Bucket, Access, Secret, Prefix, Key string
	Round                                         uint64
}

// numberedFile is the deterministic content of file number i.
func numberedFile(i int) []byte {
	r := rand.New(rand.NewPCG(uint64(i), 4242))
	var n int
	switch i % 5 {
	case 0:
		n = r.IntN(2000)
	case 1:
		n = 15<<10 + r.IntN(3<<10)
	case 2:
		n = 100<<10 + r.IntN(900<<10)
	case 3:
		n = 1<<20 + r.IntN(5<<20)
	default:
		n = 20<<10 + r.IntN(40<<10)
	}
	b := make([]byte, n)
	for j := range b {
		b[j] = byte(r.Uint32())
	}
	return b
}

// TestKillChild is the process TestKillOverRealServer kills at a random
// moment. It reads its arguments, as JSON, from standard input (never the
// environment), attaches at its round and writes numbered files, flushing every
// few, printing the highest file number whose Flush returned.
func TestKillChild(t *testing.T) {
	if flag.Arg(0) != "killchild" {
		t.Skip("child of TestKillOverRealServer")
	}
	line, err := bufio.NewReader(os.Stdin).ReadBytes('\n')
	if err != nil {
		t.Fatal(err)
	}
	var a killArgs
	if err := json.Unmarshal(line, &a); err != nil {
		t.Fatal(err)
	}
	k, _ := age.ParseHybridIdentity(a.Key)
	be, err := s3.New(s3.Config{Endpoint: a.Endpoint, Insecure: true, Region: "us-east-1", Bucket: a.Bucket, Prefix: a.Prefix, AccessKey: a.Access, SecretKey: a.Secret})
	if err != nil {
		t.Fatal(err)
	}
	v, err := vfs.Attach(ctx, cfg(be, vfs.Hybrid(k), a.Round))
	if err != nil {
		t.Fatal(err)
	}
	ents, _ := v.ReadDir(ctx, vfs.RootInode)
	next := len(ents)
	for {
		first := next
		for range 1 + next%4 {
			write(t, v, vfs.RootInode, fmt.Sprintf("f%06d", next), numberedFile(next))
			next++
		}
		if err := v.Flush(ctx); err != nil {
			t.Fatal(err)
		}
		fmt.Println("COMMITTED", next, first)
	}
}

// TestKillOverRealServer kills the writer at a random moment, 25 times in a
// row, against the real server over HTTP (so the kill lands inside an upload as
// often as between them), and after each kill checks from a fresh process that
// the tree is whole: every file present is byte-identical to its generator, and
// every file whose Flush was acknowledged is present.
func TestKillOverRealServer(t *testing.T) {
	if testing.Short() {
		t.Skip("kills")
	}
	srv := storetest.RealS3(t)
	prefix := srv.Prefix("redkill")
	key := newKey(t)
	c := vfs.Hybrid(key)
	be := openOn(t, srv, prefix)
	if err := vfs.Create(ctx, cfg(be, c, 0)); err != nil {
		t.Fatal(err)
	}
	r := rand.New(rand.NewPCG(11, 11))
	acked := 0
	for round := uint64(1); round <= 15; round++ {
		args, _ := json.Marshal(killArgs{srv.Endpoint, srv.Bucket, srv.AccessKey, srv.SecretKey, prefix, key.String(), round})
		cmd := exec.Command(os.Args[0], "-test.run=^TestKillChild$", "-test.count=1", "killchild")
		cmd.Stdin = bytes.NewReader(append(args, '\n'))
		out, _ := cmd.StdoutPipe()
		cmd.Stderr = os.Stderr
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		lines := make(chan string, 1000)
		go func() {
			sc := bufio.NewScanner(out)
			for sc.Scan() {
				lines <- sc.Text()
			}
			close(lines)
		}()
		time.Sleep(time.Duration(300+r.IntN(1500)) * time.Millisecond)
		_ = cmd.Process.Signal(syscall.SIGKILL)
		_ = cmd.Wait()
		for l := range lines {
			if f := strings.Fields(l); len(f) == 3 && f[0] == "COMMITTED" {
				n, _ := strconv.Atoi(f[1])
				acked = max(acked, n)
			}
		}
		ro, err := vfs.Open(ctx, cfg(openOn(t, srv, prefix), c, 0))
		if err != nil {
			t.Fatalf("round %d: open after kill: %v", round, err)
		}
		ents, err := ro.ReadDir(ctx, vfs.RootInode)
		if err != nil {
			t.Fatal(err)
		}
		if len(ents) < acked {
			t.Fatalf("round %d: %d files present, %d acknowledged", round, len(ents), acked)
		}
		for i, e := range ents {
			if e.Name != fmt.Sprintf("f%06d", i) {
				t.Fatalf("round %d: entry %d is %q", round, i, e.Name)
			}
			f, _ := ro.Open(ctx, e.ID)
			in, _ := ro.Stat(ctx, e.ID)
			buf := make([]byte, in.Size)
			if _, err := f.ReadAt(ctx, buf, 0); err != nil && in.Size > 0 && err.Error() != "EOF" {
				t.Fatalf("round %d file %d: %v", round, i, err)
			}
			if !bytes.Equal(buf, numberedFile(i)) {
				t.Fatalf("round %d: file %d torn or wrong (%d bytes)", round, i, len(buf))
			}
			f.Close()
		}
		t.Logf("round %d: killed; %d files present (>= %d acknowledged), all byte-exact", round, len(ents), acked)
	}
}
