package storetest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	hs3 "github.com/hanzos3/go"
	"github.com/hanzos3/go/pkg/credentials"
)

// Real is a running Hanzo S3 server (github.com/hanzoai/s3, its own-storage mode:
// master, volume server, filer and S3 API in one process) started for the tests.
type Real struct {
	Endpoint  string
	Bucket    string
	AccessKey string
	SecretKey string

	cmd  *exec.Cmd
	dir  string
	next atomic.Int64
}

var (
	realOnce sync.Once
	real     *Real
	realErr  error
)

// HaveReal reports whether an `s3` binary is on PATH, so a test can add the real
// server to a list of stores without skipping the whole test when it is absent.
func HaveReal() bool {
	_, err := exec.LookPath("s3")
	return err == nil
}

// RealS3 returns a server for the life of the test process, started on the first
// call, or skips the test if no `s3` binary is on PATH. Build it from
// github.com/hanzoai/s3 (`go build -o s3 ./s3`) and put it on PATH to run these
// tests; nothing is faked. The server keeps its data in a temp dir, listens only on
// 127.0.0.1, and uses a key pair generated for this process and never written
// anywhere else. StopReal stops it.
func RealS3(t *testing.T) *Real {
	t.Helper()
	bin, err := exec.LookPath("s3")
	if err != nil {
		t.Skip("no s3 binary on PATH: build github.com/hanzoai/s3 to run the real-server tests")
	}
	realOnce.Do(func() { real, realErr = startReal(bin) })
	if realErr != nil {
		t.Fatalf("start the s3 server: %v", realErr)
	}
	return real
}

// Prefix returns a key prefix no other caller of this server has used.
func (r *Real) Prefix(name string) string { return fmt.Sprintf("%s-%d", name, r.next.Add(1)) }

// StopReal stops the server if one was started. Call it from TestMain.
func StopReal() {
	if real == nil || real.cmd == nil {
		return
	}
	_ = real.cmd.Process.Kill()
	_ = real.cmd.Wait()
	_ = os.RemoveAll(real.dir)
}

// outlive stops the server when this process ends however it ends: a test binary killed
// by a timeout or a signal runs no cleanup, and the server it started would run on. A
// watcher in a session of its own notices that the process is gone and kills the
// server. It is best effort: without a shell there is no watcher, and StopReal is all.
func outlive(server int) {
	const watch = `while kill -0 "$1" 2>/dev/null; do sleep 1; done; kill -9 "$2" 2>/dev/null`
	w := exec.Command("sh", "-c", watch, "watch", strconv.Itoa(os.Getpid()), strconv.Itoa(server))
	w.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if w.Start() == nil {
		go func() { _ = w.Wait() }()
	}
}

func randHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// freePorts returns n ports p such that p and p+10000 are both free: the server
// puts its gRPC listener 10000 above each HTTP port.
func freePorts(n int) ([]int, error) {
	var out []int
	for len(out) < n {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return nil, err
		}
		p := l.Addr().(*net.TCPAddr).Port
		l.Close()
		if p > 50000 {
			continue
		}
		g, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", p+10000))
		if err != nil {
			continue
		}
		g.Close()
		out = append(out, p)
	}
	return out, nil
}

func startReal(bin string) (*Real, error) {
	dir, err := os.MkdirTemp("", "vfs-s3-")
	if err != nil {
		return nil, err
	}
	r := &Real{Bucket: "disk", AccessKey: randHex(10), SecretKey: randHex(20), dir: dir}
	conf, _ := json.Marshal(map[string]any{"identities": []any{map[string]any{
		"name":        "vfs",
		"credentials": []any{map[string]string{"accessKey": r.AccessKey, "secretKey": r.SecretKey}},
		"actions":     []string{"Admin", "Read", "Write", "List", "Tagging"},
	}}})
	confPath := filepath.Join(dir, "s3.json")
	if err := os.WriteFile(confPath, conf, 0o600); err != nil {
		return nil, err
	}
	ports, err := freePorts(4)
	if err != nil {
		return nil, err
	}
	r.Endpoint = fmt.Sprintf("127.0.0.1:%d", ports[3])
	data := filepath.Join(dir, "data")
	if err := os.MkdirAll(data, 0o755); err != nil {
		return nil, err
	}
	r.cmd = exec.Command(bin, "server", "-dir="+data, "-ip=127.0.0.1", "-ip.bind=127.0.0.1",
		fmt.Sprintf("-master.port=%d", ports[0]), fmt.Sprintf("-volume.port=%d", ports[1]), fmt.Sprintf("-filer.port=%d", ports[2]),
		"-s3", fmt.Sprintf("-s3.port=%d", ports[3]), "-s3.config="+confPath,
		"-master.volumeSizeLimitMB=64", "-volume.max=128", "-s3.allowDeleteBucketNotEmpty")
	r.cmd.Dir = dir
	r.cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + dir}
	if err := r.cmd.Start(); err != nil {
		return nil, err
	}
	outlive(r.cmd.Process.Pid)

	cli, err := hs3.New(r.Endpoint, &hs3.Options{Creds: credentials.NewStaticV4(r.AccessKey, r.SecretKey, ""), Region: "us-east-1"})
	if err != nil {
		return nil, err
	}
	// The server reports ready when it can make a bucket; that takes a few seconds
	// while the master elects itself and the first volume is grown.
	deadline := time.Now().Add(120 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err = cli.MakeBucket(ctx, r.Bucket, hs3.MakeBucketOptions{Region: "us-east-1"})
		cancel()
		if err == nil || hs3.ToErrorResponse(err).Code == "BucketAlreadyOwnedByYou" {
			return r, nil
		}
		if time.Now().After(deadline) {
			_ = r.cmd.Process.Kill()
			return nil, fmt.Errorf("no bucket after 120s: %w", err)
		}
		time.Sleep(500 * time.Millisecond)
	}
}
