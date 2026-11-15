//go:build fuse && (linux || darwin)

package mount_test

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	_ "github.com/hanzoai/sqlite"
	"github.com/luxfi/age"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/pkg/backend"
	_ "github.com/hanzoai/vfs/pkg/backend/file"
	"github.com/hanzoai/vfs/pkg/mount"
)

// needFUSE skips the test where the kernel cannot mount: no /dev/fuse, or no
// helper to unmount with. Everything else runs.
func needFUSE(t *testing.T) {
	t.Helper()
	switch runtime.GOOS {
	case "linux":
		if _, err := os.Stat("/dev/fuse"); err != nil {
			t.Skip("no /dev/fuse on this machine")
		}
		if _, err := exec.LookPath("fusermount3"); err != nil {
			if _, err := exec.LookPath("fusermount"); err != nil {
				t.Skip("no fusermount on this machine")
			}
		}
	case "darwin":
		_, e1 := os.Stat("/Library/Filesystems/macfuse.fs")
		_, e2 := os.Stat("/Library/Application Support/fuse-t")
		if e1 != nil && e2 != nil {
			t.Skip("neither macFUSE nor fuse-t is installed")
		}
	}
}

func unmount(mountpoint string) {
	switch runtime.GOOS {
	case "linux":
		if exec.Command("fusermount3", "-u", mountpoint).Run() != nil {
			_ = exec.Command("fusermount", "-u", mountpoint).Run()
		}
	case "darwin":
		_ = exec.Command("umount", mountpoint).Run()
		_ = exec.Command("diskutil", "unmount", mountpoint).Run()
	}
}

// The server runs in a child process: the test binary run again with the
// arguments "mountchild <store> <mountpoint> <round>" and the key on standard
// input. A process that mounts a Volume must not use files on its own mount: Go
// registers an open file with its event poller, the kernel asks the file system
// for readiness, and the server, which needs that same poller to read its next
// request, never answers. That deadlock is a property of one process playing both
// sides, not of the mount, and it cannot be undone without root. So the test
// process only ever acts as a client.
func TestMountChild(t *testing.T) {
	if flag.Arg(0) != "mountchild" {
		t.Skip("runs only as the server process of the mount tests")
	}
	store, mp := flag.Arg(1), flag.Arg(2)
	round, _ := strconv.ParseUint(flag.Arg(3), 10, 64)
	line, err := bufio.NewReader(os.Stdin).ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	key, err := age.ParseHybridIdentity(strings.TrimSpace(line))
	if err != nil {
		t.Fatal(err)
	}
	be, err := backend.Open(context.Background(), "file://"+store)
	if err != nil {
		t.Fatal(err)
	}
	cfg := vfs.Config{Backend: be, Cipher: vfs.NewAgeCipher(key), Round: round}
	if round == 1 {
		if err := vfs.Create(context.Background(), cfg); err != nil {
			t.Fatal(err)
		}
	}
	vol, err := vfs.Attach(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM)
	defer stop()
	if err := mount.Mount(ctx, vol, mp); err != nil && !strings.Contains(err.Error(), "transport endpoint") {
		t.Fatal(err)
	}
}

// mounted is one mount of a Volume, served by a child process.
type mounted struct {
	cmd  *exec.Cmd
	dir  string
	done chan error
}

// stop asks the server to finish: it unmounts, flushes the Volume and exits.
func (m *mounted) stop(t *testing.T) {
	t.Helper()
	_ = m.cmd.Process.Signal(syscall.SIGTERM)
	select {
	case err := <-m.done:
		if err != nil {
			t.Errorf("the mount server exited: %v", err)
		}
	case <-time.After(30 * time.Second):
		unmount(m.dir)
		_ = m.cmd.Process.Kill()
		t.Fatal("the mount server did not stop")
	}
}

// mountAt starts a server that attaches the Volume at storeDir (creating it at
// round 1) at round and mounts it at mp. The caller stops it.
func mountAt(t *testing.T, storeDir, mp string, key *age.HybridIdentity, round uint64) *mounted {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestMountChild$", "-test.count=1", "mountchild", storeDir, mp, strconv.FormatUint(round, 10))
	cmd.Stdin = strings.NewReader(key.String() + "\n")
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	m := &mounted{cmd: cmd, dir: mp, done: make(chan error, 1)}
	go func() { m.done <- cmd.Wait() }()
	t.Cleanup(func() {
		unmount(mp)
		_ = cmd.Process.Kill()
	})
	deadline := time.Now().Add(20 * time.Second)
	for {
		if out, _ := os.ReadFile("/proc/self/mountinfo"); runtime.GOOS != "linux" || bytes.Contains(out, []byte(mp)) {
			return m
		}
		select {
		case err := <-m.done:
			t.Fatalf("the mount server exited before the mount appeared: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("the mount never appeared")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func newMountpoint(t *testing.T) (store, mp string) {
	tmp := t.TempDir()
	mp = filepath.Join(tmp, "mnt")
	if err := os.MkdirAll(mp, 0o755); err != nil {
		t.Fatal(err)
	}
	return filepath.Join(tmp, "store"), mp
}

// TestFUSEPOSIX drives every operation the mount supports through the kernel,
// and checks that Fsync and Flush reach the store: a second, read-only view of the
// store sees the file before the mount is released.
func TestFUSEPOSIX(t *testing.T) {
	needFUSE(t)
	store, mp := newMountpoint(t)
	key, _ := age.GenerateHybridIdentity()
	m := mountAt(t, store, mp, key, 1)

	if err := os.Mkdir(filepath.Join(mp, "dir"), 0o755); err != nil {
		t.Fatal(err)
	}
	data := bytes.Repeat([]byte("0123456789abcdef"), 1<<16) // 1 MiB, chunked
	path := filepath.Join(mp, "dir", "file.bin")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil { // Fsync: durable in the store on return
		t.Fatal(err)
	}
	be, _ := backend.Open(context.Background(), "file://"+store)
	ro, err := vfs.Open(context.Background(), vfs.Config{Backend: be, Cipher: vfs.NewAgeCipher(key)})
	if err != nil {
		t.Fatal(err)
	}
	in, err := ro.Resolve(context.Background(), "/dir/file.bin")
	if err != nil || in.Size != uint64(len(data)) {
		t.Fatalf("after fsync a second view sees %+v, %v; want %d bytes", in, err, len(data))
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	got, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("read back %d bytes, %v", len(got), err)
	}
	if err := os.Truncate(path, 100); err != nil {
		t.Fatal(err)
	}
	st, err := os.Stat(path)
	if err != nil || st.Size() != 100 || st.IsDir() {
		t.Fatalf("stat after truncate: %v %v", st, err)
	}
	ents, err := os.ReadDir(filepath.Join(mp, "dir"))
	if err != nil || len(ents) != 1 || ents[0].Name() != "file.bin" {
		t.Fatalf("readdir: %v %v", ents, err)
	}
	if err := os.Remove(filepath.Join(mp, "dir")); !errors.Is(err, syscall.ENOTEMPTY) {
		t.Fatalf("rmdir of a non-empty directory: %v, want ENOTEMPTY", err)
	}
	if _, err := os.Stat(filepath.Join(mp, "dir", "none")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stat of absent: %v", err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(mp, "dir")); err != nil {
		t.Fatal(err)
	}
	m.stop(t)
}

// TestFUSESQLite runs a real SQLite database directly on the mount: create,
// insert, close, release the mount, mount again, and the database is whole.
func TestFUSESQLite(t *testing.T) {
	needFUSE(t)
	store, mp := newMountpoint(t)
	key, _ := age.GenerateHybridIdentity()
	dbPath := filepath.Join(mp, "test.db")

	m := mountAt(t, store, mp, key, 1)
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE coins (sym TEXT PRIMARY KEY, name TEXT, decimals INT)`); err != nil {
		t.Fatal(err)
	}
	for i := range 100 {
		if _, err := db.Exec(`INSERT INTO coins VALUES (?, ?, ?)`, fmt.Sprintf("USDC_%d", i), "USD Coin", 6); err != nil {
			t.Fatalf("INSERT %d: %v", i, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	m.stop(t)

	m = mountAt(t, store, mp, key, 2)
	defer m.stop(t)
	db, err = sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var ok string
	if err := db.QueryRow(`PRAGMA integrity_check`).Scan(&ok); err != nil || !strings.EqualFold(ok, "ok") {
		t.Fatalf("integrity_check = %q, %v", ok, err)
	}
	var n int
	if err := db.QueryRow(`SELECT count(*) FROM coins`).Scan(&n); err != nil || n != 100 {
		t.Fatalf("count = %d, %v; want 100", n, err)
	}
	t.Logf("%d rows survived release and remount, integrity_check=%s", n, ok)
}
