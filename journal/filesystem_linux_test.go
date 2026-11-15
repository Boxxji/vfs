package journal_test

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/hanzoai/vfs/journal"
)

// The journal promises records are durable on the local disk's media. On tmpfs
// fdatasync is a no-op, so the promise would be false: Open refuses a volatile
// file system.
func TestOpenRefusesMemory(t *testing.T) {
	var dir string
	for _, c := range []string{"/dev/shm", "/run", os.TempDir()} {
		var st syscall.Statfs_t
		if syscall.Statfs(c, &st) == nil && uint32(st.Type) == 0x01021994 {
			dir = c
			break
		}
	}
	if dir == "" {
		t.Fatal("no tmpfs found to test against (looked in /dev/shm, /run and the temporary directory)")
	}
	dir, err := os.MkdirTemp(dir, "journal-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	j, err := journal.Open(dir, journal.Options{Capacity: 4 * 65536, Segment: 65536})
	if err == nil {
		j.Close()
		t.Fatal("Open accepted a tmpfs directory: Sync would return with nothing on stable storage")
	}
	if !errors.Is(err, journal.ErrVolatile) {
		t.Fatalf("Open on tmpfs: %v, want ErrVolatile", err)
	}
	if _, err := journal.Files(dir); !errors.Is(err, journal.ErrVolatile) {
		t.Fatalf("Files on tmpfs: %v, want ErrVolatile", err)
	}
}

// The file system types: memory is refused, layered and remote ones are warned
// about, a disk file system says nothing.
func TestFileSystemTypes(t *testing.T) {
	for _, c := range []struct {
		name  string
		magic uint32
		err   error
		warn  string
	}{
		{"tmpfs", 0x01021994, journal.ErrVolatile, ""},
		{"ramfs", 0x858458f6, journal.ErrVolatile, ""},
		{"overlay", 0x794c7630, nil, "overlay"},
		{"fuse", 0x65735546, nil, "FUSE"},
		{"9p", 0x01021997, nil, "9p"},
		{"nfs", 0x6969, nil, "network"},
		{"ext4", 0xef53, nil, ""},
		{"xfs", 0x58465342, nil, ""},
	} {
		warnings, err := journal.Classify(c.magic, "/x")
		if !errors.Is(err, c.err) {
			t.Errorf("%s: error %v, want %v", c.name, err, c.err)
		}
		switch {
		case c.warn == "" && len(warnings) != 0:
			t.Errorf("%s: warnings %v", c.name, warnings)
		case c.warn != "" && (len(warnings) != 1 || !strings.Contains(warnings[0], c.warn)):
			t.Errorf("%s: warnings %v, want one about %s", c.name, warnings, c.warn)
		}
	}
}

// The segments hold every write of the volume in the clear (the Volume seals
// what it appends); no other user of the node may read them.
func TestFilesAreNotReadableByOthers(t *testing.T) {
	dir := filepath.Join(scratch(t), "journal")
	j, err := journal.Open(dir, journal.Options{Capacity: 4 * 65536, Segment: 65536})
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	mustSync(t, j, mustAppend(t, j, []byte("secret")))
	names, _ := filepath.Glob(filepath.Join(dir, "*"))
	if len(names) == 0 {
		t.Fatal("no segment files")
	}
	for _, p := range append(names, dir) {
		info, err := os.Stat(p)
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm()&0o077 != 0 {
			t.Errorf("%s has mode %v; any local user can read the node's journal", p, info.Mode().Perm())
		}
	}
}
