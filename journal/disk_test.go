package journal_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/hanzoai/vfs/journal"
)

// The operating system's Disk: a created file is whole (every byte written),
// survives as one file, and can be written, read, synced and removed.
func TestFilesDisk(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "nested", "journal")
	d, err := journal.Files(dir)
	if err != nil {
		t.Fatal(err)
	}
	f, err := d.Create("000000.seg", 3<<20+17)
	if err != nil {
		t.Fatal(err)
	}
	if f.Size() != 3<<20+17 {
		t.Fatalf("size %d", f.Size())
	}
	info, err := os.Stat(filepath.Join(dir, "000000.seg"))
	if err != nil || info.Size() != f.Size() {
		t.Fatalf("on disk: %v %v", info, err)
	}
	whole := make([]byte, f.Size())
	if _, err := f.ReadAt(whole, 0); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(whole, make([]byte, len(whole))) {
		t.Fatal("a created file is not zero")
	}
	if _, err := f.WriteAt([]byte("hello"), 1<<20); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	f.Close()

	g, err := d.Open("000000.seg")
	if err != nil {
		t.Fatal(err)
	}
	got := make([]byte, 5)
	if _, err := g.ReadAt(got, 1<<20); err != nil || string(got) != "hello" {
		t.Fatalf("read back %q %v", got, err)
	}
	g.Close()

	names, err := d.List()
	if err != nil || len(names) != 1 || names[0] != "000000.seg" {
		t.Fatalf("List: %v %v", names, err)
	}
	if err := d.Remove("000000.seg"); err != nil {
		t.Fatal(err)
	}
	if names, _ := d.List(); len(names) != 0 {
		t.Fatalf("List after Remove: %v", names)
	}
	if _, err := d.Open("000000.seg"); err == nil {
		t.Fatal("opened a removed file")
	}
}

// A leftover from a crash during Create is not a segment, and is removed.
func TestFilesDiskRemovesLeftovers(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "000007.seg.tmp"), make([]byte, 100), 0o644); err != nil {
		t.Fatal(err)
	}
	j, err := journal.Open(dir, journal.Options{Capacity: 4 * 4096, Segment: 4096})
	if err != nil {
		t.Fatal(err)
	}
	mustSync(t, j, mustAppend(t, j, []byte("x")))
	j.Close()
	if _, err := os.Stat(filepath.Join(dir, "000007.seg.tmp")); !os.IsNotExist(err) {
		t.Fatalf("the leftover is still there: %v", err)
	}
}
