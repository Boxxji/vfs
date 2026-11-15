package main

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"unsafe"
)

// resident counts the pages of a file the kernel holds in memory.
func resident(t *testing.T, path string, size int) int {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	m, err := syscall.Mmap(int(f.Fd()), 0, size, syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		t.Fatal(err)
	}
	defer syscall.Munmap(m)
	vec := make([]byte, size/page)
	if _, _, e := syscall.Syscall(syscall.SYS_MINCORE, uintptr(unsafe.Pointer(&m[0])), uintptr(size), uintptr(unsafe.Pointer(&vec[0]))); e != 0 {
		t.Fatal(e)
	}
	n := 0
	for _, v := range vec {
		n += int(v & 1)
	}
	return n
}

func TestEvictDropsCachedPages(t *testing.T) {
	dir := t.TempDir()
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		t.Fatal(err)
	}
	if st.Type == 0x01021994 { // tmpfs keeps its pages: they are the file
		t.Skip("temporary directory is on tmpfs")
	}
	const size = 4 << 20
	path := filepath.Join(dir, "f")
	if err := os.WriteFile(path, make([]byte, size), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := os.ReadFile(path); err != nil {
		t.Fatal(err)
	}
	if got := resident(t, path, size); got != size/page {
		t.Skipf("only %d of %d pages cached to begin with", got, size/page)
	}
	files, bytes, err := evict([]string{dir})
	if err != nil || files != 1 || bytes != size {
		t.Fatalf("evict: %d files %d bytes %v", files, bytes, err)
	}
	if got := resident(t, path, size); got > size/page/10 {
		t.Errorf("%d of %d pages still cached", got, size/page)
	}
}
