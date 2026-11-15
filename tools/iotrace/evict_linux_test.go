package main

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"syscall"
	"testing"
	"unsafe"
)

// These tests check what evict asks the kernel for with a stand-in for the
// advice call, because whether a kernel drops the pages, and whether a test
// can see it, depends on the file system underneath (an overlay in a virtual
// machine often does neither). One test then asks the real kernel, but only
// after a control file has shown that this machine can show a difference.

func TestEvictAdvisesEachRegularFileOnce(t *testing.T) {
	dir := t.TempDir()
	for name, size := range map[string]int{"a": 10, "sub/b": 20, "sub/deep/c": 0} {
		p := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, make([]byte, size), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Symlink("a", filepath.Join(dir, "link")); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(dir, "empty"), 0o755); err != nil {
		t.Fatal(err)
	}
	var seen []string
	files, bytes, err := evictWith([]string{dir}, func(f *os.File) error {
		seen = append(seen, f.Name())
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(seen)
	want := []string{filepath.Join(dir, "a"), filepath.Join(dir, "sub/b"), filepath.Join(dir, "sub/deep/c")}
	sort.Strings(want)
	if !reflect.DeepEqual(seen, want) {
		t.Errorf("advised %v, want the three regular files %v (not the link or the directories)", seen, want)
	}
	if files != 3 || bytes != 30 {
		t.Errorf("%d files %d bytes, want 3 and 30", files, bytes)
	}
}

func TestEvictTakesFilesAsRootsAndSkipsWhatIsMissing(t *testing.T) {
	dir := t.TempDir()
	one := filepath.Join(dir, "one")
	if err := os.WriteFile(one, make([]byte, 7), 0o644); err != nil {
		t.Fatal(err)
	}
	var seen []string
	files, bytes, err := evictWith([]string{filepath.Join(dir, "missing"), one}, func(f *os.File) error {
		seen = append(seen, f.Name())
		return nil
	})
	if err != nil || files != 1 || bytes != 7 || !reflect.DeepEqual(seen, []string{one}) {
		t.Errorf("%d files %d bytes %v err %v", files, bytes, seen, err)
	}
}

func TestEvictReturnsWhatTheKernelRefused(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "f"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	refused := errors.New("advice refused")
	files, _, err := evictWith([]string{dir}, func(*os.File) error { return refused })
	if !errors.Is(err, refused) {
		t.Fatalf("error %v, want the advice error", err)
	}
	if files != 0 {
		t.Errorf("%d files counted although the advice failed", files)
	}
}

// resident counts the pages of a file the kernel holds in memory.
func resident(path string, size int) (int, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	m, err := syscall.Mmap(int(f.Fd()), 0, size, syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return 0, err
	}
	defer syscall.Munmap(m)
	vec := make([]byte, size/page)
	if _, _, e := syscall.Syscall(syscall.SYS_MINCORE, uintptr(unsafe.Pointer(&m[0])), uintptr(size), uintptr(unsafe.Pointer(&vec[0]))); e != 0 {
		return 0, e
	}
	n := 0
	for _, v := range vec {
		n += int(v & 1)
	}
	return n, nil
}

// cached writes a file, flushes it and reads it back, and reports how many
// of its pages the kernel then holds.
func cached(t *testing.T, path string, size int) int {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(make([]byte, size)); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.ReadFile(path); err != nil {
		t.Fatal(err)
	}
	n, err := resident(path, size)
	if err != nil {
		t.Skipf("mincore cannot report page residency here: %v", err)
	}
	return n
}

func TestEvictDropsCachedPagesWhereTheKernelHonoursIt(t *testing.T) {
	const size = 4 << 20
	const pages = size / page
	dir := t.TempDir()
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		t.Fatal(err)
	}
	if st.Type == 0x01021994 { // tmpfs keeps its pages: they are the file
		t.Skip("the temporary directory is on tmpfs, whose pages are the file")
	}

	// The control file takes the advice directly. If its pages stay, this
	// machine cannot show an eviction and nothing can be asserted about it.
	control := filepath.Join(dir, "control", "f")
	if got := cached(t, control, size); got != pages {
		t.Skipf("only %d of %d pages of a control file are cached to begin with", got, pages)
	}
	cf, err := os.Open(control)
	if err != nil {
		t.Fatal(err)
	}
	_, _, e := syscall.Syscall6(syscall.SYS_FADVISE64, cf.Fd(), 0, 0, adviseDontNeed, 0, 0)
	cf.Close()
	if e != 0 {
		t.Skipf("posix_fadvise is refused here: %v", e)
	}
	left, err := resident(control, size)
	if err != nil {
		t.Skipf("mincore cannot report page residency here: %v", err)
	}
	if left > pages/10 {
		t.Skipf("posix_fadvise(DONTNEED) leaves %d of %d pages of a control file cached on this file system: "+
			"the kernel here ignores the advice, so an eviction cannot be observed; the tests above check what evict asks for",
			left, pages)
	}

	target := filepath.Join(dir, "target", "f")
	if got := cached(t, target, size); got != pages {
		t.Skipf("only %d of %d pages of the target are cached to begin with", got, pages)
	}
	files, bytes, err := evict([]string{filepath.Dir(target)})
	if err != nil || files != 1 || bytes != size {
		t.Fatalf("evict: %d files %d bytes: %v", files, bytes, err)
	}
	left, err = resident(target, size)
	if err != nil {
		t.Fatal(err)
	}
	if left > pages/10 {
		t.Errorf("evict returned no error, but %d of %d pages are still cached although the control file dropped its pages", left, pages)
	}
}
