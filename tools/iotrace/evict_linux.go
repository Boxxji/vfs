package main

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
)

const adviseDontNeed = 4 // POSIX_FADV_DONTNEED

// advise tells the kernel that the clean pages of f are no longer wanted.
func advise(f *os.File) error {
	if _, _, e := syscall.Syscall6(syscall.SYS_FADVISE64, f.Fd(), 0, 0, adviseDontNeed, 0, 0); e != 0 {
		return e
	}
	return nil
}

// evict drops the clean page-cache pages of every regular file under the
// roots. It needs no privilege: the kernel honours the advice for any
// file the caller can open. Dirty pages stay until written back, so it
// syncs first. It returns the number of files advised and their bytes.
func evict(roots []string) (files int, bytes int64, err error) {
	return evictWith(roots, advise)
}

// evictWith is evict with the advice call given. A root that does not
// exist and a file that cannot be opened are skipped; an advice call
// that fails stops the walk and is returned with the path it failed on.
func evictWith(roots []string, advise func(*os.File) error) (files int, bytes int64, err error) {
	syscall.Sync()
	for _, root := range roots {
		err = filepath.WalkDir(root, func(p string, d fs.DirEntry, werr error) error {
			if werr != nil || !d.Type().IsRegular() {
				return nil
			}
			f, oerr := os.Open(p)
			if oerr != nil {
				return nil
			}
			defer f.Close()
			if aerr := advise(f); aerr != nil {
				return fmt.Errorf("evict %s: %w", p, aerr)
			}
			files++
			if st, serr := f.Stat(); serr == nil {
				bytes += st.Size()
			}
			return nil
		})
		if err != nil {
			return files, bytes, err
		}
	}
	return files, bytes, nil
}
