package main

import (
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
)

const adviseDontNeed = 4 // POSIX_FADV_DONTNEED

// evict drops the clean page-cache pages of every regular file under the
// roots. It needs no privilege: the kernel honours the advice for any
// file the caller can open. Dirty pages stay until written back, so the
// caller syncs first. It returns the number of files and bytes dropped.
func evict(roots []string) (files int, bytes int64, err error) {
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
			if st, serr := f.Stat(); serr == nil {
				bytes += st.Size()
			}
			if _, _, e := syscall.Syscall6(syscall.SYS_FADVISE64, f.Fd(), 0, 0, adviseDontNeed, 0, 0); e == 0 {
				files++
			}
			return nil
		})
		if err != nil {
			return files, bytes, err
		}
	}
	return files, bytes, nil
}
