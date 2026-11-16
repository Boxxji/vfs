//go:build !linux

package journal

import "os"

func datasync(f *os.File) error { return f.Sync() }

// writeback has no portable form; the journal then relies on Sync alone.
func writeback(*os.File, int64, int64) error { return nil }
