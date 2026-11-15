//go:build !linux

package journal

import "os"

func datasync(f *os.File) error { return f.Sync() }
