//go:build !unix

package journal

import "os"

// lockDirectory: this platform has no advisory file lock; one journal per
// directory is then the caller's to keep.
func lockDirectory(string) (*os.File, error) { return nil, nil }
