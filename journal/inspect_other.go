//go:build !linux

package journal

// inspect: the file system type is not read on this platform.
func inspect(string) ([]string, error) { return nil, nil }
