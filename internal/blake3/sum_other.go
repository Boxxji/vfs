//go:build !amd64

package blake3

// Sum returns the digest of p.
func Sum(p []byte) [Size]byte { return canon(p) }
