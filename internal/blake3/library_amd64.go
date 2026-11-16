package blake3

import zeebo "github.com/zeebo/blake3"

// On amd64 the one-shot and streaming hashes are the library the engine already
// links: its AVX2 code is faster than the tree over the guts kernel (docs/hashing.md
// has the measurements). Parallel keeps the tree, which that library cannot offer.
// Sum returns the digest of p. It is a plain call, so that the compiler can inline
// it: the library's speed depends on the stack it runs on, and through a function
// variable it measures 15 to 20 percent slower.
func Sum(p []byte) [Size]byte { return zeebo.Sum256(p) }

func init() {
	stream = func(key *[32]byte) hasher {
		if key == nil {
			return library{zeebo.New()}
		}
		h, err := zeebo.NewKeyed(key[:])
		if err != nil {
			panic(err) // a 32-byte key is the only length NewKeyed takes
		}
		return library{h}
	}
}

// library adapts the library's hasher to the interface the tree's streamer has.
type library struct{ *zeebo.Hasher }

func (l library) WriteByte(c byte) error {
	l.Hasher.Write([]byte{c})
	return nil
}

func (l library) Expand(out []byte) {
	l.Digest().Read(out)
}
