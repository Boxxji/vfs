package blake3

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"math/rand"
	"os"
	"testing"

	zeebo "github.com/zeebo/blake3"
)

// The engine's layout (docs/SPECIFICATION.md of the volume) uses BLAKE3 in four modes:
// the plain hash with 32 bytes of output (chunk ids, shard refs, key ids and the seal
// contexts), the extendable output of the plain hash (the gear table), the keyed hash
// with the key derived from the Volume's seed (the tree boundaries and the extension
// names), and the key derivation mode (that derivation, and the seal tag key). The
// functions below are those derivations written from the text of the layout, over this
// package. Each test holds them to the same derivation over the library the engine
// links today, on random seeds, and the extension names to sixty vectors that another
// BLAKE3 library computed from the text alone. A call site that moves to this package
// gives the same bytes, so no chunk id, shard name, cut or boundary changes.

func uvarint(n uint64) []byte { return binary.AppendUvarint(nil, n) }

// context is the seal context, SPEC 4.2: the hash of the domain string and the framed
// kind and fields.
func contextOf(kind string, fields ...[]byte) []byte {
	h := New()
	h.WriteString("hanzoai/vfs seal context")
	frame := func(p []byte) {
		h.Write(uvarint(uint64(len(p))))
		h.Write(p)
	}
	frame([]byte(kind))
	for _, f := range fields {
		frame(f)
	}
	return h.Sum(nil)
}

func contextReference(kind string, fields ...[]byte) []byte {
	in := []byte("hanzoai/vfs seal context")
	frame := func(p []byte) { in = append(append(in, uvarint(uint64(len(p)))...), p...) }
	frame([]byte(kind))
	for _, f := range fields {
		frame(f)
	}
	d := zeebo.Sum256(in)
	return d[:]
}

// tagOf is the seal tag, SPEC 4.2: the keyed hash of everything before it, under the
// key derived from the secret identity.
func tagOf(identity string, plain []byte) []byte {
	var key [32]byte
	DeriveKey("hanzoai/vfs seal tag", []byte(identity), key[:])
	h := Keyed(&key)
	h.Write(plain)
	return h.Sum(nil)
}

func tagReference(identity string, plain []byte) []byte {
	var key [32]byte
	zeebo.DeriveKey("hanzoai/vfs seal tag", []byte(identity), key[:])
	h, _ := zeebo.NewKeyed(key[:])
	h.Write(plain)
	return h.Sum(nil)
}

// boundary is SPEC 6.3: the first four bytes of the keyed hash of level || key, under
// the key derived from the seed.
func boundary(seed [32]byte, level int, key []byte) [4]byte {
	var k [32]byte
	DeriveKey("hanzoai/vfs tree boundaries", seed[:], k[:])
	h := Keyed(&k)
	h.Write([]byte{byte(level)})
	h.Write(key)
	var sum [32]byte
	h.Sum(sum[:0])
	return [4]byte(sum[:4])
}

func boundaryReference(seed [32]byte, level int, key []byte) [4]byte {
	var k [32]byte
	zeebo.DeriveKey("hanzoai/vfs tree boundaries", seed[:], k[:])
	h, _ := zeebo.NewKeyed(k[:])
	h.Write(append([]byte{byte(level)}, key...))
	return [4]byte(h.Sum(nil)[:4])
}

// extName is SPEC 3.1: the keyed hash, under the key derived from the seed, of the
// length of the name as a uvarint, the name and the caller's key, in hex.
func extName(seed [32]byte, name string, key []byte) string {
	var k [32]byte
	DeriveKey("hanzoai/vfs extension names", seed[:], k[:])
	h := Keyed(&k)
	h.Write(uvarint(uint64(len(name))))
	h.WriteString(name)
	h.Write(key)
	return hex.EncodeToString(h.Sum(nil))
}

// gearTable is SPEC 7.2: the first 2048 bytes of the extendable output over the domain
// string and the seed, as 256 little-endian words.
func gearTable(seed [32]byte) (g [256]uint64) {
	h := New()
	h.WriteString("hanzoai/vfs chunker gear table")
	h.Write(seed[:])
	var raw [256 * 8]byte
	h.Expand(raw[:])
	for i := range g {
		g[i] = binary.LittleEndian.Uint64(raw[i*8:])
	}
	return g
}

func gearReference(seed [32]byte) (g [256]uint64) {
	h := zeebo.New()
	h.WriteString("hanzoai/vfs chunker gear table")
	h.Write(seed[:])
	var raw [256 * 8]byte
	h.Digest().Read(raw[:])
	for i := range g {
		g[i] = binary.LittleEndian.Uint64(raw[i*8:])
	}
	return g
}

func seeds(r *rand.Rand, n int) (out [][32]byte) {
	out = append(out, [32]byte{}, [32]byte{1}, [32]byte{0xff, 0xfe})
	for len(out) < n {
		var s [32]byte
		r.Read(s[:])
		out = append(out, s)
	}
	return out
}

// TestSpecDerivations holds every derivation of the layout to the library the engine
// links, on every path.
func TestSpecDerivations(t *testing.T) {
	r := rand.New(rand.NewSource(13))
	eachPath(t, func(t *testing.T, _ path) {
		for _, seed := range seeds(r, 12) {
			if gearTable(seed) != gearReference(seed) {
				t.Fatalf("gear table of seed %x differs", seed[:4])
			}
			for range 60 {
				key := random(r, r.Intn(48))
				level := r.Intn(5)
				if boundary(seed, level, key) != boundaryReference(seed, level, key) {
					t.Fatalf("boundary of seed %x, level %d, key %x differs", seed[:4], level, key)
				}
			}
			var k [32]byte
			zeebo.DeriveKey("hanzoai/vfs extension names", seed[:], k[:])
			for range 20 {
				name := string(random(r, 1+r.Intn(32)))
				key := random(r, r.Intn(70))
				h, _ := zeebo.NewKeyed(k[:])
				h.Write(uvarint(uint64(len(name))))
				h.WriteString(name)
				h.Write(key)
				if got, want := extName(seed, name, key), hex.EncodeToString(h.Sum(nil)); got != want {
					t.Fatalf("extension name differs: %s, want %s", got, want)
				}
			}
		}
		for range 40 {
			kind := string(random(r, r.Intn(8)))
			fields := [][]byte{random(r, r.Intn(40)), random(r, 16), random(r, 32), random(r, r.Intn(300))}
			if !bytes.Equal(contextOf(kind, fields...), contextReference(kind, fields...)) {
				t.Fatalf("seal context of %q differs", kind)
			}
			identity := string(random(r, 40+r.Intn(60)))
			plain := random(r, r.Intn(70000))
			if !bytes.Equal(tagOf(identity, plain), tagReference(identity, plain)) {
				t.Fatalf("seal tag over %d bytes differs", len(plain))
			}
		}
		// Chunk ids and shard refs are the plain hash of the bytes; key ids are the
		// first 8 bytes of the plain hash of a string.
		for _, n := range []int{0, 1, 4096, 65536, 524288, 4 << 20} {
			b := random(r, n)
			if Sum(b) != zeebo.Sum256(b) {
				t.Fatalf("chunk id of %d bytes differs", n)
			}
		}
	})
}

// TestSpecExtensionNames holds the extension names to sixty vectors that another BLAKE3
// library (lukechampine.com/blake3 v1.4.1) computed from the text of the layout alone.
func TestSpecExtensionNames(t *testing.T) {
	raw, err := os.ReadFile("testdata/extnames.json")
	if err != nil {
		t.Fatal(err)
	}
	var vecs []struct{ Seed, Name, Key, Rest string }
	if err := json.Unmarshal(raw, &vecs); err != nil {
		t.Fatal(err)
	}
	if len(vecs) != 60 {
		t.Fatalf("%d vectors, want 60", len(vecs))
	}
	eachPath(t, func(t *testing.T, _ path) {
		for i, x := range vecs {
			seed, _ := hex.DecodeString(x.Seed)
			key, _ := hex.DecodeString(x.Key)
			if got := extName([32]byte(seed), x.Name, key); got != x.Rest {
				t.Fatalf("vector %d: %s, the independent library says %s", i, got, x.Rest)
			}
		}
	})
}

// TestSpecChunker holds the cut of a window to the text: the gear table of this package
// gives the cuts the layout says, 64 KiB minimum, 512 KiB average, 4 MiB maximum.
func TestSpecChunker(t *testing.T) {
	seed := [32]byte{9, 8, 7}
	g := gearTable(seed)
	maskS := uint64(1<<21-1) << 43
	maskL := uint64(1<<17-1) << 47
	cut := func(g *[256]uint64, b []byte) int {
		if len(b) <= 64<<10 {
			return len(b)
		}
		var fp uint64
		i := 64 << 10
		for ; i < min(512<<10, len(b)); i++ {
			fp = fp<<1 + g[b[i]]
			if fp&maskS == 0 {
				return i + 1
			}
		}
		for ; i < len(b); i++ {
			fp = fp<<1 + g[b[i]]
			if fp&maskL == 0 {
				return i + 1
			}
		}
		return len(b)
	}
	ref := gearReference(seed)
	r := rand.New(rand.NewSource(14))
	for range 12 {
		b := random(r, 64<<10+r.Intn(4<<20-64<<10))
		if got, want := cut(&g, b), cut(&ref, b); got != want {
			t.Fatalf("cut with this package's table = %d, with the library's = %d", got, want)
		}
	}
}
