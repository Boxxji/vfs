package vfs

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"

	"github.com/luxfi/age"
	"github.com/zeebo/blake3"
)

// Cipher seals and opens every object a Volume writes: chunks, metadata shards
// and the Head. It is the only door to key material: a Volume is handed a Cipher
// and never reads a key from a file or the environment, never stores one, and
// never sees the key itself.
//
// Seal must authenticate as well as encrypt (a tampered object must fail Open)
// and returns the id of the key it used. The Volume writes that id next to the
// sealed bytes, and hands it back to Open, so a Cipher that holds several keys
// can keep reading old objects while it seals new ones under the current key:
// that is rotation, with no bulk re-encryption.
//
// Implementations must be safe for concurrent use.
type Cipher interface {
	Seal(plaintext []byte) (sealed []byte, keyID string, err error)
	Open(sealed []byte, keyID string) (plaintext []byte, err error)
}

// Provider yields the Cipher for a Volume. The agent implements it with Hanzo
// KMS; the command line implements it by reading keys from standard input.
type Provider interface {
	Cipher(ctx context.Context) (Cipher, error)
}

// ErrUnknownKey is returned by Open when an object was sealed under a key the
// Cipher does not hold: the wrong Volume key, or a retired key already dropped.
var ErrUnknownKey = errors.New("vfs: object is sealed under a key this cipher does not hold")

// seal frames a sealed object as [len(keyID) byte][keyID][sealed], which makes
// every object self-describing: no table anywhere records which key sealed what.
func seal(c Cipher, plain []byte) ([]byte, error) {
	sealed, id, err := c.Seal(plain)
	if err != nil {
		return nil, fmt.Errorf("vfs: seal: %w", err)
	}
	if len(id) == 0 || len(id) > 255 {
		return nil, fmt.Errorf("vfs: cipher returned a key id of %d bytes, want 1 to 255", len(id))
	}
	out := make([]byte, 0, 1+len(id)+len(sealed))
	out = append(out, byte(len(id)))
	out = append(out, id...)
	return append(out, sealed...), nil
}

// unseal reverses seal.
func unseal(c Cipher, frame []byte) ([]byte, error) {
	if len(frame) < 1 || len(frame) < 1+int(frame[0]) {
		return nil, fmt.Errorf("%w: truncated frame", ErrCorrupt)
	}
	n := 1 + int(frame[0])
	return c.Open(frame[n:], string(frame[1:n]))
}

// ageCipher is the default Cipher: every object is an age file sealed to a
// hybrid post-quantum recipient (X25519 with ML-KEM-768), so what is stored today
// stays confidential against a future quantum adversary who recorded it.
type ageCipher struct {
	current age.Recipient
	curID   string
	ids     map[string]age.Identity
}

// NewAgeCipher returns the default Cipher. New objects are sealed to current;
// objects sealed to any of retired still open. Each key is an independent
// identity, so a Volume's key is per Volume by construction: nothing is
// shared across Volumes, and dedupe never crosses one.
func NewAgeCipher(current *age.HybridIdentity, retired ...*age.HybridIdentity) Cipher {
	c := &ageCipher{current: current.Recipient(), curID: ageKeyID(current), ids: map[string]age.Identity{}}
	for _, k := range append([]*age.HybridIdentity{current}, retired...) {
		c.ids[ageKeyID(k)] = k
	}
	return c
}

// ageKeyID is the first 8 bytes of blake3 over the public recipient string: it
// names a key without revealing anything secret.
func ageKeyID(k *age.HybridIdentity) string {
	sum := blake3.Sum256([]byte(k.Recipient().String()))
	return hex.EncodeToString(sum[:8])
}

func (c *ageCipher) Seal(plain []byte) ([]byte, string, error) {
	var buf bytes.Buffer
	w, err := age.Encrypt(&buf, c.current)
	if err != nil {
		return nil, "", err
	}
	if _, err := w.Write(plain); err != nil {
		return nil, "", err
	}
	if err := w.Close(); err != nil {
		return nil, "", err
	}
	return buf.Bytes(), c.curID, nil
}

func (c *ageCipher) Open(sealed []byte, keyID string) ([]byte, error) {
	id, ok := c.ids[keyID]
	if !ok {
		return nil, fmt.Errorf("%w: key %s", ErrUnknownKey, keyID)
	}
	r, err := age.Decrypt(bytes.NewReader(sealed), id)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrCorrupt, err)
	}
	plain, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrCorrupt, err)
	}
	return plain, nil
}
