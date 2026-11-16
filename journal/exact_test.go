package journal_test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/hanzoai/vfs/journal"
	"github.com/hanzoai/vfs/journal/crash"
)

// A foreign file named like a segment whose bytes 4..8 are zeros and parts of
// the magic: what Open does with it. Report the size cases: a file of segment
// size is overwritten, any other size is DELETED.
func TestForeignLookingLikeATornMagic(t *testing.T) {
	for _, tc := range []struct {
		name string
		m    []byte
		size int
	}{
		{"zeros/segment-size", []byte{0, 0, 0, 0}, 4096},
		{"J000/segment-size", []byte{'J', 0, 0, 0}, 4096},
		{"0R00/segment-size", []byte{0, 'R', 0, 0}, 4096},
		{"JR00/segment-size", []byte{'J', 'R', 0, 0}, 4096},
		{"J0NL/segment-size", []byte{'J', 0, 'N', 'L'}, 4096},
		{"zeros/other-size", []byte{0, 0, 0, 0}, 5000},
		{"JRN0/other-size", []byte{'J', 'R', 'N', 0}, 5000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := crash.New()
			content := bytes.Repeat([]byte("precious"), tc.size/8+1)[:tc.size]
			copy(content[4:8], tc.m)
			put(t, d, "000000.seg", int64(tc.size), content)
			j, err := journal.Open("", small(d, newClock(), 0))
			if err != nil {
				t.Fatalf("a file that looks like a torn header was refused: %v", err)
			}
			closeWithin(t, j)
			// Free: it is reused (kept here, until the next record is placed in it)
			// when it has a segment's size, and deleted when it has not.
			_, err = d.Open("000000.seg")
			if kept := err == nil; kept != (tc.size == 4096) {
				t.Fatalf("size %d: kept %v; a free file is kept if it is a segment's size and deleted if not", tc.size, kept)
			}
		})
	}
}

func TestMagicRuleExhaustive(t *testing.T) {
	m := []byte("JRNL")
	for place := 0; place < 4; place++ {
		for _, v := range []int{0, int(m[place]), 1, 0x20, 0x4a, 0x7f, 0xff, int(m[(place+1)%4])} {
			d := crash.New()
			content := bytes.Repeat([]byte{0x5a}, 4096)
			copy(content[4:8], []byte{0, 0, 0, 0})
			content[4+place] = byte(v)
			put(t, d, "000000.seg", 4096, content)
			j, err := journal.Open("", small(d, newClock(), 0))
			free := err == nil
			if free {
				closeWithin(t, j)
			} else if !errors.Is(err, journal.ErrFormat) {
				t.Fatalf("place %d value %#x: %v", place, v, err)
			}
			want := v == 0 || v == int(m[place])
			if free != want {
				t.Errorf("place %d value %#x: free=%v, want %v", place, v, free, want)
			}
		}
	}
	// two places at once: one torn-looking byte and one foreign
	for a := 0; a < 4; a++ {
		for b := 0; b < 4; b++ {
			if a == b {
				continue
			}
			d := crash.New()
			content := bytes.Repeat([]byte{0x5a}, 4096)
			copy(content[4:8], []byte{0, 0, 0, 0})
			content[4+a] = m[a]
			content[4+b] = 0xEE
			put(t, d, "000000.seg", 4096, content)
			if j, err := journal.Open("", small(d, newClock(), 0)); err == nil {
				closeWithin(t, j)
				t.Errorf("bytes %d magic and %d foreign was taken for free", a, b)
			}
		}
	}
}
