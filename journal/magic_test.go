package journal_test

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"

	"github.com/hanzoai/vfs/journal"
	"github.com/hanzoai/vfs/journal/crash"
)

func put(t testing.TB, d *crash.Disk, name string, size int64, content []byte) {
	t.Helper()
	f, err := d.Create(name, size)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt(content, 0); err != nil {
		t.Fatal(err)
	}
	f.Sync()
}

// oldHeader is a header of a layout without the magic: checksum, first, life,
// lineage, in 36 bytes.
func oldHeader(first, life uint64) []byte {
	h := make([]byte, 36)
	binary.LittleEndian.PutUint64(h[4:], first)
	binary.LittleEndian.PutUint64(h[12:], life)
	copy(h[20:], bytes.Repeat([]byte{7}, 16))
	binary.LittleEndian.PutUint32(h, crc32.Checksum(h[4:], crc32.MakeTable(crc32.Castagnoli)))
	return h
}

// A foreign file named like a segment is refused, whatever it holds, and the
// refusal writes nothing: not a byte, not a sync, not a create, not a remove.
func TestForeignFilesAreRefusedAndUntouched(t *testing.T) {
	rnd := make([]byte, 4096)
	rand.Read(rnd)
	cases := map[string][]byte{
		"random": rnd,
		"ones":   bytes.Repeat([]byte{0xff}, 4096),
		"text":   append([]byte("SQLite format 3\x00"), make([]byte, 4096-16)...),
		"binary": append([]byte{0x7f, 'E', 'L', 'F', 2, 1, 1, 0}, bytes.Repeat([]byte{1}, 4096-8)...),
		"old-1":  append(oldHeader(1, 99), make([]byte, 4096-36)...),
		"old-7":  append(oldHeader(7, 99), make([]byte, 4096-36)...),
	}
	for name, content := range cases {
		for _, withLive := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/live=%v", name, withLive), func(t *testing.T) {
				d := crash.New()
				if withLive {
					j := open(t, small(d, newClock(), 0))
					for i := 1; i <= 4; i++ {
						mustAppend(t, j, bigRecord(i))
					}
					mustSync(t, j, 4)
					closeWithin(t, j)
				}
				put(t, d, "000099.seg", 4096, content)
				before, n0 := d.Fingerprint(), d.Len()
				j, err := journal.Open("", small(d, newClock(), 0))
				if !errors.Is(err, journal.ErrFormat) {
					if err == nil {
						closeWithin(t, j)
					}
					t.Fatalf("Open: %v, want ErrFormat", err)
				}
				if err := journal.Discard("", small(d, newClock(), 0), 1, 4); !errors.Is(err, journal.ErrFormat) {
					t.Fatalf("Discard: %v, want ErrFormat", err)
				}
				if d.Len() != n0 || d.Fingerprint() != before {
					t.Fatalf("a refusal touched the disk: %d points became %d", n0, d.Len())
				}
			})
		}
	}
}

// A write is torn at any byte. Whatever prefix of a header over zeros, and
// whatever prefix of zeros over a header, a directory that is merely torn opens:
// the magic of such a file is zeros and its own bytes, and nothing else.
func TestTornMagicIsFreeNotForeign(t *testing.T) {
	d := crash.New()
	j := open(t, small(d, newClock(), 0))
	for i := 1; i <= 3; i++ {
		mustAppend(t, j, bigRecord(i))
	}
	mustSync(t, j, 3)
	closeWithin(t, j)
	live := make([]byte, headerSize)
	f, _ := d.Open("000000.seg")
	f.ReadAt(live, 0)
	for keep := 1; keep < headerSize; keep++ {
		for name, header := range map[string]func() []byte{
			"a prefix of a header over zeros": func() []byte { h := make([]byte, headerSize); copy(h, live[:keep]); return h },
			"a prefix of zeros over a header": func() []byte { h := append([]byte(nil), live...); clear(h[:keep]); return h },
		} {
			img := d.Image(d.Len(), crash.Variant{Kind: crash.All})
			g, _ := img.Open("000001.seg") // the spare: free, not live
			if _, err := g.WriteAt(header(), 0); err != nil {
				t.Fatal(err)
			}
			jj, err := journal.Open("", small(img, newClock(), 0))
			if err != nil {
				t.Fatalf("%s, after %d bytes: %v", name, keep, err)
			}
			closeWithin(t, jj)
		}
	}
}

// The same through the harness: every write at the start of a file, in a
// workload that recycles segments, torn after each of its first 48 bytes.
func TestByteGranularTearsOfTheHeaderOpen(t *testing.T) {
	defer journal.MarkEvery(16)()
	r := newRun()
	j := open(t, r.opts(0))
	for i := 1; i <= 18; i++ {
		seq := r.append(t, j, bigRecord(i))
		if i%3 == 0 {
			r.sync(t, j, seq)
		}
		if i%6 == 0 {
			r.publish(t, j, seq-3)
			r.reclaim(t, j)
		}
	}
	closeBounded(t, j)
	tears := 0
	for n := 0; n <= r.d.Len(); n++ {
		for _, img := range r.d.Tears(n, 48) {
			tears++
			s := r.state(img, n)
			jj, err := journal.Open("", small(img, newClock(), s.pub))
			if err != nil {
				t.Fatalf("a torn header at point %d: %v", n, err)
			}
			closeWithin(t, jj)
		}
	}
	if tears < 100 {
		t.Fatalf("only %d torn headers were tried", tears)
	}
}

// Open refused the directory and "touched nothing": a file the journal did not
// make and that merely ends in .tmp is not deleted.
func TestRefusalLeavesForeignTempFilesAlone(t *testing.T) {
	dir := filepath.Join(scratch(t), "j")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	os.WriteFile(filepath.Join(dir, "000000.seg"), bytes.Repeat([]byte("NOTMINE!"), 512), 0o600)
	os.WriteFile(filepath.Join(dir, "operator-notes.tmp"), []byte("do not delete"), 0o600)
	_, err := journal.Open(dir, journal.Options{Capacity: 4 * 4096, Segment: 4096})
	if !errors.Is(err, journal.ErrFormat) {
		t.Fatalf("Open: %v, want ErrFormat", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "operator-notes.tmp")); err != nil {
		t.Errorf("a refused Open deleted a file it did not make: %v", err)
	}
}

// A header whose checksum holds but whose first sequence is zero is not a
// segment's: it is free, and nothing is done to it.
func TestAHeaderWithoutAFirstSequenceIsFree(t *testing.T) {
	d := crash.New()
	h := make([]byte, headerSize)
	copy(h[4:], []byte("JRNL"))
	binary.LittleEndian.PutUint64(h[16:], 42) // a life; first stays zero
	binary.LittleEndian.PutUint32(h, crc32.Checksum(h[4:], crc32.MakeTable(crc32.Castagnoli)))
	put(t, d, "000000.seg", 4096, h)
	j, err := journal.Open("", small(d, newClock(), 0))
	if err != nil {
		t.Fatal(err)
	}
	defer closeWithin(t, j)
	if r := j.Recovery(); len(r.Dropped) != 0 || r.Last != 0 {
		t.Fatalf("recovery %+v", r)
	}
	g, _ := d.Open("000000.seg")
	got := make([]byte, headerSize)
	g.ReadAt(got, 0)
	if !bytes.Equal(got, h) {
		t.Fatalf("a free segment was wiped as if it had held records")
	}
}
