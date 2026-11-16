package journal_test

import (
	"crypto/sha256"
	"encoding/binary"
	"hash/crc32"
	"testing"

	"github.com/hanzoai/vfs/journal"
	"github.com/hanzoai/vfs/journal/crash"
)

// A header can be rewritten and its checksum recomputed; the records behind it
// cannot be. The records of one lineage are checksummed under a seed that
// includes it, so a segment carried into another lineage's directory with its
// header made to agree still yields nothing.
func TestRecordsOfAnotherLineageDoNotValidateUnderAForgedHeader(t *testing.T) {
	d := crash.New()
	opt := small(d, newClock(), 0)
	opt.Lineage = []byte("volume 7, round 1")
	j := open(t, opt)
	var last uint64
	for i := 1; i <= 6; i++ {
		last = mustAppend(t, j, []byte{byte('A'), byte(i)})
	}
	mustSync(t, j, last)
	closeWithin(t, j)

	other := sha256.Sum256([]byte("volume 7, round 2"))
	names, err := d.List()
	if err != nil {
		t.Fatal(err)
	}
	forged := 0
	for _, name := range names {
		f, err := d.Open(name)
		if err != nil {
			t.Fatal(err)
		}
		h := make([]byte, 40)
		if _, err := f.ReadAt(h, 0); err != nil {
			t.Fatal(err)
		}
		if crc32.Checksum(h[4:], crc32.MakeTable(crc32.Castagnoli)) != binary.LittleEndian.Uint32(h) {
			continue // a free segment
		}
		copy(h[24:], other[:16])
		binary.LittleEndian.PutUint32(h, crc32.Checksum(h[4:], crc32.MakeTable(crc32.Castagnoli)))
		if _, err := f.WriteAt(h, 0); err != nil {
			t.Fatal(err)
		}
		if err := f.Sync(); err != nil {
			t.Fatal(err)
		}
		forged++
	}
	if forged == 0 {
		t.Fatal("no segment to forge")
	}

	again := small(d, newClock(), 0)
	again.Lineage = []byte("volume 7, round 2")
	j, err = journal.Open("", again)
	if err != nil {
		return // refusing is as good as reading nothing
	}
	defer closeWithin(t, j)
	if got := replayed(t, j); len(got) != 0 {
		t.Fatalf("records written under another lineage replayed: %v", seqs(got))
	}
}
